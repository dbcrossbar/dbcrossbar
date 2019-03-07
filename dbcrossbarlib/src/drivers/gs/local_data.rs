//! Reading data from Google Cloud Storage.

use std::{
    io::BufReader,
    process::{Command, Stdio},
};
use tokio::io;
use tokio_process::CommandExt;

use crate::common::*;
use crate::tokio_glue::copy_reader_to_stream;

/// Implementation of `local_data`, but as a real `async` function.
pub(crate) async fn local_data_helper(
    ctx: Context,
    url: Url,
) -> Result<Option<BoxStream<CsvStream>>> {
    debug!(ctx.log(), "getting CSV files from {}", url);

    // Build a URL to list.
    let ls_url = if url.path().ends_with('/') {
        url.join("**/*.csv")?
    } else {
        url.clone()
    };

    // Start a child process to list files at that URL.
    trace!(ctx.log(), "listing {}", ls_url);
    let mut child = Command::new("gsutil")
        .args(&["ls", url.as_str()])
        .stdout(Stdio::piped())
        .spawn_async()
        .context("error running gsutil")?;
    let child_stdout = child.stdout().take().expect("child should have stdout");
    ctx.spawn_process(format!("gsutil ls {}", url), child);

    // Parse `ls` output into lines, and convert into `CsvStream` values lazily
    // in case there are a lot of CSV files we need to read.
    let file_urls = io::lines(BufReader::with_capacity(BUFFER_SIZE, child_stdout))
        .map_err(|e| format_err!("error reading gsutil output: {}", e));
    let csv_streams = file_urls.and_then(move |file_url| -> BoxFuture<CsvStream> {
        let ctx = ctx.clone();
        let url = url.clone();
        tokio_fut(
            async move {
                trace!(ctx.log(), "streaming data from {}", file_url);

                // Extract either the basename of the URL (if it's a file URL),
                // or the relative part of the URL (if we were given a directory
                // URL and found a file URL inside it).
                let basename_or_relative = if file_url == url.as_str() {
                    // We have just a regular file URL, so take everything after
                    // the last '/'.
                    file_url
                        .rsplitn(2, '/')
                        .last()
                        .expect("should have '/' in URL")
                } else if file_url.starts_with(url.as_str()) {
                    // We have a directory URL, so attempt to preserve directory structure
                    // including '/' characters below that point.
                    &file_url[url.as_str().len()..]
                } else {
                    return Err(format_err!(
                        "expected {} to start with {}",
                        file_url,
                        url
                    ));
                };

                // Now strip any extension.
                let name = basename_or_relative
                    .splitn(2, '.')
                    .next()
                    .ok_or_else(|| format_err!("can't get basename of {}", file_url))?
                    .to_owned();
                let ctx =
                    ctx.child(o!("stream" => name.clone(), "url" => file_url.clone()));
                debug!(ctx.log(), "streaming from `gsutil cp`");

                // Stream the file from the cloud.
                let mut child = Command::new("gsutil")
                    .args(&["cp", file_url.as_str(), "-"])
                    .stdout(Stdio::piped())
                    .spawn_async()
                    .context("error running gsutil")?;
                let child_stdout =
                    child.stdout().take().expect("child should have stdout");
                let data = copy_reader_to_stream(ctx.clone(), child_stdout)?;
                ctx.spawn_process(format!("gsutil cp {} -", file_url), child);

                // Assemble everything into a CSV stream.
                Ok(CsvStream {
                    name,
                    data: Box::new(data),
                })
            },
        )
        .into_boxed()
    });

    Ok(Some(Box::new(csv_streams) as BoxStream<CsvStream>))
}
