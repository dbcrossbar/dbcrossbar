//! Reading data from Google Cloud Storage.

use std::process::Stdio;
use tokio::{io::BufReader, process::Command};

use super::GsLocator;
use crate::common::*;
use crate::csv_stream::csv_stream_name;
use crate::tokio_glue::copy_reader_to_stream;

/// Implementation of `local_data`, but as a real `async` function.
pub(crate) async fn local_data_helper(
    ctx: Context,
    url: Url,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let _shared_args = shared_args.verify(GsLocator::features())?;
    let _source_args = source_args.verify(GsLocator::features())?;
    debug!(ctx.log(), "getting CSV files from {}", url);

    // Build a URL to list.
    let ls_url = if url.path().ends_with('/') {
        url.join("**/*.csv")?
    } else {
        url.clone()
    };

    // Start a child process to list files at that URL.
    //
    // XXX - Shouldn't we be using `ls_url` below?
    debug!(ctx.log(), "listing {}", ls_url);
    let mut child = Command::new("gsutil")
        .args(&["ls", url.as_str()])
        .stdout(Stdio::piped())
        .spawn()
        .context("error running gsutil")?;
    let child_stdout = child.stdout.take().expect("child should have stdout");
    ctx.spawn_process(format!("gsutil ls {}", url), child);

    // Parse `ls` output into lines, and convert into `CsvStream` values lazily
    // in case there are a lot of CSV files we need to read.
    let file_urls = BufReader::with_capacity(BUFFER_SIZE, child_stdout)
        .lines()
        .map_err(|e| format_err!("error reading gsutil output: {}", e));
    let csv_streams = file_urls.and_then(move |file_url| {
        let ctx = ctx.clone();
        let url = url.clone();
        async move {
            // Stream the file from the cloud.
            let name = csv_stream_name(url.as_str(), &file_url)?;
            let ctx =
                ctx.child(o!("stream" => name.to_owned(), "url" => file_url.clone()));
            debug!(ctx.log(), "streaming from {} using `gsutil cp`", file_url);
            let mut child = Command::new("gsutil")
                .args(&[
                    "-o",
                    "GSUtil:parallel_process_count=1",
                    "-o",
                    "GSUtil:parallel_thread_count=1",
                    "cp",
                    file_url.as_str(),
                    "-",
                ])
                .stdout(Stdio::piped())
                .spawn()
                .context("error running gsutil")?;
            let child_stdout = child.stdout.take().expect("child should have stdout");
            let child_stdout = BufReader::with_capacity(BUFFER_SIZE, child_stdout);
            let data = copy_reader_to_stream(ctx.clone(), child_stdout)?;
            ctx.spawn_process(format!("gsutil cp {} -", file_url), child);

            // Assemble everything into a CSV stream.
            Ok(CsvStream {
                name: name.to_owned(),
                data: data.boxed(),
            })
        }
        .boxed()
    });

    Ok(Some(csv_streams.boxed()))
}
