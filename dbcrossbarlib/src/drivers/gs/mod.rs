//! Support for Google Cloud Storage.

use std::{
    fmt,
    io::BufReader,
    process::{Command, Stdio},
    str::FromStr,
};
use tokio::io;
use tokio_process::CommandExt;

use crate::common::*;
use crate::tokio_glue::{copy_reader_to_stream, copy_stream_to_writer};

/// Locator scheme for Google Cloud Storage.
pub(crate) const GS_SCHEME: &str = "gs:";

#[derive(Debug)]
pub(crate) struct GsLocator {
    url: Url,
}

impl GsLocator {
    /// Access the `gs://` URL in this locator.
    pub(crate) fn as_url(&self) -> &Url {
        &self.url
    }
}

impl fmt::Display for GsLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(f)
    }
}

impl FromStr for GsLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.starts_with(GS_SCHEME) {
            let url = s
                .parse::<Url>()
                .with_context(|_| format!("cannot parse {}", s))?;
            if !url.path().starts_with('/') {
                Err(format_err!("{} must start with gs://", url))
            } else if !url.path().ends_with('/') {
                Err(format_err!("{} must end with a '/'", url))
            } else {
                Ok(GsLocator { url })
            }
        } else {
            Err(format_err!("expected {} to begin with gs://", s))
        }
    }
}

impl Locator for GsLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn local_data(&self, ctx: Context) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(ctx, self.url.clone()).into_boxed()
    }

    fn write_local_data(
        &self,
        ctx: Context,
        schema: Table,
        data: BoxStream<CsvStream>,
        if_exists: IfExists,
    ) -> BoxFuture<BoxStream<BoxFuture<()>>> {
        write_local_data_helper(ctx, self.url.clone(), schema, data, if_exists)
            .into_boxed()
    }
}

async fn local_data_helper(
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
    let file_urls = io::lines(BufReader::new(child_stdout))
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

async fn write_local_data_helper(
    ctx: Context,
    url: Url,
    _schema: Table,
    data: BoxStream<CsvStream>,
    if_exists: IfExists,
) -> Result<BoxStream<BoxFuture<()>>> {
    // Delete the existing output, if it exists.
    if if_exists == IfExists::Overwrite {
        // Delete all the files under `self.url`, but be careful not to
        // delete the entire bucket. See `gsutil rm --help` for details.
        debug!(ctx.log(), "deleting existing {}", url);
        assert!(url.path().ends_with('/'));
        let delete_url = url.join("**")?;
        let status = Command::new("gsutil")
            .args(&["rm", "-f", delete_url.as_str()])
            .status_async()
            .context("error running gsutil")?;
        if !await!(status)?.success() {
            warn!(
                ctx.log(),
                "can't delete contents of {}, possibly because it doesn't exist", url
            );
        }
    } else {
        return Err(format_err!(
            "must specify `overwrite` for gs:// destination"
        ));
    }

    // Spawn our uploader threads.
    let written = data.map(move |stream| {
        let url = url.clone();
        let ctx = ctx.clone();
        tokio_fut(
            async move {
                let url = url.join(&format!("{}.csv", stream.name))?;
                let ctx = ctx.child(
                    o!("stream" => stream.name.clone(), "url" => url.to_string()),
                );

                // Run `gsutil cp - $URL` as a background process.
                debug!(ctx.log(), "uploading stream to gsutil");
                let mut child = Command::new("gsutil")
                    .args(&["cp", "-", url.as_str()])
                    .stdin(Stdio::piped())
                    .spawn_async()
                    .context("error running gsutil")?;
                let child_stdin =
                    child.stdin().take().expect("child should have stdin");

                // Copy data to our child process.
                await!(copy_stream_to_writer(ctx.clone(), stream.data, child_stdin))
                    .context("error copying data to gsutil")?;

                // Wait for `gsutil` to finish.
                let status = await!(child)
                    .with_context(|_| format!("error finishing upload to {}", url))?;
                if status.success() {
                    Ok(())
                } else {
                    Err(format_err!("gsutil returned error: {}", status))
                }
            },
        )
        .into_boxed()
    });

    Ok(Box::new(written) as BoxStream<BoxFuture<()>>)
}
