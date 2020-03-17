//! Interfaces to Google Cloud Storage.

use std::process::Stdio;
use tokio::{io::BufReader, process::Command};

use crate::common::*;
use crate::tokio_glue::{copy_reader_to_stream, copy_stream_to_writer};

/// List all the files at the specified `gs://` URL, recursively.
pub(crate) async fn ls(
    ctx: &Context,
    url: &Url,
) -> Result<impl Stream<Item = Result<String>> + Send + Unpin + 'static> {
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

    Ok(file_urls)
}

/// Recursively delete a `gs://` directory without deleting the bucket.
pub(crate) async fn rmdir(ctx: &Context, url: &Url) -> Result<()> {
    // Delete all the files under `self.url`, but be careful not to
    // delete the entire bucket. See `gsutil rm --help` for details.
    debug!(ctx.log(), "deleting existing {}", url);
    if !url.path().ends_with('/') {
        return Err(format_err!(
            "can only write to gs:// URL ending in '/', got {}",
            url,
        ));
    }
    let delete_url = url.join("**")?;
    let status = Command::new("gsutil")
        .args(&["rm", "-f", delete_url.as_str()])
        // Throw away stdout so it doesn't corrupt our output.
        .stdout(Stdio::null())
        .status()
        .await
        .context("error running gsutil")?;
    if !status.success() {
        warn!(
            ctx.log(),
            "can't delete contents of {}, possibly because it doesn't exist", url,
        );
    }
    Ok(())
}

/// Download the file at the specified URL as a stream.
pub(crate) async fn download_file(
    ctx: &Context,
    file_url: &Url,
) -> Result<BoxStream<BytesMut>> {
    // Stream the file from the cloud.
    debug!(ctx.log(), "streaming from {} using `gsutil cp`", file_url);
    let mut child = Command::new("gsutil")
        .args(&["cp", file_url.as_str(), "-"])
        .stdout(Stdio::piped())
        .spawn()
        .context("error running gsutil")?;
    let child_stdout = child.stdout.take().expect("child should have stdout");
    let child_stdout = BufReader::with_capacity(BUFFER_SIZE, child_stdout);
    let data = copy_reader_to_stream(ctx.clone(), child_stdout)?;
    ctx.spawn_process(format!("gsutil cp {} -", file_url), child);
    Ok(data.boxed())
}

/// Upload `data` as a file at `url`.
pub(crate) async fn upload_file(
    ctx: Context,
    data: BoxStream<BytesMut>,
    url: &Url,
) -> Result<()> {
    // Run `gsutil cp - $URL` as a background process.
    debug!(ctx.log(), "uploading stream to gsutil");
    let mut child = Command::new("gsutil")
        .args(&["cp", "-", url.as_str()])
        .stdin(Stdio::piped())
        // Throw away stdout so it doesn't corrupt our output.
        .stdout(Stdio::null())
        .spawn()
        .context("error running gsutil")?;
    let child_stdin = child.stdin.take().expect("child should have stdin");

    // Copy data to our child process.
    copy_stream_to_writer(ctx.clone(), data, child_stdin)
        .await
        .context("error copying data to gsutil")?;

    // Wait for `gsutil` to finish.
    let status = child
        .await
        .with_context(|_| format!("error finishing upload to {}", url))?;
    if status.success() {
        Ok(())
    } else {
        Err(format_err!("gsutil returned error: {}", status))
    }
}
