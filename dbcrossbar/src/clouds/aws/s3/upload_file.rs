//! Upload files to S3.

use std::process::Stdio;

use super::aws_s3_command;
use crate::common::*;
use crate::tokio_glue::copy_stream_to_writer;

/// Upload `data` as a file at `url`.
#[instrument(level = "trace", skip(data))]
pub(crate) async fn upload_file(
    data: BoxStream<BytesMut>,
    file_url: &Url,
) -> Result<()> {
    // Run `aws cp - $URL` as a background process.
    debug!("uploading stream to `aws s3`");
    let mut child = aws_s3_command()
        .await?
        .args(["cp", "-", file_url.as_str()])
        .stdin(Stdio::piped())
        // Throw away stdout so it doesn't corrupt our output.
        .stdout(Stdio::null())
        .spawn()
        .context("error running `aws s3`")?;
    let child_stdin = child.stdin.take().expect("child should have stdin");

    // Copy data to our child process.
    copy_stream_to_writer(data, child_stdin)
        .await
        .context("error copying data to `aws s3`")?;

    // Wait for `aws s3` to finish.
    let status = child
        .wait()
        .await
        .with_context(|| format!("error finishing upload to {}", file_url))?;
    if status.success() {
        Ok(())
    } else {
        Err(format_err!("`aws s3` returned error: {}", status))
    }
}
