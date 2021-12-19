//! Deleting data from S3.

use std::process::Stdio;

use super::aws_s3_command;
use crate::common::*;

/// Recursively delete a `s3://` directory without deleting the bucket.
#[instrument(level = "trace")]
pub(crate) async fn rmdir(url: &Url) -> Result<()> {
    // Delete all the files under `url`.
    debug!("deleting existing {}", url);
    if !url.path().ends_with('/') {
        return Err(format_err!(
            "can only write to s3:// URL ending in '/', got {}",
            url,
        ));
    }
    let status = aws_s3_command()
        .await?
        .args(&["rm", "--recursive", url.as_str()])
        // Throw away stdout so it doesn't corrupt our output.
        .stdout(Stdio::null())
        .status()
        .await
        .context("error running `aws s3`")?;
    if !status.success() {
        warn!(
            "can't delete contents of {}, possibly because it doesn't exist",
            url,
        );
    }
    Ok(())
}
