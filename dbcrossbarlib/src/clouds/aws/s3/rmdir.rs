//! Deleting data from S3.

use std::process::Stdio;

use super::aws_s3_command;
use crate::common::*;

/// Recursively delete a `s3://` directory without deleting the bucket.
pub(crate) async fn rmdir(ctx: &Context, url: &Url) -> Result<()> {
    // Delete all the files under `url`.
    debug!(ctx.log(), "deleting existing {}", url);
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
            ctx.log(),
            "can't delete contents of {}, possibly because it doesn't exist", url,
        );
    }
    Ok(())
}
