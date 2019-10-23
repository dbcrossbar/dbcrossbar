//! Preparing bucket directories as output destinations.

use std::process::{Command, Stdio};
use tokio_process::CommandExt;

use crate::common::*;

/// Prepare the target of this locator for use as a destination.
pub(crate) async fn prepare_as_destination_helper(
    ctx: Context,
    s3_url: Url,
    if_exists: IfExists,
) -> Result<()> {
    // Delete the existing output, if it exists.
    if if_exists == IfExists::Overwrite {
        // Delete all the files under `self.url`.
        debug!(ctx.log(), "deleting existing {}", s3_url);
        if !s3_url.path().ends_with('/') {
            return Err(format_err!(
                "can only write to s3:// URL ending in '/', got {}",
                s3_url,
            ));
        }
        let status = Command::new("aws")
            .args(&["s3", "rm", "--recursive", s3_url.as_str()])
            // Throw away stdout so it doesn't corrupt our output.
            .stdout(Stdio::null())
            .status_async()
            .context("error running `aws s3`")?;
        if !status.compat().await?.success() {
            warn!(
                ctx.log(),
                "can't delete contents of {}, possibly because it doesn't exist",
                s3_url,
            );
        }
        Ok(())
    } else {
        Err(format_err!(
            "must specify `overwrite` for {} destination",
            s3_url,
        ))
    }
}
