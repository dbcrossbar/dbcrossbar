//! Preparing bucket directories as output destinations.

use std::process::Stdio;
use tokio::process::Command;

use crate::common::*;

/// Prepare the target of this locator for use as a destination.
pub(crate) async fn prepare_as_destination_helper(
    ctx: Context,
    gs_url: Url,
    if_exists: IfExists,
) -> Result<()> {
    // Delete the existing output, if it exists.
    if if_exists == IfExists::Overwrite {
        // Delete all the files under `self.url`, but be careful not to
        // delete the entire bucket. See `gsutil rm --help` for details.
        debug!(ctx.log(), "deleting existing {}", gs_url);
        if !gs_url.path().ends_with('/') {
            return Err(format_err!(
                "can only write to gs:// URL ending in '/', got {}",
                gs_url,
            ));
        }
        let delete_url = gs_url.join("**")?;
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
                "can't delete contents of {}, possibly because it doesn't exist",
                gs_url,
            );
        }
        Ok(())
    } else {
        Err(format_err!(
            "must specify `overwrite` for {} destination",
            gs_url,
        ))
    }
}
