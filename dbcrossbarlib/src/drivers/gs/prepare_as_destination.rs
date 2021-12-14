//! Preparing bucket directories as output destinations.

use crate::clouds::gcloud::storage;
use crate::common::*;

/// Prepare the target of this locator for use as a destination.
pub(crate) async fn prepare_as_destination_helper(
    ctx: Context,
    gs_url: Url,
    if_exists: IfExists,
) -> Result<()> {
    // Delete the existing output, if it exists.
    if if_exists == IfExists::Overwrite {
        storage::rm_r(&ctx, &gs_url).await?;
        Ok(())
    } else {
        Err(format_err!(
            "must specify `overwrite` for {} destination",
            gs_url,
        ))
    }
}
