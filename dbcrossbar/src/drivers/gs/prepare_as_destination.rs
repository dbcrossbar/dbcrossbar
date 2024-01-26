//! Preparing bucket directories as output destinations.

use crate::clouds::gcloud::{storage, Client};
use crate::common::*;

/// Prepare the target of this locator for use as a destination.
#[instrument(level = "trace", skip(ctx, client))]
pub(crate) async fn prepare_as_destination_helper(
    ctx: Context,
    client: &Client,
    gs_url: Url,
    if_exists: IfExists,
) -> Result<()> {
    // Delete the existing output, if it exists.
    if if_exists == IfExists::Overwrite {
        storage::rm_r(&ctx, client, &gs_url).await?;
        Ok(())
    } else {
        Err(format_err!(
            "must specify `overwrite` for {} destination",
            gs_url,
        ))
    }
}
