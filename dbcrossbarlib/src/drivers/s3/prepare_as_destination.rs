//! Preparing bucket directories as output destinations.

use crate::clouds::aws::s3;
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
        s3::rmdir(&ctx, &s3_url).await
    } else {
        Err(format_err!(
            "must specify `overwrite` for {} destination",
            s3_url,
        ))
    }
}
