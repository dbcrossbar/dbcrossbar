//! Writing data to AWS S3.

use super::{prepare_as_destination_helper, S3Locator};
use crate::clouds::aws::s3;
use crate::common::*;

/// Implementation of `write_local_data`, but as a real `async` function.
#[instrument(
    level = "debug",
    name = "s3::write_local_data",
    skip_all,
    fields(url = %url)
)]
pub(crate) async fn write_local_data_helper(
    url: Url,
    data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    let _shared_args = shared_args.verify(S3Locator::features())?;
    let dest_args = dest_args.verify(S3Locator::features())?;

    // Look up our arguments.
    let if_exists = dest_args.if_exists().to_owned();

    // Delete the existing output, if it exists.
    prepare_as_destination_helper(url.clone(), if_exists).await?;

    // Spawn our uploader threads.
    let written = data.map_ok(move |stream| {
        let url = url.clone();
        async move {
            let url = url.join(&format!("{}.csv", stream.name))?;
            s3::upload_file(stream.data, &url)
                .instrument(
                    debug_span!("write_stream", stream.name = %stream.name, url = %url),
                )
                .await?;
            Ok(S3Locator { url }.boxed())
        }
        .boxed()
    });

    Ok(written.boxed())
}
