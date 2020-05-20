//! Writing data to Google Cloud Storage.

use super::{prepare_as_destination_helper, GsLocator};
use crate::clouds::gcloud::storage;
use crate::common::*;

/// Implementation of `write_local_data`, but as a real `async` function.
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    url: Url,
    data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    let _shared_args = shared_args.verify(GsLocator::features())?;
    let dest_args = dest_args.verify(GsLocator::features())?;

    // Delete the existing output, if it exists.
    let if_exists = dest_args.if_exists().to_owned();
    prepare_as_destination_helper(ctx.clone(), url.clone(), if_exists).await?;

    // Spawn our uploader processes.
    let written = data.map_ok(move |stream| {
        let url = url.clone();
        let ctx = ctx.clone();
        async move {
            let url = url.join(&format!("{}.csv", stream.name))?;
            let ctx = ctx
                .child(o!("stream" => stream.name.clone(), "url" => url.to_string()));

            storage::upload_file(&ctx, stream.data, &url).await?;
            Ok(GsLocator { url }.boxed())
        }
        .boxed()
    });

    Ok(written.boxed())
}
