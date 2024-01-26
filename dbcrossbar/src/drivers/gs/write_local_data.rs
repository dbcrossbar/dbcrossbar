//! Writing data to Google Cloud Storage.

use super::{prepare_as_destination_helper, GsLocator};
use crate::clouds::gcloud::storage;
use crate::common::*;
use crate::concat::concatenate_csv_streams;
use crate::drivers::bigquery_shared::GCloudDriverArguments;

/// Implementation of `write_local_data`, but as a real `async` function.
#[instrument(
    level = "debug",
    name = "gs::write_local_data",
    skip_all,
    fields(dest = %dest)
)]
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    dest: GsLocator,
    data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    let _shared_args = shared_args.verify(GsLocator::features())?;
    let dest_args = dest_args.verify(GsLocator::features())?;

    let driver_args = GCloudDriverArguments::try_from(&dest_args)?;
    let client = driver_args.client().await?;

    // Delete the existing output, if it exists.
    let if_exists = dest_args.if_exists().to_owned();
    prepare_as_destination_helper(ctx.clone(), &client, dest.url.clone(), if_exists)
        .await?;

    // Spawn our uploader processes.
    if dest.is_directory() {
        let written = data.map_ok(move |stream| {
            let dest = dest.clone();
            let ctx = ctx.clone();
            let client = client.clone();
            async move {
                let url = dest.url.join(&format!("{}.csv", stream.name))?;
                storage::upload_file(&ctx, &client, stream.data, &url)
                    .instrument(trace_span!("stream_to_gs", stream.name = %stream.name, url = %url))
                    .await?;
                Ok(GsLocator { url }.boxed())
            }
            .boxed()
        });

        Ok(written.boxed())
    } else if dest.is_csv_file() {
        // We are writing to a single output file, so concatenate our CSV
        // streams.
        let stream = concatenate_csv_streams(ctx.clone(), data)?;
        let fut = async move {
            let url = &dest.url;
            storage::upload_file(&ctx, &client, stream.data, url).instrument(trace_span!("stream_to_gs", stream.name = %stream.name, url = %url)).await?;
            Ok(GsLocator {
                url: url.to_owned(),
            }
            .boxed())
        };
        Ok(box_stream_once(Ok(fut.boxed())))
    } else {
        Err(format_err!("do not know how to write to {}", dest))
    }
}
