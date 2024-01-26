//! Reading data from Google Cloud Storage.

use super::GsLocator;
use crate::clouds::gcloud::storage;
use crate::common::*;
use crate::csv_stream::csv_stream_name;
use crate::drivers::bigquery_shared::GCloudDriverArguments;

/// Implementation of `local_data`, but as a real `async` function.
#[instrument(
    level = "trace",
    name = "gs::local_data",
    skip_all,
    fields(url = %url)
)]
pub(crate) async fn local_data_helper(
    ctx: Context,
    url: Url,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let _shared_args = shared_args.verify(GsLocator::features())?;
    let source_args = source_args.verify(GsLocator::features())?;
    debug!("getting CSV files from {}", url);

    let driver_args = GCloudDriverArguments::try_from(&source_args)?;
    let client = driver_args.client().await?;

    let file_urls = storage::ls(&ctx, &client, &url).await?;

    let csv_streams = file_urls.and_then(move |item| {
        let url = url.clone();
        let client = client.clone();
        async move {
            // Stream the file from the cloud.
            let file_url = item.to_url_string();
            let name = csv_stream_name(url.as_str(), &file_url)?;
            let data = storage::download_file(&client, &item)
                .instrument(trace_span!("stream_from_gs", stream = %name))
                .await?;

            // Assemble everything into a CSV stream.
            Ok(CsvStream {
                name: name.to_owned(),
                data,
            })
        }
        .boxed()
    });

    Ok(Some(csv_streams.boxed()))
}
