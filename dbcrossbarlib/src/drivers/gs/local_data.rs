//! Reading data from Google Cloud Storage.

use super::GsLocator;
use crate::clouds::gcloud::storage;
use crate::common::*;
use crate::csv_stream::csv_stream_name;

/// Implementation of `local_data`, but as a real `async` function.
pub(crate) async fn local_data_helper(
    ctx: Context,
    url: Url,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let _shared_args = shared_args.verify(GsLocator::features())?;
    let _source_args = source_args.verify(GsLocator::features())?;
    debug!(ctx.log(), "getting CSV files from {}", url);

    let file_urls = storage::ls(&ctx, &url).await?;

    let csv_streams = file_urls.and_then(move |item| {
        let ctx = ctx.clone();
        let url = url.clone();
        async move {
            // Stream the file from the cloud.
            let file_url = item.to_url_string();
            let name = csv_stream_name(url.as_str(), &file_url)?;
            let ctx =
                ctx.child(o!("stream" => name.to_owned(), "url" => file_url.clone()));
            let data = storage::download_file(&ctx, &item).await?;

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
