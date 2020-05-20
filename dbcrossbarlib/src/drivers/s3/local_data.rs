//! Reading data from AWS S3.

use super::S3Locator;
use crate::clouds::aws::s3;
use crate::common::*;
use crate::csv_stream::csv_stream_name;

/// Implementation of `local_data`, but as a real `async` function.
pub(crate) async fn local_data_helper(
    ctx: Context,
    url: Url,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let _shared_args = shared_args.verify(S3Locator::features())?;
    let _source_args = source_args.verify(S3Locator::features())?;

    debug!(ctx.log(), "getting CSV files from {}", url);

    // List the files at our URL.
    let file_urls = s3::ls(&ctx, &url).await?;

    // Convert into `CsvStream` values lazily in case there are a lot of CSV
    // files we need to read.
    //
    // XXX - This will fail (either silently or noisily, I'm not sure) if there
    // are 1000+ files in the S3 directory, and we can't fix this without
    // switching from `aws s3` to native S3 API calls from Rust.
    let csv_streams = file_urls.and_then(move |file_url| {
        let ctx = ctx.clone();
        let url = url.clone();
        async move {
            // Stream the file from the cloud.
            let name = csv_stream_name(url.as_str(), file_url.as_str())?.to_owned();
            let ctx = ctx.child(
                o!("stream" => name.clone(), "url" => file_url.as_str().to_owned()),
            );
            let data = s3::download_file(&ctx, &file_url).await?;

            // Assemble everything into a CSV stream.
            Ok(CsvStream { name, data })
        }
        .boxed()
    });

    Ok(Some(csv_streams.boxed()))
}
