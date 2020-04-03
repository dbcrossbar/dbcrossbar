//! Deleting files from Google Cloud Storage.

use super::{
    super::{percent_encode, Client, NoQuery},
    ls, parse_gs_url,
};
use crate::common::*;
use crate::tokio_glue::ConsumeWithParallelism;

/// How many objects should we try to delete at a time?
const PARALLEL_DELETIONS: usize = 10;

/// Recursively delete a `gs://` directory without deleting the bucket.
pub(crate) async fn rmdir(ctx: &Context, url: &Url) -> Result<()> {
    debug!(ctx.log(), "deleting existing {}", url);

    if !url.path().ends_with('/') {
        return Err(format_err!(
            "can only delete gs:// URL ending in '/', got {}",
            url,
        ));
    }

    // TODO: Used batched commands to delete 100 URLs at a time.
    let url_stream = ls(ctx, url).await?;
    let ctx = ctx.clone();
    let del_fut_stream: BoxStream<BoxFuture<()>> = url_stream
        .map_ok(move |url| {
            let ctx = ctx.clone();
            async move {
                trace!(ctx.log(), "deleting {}", url);
                let url = url.parse::<Url>()?;
                let (bucket, object) = parse_gs_url(&url)?;
                let req_url = format!(
                    "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
                    percent_encode(&bucket),
                    percent_encode(&object),
                );
                let client = Client::new(&ctx).await?;
                client.delete(&ctx, &req_url, NoQuery).await?;
                Ok(())
            }
            .boxed()
        })
        .boxed();
    del_fut_stream
        .consume_with_parallelism(PARALLEL_DELETIONS)
        .await?;
    Ok(())
}
