//! Deleting files from Google Cloud Storage.

use bigml::wait::{wait, WaitStatus};
use hyper::StatusCode;

use super::{
    super::{client::original_http_error, percent_encode, Client, NoQuery},
    gcs_write_access_denied_wait_options, ls, parse_gs_url,
};
use crate::tokio_glue::ConsumeWithParallelism;
use crate::{clouds::gcloud::ClientError, common::*};

/// How many objects should we try to delete at a time?
const PARALLEL_DELETIONS: usize = 10;

/// Recursively delete a `gs://` path without deleting the bucket.
#[instrument(level = "trace", skip(ctx))]
pub(crate) async fn rm_r(ctx: &Context, url: &Url) -> Result<()> {
    debug!("deleting existing {}", url);

    // TODO: Used batched commands to delete 100 URLs at a time.
    let url_stream = ls(ctx, url).await?;
    let del_fut_stream: BoxStream<BoxFuture<()>> = url_stream
        .map_ok(move |item| {
            async move {
                let url = item.to_url_string();
                trace!("deleting {}", url);
                let url = url.parse::<Url>()?;
                let (bucket, object) = parse_gs_url(&url)?;
                let req_url = format!(
                    "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
                    percent_encode(&bucket),
                    percent_encode(&object),
                );
                let client = Client::new().await?;

                let opt = gcs_write_access_denied_wait_options();
                wait(&opt, || async {
                    match client.delete(&req_url, NoQuery).await {
                        Ok(()) => WaitStatus::Finished(()),
                        Err(err) if should_retry_delete(&err) => {
                            WaitStatus::FailedTemporarily(err)
                        }
                        Err(err) => WaitStatus::FailedPermanently(err),
                    }
                })
                .await?;

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

/// Should we retry an attempted deletion?
fn should_retry_delete(err: &ClientError) -> bool {
    match err {
        ClientError::NotFound { .. } => false,
        ClientError::Other(err) => {
            if let Some(err) = original_http_error(err) {
                // There appears to be some sort of Google Cloud Storage 403 race
                // condition on delete that shows up when preparing buckets. We have no
                // idea what causes this.
                err.status() == Some(StatusCode::FORBIDDEN)
            } else {
                false
            }
        }
    }
}
