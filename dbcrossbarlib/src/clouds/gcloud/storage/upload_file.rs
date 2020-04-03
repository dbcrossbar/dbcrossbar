//! Download a file from Google Cloud storage.

use serde::Serialize;

use super::{
    super::{percent_encode, Client},
    parse_gs_url,
};
use crate::common::*;
use crate::tokio_glue::idiomatic_bytes_stream;

/// Parameters for an upload query.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadQuery {
    /// The type of the upload we're performing.
    upload_type: &'static str,

    /// The name of the object we're creating.
    name: String,
}

/// Upload `data` as a file at `url`.
///
/// Docs: https://cloud.google.com/storage/docs/json_api/v1/objects/insert
///
/// TODO: Support https://cloud.google.com/storage/docs/performing-resumable-uploads.
pub(crate) async fn upload_file(
    // Pass `ctx` by value, not reference, because of a weird async lifetime error.
    ctx: Context,
    data: BoxStream<BytesMut>,
    file_url: &Url,
) -> Result<()> {
    debug!(ctx.log(), "streaming to {}", file_url);
    let (bucket, object) = parse_gs_url(file_url)?;

    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o",
        percent_encode(&bucket),
    );
    let query = UploadQuery {
        upload_type: "media",
        name: object,
    };
    let client = Client::new(&ctx).await?;
    client
        .post_stream(ctx.clone(), &url, query, idiomatic_bytes_stream(&ctx, data))
        .await
}
