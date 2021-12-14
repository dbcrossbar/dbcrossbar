//! Download a file from Google Cloud storage.

use serde::Serialize;

use super::{
    super::{crc32c_stream::Crc32cStream, percent_encode, Client, NoQuery},
    parse_gs_url, StorageObject,
};
use crate::common::*;
use crate::tokio_glue::idiomatic_bytes_stream;

/// Parameters for an upload query.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadQuery {
    /// The type of the upload we're performing.
    upload_type: &'static str,

    /// Only accept the upload if the existing object has the specified
    /// generation number. Use 0 to specify a non-existant object.
    if_generation_match: i64,

    /// The name of the object we're creating.
    name: String,
}

/// Upload `data` as a file at `url`.
///
/// Docs: https://cloud.google.com/storage/docs/json_api/v1/objects/insert
///
/// TODO: Support https://cloud.google.com/storage/docs/performing-resumable-uploads.
pub(crate) async fn upload_file<'a>(
    ctx: &'a Context,
    data: BoxStream<BytesMut>,
    file_url: &'a Url,
) -> Result<StorageObject> {
    debug!(ctx.log(), "streaming to {}", file_url);
    let (bucket, object) = parse_gs_url(file_url)?;

    // Compute a running CRC32 sum.
    let (stream, crc32c_reciever) = Crc32cStream::new(data);

    // Post our data.
    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o",
        percent_encode(&bucket),
    );
    let query = UploadQuery {
        upload_type: "media",
        if_generation_match: 0,
        name: object.clone(),
    };
    let client = Client::new(ctx).await?;
    client
        .post_stream(
            ctx.clone(),
            &url,
            query,
            idiomatic_bytes_stream(ctx, stream.boxed()),
        )
        .await?;

    // Wait for our computed hash code.
    let hasher = crc32c_reciever
        .await
        .map_err(|_| format_err!("error waiting for checksum"))?;
    let crc32c = hasher.finish_encoded();

    // Verify that our uploaded file has the right checksum.
    let obj_url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
        percent_encode(&bucket),
        percent_encode(&object),
    );
    let obj: StorageObject = client.get(ctx, &obj_url, NoQuery).await?;
    if obj.crc32c == crc32c {
        Ok(obj)
    } else {
        Err(format_err!(
            "{} does not have the expected checksum, did it change?",
            file_url,
        ))
    }
}
