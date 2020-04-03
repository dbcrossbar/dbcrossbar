//! Download a file from Google Cloud storage.

use super::{
    super::{percent_encode, AltQuery, Client},
    parse_gs_url,
};
use crate::common::*;
use crate::tokio_glue::http_response_stream;

/// Download the file at the specified URL as a stream.
pub(crate) async fn download_file(
    ctx: &Context,
    file_url: &Url,
) -> Result<BoxStream<BytesMut>> {
    debug!(ctx.log(), "streaming from {}", file_url);
    let (bucket, object) = parse_gs_url(file_url)?;

    // Make our request.
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
        percent_encode(&bucket),
        percent_encode(&object),
    );
    let client = Client::new(ctx).await?;
    let resp = client.get_response(ctx, &url, AltQuery::media()).await?;
    Ok(http_response_stream(resp))
}
