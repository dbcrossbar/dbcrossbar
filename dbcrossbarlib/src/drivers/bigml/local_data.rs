//! Helper for reading data from BigML.

use super::{BigMlAction, BigMlCredentials, BigMlLocator};
use crate::common::*;

/// Implementation of `local_data`, but as a real `async` function.
pub(crate) async fn local_data_helper(
    ctx: Context,
    source: BigMlLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    let _shared_args = shared_args.verify(BigMlLocator::features())?;
    let _source_args = source_args.verify(BigMlLocator::features())?;

    let id = match source.action {
        BigMlAction::ReadDataset(id) => id,
        _ => return Err(format_err!("cannot read data from {}", source)),
    };
    debug!(ctx.log(), "reading data from {}", id);

    let creds = BigMlCredentials::try_default()?;
    let client = creds.client()?;
    let response = client.download(&id).await?;
    let csv_stream = CsvStream::from_http_response(id.to_string(), response)?;

    Ok(Some(box_stream_once(Ok(csv_stream))))
}
