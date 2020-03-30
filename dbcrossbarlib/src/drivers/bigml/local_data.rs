//! Helper for reading data from BigML.

use bigml::{
    self,
    resource::{Id, Resource},
};

use super::{BigMlAction, BigMlLocator};
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

    let client = bigml::Client::new_from_env()?;
    let response = client.download(&id).await?;
    let csv_stream =
        CsvStream::from_http_response(strip_id_prefix(&id).to_owned(), response)?;

    Ok(Some(box_stream_once(Ok(csv_stream))))
}

/// Remove the "dataset/" prefix from `id`.
fn strip_id_prefix<R: Resource>(id: &Id<R>) -> &str {
    // For any given `Resource` type `R`, we know the actual ID prefix, so we
    // can strip it like this.
    &id.as_str()[R::id_prefix().len()..]
}

#[test]
fn strips_id_prefix() {
    use bigml::resource::Dataset;
    let id = "dataset/abc123".parse::<Id<Dataset>>().unwrap();
    assert_eq!(strip_id_prefix(&id), "abc123");
}
