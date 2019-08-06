//! Helper for reading data from BigQuery.

use super::{find_s3_temp_dir, RedshiftLocator};
use crate::common::*;

/// Implementation of `local_data`, but as a real `async` function.
pub(crate) async fn local_data_helper(
    ctx: Context,
    source: RedshiftLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    // Build a temporary location.
    let shared_args_v = shared_args.clone().verify(RedshiftLocator::features())?;
    let s3_temp = find_s3_temp_dir(shared_args_v.temporary_storage())?;
    let s3_dest_args = DestinationArguments::for_temporary();
    let s3_source_args = SourceArguments::for_temporary();

    // Extract from Redshift to s3://.
    let to_temp_ctx = ctx.child(o!("to_temp" => s3_temp.to_string()));
    s3_temp
        .write_remote_data(
            to_temp_ctx,
            Box::new(source),
            shared_args.clone(),
            source_args,
            s3_dest_args,
        )
        .await?;

    // Copy from a temporary gs:// location.
    let from_temp_ctx = ctx.child(o!("from_temp" => s3_temp.to_string()));
    s3_temp
        .local_data(from_temp_ctx, shared_args, s3_source_args)
        .await
}
