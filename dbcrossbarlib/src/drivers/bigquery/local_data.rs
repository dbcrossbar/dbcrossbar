//! Helper for reading data from BigQuery.

use super::find_gs_temp_dir;
use crate::common::*;
use crate::drivers::bigquery::BigQueryLocator;

/// Implementation of `local_data`, but as a real `async` function.
pub(crate) async fn local_data_helper(
    ctx: Context,
    source: BigQueryLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    // Build a temporary location.
    let shared_args_v = shared_args.clone().verify(BigQueryLocator::features())?;
    let gs_temp = find_gs_temp_dir(shared_args_v.temporary_storage())?;
    let gs_dest_args = DestinationArguments::for_temporary();
    let gs_source_args = SourceArguments::for_temporary();

    // Extract from BigQuery to gs://.
    let to_temp_ctx = ctx.child(o!("to_temp" => gs_temp.to_string()));
    gs_temp
        .write_remote_data(
            to_temp_ctx,
            Box::new(source),
            shared_args.clone(),
            source_args,
            gs_dest_args,
        )
        .await?;

    // Copy from a temporary gs:// location.
    let from_temp_ctx = ctx.child(o!("from_temp" => gs_temp.to_string()));
    gs_temp
        .local_data(from_temp_ctx, shared_args, gs_source_args)
        .await
}
