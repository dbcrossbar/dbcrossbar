//! Implementation of `write_local_data` for BigQuery.

use crate::common::*;
use crate::drivers::{bigquery::BigQueryLocator, gs::find_gs_temp_dir};
use crate::tokio_glue::ConsumeWithParallelism;

/// Implementation of `write_local_data`, but as a real `async` function.
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    dest: BigQueryLocator,
    data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    // Build a temporary location.
    let shared_args_v = shared_args.clone().verify(BigQueryLocator::features())?;
    let gs_temp = find_gs_temp_dir(shared_args_v.temporary_storage())?;
    let gs_dest_args = DestinationArguments::for_temporary();
    let gs_source_args = SourceArguments::for_temporary();

    // Copy to a temporary gs:// location.
    let to_temp_ctx = ctx.child(o!("to_temp" => gs_temp.to_string()));
    let result_stream = gs_temp
        .write_local_data(to_temp_ctx, data, shared_args.clone(), gs_dest_args)
        .await?;

    // Wait for all gs:// uploads to finish with controllable parallelism.
    //
    // TODO: This duplicates our top-level `cp` code and we need to implement
    // the same rules for picking a good argument to `consume_with_parallelism`
    // and not just hard code our parallelism.
    result_stream.consume_with_parallelism(shared_args_v.max_streams()).await?;

    // Load from gs:// to BigQuery.
    let from_temp_ctx = ctx.child(o!("from_temp" => gs_temp.to_string()));
    dest.write_remote_data(
        from_temp_ctx,
        Box::new(gs_temp),
        shared_args,
        gs_source_args,
        dest_args,
    )
    .await?;

    // We don't need any parallelism after the BigQuery step, so just return
    // a stream containing a single future.
    let fut = async { Ok(dest.boxed()) }.boxed();
    Ok(box_stream_once(Ok(fut)))
}
