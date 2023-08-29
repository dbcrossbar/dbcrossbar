//! Implementation of `write_local_data` for Redshift.

use super::RedshiftLocator;
use crate::common::*;
use crate::drivers::s3::find_s3_temp_dir;
use crate::tokio_glue::ConsumeWithParallelism;

/// Implementation of `write_local_data`, but as a real `async` function.
#[instrument(
    level = "debug",
    name = "redshift::write_local_data",
    skip_all,
    fields(dest = %dest)
)]
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    dest: RedshiftLocator,
    data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    // Build a temporary location.
    let shared_args_v = shared_args.clone().verify(RedshiftLocator::features())?;
    let s3_temp = find_s3_temp_dir(shared_args_v.temporary_storage())?;
    let s3_dest_args = DestinationArguments::for_temporary();
    let s3_source_args = SourceArguments::for_temporary();

    // Copy to a temporary s3:// location.
    let result_stream = s3_temp
        .write_local_data(ctx.clone(), data, shared_args.clone(), s3_dest_args)
        .instrument(debug_span!("stream_to_s3_temp", url = %s3_temp))
        .await?;

    // Wait for all s3:// uploads to finish with controllable parallelism.
    //
    // TODO: This duplicates our top-level `cp` code and we need to implement
    // the same rules for picking a good argument to `consume_with_parallelism`
    // and not just hard code our parallelism.
    result_stream
        .consume_with_parallelism(shared_args_v.max_streams())
        .await?;

    // Load from s3:// to Redshift.
    dest.write_remote_data(
        ctx,
        Box::new(s3_temp.clone()),
        shared_args,
        s3_source_args,
        dest_args,
    )
    .instrument(trace_span!("load_from_s3_temp", url = %s3_temp))
    .await?;

    // We don't need any parallelism after the Redshift step, so just return
    // a stream containing a single future.
    let fut = async { Ok(dest.boxed()) }.boxed();
    Ok(box_stream_once(Ok(fut)))
}
