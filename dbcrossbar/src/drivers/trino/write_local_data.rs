//! Write data from a stream of streams of CSV data to a Trino table.

use crate::{
    clouds::aws::s3, common::*, drivers::s3::find_s3_temp_dir,
    ConsumeWithParallelism as _,
};

use super::TrinoLocator;

/// Implementation of [`TrinoLocator::write_local_data`], but as a real `async`
/// function.
///
/// This duplicates a fair bit of code with the Redshift-via-S3 uploader.
#[instrument(
    level = "debug",
    name = "trino::write_local_data",
    skip_all,
    fields(dest = %dest)
)]
pub(super) async fn write_local_data_helper(
    ctx: Context,
    dest: TrinoLocator,
    data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    let shared_args_v = shared_args.clone().verify(TrinoLocator::features())?;

    // Copy the data to a temporary location. We might eventually want to
    // support Google Cloud Storage here, too.
    let s3_temp = find_s3_temp_dir(shared_args_v.temporary_storage())?;
    let s3_dest_args = DestinationArguments::for_temporary();
    let s3_source_args = SourceArguments::for_temporary();
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

    // Load from s3:// to Trino.
    dest.write_remote_data(
        ctx,
        Box::new(s3_temp.clone()),
        shared_args,
        s3_source_args,
        dest_args,
    )
    .instrument(trace_span!("load_from_s3_temp", url = %s3_temp))
    .await?;

    // Drop our temporary data.
    s3::rmdir(s3_temp.as_url()).await?;

    // We don't need any parallelism after the Trino step, so just return
    // a stream containing a single future.
    let fut = async { Ok(dest.boxed()) }.boxed();
    Ok(box_stream_once(Ok(fut)))
}
