//! Helper for reading data from BigQuery.

use super::RedshiftLocator;
use crate::common::*;
use crate::drivers::s3::find_s3_temp_dir;

/// Implementation of `local_data`, but as a real `async` function.
#[instrument(
    level = "trace",
    name = "redshift::local_data",
    skip_all,
    fields(source = %source)
)]
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
    s3_temp
        .write_remote_data(
            ctx.clone(),
            Box::new(source),
            shared_args.clone(),
            source_args,
            s3_dest_args,
        )
        .instrument(trace_span!("extract_to_s3_tmp", url = %s3_temp))
        .await?;

    // Copy from a temporary gs:// location.
    s3_temp
        .local_data(ctx, shared_args, s3_source_args)
        .instrument(debug_span!("stream_from_s3", url = %s3_temp))
        .await
}
