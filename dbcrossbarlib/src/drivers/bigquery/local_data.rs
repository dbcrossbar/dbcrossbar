//! Helper for reading data from BigQuery.

use crate::common::*;
use crate::drivers::{bigquery::BigQueryLocator, gs::find_gs_temp_dir};

/// Implementation of `local_data`, but as a real `async` function.
#[instrument(
    level = "trace",
    name = "bigquery::local_data",
    skip_all,
    fields(source = %source)
)]
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
    gs_temp
        .write_remote_data(
            ctx.clone(),
            Box::new(source),
            shared_args.clone(),
            source_args,
            gs_dest_args,
        )
        .instrument(trace_span!("extract_to_temp_gs"))
        .await?;

    // Copy from a temporary gs:// location.
    gs_temp
        .local_data(ctx, shared_args, gs_source_args)
        .instrument(trace_span!("stream_from_temp_gs"))
        .await
}
