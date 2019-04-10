//! Implementation of `write_local_data` for BigQuery.

use super::find_gs_temp_dir;
use crate::common::*;
use crate::drivers::bigquery::BigQueryLocator;

/// Implementation of `write_local_data`, but as a real `async` function.
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    dest: BigQueryLocator,
    schema: Table,
    data: BoxStream<CsvStream>,
    temporary_storage: TemporaryStorage,
    if_exists: IfExists,
) -> Result<BoxStream<BoxFuture<()>>> {
    // Build a temporary location.
    let gs_temp = find_gs_temp_dir(&temporary_storage)?;

    // Copy to a temporary gs:// location.
    let to_temp_ctx = ctx.child(o!("to_temp" => gs_temp.to_string()));
    let result_stream = await!(gs_temp.write_local_data(
        to_temp_ctx,
        schema.clone(),
        data,
        temporary_storage.clone(),
        IfExists::Overwrite,
    ))?;

    // Wait for all gs:// uploads to finish with controllable parallelism.
    //
    // TODO: This duplicates our top-level `cp` code and we need to implement the
    // same rules for picking a good argument to `buffered` and not just hard code
    // our parallelism.
    await!(result_stream.buffered(4).collect())?;

    // Load from gs:// to BigQuery.
    let from_temp_ctx = ctx.child(o!("from_temp" => gs_temp.to_string()));
    await!(dest.write_remote_data(
        from_temp_ctx,
        schema,
        Box::new(gs_temp),
        if_exists
    ))?;

    // We don't need any parallelism after the BigQuery step, so just return
    // a stream containing a single future.
    let fut = Ok(()).into_boxed_future();
    Ok(box_stream_once(Ok(fut)))
}
