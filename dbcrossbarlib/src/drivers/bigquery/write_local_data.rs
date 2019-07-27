//! Implementation of `write_local_data` for BigQuery.

use super::find_gs_temp_dir;
use crate::common::*;
use crate::drivers::bigquery::BigQueryLocator;
use crate::tokio_glue::ConsumeWithParallelism;

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
    let result_stream = gs_temp
        .write_local_data(
            to_temp_ctx,
            schema.clone(),
            data,
            temporary_storage.clone(),
            IfExists::Overwrite,
        )
        .await?;

    // Wait for all gs:// uploads to finish with controllable parallelism.
    //
    // TODO: This duplicates our top-level `cp` code and we need to implement
    // the same rules for picking a good argument to `consume_with_parallelism`
    // and not just hard code our parallelism.
    result_stream.consume_with_parallelism(4).await?;

    // Load from gs:// to BigQuery.
    let from_temp_ctx = ctx.child(o!("from_temp" => gs_temp.to_string()));
    dest.write_remote_data(
        from_temp_ctx,
        schema,
        Box::new(gs_temp),
        Query::default(),
        temporary_storage,
        if_exists,
    )
    .await?;

    // We don't need any parallelism after the BigQuery step, so just return
    // a stream containing a single future.
    let fut = async { Ok(()) }.boxed();
    Ok(box_stream_once(Ok(fut)))
}
