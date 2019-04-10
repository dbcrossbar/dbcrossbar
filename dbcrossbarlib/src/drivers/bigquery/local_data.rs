//! Helper for reading data from BigQuery.

use super::find_gs_temp_dir;
use crate::common::*;
use crate::drivers::bigquery::BigQueryLocator;

/// Implementation of `local_data`, but as a real `async` function.
pub(crate) async fn local_data_helper(
    ctx: Context,
    source: BigQueryLocator,
    schema: Table,
    temporary_storage: TemporaryStorage,
) -> Result<Option<BoxStream<CsvStream>>> {
    // Build a temporary location.
    let gs_temp = find_gs_temp_dir(&temporary_storage)?;

    // Extract from BigQuery to gs://.
    let to_temp_ctx = ctx.child(o!("to_temp" => gs_temp.to_string()));
    await!(gs_temp.write_remote_data(
        to_temp_ctx,
        schema.clone(),
        Box::new(source),
        IfExists::Overwrite,
    ))?;

    // Copy from a temporary gs:// location.
    let from_temp_ctx = ctx.child(o!("from_temp" => gs_temp.to_string()));
    Ok(await!(gs_temp.local_data(
        from_temp_ctx,
        schema,
        temporary_storage.clone(),
    ))?)
}
