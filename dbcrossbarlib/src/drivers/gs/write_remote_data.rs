//! Implementation of `GsLocator::write_remote_data`.

use super::{prepare_as_destination_helper, GsLocator};
use crate::clouds::gcloud::bigquery;
use crate::common::*;
use crate::drivers::{
    bigquery::BigQueryLocator,
    bigquery_shared::{BqTable, Usage},
};

/// Copy `source` to `dest` using `schema`.
///
/// The function `BigQueryLocator::write_remote_data` isn't (yet) allowed to be
/// async, because it's part of a trait. This version is an `async fn`, which
/// makes the code much clearer.
pub(crate) async fn write_remote_data_helper(
    ctx: Context,
    source: BoxLocator,
    dest: GsLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<Vec<BoxLocator>> {
    // Convert the source locator into the underlying `TableName. This is a bit
    // fiddly because we're downcasting `source` and relying on knowledge about
    // the `GsLocator` type, and Rust doesn't make that especially easy.
    let source = source
        .as_any()
        .downcast_ref::<BigQueryLocator>()
        .ok_or_else(|| format_err!("not a bigquery locator: {}", source))?;
    let source_table_name = source.as_table_name().to_owned();

    // Verify our arguments.
    let shared_args = shared_args.verify(GsLocator::features())?;
    let source_args = source_args.verify(BigQueryLocator::features())?;
    let dest_args = dest_args.verify(GsLocator::features())?;

    // Look up the arguments we need.
    let schema = shared_args.schema();
    let temporary_storage = shared_args.temporary_storage();
    let if_exists = dest_args.if_exists().to_owned();

    // Construct a `BqTable` describing our source table.
    let source_table = BqTable::for_table_name_and_columns(
        source_table_name.clone(),
        &schema.columns,
        Usage::FinalTable,
    )?;

    // Look up our _actual_ table schema, which we'll need to handle the finer
    // details of exporting RECORDs and other things which aren't visible in the
    // portable schema. We do something similar in PostgreSQL imports.
    let mut real_source_table =
        BqTable::read_from_table(&ctx, &source_table_name).await?;
    real_source_table = real_source_table.aligned_with(&source_table)?;

    // We need to build a temporary export table.
    let temp_table_name = source_table
        .name()
        .temporary_table_name(&temporary_storage)?;
    let mut export_sql_data = vec![];
    real_source_table.write_export_sql(&source_args, &mut export_sql_data)?;
    let export_sql =
        String::from_utf8(export_sql_data).expect("should always be UTF-8");
    debug!(ctx.log(), "export SQL: {}", export_sql);

    // Run our query.
    bigquery::query_to_table(
        &ctx,
        source.project(),
        &export_sql,
        &temp_table_name,
        &IfExists::Overwrite,
    )
    .await?;

    // Delete the existing output, if it exists.
    prepare_as_destination_helper(ctx.clone(), dest.as_url().to_owned(), if_exists)
        .await?;

    // Build and run a `bq extract` command.
    bigquery::extract(&ctx, &temp_table_name, dest.as_url()).await?;

    // Delete temp table.
    bigquery::drop_table(&ctx, &temp_table_name).await?;
    Ok(vec![dest.boxed()])
}
