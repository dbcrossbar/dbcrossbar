//! Implementation of `GsLocator::write_remote_data`.

use super::{prepare_as_destination_helper, GsLocator};
use crate::clouds::gcloud::{bigquery, storage};
use crate::common::*;
use crate::drivers::{
    bigquery::BigQueryLocator,
    bigquery_shared::{BqTable, GCloudDriverArguments, Usage},
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

    // Get our billing labels.
    let job_labels = source_args
        .driver_args()
        .deserialize::<GCloudDriverArguments>()
        .context("error parsing --from-args")?
        .job_labels
        .to_owned();

    // Construct a `BqTable` describing our source table.
    let source_table = BqTable::for_table_name_and_columns(
        schema,
        source_table_name.clone(),
        &schema.table.columns,
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
        .temporary_table_name(temporary_storage)?;
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
        &job_labels,
    )
    .await?;

    // Delete the existing output, if it exists.
    prepare_as_destination_helper(
        ctx.clone(),
        dest.as_url().to_owned(),
        if_exists.clone(),
    )
    .await?;

    // Build and run a `bq extract` command.
    bigquery::extract(&ctx, &temp_table_name, dest.as_url(), &job_labels).await?;

    // Delete temp table.
    bigquery::drop_table(&ctx, &temp_table_name, &job_labels).await?;

    // List the files in that bucket and return them, at least in
    // `IfExists::Overwrite` mode, where we know we created them (barring race
    // conditions).
    if if_exists == IfExists::Overwrite {
        let mut storage_object_stream = storage::ls(&ctx, &dest.url).await?;
        let mut dest_urls = vec![];
        while let Some(storage_object) = storage_object_stream.next().await {
            let storage_object = storage_object?;
            let locator = storage_object.to_url_string().parse::<GsLocator>()?;
            dest_urls.push(locator.boxed());
        }
        Ok(dest_urls)
    } else {
        // This is probably not the perfect thing to do here, but at least it's
        // backwards compatible.
        Ok(vec![dest.boxed()])
    }
}
