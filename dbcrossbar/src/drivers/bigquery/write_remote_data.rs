//! Implementation of `BigQueryLocator::write_remote_data`.

use super::BigQueryLocator;
use crate::clouds::gcloud::bigquery;
use crate::common::*;
use crate::drivers::{
    bigquery_shared::{BqTable, GCloudDriverArguments, SchemaBigQueryExt, Usage},
    gs::GsLocator,
};

/// Copy `source` to `dest` using `schema`.
///
/// The function `BigQueryLocator::write_remote_data` isn't (yet) allowed to be
/// async, because it's part of a trait. This version is an `async fn`, which
/// makes the code much clearer.
#[instrument(
    level = "debug",
    name = "bigquery::write_remote_data",
    skip_all,
    fields(source = %source, dest = %dest)
)]
pub(crate) async fn write_remote_data_helper(
    source: BoxLocator,
    dest: BigQueryLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<Vec<BoxLocator>> {
    // Convert the source locator into the underlying `gs://` URL. This is a bit
    // fiddly because we're downcasting `source` and relying on knowledge about
    // the `GsLocator` type, and Rust doesn't make that especially easy.
    let mut source_url = source
        .as_any()
        .downcast_ref::<GsLocator>()
        .ok_or_else(|| format_err!("not a gs:// locator: {}", source))?
        .as_url()
        .to_owned();

    // Verify our arguments.
    let shared_args = shared_args.verify(BigQueryLocator::features())?;
    let _source_args = source_args.verify(Features::empty())?;
    let dest_args = dest_args.verify(BigQueryLocator::features())?;

    // Get the arguments we care about.
    let schema = shared_args.schema();
    let temporary_storage = shared_args.temporary_storage();
    let if_exists = dest_args.if_exists();

    let driver_args = dest_args
        .driver_args()
        .deserialize::<GCloudDriverArguments>()
        .context("error parsing --to-args")?;

    // Get our billing labels.
    let dest_driver_args = GCloudDriverArguments::try_from(&dest_args)?;
    let job_labels = dest_driver_args.job_labels.to_owned();

    // We want to use the `GCloudDriverArgs` for our destination, because that's
    // the part that does the actual work.
    //
    // TODO: Technically, if the `GCloudDriverArgs` for `SourceArguments` and
    // `DestinationArguments` are different enough, we probably want to avoid
    // calling `write_remote_data` at all.
    let dest_client = dest_driver_args.client().await?;

    // In case the user wants to run the job in a different project for billing purposes
    let final_job_project_id = driver_args
        .job_project_id
        .unwrap_or_else(|| dest.project().to_owned());

    // If our URL looks like a directory, add a glob.
    //
    // TODO: Is this the right way to default this? Or should we make users
    // always specify `*.csv`? This should probably be part of some larger
    // `dbcrossbar` property. Elsewhere, we're trying to default to adding
    // `**/*.csv`, but that's not supported by BigQuery.
    if source_url.as_str().ends_with('/') {
        source_url = source_url.join("*.csv")?;
    }

    // Decide if we need to use a temp table.
    let use_temp = !schema.bigquery_can_import_from_csv()? || if_exists.is_upsert();
    let initial_table_name = if use_temp {
        let initial_table_name =
            dest.table_name.temporary_table_name(temporary_storage)?;
        debug!("loading into temporary table {}", initial_table_name);
        initial_table_name
    } else {
        let initial_table_name = dest.table_name.clone();
        debug!("loading directly into final table {}", initial_table_name,);
        initial_table_name
    };

    // Build the information we'll need about our initial table.
    let initial_table = BqTable::for_table_name_and_columns(
        schema,
        initial_table_name,
        &schema.table.columns,
        if use_temp {
            Usage::CsvLoad
        } else {
            Usage::FinalTable
        },
    )?;

    // Decide how to handle overwrites of the initial table.
    let if_initial_table_exists = if use_temp {
        &IfExists::Overwrite
    } else {
        if_exists
    };

    // Load our data.
    bigquery::load(
        &dest_client,
        &source_url,
        &initial_table,
        if_initial_table_exists,
        &job_labels,
        &final_job_project_id,
    )
    .await?;

    // If `use_temp` is false, then we're done. Otherwise, run the update SQL to
    // build the final table (if needed).
    if use_temp {
        // Build a `BqTable` for our final table.
        let dest_table = BqTable::for_table_name_and_columns(
            schema,
            dest.table_name.clone(),
            &schema.table.columns,
            Usage::FinalTable,
        )?;
        debug!("transforming data into final table {}", dest_table.name(),);

        // Generate and run our import SQL.
        let mut query = Vec::new();
        dest_table.write_import_sql(initial_table.name(), if_exists, &mut query)?;
        let query =
            String::from_utf8(query).expect("generated SQL should always be UTF-8");
        debug!("import sql: {}", query);
        bigquery::execute_sql(
            &dest_client,
            &final_job_project_id,
            &query,
            &job_labels,
        )
        .await?;

        // Delete temp table.
        bigquery::drop_table(&dest_client, initial_table.name(), &job_labels).await?;
    }

    Ok(vec![dest.boxed()])
}
