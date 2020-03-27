//! Implementation of `BigQueryLocator::write_remote_data`.

use std::fs::File;
use tempdir::TempDir;

use super::BigQueryLocator;
use crate::clouds::gcloud::bigquery;
use crate::common::*;
use crate::drivers::{
    bigquery_shared::{BqTable, TableBigQueryExt, Usage},
    gs::GsLocator,
};

/// Copy `source` to `dest` using `schema`.
///
/// The function `BigQueryLocator::write_remote_data` isn't (yet) allowed to be
/// async, because it's part of a trait. This version is an `async fn`, which
/// makes the code much clearer.
pub(crate) async fn write_remote_data_helper(
    ctx: Context,
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

    // If our URL looks like a directory, add a glob.
    //
    // TODO: Is this the right way to default this? Or should we make users
    // always specify `*.csv`? This should probably be part of some larger
    // `dbcrossbar` property. Elsewhere, we're trying to default to adding
    // `**/*.csv`, but that's not supported by BigQuery.
    if source_url.as_str().ends_with('/') {
        source_url = source_url.join("*.csv")?;
    }
    let ctx = ctx.child(o!("source_url" => source_url.as_str().to_owned()));

    // Decide if we need to use a temp table.
    let use_temp = !schema.bigquery_can_import_from_csv()? || if_exists.is_upsert();
    let initial_table_name = if use_temp {
        let initial_table_name =
            dest.table_name.temporary_table_name(temporary_storage)?;
        debug!(
            ctx.log(),
            "loading into temporary table {}", initial_table_name
        );
        initial_table_name
    } else {
        let initial_table_name = dest.table_name.clone();
        debug!(
            ctx.log(),
            "loading directly into final table {}", initial_table_name,
        );
        initial_table_name
    };

    // Build the information we'll need about our initial table.
    let initial_table = BqTable::for_table_name_and_columns(
        initial_table_name,
        &schema.columns,
        if use_temp {
            Usage::CsvLoad
        } else {
            Usage::FinalTable
        },
    )?;

    // Write our schema to a temp file. This actually needs to be somewhere on
    // disk, and `bq` uses various hueristics to detect that it's a file
    // containing a schema, and not just a string with schema text. (Note this
    // code is synchronous, but that's not a huge deal.)
    //
    // We use `use_temp` to decide whether to generate the final schema or a
    // temporary schema that we'll fix later.
    let tmp_dir = TempDir::new("bq_load")?;
    let initial_schema_path = tmp_dir.path().join("schema.json");
    let mut initial_schema_file = File::create(&initial_schema_path)?;
    initial_table.write_json_schema(&mut initial_schema_file)?;

    // Decide how to handle overwrites of the initial table.
    let if_initial_table_exists = if use_temp {
        &IfExists::Overwrite
    } else {
        if_exists
    };

    // Load our data.
    bigquery::load(&ctx, &source_url, &initial_table, if_initial_table_exists).await?;

    // If `use_temp` is false, then we're done. Otherwise, run the update SQL to
    // build the final table (if needed).
    if use_temp {
        // Build a `BqTable` for our final table.
        let dest_table = BqTable::for_table_name_and_columns(
            dest.table_name.clone(),
            &schema.columns,
            Usage::FinalTable,
        )?;
        debug!(
            ctx.log(),
            "transforming data into final table {}",
            dest_table.name(),
        );

        // Generate and run our import SQL.
        let mut query = Vec::new();
        dest_table.write_import_sql(initial_table.name(), if_exists, &mut query)?;
        let query =
            String::from_utf8(query).expect("generated SQL should always be UTF-8");
        debug!(ctx.log(), "import sql: {}", query);
        bigquery::execute_sql(&ctx, dest.project(), &query).await?;

        // Delete temp table.
        bigquery::drop_table(&ctx, initial_table.name()).await?;
    }

    Ok(vec![dest.boxed()])
}
