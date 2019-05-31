//! Implementation of `BigQueryLocator::write_remote_data`.

use std::{
    fs::File,
    process::{Command, Stdio},
};
use tempdir::TempDir;
use tokio::io;
use tokio_process::CommandExt;

use super::BigQueryLocator;
use crate::common::*;
use crate::drivers::{
    bigquery_shared::{if_exists_to_bq_load_arg, BqTable, TableBigQueryExt, Usage},
    gs::GsLocator,
};

/// Copy `source` to `dest` using `schema`.
///
/// The function `BigQueryLocator::write_remote_data` isn't (yet) allowed to be
/// async, because it's part of a trait. This version is an `async fn`, which
/// makes the code much clearer.
pub(crate) async fn write_remote_data_helper(
    ctx: Context,
    schema: Table,
    source: BoxLocator,
    dest: BigQueryLocator,
    temporary_storage: TemporaryStorage,
    if_exists: IfExists,
) -> Result<()> {
    // Convert the source locator into the underlying `gs://` URL. This is a bit
    // fiddly because we're downcasting `source` and relying on knowledge about
    // the `GsLocator` type, and Rust doesn't make that especially easy.
    let mut source_url = source
        .as_any()
        .downcast_ref::<GsLocator>()
        .ok_or_else(|| format_err!("not a gs:// locator: {}", source))?
        .as_url()
        .to_owned();

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
            dest.table_name.temporary_table_name(&temporary_storage)?;
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
    let initial_table_replace = if use_temp {
        "--replace"
    } else {
        if_exists_to_bq_load_arg(&if_exists)?
    };

    // Build and run a `bq load` command.
    debug!(ctx.log(), "running `bq load`");
    let load_child = Command::new("bq")
        // These arguments can all be represented as UTF-8 `&str`.
        .args(&[
            "load",
            "--skip_leading_rows=1",
            initial_table_replace,
            &initial_table.name().to_string(),
            source_url.as_str(),
        ])
        // This argument is a path, and so it might contain non-UTF-8
        // characters. We pass it separately because Rust won't allow us to
        // create an array of mixed strings and paths.
        .arg(&initial_schema_path)
        .spawn_async()
        .context("error starting `bq load`")?;
    let status = await!(load_child).context("error running `bq load`")?;
    if !status.success() {
        return Err(format_err!("`bq load` failed with {}", status));
    }

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

        // If we're doing an upsert, make sure the destination table exists.
        if if_exists.is_upsert() {
            debug!(ctx.log(), "making sure table {} exists", dest_table.name(),);
            let dest_schema_path = tmp_dir.path().join("schema.json");
            let mut dest_schema_file = File::create(&dest_schema_path)?;
            dest_table.write_json_schema(&mut dest_schema_file)?;
            let mk_child = Command::new("bq")
                // Use `--force` to ignore existing tables.
                .args(&["mk", "--force", "--schema"])
                // Pass separately, because paths may not be UTF-8.
                .arg(&dest_schema_path)
                .arg(&dest_table.name().to_string())
                .spawn_async()
                .context("error starting `bq mk`")?;
            let status = await!(mk_child).context("error running `bq mk`")?;
            if !status.success() {
                return Err(format_err!("`bq mk` failed with {}", status));
            }
        }

        // Generate our import query.
        let mut query = Vec::new();
        if let IfExists::Upsert(merge_keys) = &if_exists {
            dest_table.write_merge_sql(
                initial_table.name(),
                &merge_keys[..],
                &mut query,
            )?;
        } else {
            dest_table.write_import_sql(initial_table.name(), &mut query)?;
        }
        debug!(ctx.log(), "import sql: {}", String::from_utf8_lossy(&query));

        // Pipe our query text to `bq load`.
        debug!(ctx.log(), "running `bq query`");
        let mut query_command = Command::new("bq");
        query_command
            // We'll pass the query on `stdin`.
            .stdin(Stdio::piped())
            // Run query with no output.
            .args(&[
                "query",
                "--format=none",
                if_exists_to_bq_load_arg(&if_exists)?,
                "--nouse_legacy_sql",
            ]);
        if !if_exists.is_upsert() {
            query_command.arg(&format!("--destination_table={}", dest_table.name()));
        }
        let mut query_child = query_command
            .spawn_async()
            .context("error starting `bq query`")?;
        let child_stdin = query_child
            .stdin()
            .take()
            .expect("don't have stdio that we requested");
        await!(io::write_all(child_stdin, query))
            .context("error piping query to `bq load`")?;
        let status = await!(query_child).context("error running `bq query`")?;
        if !status.success() {
            return Err(format_err!("`bq load` failed with {}", status));
        }

        // Delete temp table.
        debug!(
            ctx.log(),
            "deleting import temp table: {}",
            initial_table.name()
        );
        let rm_child = Command::new("bq")
            .args(&["rm", "-f", "-t", &initial_table.name().to_string()])
            .spawn_async()
            .context("error starting `bq rm`")?;
        let status = await!(rm_child).context("error running `bq rm`")?;
        if !status.success() {
            return Err(format_err!("`bq rm` failed with {}", status));
        }
    }

    Ok(())
}
