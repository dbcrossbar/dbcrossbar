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
use crate::drivers::{bigquery_schema::write_schema, gs::GsLocator};

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
    let (use_temp, initial_table) = if write_schema::need_import_sql(&schema) {
        let initial_table = dest.temp_table_name();
        debug!(ctx.log(), "loading into temporary table {}", initial_table);
        (true, initial_table)
    } else {
        let initial_table = dest.to_full_table_name();
        debug!(
            ctx.log(),
            "loading directly into final table {}", initial_table
        );
        (false, initial_table)
    };

    // Write our schema to a temp file. This actually needs to be somewhere on
    // disk, and `bq` uses various hueristics to detect that it's a file
    // containing a schema, and not just a string with schema text. (Note this
    // code is synchronous, but that's not a huge deal.)
    //
    // We use `use_temp` to decide whether to generate the final schema or a
    // temporary schema that we'll fix later.
    let tmp_dir = TempDir::new("bq_load")?;
    let schema_path = tmp_dir.path().join("schema.json");
    let mut schema_file = File::create(&schema_path)?;
    write_schema::write_json(&mut schema_file, &schema, use_temp)?;

    // Decide how to handle overwrites of the initial table.
    let initial_table_replace = if use_temp {
        "--replace"
    } else {
        if_exists_to_bq_load_arg(if_exists)?
    };

    // Build and run a `bq load` command.
    debug!(ctx.log(), "running `bq load`");
    let load_child = Command::new("bq")
        // These arguments can all be represented as UTF-8 `&str`.
        .args(&[
            "load",
            "--skip_leading_rows=1",
            initial_table_replace,
            &initial_table,
            source_url.as_str(),
        ])
        // This argument is a path, and so it might contain non-UTF-8
        // characters. We pass it separately because Rust won't allow us to
        // create an array of mixed strings and paths.
        .arg(&schema_path)
        .spawn_async()
        .context("error starting `bq load`")?;
    let status = await!(load_child).context("error running `bq load`")?;
    if !status.success() {
        return Err(format_err!("`bq load` failed with {}", status));
    }

    // If `use_temp` is false, then we're done. Otherwise, run the update SQL to
    // build the final table (if needed).
    if use_temp {
        // Get our target table name.
        let dest_table = dest.to_full_table_name();
        debug!(
            ctx.log(),
            "transforming data into final table {}", dest_table
        );

        // Generate our import query.
        let mut query = Vec::new();
        let mut new_schema = schema.clone();
        new_schema.name = initial_table.clone();
        write_schema::write_import_sql(&mut query, &new_schema)?;
        trace!(ctx.log(), "import sql: {}", String::from_utf8_lossy(&query));

        // Pipe our query text to `bq load`.
        debug!(ctx.log(), "running `bq query`");
        let mut query_child = Command::new("bq")
            // We'll pass the query on `stdin`.
            .stdin(Stdio::piped())
            // Run query with no output.
            .args(&[
                "query",
                "--format=none",
                &format!("--destination_table={}", dest_table),
                if_exists_to_bq_load_arg(if_exists)?,
                "--nouse_legacy_sql",
            ])
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

        // TODO: Delete temp table!
    }

    Ok(())
}

/// Convert an `IfExists` value to the corresponding `bq load` argument, or
/// return an error if we can't.
fn if_exists_to_bq_load_arg(if_exists: IfExists) -> Result<&'static str> {
    match if_exists {
        IfExists::Overwrite => Ok("--replace"),
        // TODO: Verify that this is the actual behavior of `--noreplace`.
        IfExists::Append => Ok("--noreplace"),
        // We need to be careful about race conditions--we don't want to try to
        // emulate this if we can't do it natively.
        IfExists::Error => Err(format_err!(
            "BigQuery only supports --if-exists=overwrite or --if-exists=append"
        )),
    }
}
