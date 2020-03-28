//! Interfaces to BigQuery.

use serde::de::DeserializeOwned;
use std::{fs::File, process::Stdio};
use tempdir::TempDir;
use tokio::process::Command;

use crate::common::*;
use crate::drivers::bigquery_shared::{
    if_exists_to_bq_load_arg, BqColumn, BqTable, TableName,
};
use crate::tokio_glue::write_to_stdin;

/// Run a query that should return a small number of records, and return them as
/// a JSON string.
async fn query_all_json(ctx: &Context, project: &str, sql: &str) -> Result<String> {
    // Run our query.
    debug!(ctx.log(), "running `bq query`");
    let mut query_child = Command::new("bq")
        // We'll pass the query on `stdin`.
        .stdin(Stdio::piped())
        // We'll read output from `stdout`.
        .stdout(Stdio::piped())
        // Run query with no output.
        .args(&["query", "--headless", "--format=json", "--nouse_legacy_sql"])
        .arg(format!("--project_id={}", project))
        .spawn()
        .context("error starting `bq query`")?;
    write_to_stdin("bq query", &mut query_child, sql.as_bytes()).await?;
    let mut child_stdout = query_child
        .stdout
        .take()
        .expect("don't have stdout that we requested");
    let mut output = vec![];
    child_stdout
        .read_to_end(&mut output)
        .await
        .context("error reading output from `bq query`")?;
    let output = String::from_utf8(output)?;
    debug!(ctx.log(), "bq count output: {}", output.trim());

    let status = query_child.await.context("error running `bq query`")?;
    if status.success() {
        Ok(output)
    } else {
        Err(format_err!("`bq query` failed with {}", status))
    }
}

/// Run a query that should return a small number of records, and deserialize them.
pub(crate) async fn query_all<T>(
    ctx: &Context,
    project: &str,
    sql: &str,
) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    let output = query_all_json(ctx, project, sql).await?;
    // Parse our output.
    Ok(serde_json::from_str::<Vec<T>>(&output)
        .context("could not parse count output")?)
}

/// Run a query that should return exactly one record, and deserialize it.
pub(crate) async fn query_one<T>(ctx: &Context, project: &str, sql: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let mut rows = query_all(ctx, project, sql).await?;
    if rows.len() == 1 {
        Ok(rows.remove(0))
    } else {
        Err(format_err!("expected 1 row, found {}", rows.len()))
    }
}

/// Run an SQL query and save the results to a table.
pub(crate) async fn query_to_table(
    ctx: &Context,
    project: &str,
    sql: &str,
    dest_table: &TableName,
    if_exists: &IfExists,
) -> Result<()> {
    // Run our query.
    debug!(ctx.log(), "running `bq query`");
    let mut query_child = Command::new("bq")
        // We'll pass the query on `stdin`.
        .stdin(Stdio::piped())
        // Throw away stdout so it doesn't corrupt our output.
        .stdout(Stdio::null())
        // Run query with no output.
        .args(&[
            "query",
            "--headless",
            "--format=none",
            &format!("--destination_table={}", dest_table),
            if_exists_to_bq_load_arg(&if_exists)?,
            "--nouse_legacy_sql",
            &format!("--project_id={}", project),
        ])
        .spawn()
        .context("error starting `bq query`")?;
    write_to_stdin("bq query", &mut query_child, sql.as_bytes()).await?;
    let status = query_child.await.context("error running `bq query`")?;
    if status.success() {
        Ok(())
    } else {
        Err(format_err!("`bq query` failed with {}", status))
    }
}

/// Execute an SQL statement.
pub(crate) async fn execute_sql(
    ctx: &Context,
    project: &str,
    sql: &str,
) -> Result<()> {
    // Run our SQL.
    debug!(ctx.log(), "running `bq query`");
    let mut query_child = Command::new("bq")
        // We'll pass the SQL on `stdin`.
        .stdin(Stdio::piped())
        // Throw away stdout so it doesn't corrupt our output.
        .stdout(Stdio::null())
        // Run SQL with no output.
        .args(&[
            "query",
            "--headless",
            "--format=none",
            "--nouse_legacy_sql",
            &format!("--project_id={}", project),
        ])
        .spawn()
        .context("error starting `bq query`")?;
    write_to_stdin("bq query", &mut query_child, sql.as_bytes()).await?;
    let status = query_child.await.context("error running `bq query`")?;
    if status.success() {
        Ok(())
    } else {
        Err(format_err!("`bq query` failed with {}", status))
    }
}

/// Load data from `gs_url` into `dest_table`.
pub(crate) async fn load(
    ctx: &Context,
    gs_url: &Url,
    dest_table: &BqTable,
    if_exists: &IfExists,
) -> Result<()> {
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
    dest_table.write_json_schema(&mut initial_schema_file)?;

    // Build and run a `bq load` command.
    debug!(ctx.log(), "running `bq load`");
    let load_child = Command::new("bq")
        // These arguments can all be represented as UTF-8 `&str`.
        .args(&[
            "load",
            "--headless",
            "--skip_leading_rows=1",
            &format!("--project_id={}", dest_table.name().project()),
            if_exists_to_bq_load_arg(&if_exists)?,
            &dest_table.name().to_string(),
            gs_url.as_str(),
        ])
        // Throw away stdout so it doesn't corrupt our output.
        .stdout(Stdio::null())
        // This argument is a path, and so it might contain non-UTF-8
        // characters. We pass it separately because Rust won't allow us to
        // create an array of mixed strings and paths.
        .arg(&initial_schema_path)
        .spawn()
        .context("error starting `bq load`")?;
    let status = load_child.await.context("error running `bq load`")?;
    if status.success() {
        Ok(())
    } else {
        Err(format_err!("`bq load` failed with {}", status))
    }
}

/// Drop a table from BigQuery.
pub(crate) async fn drop_table(ctx: &Context, table_name: &TableName) -> Result<()> {
    // Delete temp table.
    debug!(ctx.log(), "deleting table: {}", table_name);
    let sql = format!("DROP TABLE {};\n", table_name.dotted_and_quoted());
    execute_sql(ctx, table_name.project(), &sql).await
}

/// Look up the schema of the specified table.
pub(crate) async fn schema(ctx: &Context, name: &TableName) -> Result<BqTable> {
    let project_id = format!("--project_id={}", name.project());
    let output = Command::new("bq")
        .args(&[
            "show",
            "--headless",
            "--schema",
            "--format=json",
            &project_id,
            &name.to_string(),
        ])
        .stderr(Stdio::inherit())
        .output()
        .await
        .context("error running `bq show --schema`")?;
    if !output.status.success() {
        return Err(format_err!(
            "`bq show --schema` failed with {}",
            output.status,
        ));
    }
    debug!(
        ctx.log(),
        "BigQuery schema: {}",
        String::from_utf8_lossy(&output.stdout).trim(),
    );
    let columns: Vec<BqColumn> = serde_json::from_slice(&output.stdout)
        .context("error parsing BigQuery schema")?;
    Ok(BqTable {
        name: name.to_owned(),
        columns,
    })
}

/// Extract a table from BigQuery to Google Cloud Storage.
pub(crate) async fn extract(
    ctx: &Context,
    source_table: &TableName,
    dest_gs_url: &Url,
) -> Result<()> {
    // Build and run a `bq extract` command.
    debug!(ctx.log(), "running `bq extract`");
    let extract_child = Command::new("bq")
        // These arguments can all be represented as UTF-8 `&str`.
        .args(&[
            "extract",
            "--headless",
            "--destination_format=CSV",
            &format!("--project_id={}", source_table.project()),
            &source_table.to_string(),
            &format!("{}/*.csv", dest_gs_url),
        ])
        // Throw away stdout so it doesn't corrupt our output.
        .stdout(Stdio::null())
        .spawn()
        .context("error starting `bq extract`")?;
    let status = extract_child.await.context("error running `bq extract`")?;
    if status.success() {
        Ok(())
    } else {
        Err(format_err!("`bq extract` failed with {}", status))
    }
}
