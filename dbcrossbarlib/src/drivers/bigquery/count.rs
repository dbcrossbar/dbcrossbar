//! Implementation of `count`, but as a real `async` function.

use serde::Deserialize;
use std::process::Stdio;
use tokio::process::Command;

use crate::common::*;
use crate::drivers::{
    bigquery::BigQueryLocator,
    bigquery_shared::{BqTable, Usage},
};
use crate::tokio_glue::write_to_stdin;

/// Implementation of `count`, but as a real `async` function.
pub(crate) async fn count_helper(
    ctx: Context,
    locator: BigQueryLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<usize> {
    let shared_args = shared_args.verify(BigQueryLocator::features())?;
    let source_args = source_args.verify(BigQueryLocator::features())?;

    // Look up the arguments we need.
    let schema = shared_args.schema();

    // Construct a `BqTable` describing our source table.
    let table_name = locator.as_table_name().to_owned();
    let table = BqTable::for_table_name_and_columns(
        table_name,
        &schema.columns,
        Usage::FinalTable,
    )?;

    // Generate our count SQL.
    let mut count_sql_data = vec![];
    table.write_count_sql(&source_args, &mut count_sql_data)?;
    let count_sql = String::from_utf8(count_sql_data).expect("should always be UTF-8");
    debug!(ctx.log(), "count SQL: {}", count_sql);

    // Run our query.
    debug!(ctx.log(), "running `bq query`");
    let mut query_child = Command::new("bq")
        // We'll pass the query on `stdin`.
        .stdin(Stdio::piped())
        // We'll read output from `stdout`.
        .stdout(Stdio::piped())
        // Run query with no output.
        .args(&["query", "--headless", "--format=json", "--nouse_legacy_sql"])
        .arg(format!("--project_id={}", locator.project()))
        .spawn()
        .context("error starting `bq query`")?;
    write_to_stdin("bq query", &mut query_child, count_sql.as_bytes()).await?;
    let mut child_stdout = query_child
        .stdout()
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
    if !status.success() {
        return Err(format_err!("`bq query` failed with {}", status));
    }

    // Parse our output, and get the count.
    #[derive(Deserialize)]
    struct CountRow {
        count: String,
    }
    let rows = serde_json::from_str::<Vec<CountRow>>(&output)
        .context("could not parse count output")?;
    if rows.len() != 1 {
        Err(format_err!(
            "expected 1 row of count output, got {}",
            rows.len(),
        ))
    } else {
        Ok(rows[0]
            .count
            .parse::<usize>()
            .context("could not parse count output")?)
    }
}
