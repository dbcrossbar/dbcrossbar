//! Implementation of `count`, but as a real `async` function.

use serde::Deserialize;

use crate::clouds::gcloud::bigquery;
use crate::common::*;
use crate::drivers::{
    bigquery::BigQueryLocator,
    bigquery_shared::{BqTable, Usage},
};

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
    #[derive(Deserialize)]
    struct CountRow {
        count: String,
    }
    let count_str =
        bigquery::query_one::<CountRow>(&ctx, locator.project(), &count_sql)
            .await?
            .count;
    Ok(count_str
        .parse::<usize>()
        .context("could not parse count output")?)
}
