//! Implementation of `count`, but as a real `async` function.

use super::PostgresLocator;
use crate::common::*;
use crate::drivers::postgres_shared::{connect, CheckCatalog, PgCreateTable};

/// Implementation of `count`, but as a real `async` function.
pub(crate) async fn count_helper(
    ctx: Context,
    locator: PostgresLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<usize> {
    let shared_args = shared_args.verify(PostgresLocator::features())?;
    let source_args = source_args.verify(PostgresLocator::features())?;

    // Get the parts of our locator.
    let url = locator.url.clone();
    let table_name = locator.table_name.clone();

    // Look up the arguments we'll need.
    let schema = shared_args.schema();

    // Convert our schema to a native PostgreSQL schema.
    let pg_create_table = PgCreateTable::from_pg_catalog_or_default(
        &ctx,
        // No need to look at the catalog, since we don't care about columns.
        CheckCatalog::No,
        &url,
        &table_name,
        schema,
    )
    .await?;

    // Generate SQL for query.
    let mut sql_bytes: Vec<u8> = vec![];
    pg_create_table.write_count_sql(&mut sql_bytes, &source_args)?;
    let sql = String::from_utf8(sql_bytes).expect("should always be UTF-8");
    debug!(ctx.log(), "count SQL: {}", sql);

    // Run our query.
    let conn = connect(&ctx, &url).await?;
    let stmt = conn.prepare(&sql).await?;
    let rows = conn
        .query(&stmt, &[])
        .await
        .context("error running count query")?;
    if rows.len() != 1 {
        Err(format_err!(
            "expected 1 row of count output, got {}",
            rows.len(),
        ))
    } else {
        let count: i64 = rows[0].get("count");
        Ok(usize::try_from(count).context("count out of range")?)
    }
}
