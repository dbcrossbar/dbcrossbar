//! Implementation of `schema`.

use super::BigQueryLocator;
use crate::common::*;
use crate::drivers::bigquery_shared::BqTable;
use crate::schema::Table;

/// Implementation of `schema`, but as a real `async` function.
pub(crate) async fn schema_helper(
    ctx: Context,
    source: BigQueryLocator,
) -> Result<Option<Table>> {
    let bq_table = BqTable::read_from_table(&ctx, &source.table_name).await?;
    Ok(Some(bq_table.to_table()?))
}
