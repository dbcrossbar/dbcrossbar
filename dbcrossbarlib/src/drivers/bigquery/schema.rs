//! Implementation of `schema`.

use super::BigQueryLocator;
use crate::common::*;
use crate::drivers::bigquery_shared::BqTable;

/// Implementation of `schema`, but as a real `async` function.
#[instrument(level = "trace", name = "bigquery::schema")]
pub(crate) async fn schema_helper(source: BigQueryLocator) -> Result<Option<Schema>> {
    let bq_table = BqTable::read_from_table(&source.table_name).await?;
    Ok(Some(Schema::from_table(bq_table.to_table()?)?))
}
