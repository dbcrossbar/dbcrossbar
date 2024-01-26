//! Implementation of `schema`.

use super::BigQueryLocator;
use crate::common::*;
use crate::drivers::bigquery_shared::{BqTable, GCloudDriverArguments};

/// Implementation of `schema`, but as a real `async` function.
#[instrument(level = "trace", name = "bigquery::schema", skip(source_args))]
pub(crate) async fn schema_helper(
    source: BigQueryLocator,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<Schema>> {
    let source_args = source_args.verify(BigQueryLocator::features())?;
    let driver_args = GCloudDriverArguments::try_from(&source_args)?;
    let client = driver_args.client().await?;

    let bq_table = BqTable::read_from_table(&client, &source.table_name).await?;
    Ok(Some(Schema::from_table(bq_table.to_table()?)?))
}
