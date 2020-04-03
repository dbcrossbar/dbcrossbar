//! Support for looking up BigQuery schemas.

use serde::Deserialize;

use super::{
    super::{percent_encode, Client, NoQuery},
    TableSchema,
};
use crate::common::*;
use crate::drivers::bigquery_shared::{BqTable, TableName};

/// Information about a table.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Table {
    schema: TableSchema,
}

/// Look up the schema of the specified table.
pub(crate) async fn schema(ctx: &Context, name: &TableName) -> Result<BqTable> {
    trace!(ctx.log(), "fetching schema for {:?}", name);

    // Build our URL.
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}",
        percent_encode(name.project()),
        percent_encode(name.dataset()),
        percent_encode(name.table()),
    );

    // Look up our schema.
    let client = Client::new(ctx).await?;
    let table = client.get::<Table, _, _>(ctx, &url, NoQuery).await?;
    Ok(BqTable {
        name: name.to_owned(),
        columns: table.schema.fields,
    })
}
