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
#[instrument(level = "trace", skip(client))]
pub(crate) async fn schema(client: &Client, name: &TableName) -> Result<BqTable> {
    trace!("fetching schema for {:?}", name);

    // Build our URL.
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}",
        percent_encode(name.project()),
        percent_encode(name.dataset()),
        percent_encode(name.table()),
    );

    // Look up our schema.
    let table = client.get::<Table, _, _>(&url, NoQuery).await?;
    Ok(BqTable {
        name: name.to_owned(),
        columns: table.schema.fields,
    })
}
