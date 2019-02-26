//! Support for `bigquery-schema` locators.

use std::{fmt, str::FromStr};

use crate::common::*;
use crate::drivers::bigquery_shared::{BqTable, TableName, Usage};

/// URL scheme for `PostgresSqlLocator`.
pub(crate) const BIGQUERY_SCHEMA_SCHEME: &str = "bigquery-schema:";

/// A JSON file containing BigQuery table schema.
#[derive(Debug)]
pub struct BigQuerySchemaLocator {
    path: PathOrStdio,
}

impl fmt::Display for BigQuerySchemaLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(BIGQUERY_SCHEMA_SCHEME, f)
    }
}

impl FromStr for BigQuerySchemaLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(BIGQUERY_SCHEMA_SCHEME, s)?;
        Ok(BigQuerySchemaLocator { path })
    }
}

impl Locator for BigQuerySchemaLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn write_schema(
        &self,
        ctx: &Context,
        table: &Table,
        if_exists: IfExists,
    ) -> Result<()> {
        // The BigQuery table name doesn't matter here, because BigQuery won't
        // use it.
        let arbitrary_name = TableName::from_str(&"unused:unused.unused")?;

        // Generate our JSON.
        let mut f = self.path.create_sync(ctx, if_exists)?;
        let bq_table = BqTable::for_table_name_and_columns(
            arbitrary_name,
            &table.columns,
            Usage::FinalTable,
        )?;
        bq_table.write_json_schema(&mut f)
    }
}
