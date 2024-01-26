//! Support for `bigquery-schema` locators.

use std::{fmt, str::FromStr};

use crate::common::*;
use crate::drivers::bigquery_shared::{BqColumn, BqTable, TableName, Usage};

/// A JSON file containing BigQuery table schema.
#[derive(Clone, Debug)]
pub struct BigQuerySchemaLocator {
    path: PathOrStdio,
}

impl fmt::Display for BigQuerySchemaLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(Self::scheme(), f)
    }
}

impl FromStr for BigQuerySchemaLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(Self::scheme(), s)?;
        Ok(BigQuerySchemaLocator { path })
    }
}

impl Locator for BigQuerySchemaLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_scheme(&self) -> &'static str {
        <Self as LocatorStatic>::scheme()
    }

    fn schema(
        &self,
        _ctx: Context,
        _source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<Schema>> {
        schema_helper(self.to_owned()).boxed()
    }

    fn write_schema(
        &self,
        _ctx: Context,
        schema: Schema,
        if_exists: IfExists,
        _source_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<()> {
        write_schema_helper(self.to_owned(), schema, if_exists).boxed()
    }
}

impl LocatorStatic for BigQuerySchemaLocator {
    fn scheme() -> &'static str {
        "bigquery-schema:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::Schema | LocatorFeatures::WriteSchema,
            write_schema_if_exists: IfExistsFeatures::no_append(),
            source_args: EnumSet::empty(),
            dest_args: EnumSet::empty(),
            dest_if_exists: EnumSet::empty(),
            _placeholder: (),
        }
    }
}

/// Implementation of `schema`, but as a real `async` function.
#[instrument(level = "trace", name = "bigquery_schema::schema")]
async fn schema_helper(source: BigQuerySchemaLocator) -> Result<Option<Schema>> {
    // Read our input.
    let input = source.path.open_async().await?;
    let data = async_read_to_end(input)
        .await
        .with_context(|| format!("error reading {}", source.path))?;

    // Parse our input as a list of columns.
    let columns: Vec<BqColumn> = serde_json::from_slice(&data)
        .with_context(|| format!("error parsing {}", source.path))?;

    // Build a `BqTable`, convert it, and set a placeholder name.
    let arbitrary_name = TableName::from_str("unused:unused.unused")?;
    let bq_table = BqTable {
        name: arbitrary_name,
        columns,
    };
    let mut table = bq_table.to_table()?;
    table.name = "unnamed".to_owned();
    Ok(Some(Schema::from_table(table)?))
}

/// Implementation of `write_schema`, but as a real `async` function.
#[instrument(level = "trace", name = "bigquery_schema::write_schema")]
async fn write_schema_helper(
    dest: BigQuerySchemaLocator,
    schema: Schema,
    if_exists: IfExists,
) -> Result<()> {
    // The BigQuery table name doesn't matter here, because our BigQuery schema
    // won't use it. We could convert `table.name` into a valid BigQuery table
    // name, but because BigQuery table names obey fairly strict restrictions,
    // it's not worth doing the work if we're just going throw it away.
    let arbitrary_name = TableName::from_str("unused:unused.unused")?;

    // Convert our schema to a BigQuery table.
    let bq_table = BqTable::for_table_name_and_columns(
        &schema,
        arbitrary_name,
        &schema.table.columns,
        Usage::FinalTable,
    )?;

    // Output our schema to our destination.
    let mut f = dest.path.create_async(if_exists).await?;
    buffer_sync_write_and_copy_to_async(&mut f, |buff| {
        bq_table.write_json_schema(buff)
    })
    .await
    .with_context(|| format!("error writing to {}", dest.path))?;
    f.flush().await?;
    Ok(())
}
