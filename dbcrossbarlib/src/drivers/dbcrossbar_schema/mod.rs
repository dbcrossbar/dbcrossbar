//! Support for `dbcrossbar-schema` locators.

use std::{fmt, str::FromStr};

use crate::common::*;

/// A JSON file containing BigQuery table schema.
#[derive(Clone, Debug)]
pub struct DbcrossbarSchemaLocator {
    path: PathOrStdio,
}

impl fmt::Display for DbcrossbarSchemaLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(Self::scheme(), f)
    }
}

impl FromStr for DbcrossbarSchemaLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(Self::scheme(), s)?;
        Ok(DbcrossbarSchemaLocator { path })
    }
}

impl Locator for DbcrossbarSchemaLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self, ctx: Context) -> BoxFuture<Option<Table>> {
        schema_helper(ctx, self.to_owned()).boxed()
    }

    fn write_schema(
        &self,
        ctx: Context,
        table: Table,
        if_exists: IfExists,
    ) -> BoxFuture<()> {
        write_schema_helper(ctx, self.to_owned(), table, if_exists).boxed()
    }
}

impl LocatorStatic for DbcrossbarSchemaLocator {
    fn scheme() -> &'static str {
        "dbcrossbar-schema:"
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
async fn schema_helper(
    _ctx: Context,
    source: DbcrossbarSchemaLocator,
) -> Result<Option<Table>> {
    // Read our input.
    let input = source.path.open_async().await?;
    let data = async_read_to_end(input)
        .await
        .with_context(|_| format!("error reading {}", source.path))?;

    // Parse our input as table JSON.
    let table: Table = serde_json::from_slice(&data)
        .with_context(|_| format!("error parsing {}", source.path))?;
    Ok(Some(table))
}

/// Implementation of `write_schema`, but as a real `async` function.
async fn write_schema_helper(
    ctx: Context,
    dest: DbcrossbarSchemaLocator,
    table: Table,
    if_exists: IfExists,
) -> Result<()> {
    // Generate our JSON.
    let f = dest.path.create_async(ctx, if_exists).await?;
    buffer_sync_write_and_copy_to_async(f, |buff| {
        serde_json::to_writer_pretty(buff, &table)
    })
    .await
    .with_context(|_| format!("error writing to {}", dest.path))?;
    Ok(())
}
