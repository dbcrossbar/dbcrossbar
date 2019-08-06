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

    fn schema(&self, _ctx: &Context) -> Result<Option<Table>> {
        // Read our input.
        let mut input = self.path.open_sync()?;
        let mut data = String::new();
        input
            .read_to_string(&mut data)
            .with_context(|_| format!("error reading {}", self.path))?;

        // Parse our input as table JSON.
        let table: Table = serde_json::from_str(&data)
            .with_context(|_| format!("error parsing {}", self.path))?;
        Ok(Some(table))
    }

    fn write_schema(
        &self,
        ctx: &Context,
        table: &Table,
        if_exists: IfExists,
    ) -> Result<()> {
        // Generate our JSON.
        let mut f = self.path.create_sync(ctx, &if_exists)?;
        serde_json::to_writer_pretty(&mut f, table)
            .with_context(|_| format!("error writing {}", self.path))?;
        Ok(())
    }
}

impl LocatorStatic for DbcrossbarSchemaLocator {
    fn scheme() -> &'static str {
        "dbcrossbar-schema:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::SCHEMA | LocatorFeatures::WRITE_SCHEMA,
            write_schema_if_exists: IfExistsFeatures::no_append(),
            source_args: SourceArgumentsFeatures::empty(),
            dest_args: DestinationArgumentsFeatures::empty(),
            dest_if_exists: IfExistsFeatures::empty(),
            _placeholder: (),
        }
    }
}
