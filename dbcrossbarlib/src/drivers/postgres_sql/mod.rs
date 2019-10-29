//! Schema-only driver for reading and writing PostgreSQL `CREATE TABLE` schema.

use std::{
    fmt,
    str::{self, FromStr},
};

use crate::common::*;
use crate::drivers::postgres_shared::PgCreateTable;

/// An SQL file containing a `CREATE TABLE` statement using Postgres syntax.
#[derive(Clone, Debug)]
pub struct PostgresSqlLocator {
    path: PathOrStdio,
}

impl fmt::Display for PostgresSqlLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(Self::scheme(), f)
    }
}

impl FromStr for PostgresSqlLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(Self::scheme(), s)?;
        Ok(PostgresSqlLocator { path })
    }
}

impl Locator for PostgresSqlLocator {
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

impl LocatorStatic for PostgresSqlLocator {
    fn scheme() -> &'static str {
        "postgres-sql:"
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
    source: PostgresSqlLocator,
) -> Result<Option<Table>> {
    let input = source
        .path
        .open_async()
        .await
        .with_context(|_| format!("error opening {}", source.path))?;
    let sql = async_read_to_string(input)
        .await
        .with_context(|_| format!("error reading {}", source.path))?;
    let pg_create_table: PgCreateTable = sql
        .parse::<PgCreateTable>()
        .with_context(|_| format!("error parsing {}", source.path))?;
    let table = pg_create_table.to_table()?;
    Ok(Some(table))
}

/// Implementation of `write_schema`, but as a real `async` function.
async fn write_schema_helper(
    ctx: Context,
    dest: PostgresSqlLocator,
    table: Table,
    if_exists: IfExists,
) -> Result<()> {
    // TODO: We use the existing `table.name` here, but this might produce
    // odd results if the input table comes from BigQuery or another
    // database with a very different naming scheme.
    let pg_create_table =
        PgCreateTable::from_name_and_columns(table.name.clone(), &table.columns)?;
    let out = dest.path.create_async(ctx, if_exists).await?;
    buffer_sync_write_and_copy_to_async(out, |buff| {
        write!(buff, "{}", pg_create_table)
    })
    .await
    .with_context(|_| format!("error writing {}", dest.path))?;
    Ok(())
}
