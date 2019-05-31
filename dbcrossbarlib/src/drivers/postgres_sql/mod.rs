//! Schema-only driver for reading and writing PostgreSQL `CREATE TABLE` schema.

use std::{
    fmt,
    str::{self, FromStr},
};

use crate::common::*;
use crate::drivers::postgres_shared::PgCreateTable;

/// URL scheme for `PostgresSqlLocator`.
pub(crate) const POSTGRES_SQL_SCHEME: &str = "postgres-sql:";

/// An SQL file containing a `CREATE TABLE` statement using Postgres syntax.
#[derive(Debug)]
pub struct PostgresSqlLocator {
    path: PathOrStdio,
}

impl fmt::Display for PostgresSqlLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(POSTGRES_SQL_SCHEME, f)
    }
}

impl FromStr for PostgresSqlLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(POSTGRES_SQL_SCHEME, s)?;
        Ok(PostgresSqlLocator { path })
    }
}

impl Locator for PostgresSqlLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self, _ctx: &Context) -> Result<Option<Table>> {
        let mut input = self.path.open_sync()?;
        let mut sql = String::new();
        input
            .read_to_string(&mut sql)
            .with_context(|_| format!("error reading {}", self.path))?;
        let pg_create_table: PgCreateTable = sql.parse()?;
        let table = pg_create_table.to_table()?;
        Ok(Some(table))
    }

    fn write_schema(
        &self,
        ctx: &Context,
        table: &Table,
        if_exists: IfExists,
    ) -> Result<()> {
        // TODO: We use the existing `table.name` here, but this might produce
        // odd results if the input table comes from BigQuery or another
        // database with a very different naming scheme.
        let pg_create_table =
            PgCreateTable::from_name_and_columns(table.name.clone(), &table.columns)?;
        let mut out = self.path.create_sync(ctx, &if_exists)?;
        write!(out, "{}", pg_create_table)
            .with_context(|_| format!("error writing {}", self.path))?;
        Ok(())
    }
}
