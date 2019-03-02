//! Schema-only driver for reading and writing PostgreSQL `CREATE TABLE` schema.

use std::{
    fmt,
    str::{self, FromStr},
};

use crate::common::*;
use crate::drivers::postgres_shared::{parse_create_table, write_create_table};

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
        Ok(Some(parse_create_table(&sql)?))
    }

    fn write_schema(
        &self,
        ctx: &Context,
        table: &Table,
        if_exists: IfExists,
    ) -> Result<()> {
        let mut out = self.path.create_sync(ctx, if_exists)?;
        // The passed-in `if_exists` applies to our output SQL file, not to whether
        // our generated schema contains `DROP TABLE ... IF EXISTS`.
        write_create_table(&mut out, table, IfExists::Error)
            .with_context(|_| format!("error writing {}", self.path))?;
        Ok(())
    }
}
