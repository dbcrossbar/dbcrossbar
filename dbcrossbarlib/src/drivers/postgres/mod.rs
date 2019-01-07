//! A driver for working with Postgres.

// See https://github.com/diesel-rs/diesel/issues/1785
#![allow(missing_docs, proc_macro_derive_resolution_fallback)]

use failure::{format_err, ResultExt};
use postgres::{tls::native_tls::NativeTls, Connection, TlsMode};
use std::{
    fmt,
    str::{self, FromStr},
};
use url::Url;

use crate::path_or_stdio::PathOrStdio;
use crate::schema::Table;
use crate::{CsvStream, Error, IfExists, Locator, Result};

pub mod citus;
mod local_data;
mod schema;
mod sql_schema;
mod write_local_data;

/// Connect to the database, using SSL if possible. If `?ssl=true` is set in the
/// URL, require SSL.
fn connect(url: &Url) -> Result<Connection> {
    // Should we enable SSL?
    let negotiator = NativeTls::new()?;
    let mut tls_mode = TlsMode::Prefer(&negotiator);
    for (key, value) in url.query_pairs() {
        if key == "ssl" && value == "true" {
            tls_mode = TlsMode::Require(&negotiator);
        }
    }
    Ok(Connection::connect(url.as_str(), tls_mode)?)
}

/// URL scheme for `PostgresLocator`.
pub(crate) const POSTGRES_SCHEME: &str = "postgres:";

/// A Postgres database URL and a table name.
///
/// This is the central point of access for talking to a running PostgreSQL
/// database.
#[derive(Debug)]
pub struct PostgresLocator {
    url: Url,
    table_name: String,
}

impl fmt::Display for PostgresLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Merge our table name back into our URL.
        let mut full_url = self.url.clone();
        full_url.set_fragment(Some(&self.table_name));
        full_url.fmt(f)
    }
}

impl FromStr for PostgresLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut url: Url = s.parse::<Url>().context("cannot parse Postgres URL")?;
        if url.scheme() != &POSTGRES_SCHEME[..POSTGRES_SCHEME.len() - 1] {
            Err(format_err!("expected URL scheme postgres: {:?}", s))
        } else {
            // Extract table name from URL.
            let table_name = url
                .fragment()
                .ok_or_else(|| {
                    format_err!("{} needs to be followed by #table_name", url)
                })?
                .to_owned();
            url.set_fragment(None);
            Ok(PostgresLocator { url, table_name })
        }
    }
}

impl Locator for PostgresLocator {
    fn schema(&self) -> Result<Option<Table>> {
        Ok(Some(schema::fetch_from_url(&self.url, &self.table_name)?))
    }

    fn local_data(&self) -> Result<Option<Vec<CsvStream>>> {
        let schema = self.schema()?.expect("should always have schema");
        let stream = local_data::copy_out_table(&self.url, &schema)?;
        Ok(Some(vec![stream]))
    }

    fn write_local_data(
        &self,
        schema: &Table,
        data: Vec<CsvStream>,
        if_exists: IfExists,
    ) -> Result<()> {
        // Use the destination table name instead of the source name.
        let mut new_schema = schema.to_owned();
        new_schema.name = self.table_name.clone();
        write_local_data::copy_in_table(&self.url, schema, data, if_exists)
    }
}

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
    fn schema(&self) -> Result<Option<Table>> {
        self.path.open(|input| {
            let mut sql = String::new();
            input
                .read_to_string(&mut sql)
                .with_context(|_| format!("error reading {}", self.path))?;
            Ok(Some(sql_schema::parse_create_table(&sql)?))
        })
    }

    fn write_schema(&self, table: &Table, if_exists: IfExists) -> Result<()> {
        self.path.create(if_exists, |mut out| {
            // The passed-in `if_exists` applies to our output file, not to our
            // generated schema.
            sql_schema::write_create_table(&mut out, table, IfExists::Error)
                .with_context(|_| format!("error writing {}", self.path))?;
            Ok(())
        })
    }
}
