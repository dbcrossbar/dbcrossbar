//! A driver for working with Postgres.

// See https://github.com/diesel-rs/diesel/issues/1785
#![allow(missing_docs, proc_macro_derive_resolution_fallback)]

use failure::{format_err, ResultExt};
use std::{fs::File, fmt, io::Read, path::PathBuf, str::FromStr};
use url::Url;

use crate::{Error, Locator, Result};
use crate::path_or_stdio::PathOrStdio;
use crate::schema::Table;

pub mod citus;
mod parser;
mod schema;

/// URL scheme for `PostgresLocator`.
pub(crate) const POSTGRES_SCHEME: &str = "postgres:";

/// A Postgres database URL and a table name.
///
/// This is the central point of access for talking to a running PostgreSQL
/// database.
pub struct PostgresLocator {
    url: Url,
}

impl fmt::Display for PostgresLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(f)
    }
}

impl FromStr for PostgresLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let url: Url = s.parse::<Url>().context("cannot parse Postgres URL")?;
        if url.scheme() != &POSTGRES_SCHEME[..POSTGRES_SCHEME.len()-1] {
            Err(format_err!("expected URL scheme postgres: {:?}", s))
        } else {
            Ok(PostgresLocator { url })
        }
    }
}

impl Locator for PostgresLocator {
    fn schema(&self) -> Result<Option<Table>> {
        let mut url: Url = self.url.clone();
        let table_name = url.fragment().ok_or_else(|| {
            format_err!("{} needs to be followed by #table_name", self.url)
        })?.to_owned();
        url.set_fragment(None);
        Ok(Some(schema::PostgresDriver::fetch_from_url(&url, &table_name)?))
    }
}

/// URL scheme for `PostgresSqlLocator`.
pub(crate) const POSTGRES_SQL_SCHEME: &str = "postgres.sql:";

/// An SQL file containing a `CREATE TABLE` statement using Postgres syntax.
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
            input.read_to_string(&mut sql).with_context(|_| {
                format!("error reading {}", self.path)
            })?;
            Ok(Some(parser::parse_create_table(&sql)?))
        })
    }
}
