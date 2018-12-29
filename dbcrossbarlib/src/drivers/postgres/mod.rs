//! A driver for working with Postgres.

// See https://github.com/diesel-rs/diesel/issues/1785
#![allow(missing_docs, proc_macro_derive_resolution_fallback)]

use failure::{format_err, ResultExt};
use lazy_static::lazy_static;
use regex::Regex;
use std::{fmt, path::{Path, PathBuf}, str::FromStr};
use url::Url;

use crate::{Error, Locator, Result};

mod citus;
mod parser;
mod schema;

pub use self::citus::*;
pub use self::parser::*;
pub use self::schema::*;

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
        if url.scheme() != "postgres" {
            Err(format_err!("expected URL scheme postgres: {:?}", s))
        } else {
            Ok(PostgresLocator { url })
        }
    }
}

impl Locator for PostgresLocator {

}

/// An SQL file containing a `CREATE TABLE` statement using Postgres syntax.
pub struct PostgresSqlLocator {
    path: PathBuf,
}

impl fmt::Display for PostgresSqlLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "postgres.sql:{}", self.path.display())
    }
}

impl FromStr for PostgresSqlLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        lazy_static! {
            static ref RE: Regex = Regex::new("^postgres.sql:(.+)$")
                .expect("could not parse built-in regex");
        }
        let cap = RE
            .captures(s)
            .ok_or_else(|| format_err!("could not parse locator: {:?}", s))?;
        let path_str = &cap[1];
        let path = Path::new(path_str).to_owned();
        Ok(PostgresSqlLocator { path })
    }
}

impl Locator for PostgresSqlLocator {

}
