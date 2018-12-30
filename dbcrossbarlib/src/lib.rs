//! A library for reading and writing table schemas in various formats.
//!
//! At the moment, the most interesting type here is the [`schema`](./schema/)
//! module, which defines a portable SQL schema.

#![warn(missing_docs, unused_extern_crates, clippy::pendantic)]

// We keep one `macro_use` here, because `diesel`'s macros do not yet play
// nicely with the new Rust 2018 macro importing features.
#[macro_use]
extern crate diesel;

use failure::format_err;
use lazy_static::lazy_static;
use regex::Regex;
use std::{fmt, result, str::FromStr};

pub mod data;
pub mod drivers;
pub(crate) mod path_or_stdio;
pub mod schema;

use self::data::CsvStream;
use self::schema::Table;

/// Standard error type for this library.
pub use failure::Error;

/// Standard result type for this library.
pub type Result<T> = result::Result<T, Error>;

/// Specify the the location of data or a schema.
pub trait Locator: fmt::Display {
    /// Return a table schema, if available.
    fn schema(&self) -> Result<Option<Table>> {
        Ok(None)
    }

    /// Write a table schema to this locator, if that's the sort of thing that
    /// we can do.
    fn write_schema(&self, _schema: &Table) -> Result<()> {
        Err(format_err!("cannot write schema to {}", self))
    }

    /// If this locator can be used as a local data source, return the local
    /// data source.
    fn local_data(&self) -> Result<Option<Vec<CsvStream>>> {
        Ok(None)
    }

    /// If this locator can be used as a local data sink, return the local data
    /// sink.
    fn write_local_data(&self, _schema: &Table, _data: &[CsvStream]) -> Result<()> {
        Err(format_err!("cannot write data to {}", self))
    }
}

/// A value of an unknown type implementing `Locator`.
pub type BoxLocator = Box<dyn Locator>;

impl FromStr for BoxLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        use self::drivers::bigquery::*;
        use self::drivers::postgres::*;

        // Parse our locator into a URL-style scheme and the rest.
        lazy_static! {
            static ref SCHEME_RE: Regex =
                Regex::new("^[A-Za-z][-A-Za-z0-0+.]*:")
                    .expect("invalid regex in source");
        }
        let cap = SCHEME_RE.captures(s).ok_or_else(|| {
            format_err!("cannot parse locator: {:?}", s)
        })?;
        let scheme = &cap[0];

        // Select an appropriate locator type.
        match scheme {
            BIGQUERY_SCHEME => Ok(Box::new(BigQueryLocator::from_str(s)?)),
            BIGQUERY_JSON_SCHEME => Ok(Box::new(BigQueryJsonLocator::from_str(s)?)),
            POSTGRES_SCHEME => Ok(Box::new(PostgresLocator::from_str(s)?)),
            POSTGRES_SQL_SCHEME => Ok(Box::new(PostgresSqlLocator::from_str(s)?)),
            _ => Err(format_err!("unknown locator scheme in {:?}", s))
        }
    }
}

#[test]
fn locator_from_str_to_string_roundtrip() {
    let locators = vec![
        "postgres://localhost:5432/db#my_table",
        "postgres.sql:dir/my_table.sql",
        "bigquery:my_project:my_dataset.my_table",
        "bigquery.json:dir/my_table.json",
    ];
    for locator in locators.into_iter() {
        let parsed: BoxLocator = locator.parse().unwrap();
        assert_eq!(parsed.to_string(), locator);
    }
}
