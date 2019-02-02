//! A library for reading and writing table schemas in various formats.
//!
//! At the moment, the most interesting type here is the [`schema`](./schema/)
//! module, which defines a portable SQL schema.

#![feature(await_macro, async_await, futures_api)]
#![warn(missing_docs, unused_extern_crates, clippy::all)]

// We keep one `macro_use` here, because `diesel`'s macros do not yet play
// nicely with the new Rust 2018 macro importing features.
#[macro_use]
extern crate diesel;

// Pull in all of `tokio`'s experimental `async` and `await` support.
#[allow(unused_imports)]
#[macro_use]
extern crate tokio;

use failure::format_err;
use lazy_static::lazy_static;
use log::warn;
use regex::Regex;
use std::{fmt, fs::OpenOptions, io::prelude::*, result, str::FromStr};
use strum;
use strum_macros::{Display, EnumString};

pub mod drivers;
pub(crate) mod path_or_stdio;
pub mod schema;

use self::schema::Table;

/// Standard error type for this library.
pub use failure::Error;

/// Standard result type for this library.
pub type Result<T> = result::Result<T, Error>;

/// What to do if the destination already exists.
#[derive(Clone, Copy, Debug, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum IfExists {
    /// If the destination exists, return an error.
    Error,
    /// If the destination exists, try to append the new data.
    Append,
    /// If the destination exists, overrwrite the existing data.
    Overwrite,
}

impl IfExists {
    /// Convert to an `OpenOptions` value, returning an error for
    /// `IfExists::Append`.
    pub(crate) fn to_open_options_no_append(self) -> Result<OpenOptions> {
        let mut open_options = OpenOptions::new();
        open_options.write(true);
        match self {
            IfExists::Error => {
                open_options.create_new(true);
            }
            IfExists::Overwrite => {
                open_options.create(true).append(true);
            }
            IfExists::Append => {
                return Err(format_err!("appending not supported"));
            }
        }
        Ok(open_options)
    }

    pub(crate) fn warn_if_not_default_for_stdout(self) {
        if self != IfExists::default() {
            warn!("{} ignored for stdout", self)
        }
    }
}

impl Default for IfExists {
    fn default() -> Self {
        IfExists::Error
    }
}

/// Specify the the location of data or a schema.
pub trait Locator: fmt::Debug + fmt::Display {
    /// Return a table schema, if available.
    fn schema(&self) -> Result<Option<Table>> {
        Ok(None)
    }

    /// Write a table schema to this locator, if that's the sort of thing that
    /// we can do.
    fn write_schema(&self, _schema: &Table, _if_exists: IfExists) -> Result<()> {
        Err(format_err!("cannot write schema to {}", self))
    }

    /// If this locator can be used as a local data source, return the local
    /// data source.
    fn local_data(&self) -> Result<Option<Vec<CsvStream>>> {
        Ok(None)
    }

    /// If this locator can be used as a local data sink, return the local data
    /// sink.
    fn write_local_data(
        &self,
        _schema: &Table,
        _data: Vec<CsvStream>,
        _if_exists: IfExists,
    ) -> Result<()> {
        Err(format_err!("cannot write data to {}", self))
    }
}

/// A value of an unknown type implementing `Locator`.
pub type BoxLocator = Box<dyn Locator>;

impl FromStr for BoxLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        use self::drivers::{bigquery::*, csv::*, gs::*, postgres::*};

        // Parse our locator into a URL-style scheme and the rest.
        lazy_static! {
            static ref SCHEME_RE: Regex = Regex::new("^[A-Za-z][-A-Za-z0-0+.]*:")
                .expect("invalid regex in source");
        }
        let cap = SCHEME_RE
            .captures(s)
            .ok_or_else(|| format_err!("cannot parse locator: {:?}", s))?;
        let scheme = &cap[0];

        // Select an appropriate locator type.
        match scheme {
            BIGQUERY_SCHEME => Ok(Box::new(BigQueryLocator::from_str(s)?)),
            BIGQUERY_SCHEMA_SCHEME => {
                Ok(Box::new(BigQuerySchemaLocator::from_str(s)?))
            }
            CSV_SCHEME => Ok(Box::new(CsvLocator::from_str(s)?)),
            GS_SCHEME => Ok(Box::new(GsLocator::from_str(s)?)),
            POSTGRES_SCHEME => Ok(Box::new(PostgresLocator::from_str(s)?)),
            POSTGRES_SQL_SCHEME => Ok(Box::new(PostgresSqlLocator::from_str(s)?)),
            _ => Err(format_err!("unknown locator scheme in {:?}", s)),
        }
    }
}

#[test]
fn locator_from_str_to_string_roundtrip() {
    let locators = vec![
        "bigquery:my_project:my_dataset.my_table",
        "bigquery-schema:dir/my_table.json",
        "csv:file.csv",
        "csv:dir/",
        "gs://example-bucket/tmp/",
        "postgres://localhost:5432/db#my_table",
        "postgres-sql:dir/my_table.sql",
    ];
    for locator in locators.into_iter() {
        let parsed: BoxLocator = locator.parse().unwrap();
        assert_eq!(parsed.to_string(), locator);
    }
}

/// A stream of CSV data, with a unique name.
pub struct CsvStream {
    /// The name of this stream.
    pub name: String,
    /// A reader associated with this stream.
    pub data: Box<dyn Read + Send + 'static>,
}
