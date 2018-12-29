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
pub mod schema;

use self::data::{LocalSink, LocalSource};
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

    /// If this locator can be used as a local data source, return the local
    /// data source.
    fn local_source(&self) -> Result<Option<Box<dyn LocalSource>>> {
        Ok(None)
    }

    /// If this locator can be used as a local data sink, return the local data
    /// sink.
    fn local_sink(&self) -> Result<Option<Box<dyn LocalSink>>> {
        Ok(None)
    }
}

/// A value of an unknown type implementing `Locator`.
pub type BoxLocator = Box<dyn Locator>;

impl FromStr for BoxLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        // Parse our locator into a URL-style scheme and the rest.
        lazy_static! {
            static ref LOCATOR_RE: Regex =
                Regex::new("^([A-Za-z][-A-Za-z0-0+.]*):")
                    .expect("invalid regex in source");
        }
        let cap = LOCATOR_RE.captures(s).ok_or_else(|| {
            format_err!("cannot parse locator: {:?}", s)
        })?;
        let scheme = &cap[1];

        // Select an appropriate locator type.
        match scheme {
            "postgres" => Ok(Box::new(drivers::postgres::PostgresLocator::from_str(s)?)),
            "postgres.sql" => Ok(Box::new(drivers::postgres::PostgresSqlLocator::from_str(s)?)),
            "bigquery" => Ok(Box::new(drivers::bigquery::BigQueryLocator::from_str(s)?)),
            _ => Err(format_err!("unknown locator scheme in {:?}", s))
        }
    }
}

#[test]
fn locator_from_str_to_string_roundtrip() {
    let locators = vec![
        "postgres://localhost:5432/db#my_table",
        "postgres.sql:/home/user/my_table.sql",
        "bigquery:my_project:my_dataset.my_table",
    ];
    for locator in locators.into_iter() {
        let parsed: BoxLocator = locator.parse().unwrap();
        assert_eq!(parsed.to_string(), locator);
    }
}
