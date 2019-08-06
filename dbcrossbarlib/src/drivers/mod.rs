//! Drivers for various schema sources and destinations.
//!
//! These APIs are all unstable and not yet standardized.

use lazy_static::lazy_static;
use std::collections::HashMap;

use crate::common::*;
use crate::locator::{LocatorDriver, LocatorDriverWrapper};

pub mod bigquery;
pub mod bigquery_schema;
pub mod bigquery_shared;
pub mod csv;
pub mod dbcrossbar_schema;
pub mod gs;
pub mod postgres;
pub mod postgres_shared;
pub mod postgres_sql;
pub mod redshift;
pub mod s3;

lazy_static! {
    /// A list of known drivers, computed the first time we use it and cached.
    static ref KNOWN_DRIVERS: Vec<Box<dyn LocatorDriver>> = vec![
        Box::new(LocatorDriverWrapper::<bigquery::BigQueryLocator>::new()),
        Box::new(LocatorDriverWrapper::<bigquery_schema::BigQuerySchemaLocator>::new()),
        Box::new(LocatorDriverWrapper::<csv::CsvLocator>::new()),
        Box::new(LocatorDriverWrapper::<dbcrossbar_schema::DbcrossbarSchemaLocator>::new()),
        Box::new(LocatorDriverWrapper::<gs::GsLocator>::new()),
        Box::new(LocatorDriverWrapper::<postgres::PostgresLocator>::new()),
        Box::new(LocatorDriverWrapper::<postgres_sql::PostgresSqlLocator>::new()),
        Box::new(LocatorDriverWrapper::<redshift::RedshiftLocator>::new()),
        Box::new(LocatorDriverWrapper::<s3::S3Locator>::new()),
    ];

    /// A hash table of all known drivers, indexed by scheme and computed the
    /// first time we use it.
    static ref KNOWN_DRIVERS_BY_SCHEME: HashMap<&'static str, &'static dyn LocatorDriver> = {
        let mut table = HashMap::new();
        for driver in KNOWN_DRIVERS.iter() {
            table.insert(driver.scheme(), driver.as_ref());
        }
        table
    };
}

/// All known drivers.
pub fn all_drivers() -> &'static [Box<dyn LocatorDriver>] {
    &KNOWN_DRIVERS[..]
}

/// Look up a specifc driver by `Locator` scheme.
pub fn find_driver(scheme: &str) -> Result<&'static dyn LocatorDriver> {
    KNOWN_DRIVERS_BY_SCHEME
        .get(scheme)
        .copied()
        .ok_or_else(|| format_err!("unknown locator scheme {:?}", scheme))
}
