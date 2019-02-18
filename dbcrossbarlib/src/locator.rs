//! Specify the location of data or a schema.

use lazy_static::lazy_static;
use regex::Regex;
use std::{fmt, str::FromStr};

use crate::common::*;

/// Specify the the location of data or a schema.
pub trait Locator: fmt::Debug + fmt::Display + Send + Sync + 'static {
    /// Return a table schema, if available.
    fn schema(&self, _ctx: &Context) -> Result<Option<Table>> {
        Ok(None)
    }

    /// Write a table schema to this locator, if that's the sort of thing that
    /// we can do.
    fn write_schema(
        &self,
        _ctx: &Context,
        _schema: &Table,
        _if_exists: IfExists,
    ) -> Result<()> {
        Err(format_err!("cannot write schema to {}", self))
    }

    /// If this locator can be used as a local data source, return a stream of
    /// CSV streams. This function type is bit hairy:
    ///
    /// 1. The outermost `BoxFuture` is essentially an async `Result`, returning
    ///    either a value or an error. It's boxed because we don't know what
    ///    concrete type it will actually be, just that it will implement
    ///    `Future`.
    /// 2. The `Option` will be `None` if we have no local data, or `Some` if we
    ///    can provide one or more CSV streams.
    /// 3. The `BoxStream` returns a "stream of streams". This _could_ be a
    ///    `Vec<CsvStream>`, but that would force us to, say, open up hundreds
    ///    of CSV files or S3 objects at once, causing us to run out of file
    ///    descriptors. By returning a stream, we allow our caller to open up
    ///    files or start downloads only when needed.
    /// 4. The innermost `CsvStream` is a stream of raw CSV data plus some other
    ///    information, like the original filename.
    fn local_data(&self, _ctx: Context) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        // Turn our result into a future.
        Ok(None).into_boxed_future()
    }

    /// If this locator can be used as a local data sink, write data to it.
    ///
    /// This function takes a stream `data` as input, the elements of which are
    /// individual `CsvStream` values. An implementation should normally use
    /// `map` or `and_then` to write those CSV streams to storage associated
    /// with the locator, and return a stream of `BoxFuture<()>` values:
    ///
    /// ```no_compile
    /// # Pseudo code for parallel output.
    /// data.map(async |csv_stream| {
    ///     await!(write(csv_stream))?;
    ///     Ok(())
    /// })
    /// ```
    ///
    /// For cases where output must be serialized, it's OK to consume the entire
    /// `data` stream, and return a single-item stream containing `()`.
    ///
    /// The caller of `write_local_data` will pull several items at a time from
    /// the returned `BoxStream<BoxFuture<()>>` and evaluate them in parallel.
    fn write_local_data(
        &self,
        _ctx: Context,
        _schema: Table,
        _data: BoxStream<CsvStream>,
        _if_exists: IfExists,
    ) -> BoxFuture<BoxStream<BoxFuture<()>>> {
        Err(format_err!("cannot write data to {}", self)).into_boxed_future()
    }
}

/// A value of an unknown type implementing `Locator`.
pub type BoxLocator = Box<dyn Locator>;

impl FromStr for BoxLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        use crate::drivers::{bigquery::*, csv::*, gs::*, postgres::*};

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
