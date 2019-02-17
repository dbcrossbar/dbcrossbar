//! A library for reading and writing table schemas in various formats.
//!
//! At the moment, the most interesting type here is the [`schema`](./schema/)
//! module, which defines a portable SQL schema.

#![feature(await_macro, async_await, futures_api, try_blocks)]
#![warn(missing_docs, unused_extern_crates, clippy::all)]

// We keep one `macro_use` here, because `diesel`'s macros do not yet play
// nicely with the new Rust 2018 macro importing features.
#[macro_use]
extern crate diesel;

// Pull in all of `tokio`'s experimental `async` and `await` support.
#[macro_use]
extern crate tokio;

use bytes::BytesMut;
use failure::format_err;
use lazy_static::lazy_static;
use log::{debug, warn};
use regex::Regex;
use std::{fmt, fs as std_fs, io::prelude::*, result, str::FromStr};
use strum;
use strum_macros::{Display, EnumString};
use tokio::{fs as tokio_fs, prelude::*, sync::mpsc};
use tokio_process::Child;

pub mod drivers;
pub(crate) mod path_or_stdio;
pub mod schema;
pub mod tokio_glue;

use self::schema::Table;
use self::tokio_glue::{
    tokio_fut, BoxFuture, BoxStream, FutureExt, ResultExt, StdFutureExt,
};

/// Standard error type for this library.
pub use failure::Error;

/// Standard result type for this library.
pub type Result<T> = result::Result<T, Error>;

/// Context shared by our various asynchronous operations.
#[derive(Debug, Clone)]
pub struct Context {
    /// To report asynchronous errors anywhere in the application, send them to
    /// this channel.
    error_sender: mpsc::Sender<Error>,
}

impl Context {
    /// Create a new context, and a future represents our background workers,
    /// returning `()` if they all succeed, or an `Error` as soon as one of them
    /// fails.
    pub fn create() -> (Self, impl Future<Item = (), Error = Error>) {
        let (error_sender, receiver) = mpsc::channel(1);
        let context = Context { error_sender };
        let worker_future = async move {
            match await!(receiver.into_future()) {
                // An error occurred in the low-level mechanisms of our `mpsc`
                // channel.
                Err((_err, _rcvr)) => {
                    Err(format_err!("background task reporting failed"))
                }
                // All senders have shut down correctly.
                Ok((None, _rcvr)) => Ok(()),
                // We received an error from a background worker, so report that
                // as the result for all our background workers.
                Ok((Some(err), _rcvr)) => Err(err),
            }
        };
        (context, tokio_fut(worker_future))
    }

    /// Spawn an async worker in this context, and report any errors to the
    /// future returned by `create`.
    pub fn spawn_worker<W>(&self, worker: W)
    where
        W: Future<Item = (), Error = Error> + Send + 'static,
    {
        let error_sender = self.error_sender.clone();
        tokio::spawn_async(
            async move {
                if let Err(err) = await!(worker) {
                    debug!("reporting background worker error: {}", err);
                    if let Err(_err) = await!(error_sender.send(err)) {
                        debug!("broken pipe reporting background worker error");
                    }
                }
            },
        );
    }

    /// Monitor an asynchrnous child process, and report any errors or non-zero
    /// exit codes that occur.
    pub fn spawn_process(&self, name: String, child: Child) {
        let worker = async move {
            match await!(child) {
                Ok(ref status) if status.success() => Ok(()),
                Ok(status) => Err(format_err!("{} failed with {}", name, status)),
                Err(err) => Err(format_err!("{} failed with error: {}", name, err)),
            }
        };
        self.spawn_worker(tokio_fut(worker));
    }
}

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
    /// Convert to an `tokio::OpenOptions` value, returning an error for
    /// `IfExists::Append`.
    pub(crate) fn to_async_open_options_no_append(
        self,
    ) -> Result<tokio_fs::OpenOptions> {
        let mut open_options = tokio_fs::OpenOptions::new();
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

    /// Convert to an `std::fs::OpenOptions` value, returning an error for
    /// `IfExists::Append`.
    pub(crate) fn to_sync_open_options_no_append(self) -> Result<std_fs::OpenOptions> {
        let mut open_options = std_fs::OpenOptions::new();
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
pub trait Locator: fmt::Debug + fmt::Display + Send + Sync + 'static {
    /// Return a table schema, if available.
    fn schema(&self) -> Result<Option<Table>> {
        Ok(None)
    }

    /// Write a table schema to this locator, if that's the sort of thing that
    /// we can do.
    fn write_schema(&self, _schema: &Table, _if_exists: IfExists) -> Result<()> {
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

    /// If this locator can be used as a local data sink, return the local data
    /// sink.
    fn write_local_data(
        &self,
        _ctx: Context,
        _schema: Table,
        _data: BoxStream<CsvStream>,
        _if_exists: IfExists,
    ) -> BoxFuture<()> {
        Err(format_err!("cannot write data to {}", self)).into_boxed_future()
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
    pub data: Box<dyn Stream<Item = BytesMut, Error = Error> + Send + 'static>,
}
