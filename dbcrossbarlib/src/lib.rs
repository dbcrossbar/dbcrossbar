//! A library for reading and writing table schemas in various formats.
//!
//! At the moment, the most interesting type here is the [`schema`](./schema/)
//! module, which defines a portable SQL schema.

#![feature(async_await)]
#![warn(missing_docs, unused_extern_crates, clippy::all)]
// Work around clippy false positives.
#![allow(clippy::redundant_closure, clippy::needless_lifetimes)]

// We keep one `macro_use` here, because `diesel`'s macros do not yet play
// nicely with the new Rust 2018 macro importing features.
#[macro_use]
extern crate diesel;

use std::result;

pub(crate) mod context;
pub(crate) mod csv_stream;
pub mod drivers;
pub(crate) mod from_csv_cell;
pub(crate) mod from_json_value;
pub(crate) mod if_exists;
pub(crate) mod locator;
pub(crate) mod path_or_stdio;
mod query;
pub mod schema;
mod temporary_storage;
pub mod tokio_glue;
pub(crate) mod transform;

/// Standard error type for this library.
pub use failure::Error;

/// Standard result type for this library.
pub type Result<T> = result::Result<T, Error>;

/// The buffer size to use by default when buffering I/O.
pub(crate) const BUFFER_SIZE: usize = 64 * 1024;

pub use context::Context;
pub use csv_stream::CsvStream;
pub use if_exists::IfExists;
pub use locator::{BoxLocator, Locator};
pub use query::Query;
pub use temporary_storage::TemporaryStorage;
pub use tokio_glue::ConsumeWithParallelism;

/// Definitions included by all the files in this crate.
///
/// This forms the dialect of Rust we use for implementing our core and various
/// drivers, with an emphasis on `tokio` and structured logging.
#[allow(unused_imports)]
pub(crate) mod common {
    pub(crate) use bytes::BytesMut;
    pub(crate) use failure::{format_err, ResultExt};
    pub(crate) use futures::{
        compat::{Compat01As03, Future01CompatExt},
        FutureExt, TryFutureExt,
    };
    pub(crate) use slog::{debug, error, info, o, trace, warn, Logger};
    pub(crate) use std::any::Any;
    pub(crate) use tokio::{prelude::*, sync::mpsc};
    pub(crate) use url::Url;

    pub(crate) use crate::{
        context::Context,
        csv_stream::CsvStream,
        if_exists::IfExists,
        locator::{BoxLocator, Locator},
        path_or_stdio::PathOrStdio,
        query::Query,
        schema::Table,
        temporary_storage::TemporaryStorage,
        tokio_glue::{box_stream_once, BoxFuture, BoxStream, ConsumeWithParallelism},
        Error, Result, BUFFER_SIZE,
    };
}
