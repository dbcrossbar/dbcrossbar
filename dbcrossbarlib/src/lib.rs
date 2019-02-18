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

use std::result;
use strum;

pub(crate) mod context;
pub(crate) mod csv_stream;
pub mod drivers;
pub(crate) mod if_exists;
pub(crate) mod locator;
pub(crate) mod path_or_stdio;
pub mod schema;
pub mod tokio_glue;

/// Standard error type for this library.
pub use failure::Error;

/// Standard result type for this library.
pub type Result<T> = result::Result<T, Error>;

pub use context::Context;
pub use csv_stream::CsvStream;
pub use if_exists::IfExists;
pub use locator::{BoxLocator, Locator};

/// Definitions included by all the files in this crate.
///
/// This forms the dialect of Rust we use for implementing our core and various
/// drivers, with an emphasis on `tokio` and structured logging.
#[allow(unused_imports)]
pub(crate) mod common {
    pub(crate) use bytes::BytesMut;
    pub(crate) use failure::{format_err, ResultExt};
    pub(crate) use slog::{debug, error, info, o, trace, warn, Logger};
    pub(crate) use tokio::{prelude::*, sync::mpsc};
    pub(crate) use url::Url;

    pub(crate) use crate::{
        context::Context,
        csv_stream::CsvStream,
        if_exists::IfExists,
        locator::Locator,
        path_or_stdio::PathOrStdio,
        schema::Table,
        tokio_glue::{
            box_stream_once, tokio_fut, BoxFuture, BoxStream, FutureExt,
            ResultExt as _, StdFutureExt,
        },
        Error, Result,
    };
}
