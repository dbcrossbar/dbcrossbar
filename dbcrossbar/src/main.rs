//! A CLI tool for converting between table schema formats.

#![forbid(unsafe_code)]
#![warn(
    missing_docs,
    unused_extern_crates,
    clippy::all,
    clippy::cargo,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::inefficient_to_string
)]
// We handle this using `cargo deny` instead.
#![allow(clippy::multiple_crate_versions)]
// This has false positives on trivial code.
#![allow(clippy::needless_collect)]
// Honestly the `..Default::default()` notation is more verbose, it requires
// more Rust knowledge to read, and it breaks as soon as there are private
// fields. I don't think this warning is worthwhile.
#![allow(clippy::field_reassign_with_default)]
// Allow functions that can't fail to return `Result`. These could be simplified
// if we wanted.
#![allow(clippy::unnecessary_wraps)]

use std::env;

use anyhow::{Error, Result};
use clap::Parser;
use futures::try_join;
use opinionated_telemetry::TelemetryConfig;
use tracing::debug;

use self::config::Configuration;

pub(crate) mod args;
pub(crate) mod clouds;
mod cmd;
pub(crate) mod concat;
pub mod config;
pub(crate) mod context;
pub(crate) mod credentials;
pub(crate) mod csv_stream;
pub(crate) mod data_streams;
mod driver_args;
pub mod drivers;
pub(crate) mod from_csv_cell;
pub(crate) mod from_json_value;
pub(crate) mod if_exists;
pub(crate) mod locator;
pub(crate) mod parse_error;
pub(crate) mod path_or_stdio;
pub mod rechunk;
pub mod schema;
pub(crate) mod separator;
mod temporary_storage;
pub(crate) mod tls;
pub mod tokio_glue;
pub(crate) mod transform;
mod url_with_hidden_password;

/// The buffer size to use by default when buffering I/O.
pub(crate) const BUFFER_SIZE: usize = 64 * 1024;

pub use args::{
    ArgumentState, DestinationArguments, SharedArguments, SourceArguments, Unverified,
    Verified,
};
pub use context::Context;
pub use csv_stream::CsvStream;
pub use data_streams::DataFormat;
pub use driver_args::DriverArguments;
pub use if_exists::IfExists;
pub use locator::{BoxLocator, DisplayOutputLocators, Locator, UnparsedLocator};
pub use temporary_storage::TemporaryStorage;
pub use tokio_glue::ConsumeWithParallelism;

/// Definitions included by all the files in this crate.
///
/// This forms the dialect of Rust we use for implementing our core and various
/// drivers, with an emphasis on `tokio` and structured logging.
#[allow(unused_imports)]
pub(crate) mod common {
    pub(crate) use bytes::BytesMut;
    // `big_enum_set` is more-or-less a drop-in replacement for `enumset`, but
    // it supports more bitflags and it fixes a dependency on private `syn`
    // APIs: https://github.com/Lymia/enumset/issues/17
    pub(crate) use anyhow::{format_err, Context as _};
    pub(crate) use big_enum_set::{
        BigEnumSet as EnumSet, BigEnumSetType as EnumSetType,
    };
    pub(crate) use futures::{
        join, stream, try_join, Future, FutureExt, Stream, StreamExt, TryFutureExt,
        TryStreamExt,
    };
    pub(crate) use metrics::{counter, describe_counter, increment_counter};
    pub(crate) use std::{
        any::Any,
        convert::{TryFrom, TryInto},
        io::{Read, Write},
    };
    pub(crate) use tokio::{
        io::{
            self, AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite,
            AsyncWriteExt,
        },
        sync::mpsc,
    };
    pub(crate) use tracing::{
        debug, debug_span, error, info, instrument, trace, trace_span, warn,
        Instrument,
    };
    pub(crate) use url::Url;

    pub(crate) use crate::{
        args::{
            ArgumentState, DestinationArguments, DestinationArgumentsFeatures,
            SharedArguments, SourceArguments, SourceArgumentsFeatures, Unverified,
            Verified,
        },
        context::Context,
        csv_stream::CsvStream,
        driver_args::DriverArguments,
        if_exists::{IfExists, IfExistsFeatures},
        locator::{
            BoxLocator, DisplayOutputLocators, Features, Locator, LocatorFeatures,
            LocatorStatic,
        },
        path_or_stdio::PathOrStdio,
        schema::{Schema, Table},
        temporary_storage::TemporaryStorage,
        tokio_glue::{
            async_read_to_end, async_read_to_string, box_stream_once,
            buffer_sync_write_and_copy_to_async, spawn_blocking, BoxFuture, BoxStream,
            SendResultExt,
        },
        url_with_hidden_password::UrlWithHiddenPassword,
        Error, Result, BUFFER_SIZE,
    };
}

// Our main entry point.
#[tokio::main]
async fn main() -> Result<()> {
    // Configure telemetry.
    let telemetry_handle = TelemetryConfig::new(
        opinionated_telemetry::AppType::Cli,
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
    )
    .install()
    .await?;
    debug!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    // Find our system SSL configuration, even if we're statically linked.
    openssl_probe::init_ssl_cert_env_vars();
    debug!("SSL_CERT_DIR: {:?}", env::var("SSL_CERT_DIR").ok());
    debug!("SSL_CERT_FILE: {:?}", env::var("SSL_CERT_FILE").ok());

    // Parse our command-line arguments.
    let opt = cmd::Opt::parse();
    debug!("{:?}", opt);

    // Set up an execution context for our background workers, if any. The `ctx`
    // must be passed to all our background operations. The `worker_fut` will
    // return either success when all background workers have finished, or an
    // error as soon as one fails.
    let (ctx, worker_fut) = Context::create();

    // Load our configuration.
    let config = Configuration::try_default()?;
    debug!("{:?}", config);

    // Create a future to run our command.
    let cmd_fut = cmd::run(ctx, config, opt);

    // Run our futures.
    let result = try_join!(cmd_fut, worker_fut);
    telemetry_handle.flush_and_shutdown().await;
    result?;
    Ok(())
}
