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

use std::env;

use anyhow::Result;
use clap::Parser;
use dbcrossbarlib::{config::Configuration, Context};
use futures::try_join;
use tracing::debug;
use tracing_subscriber::{
    fmt::{format::FmtSpan, Subscriber},
    prelude::*,
    EnvFilter,
};

mod cmd;

// Our main entry point.
#[tokio::main]
async fn main() -> Result<()> {
    // Configure tracing.
    let filter = EnvFilter::from_default_env();
    Subscriber::builder()
        .with_writer(std::io::stderr)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_env_filter(filter)
        .finish()
        .init();
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
    try_join!(cmd_fut, worker_fut)?;
    Ok(())
}
