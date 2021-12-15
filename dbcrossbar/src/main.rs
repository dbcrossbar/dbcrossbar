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

// Pull in all of `tokio`'s experimental `async` and `await` support.
#[macro_use]
#[allow(unused_imports)]
extern crate tokio;

use common_failures::{quick_main, Result};
use dbcrossbarlib::{config::Configuration, run_futures_with_runtime, Context};
use slog::{debug, Drain};
use slog_async::{self, OverflowStrategy};
use structopt::{self, StructOpt};

mod cmd;
mod logging;

quick_main!(run);

fn run() -> Result<()> {
    // Set up standard Rust logging for third-party crates.
    env_logger::init();

    // Find our system SSL configuration, even if we're statically linked.
    openssl_probe::init_ssl_cert_env_vars();

    // Parse our command-line arguments.
    let opt = cmd::Opt::from_args();

    // Set up `slog`-based structured logging for our async code, because we
    // need to be able to untangle very complicated logs from many parallel
    // async tasks.
    let base_drain = opt.log_format.create_drain();
    let filtered = slog_envlogger::new(base_drain);
    let drain = slog_async::Async::new(filtered)
        .chan_size(64)
        // This may slow down application performance, even when `RUST_LOG` is
        // not set. But we've been seeing a lot of dropped messages lately, so
        // let's try it.
        .overflow_strategy(OverflowStrategy::Block)
        .build()
        .fuse();
    let log = logging::global_logger_with_extra_values(drain, &opt.log_extra)?;

    // Log our SSL cert dir, which we set up above.
    debug!(
        log,
        "SSL_CERT_DIR: {:?}",
        std::env::var("SSL_CERT_DIR").ok()
    );
    debug!(
        log,
        "SSL_CERT_FILE: {:?}",
        std::env::var("SSL_CERT_FILE").ok()
    );

    // Set up an execution context for our background workers, if any. The `ctx`
    // must be passed to all our background operations. The `worker_fut` will
    // return either success when all background workers have finished, or an
    // error as soon as one fails.
    let (ctx, worker_fut) = Context::create(log);

    // Log our command-line options.
    debug!(ctx.log(), "{:?}", opt);

    // Load our configuration.
    let config = Configuration::try_default()?;
    debug!(ctx.log(), "{:?}", config);

    // Create a future to run our command.
    let cmd_fut = cmd::run(ctx, config, opt);

    // Run our futures.
    run_futures_with_runtime(cmd_fut, worker_fut)
}
