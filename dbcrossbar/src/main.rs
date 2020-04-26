//! A CLI tool for converting between table schema formats.

#![warn(rust_2018_idioms, unused_extern_crates, clippy::all)]

// Needed to prevent linker errors about OpenSSL.
#[allow(unused_extern_crates)]
extern crate openssl;

// Pull in all of `tokio`'s experimental `async` and `await` support.
#[macro_use]
#[allow(unused_imports)]
extern crate tokio;

use common_failures::{quick_main, Result};
use dbcrossbarlib::{config::Configuration, run_futures_with_runtime, Context};
use env_logger;
use openssl_probe;
use slog::{debug, Drain};
use slog_async::{self, OverflowStrategy};
use slog_envlogger;
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
