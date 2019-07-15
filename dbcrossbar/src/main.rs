//! A CLI tool for converting between table schema formats.

#![feature(async_await)]
#![warn(rust_2018_idioms, unused_extern_crates, clippy::all)]

// Needed to prevent linker errors about OpenSSL.
#[allow(unused_extern_crates)]
extern crate openssl;

// Pull in all of `tokio`'s experimental `async` and `await` support.
#[macro_use]
#[allow(unused_imports)]
extern crate tokio;

use common_failures::{quick_main, Result};
use dbcrossbarlib::Context;
use env_logger;
use futures::{compat::Future01CompatExt, FutureExt, TryFutureExt};
use openssl_probe;
use slog::{debug, slog_o, Drain, Logger};
use slog_async::{self, OverflowStrategy};
use slog_envlogger;
use slog_term;
use structopt::{self, StructOpt};
use tokio::prelude::*;

mod cmd;

quick_main!(run);

fn run() -> Result<()> {
    // Set up standard Rust logging for third-party crates.
    env_logger::init();

    // Find our system SSL configuration, even if we're statically linked.
    openssl_probe::init_ssl_cert_env_vars();

    // Set up `slog`-based structured logging for our async code, because we
    // need to be able to untangle very complicated logs from many parallel
    // async tasks.
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let formatted = slog_term::CompactFormat::new(decorator).build().fuse();
    let filtered = slog_envlogger::new(formatted);
    let drain = slog_async::Async::new(filtered)
        .chan_size(64)
        // This may slow down application performance, even when `RUST_LOG` is
        // not set. But we've been seeing a lot of dropped messages lately, so
        // let's try it.
        .overflow_strategy(OverflowStrategy::Block)
        .build()
        .fuse();
    let log = Logger::root(
        drain,
        slog_o!(
            "app" => env!("CARGO_PKG_NAME"),
            "ver" => env!("CARGO_PKG_VERSION"),
        ),
    );

    // Set up an execution context for our background workers, if any. The `ctx`
    // must be passed to all our background operations. The `worker_fut` will
    // return either success when all background workers have finished, or an
    // error as soon as one fails.
    let (ctx, worker_fut) = Context::create(log);

    // Parse our command-line arguments.
    let opt = cmd::Opt::from_args();
    debug!(ctx.log(), "{:?}", opt);
    let cmd_fut = cmd::run(ctx, opt);

    // Wait for both `cmd_fut` and `copy_fut` to finish, but bail out as soon
    // as either returns an error. This involves some pretty deep `tokio` magic:
    // If a background worker fails, then `copy_fut` will be automatically
    // dropped, or vice vera.
    let combined_fut = async move {
        cmd_fut.join(worker_fut).compat().await?;
        let result: Result<()> = Ok(());
        result
    };

    // Pass `combined_fut` to our `tokio` runtime, and wait for it to finish.
    let mut runtime =
        tokio::runtime::Runtime::new().expect("Unable to create a runtime");
    runtime.block_on(combined_fut.boxed().compat())?;
    Ok(())
}
