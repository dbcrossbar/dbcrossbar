//! A CLI tool for converting between table schema formats.

#![feature(await_macro, async_await, futures_api)]
#![warn(rust_2018_idioms, unused_extern_crates, clippy::all)]

// Needed to prevent linker errors about OpenSSL.
#[allow(unused_extern_crates)]
extern crate openssl;

// Pull in all of `tokio`'s experimental `async` and `await` support.
#[macro_use]
#[allow(unused_imports)]
extern crate tokio;

use common_failures::{quick_main, Result};
use env_logger;
use log::debug;
use openssl_probe;
use std::{env, io::{self, prelude::*}};
use structopt::{self, StructOpt};

mod cmd;

quick_main!(run);

fn run() -> Result<()> {
    env_logger::init();
    openssl_probe::init_ssl_cert_env_vars();

    let opt = cmd::Opt::from_args();
    debug!("{:?}", opt);
    cmd::run(opt)
}
