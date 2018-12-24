//! A CLI tool for converting between table schema formats.

#![warn(unused_extern_crates, clippy::pendantic)]

// Needed to prevent linker errors about OpenSSL.
#[allow(unused_extern_crates)]
extern crate openssl;

use common_failures::{quick_main, Result};
use env_logger;
use log::debug;
use openssl_probe;
use serde_json;
use structopt::{self, StructOpt};

mod cmd;

quick_main!(run);

fn run() -> Result<()> {
    env_logger::init();
    openssl_probe::init_ssl_cert_env_vars();

    let opt = cmd::Opt::from_args();
    debug!("{:?}", opt);
    cmd::run(&opt)
}
