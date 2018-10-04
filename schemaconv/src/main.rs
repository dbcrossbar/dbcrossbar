//! A CLI tool for converting between table schema formats.

#[macro_use]
extern crate common_failures;
extern crate env_logger;
#[macro_use]
extern crate log;
extern crate schemaconvlib;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate structopt;
extern crate url;

use common_failures::Result;
use schemaconvlib::drivers::postgres::PostgresDriver;
use std::io::stdout;
use structopt::StructOpt;
use url::Url;

/// Our command-line arguments.
#[derive(Debug, StructOpt)]
#[structopt(name = "schemaconv", about = "Convert between schema formats.")]
struct Opt {
    /// The URL of the database.
    url: Url,

    /// The name of the table for which to fetch a schema.
    table_name: String,
}

quick_main!(run);

fn run() -> Result<()> {
    env_logger::init();
    let opt = Opt::from_args();
    debug!("{:?}", opt);

    let table = PostgresDriver::fetch_from_url(&opt.url, &opt.table_name)?;
    let out = stdout();
    serde_json::to_writer_pretty(out.lock(), &table)?;

    Ok(())
}

