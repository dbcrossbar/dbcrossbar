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
extern crate strum;
#[macro_use]
extern crate strum_macros;
extern crate url;

use common_failures::Result;
use schemaconvlib::drivers::{
    bigquery::BigQueryDriver,
    postgres::PostgresDriver,
};
use std::io::{stdout, Write};
use structopt::StructOpt;
use url::Url;

#[derive(Clone, Copy, Debug, EnumString)]
enum OutputFormat {
    #[strum(serialize="json")]
    Json,
    #[strum(serialize="pg:select")]
    PostgresSelect,
    #[strum(serialize="bigquery")]
    BigQuery,
}

/// Our command-line arguments.
#[derive(Debug, StructOpt)]
#[structopt(name = "schemaconv", about = "Convert between schema formats.")]
struct Opt {
    /// The URL of the database.
    url: Url,

    /// The name of the table for which to fetch a schema.
    table_name: String,

    /// The output format to use.
    #[structopt(short = "O", default_value = "json")]
    output_format: OutputFormat,
}

quick_main!(run);

fn run() -> Result<()> {
    env_logger::init();
    let opt = Opt::from_args();
    debug!("{:?}", opt);

    let table = PostgresDriver::fetch_from_url(&opt.url, &opt.table_name)?;
    let stdout = stdout();
    let mut out = stdout.lock();

    match opt.output_format {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut out, &table)?;
        }
        OutputFormat::PostgresSelect => {
            PostgresDriver::write_select_args(&mut out, &table)?;
            write!(&mut out, "\n")?;
        }
        OutputFormat::BigQuery => {
            BigQueryDriver::write_json(&mut out, &table)?;
            write!(&mut out, "\n")?;
        }
    }

    Ok(())
}

