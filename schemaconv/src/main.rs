//! A CLI tool for converting between table schema formats.

#[macro_use]
extern crate common_failures;
extern crate env_logger;
#[macro_use]
extern crate failure;
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
use failure::ResultExt;
use schemaconvlib::drivers::{
    bigquery::BigQueryDriver,
    postgres::PostgresDriver,
};
use std::io::{stdin, stdout, Write};
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
    /// The URL of the database, with the table name specified as a `#`
    /// fragment. If this URL is omitted, read a JSON table schema from stdin.
    url: Option<Url>,

    /// The output format to use.
    #[structopt(short = "O", default_value = "json")]
    output_format: OutputFormat,
}

quick_main!(run);

fn run() -> Result<()> {
    env_logger::init();
    let opt = Opt::from_args();
    debug!("{:?}", opt);

    // If we have a database URL, read our schema from the database.
    let table = if let Some(url) = &opt.url {
        let mut base_url = url.clone();
        base_url.set_fragment(None);
        let table_name = url.fragment().ok_or_else(|| {
            format_err!("Database URL must include table name after `#`")
        })?;
        PostgresDriver::fetch_from_url(&base_url, &table_name)?
    } else {
        let stdin = stdin();
        let mut input = stdin.lock();
        serde_json::from_reader(&mut input)
            .context("error reading from stdin")?
    };

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

