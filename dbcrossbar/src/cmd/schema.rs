//! The `schema` subcommand.

use common_failures::Result;
use dbcrossbarlib::{
    drivers::{bigquery::BigQueryDriver, postgres::PostgresDriver},
    parsers::postgres::parse_create_table,
};
use env_logger;
use failure::{format_err, ResultExt};
use openssl_probe;
use serde_json;
use std::io::{stdin, stdout, Read, Write};
use structopt::{self, StructOpt};
use strum;
use strum_macros::EnumString;
use url::Url;

/// The input format to our program.
#[derive(Clone, Copy, Debug, EnumString)]
enum InputFormat {
    /// dbcrossbar JSON schema.
    #[strum(serialize = "json")]
    Json,

    /// PostgreSQL `CREATE TABLE` SQL.
    #[strum(serialize = "pg")]
    Postgres,
}

/// The output format of our program.
#[derive(Clone, Copy, Debug, EnumString)]
enum OutputFormat {
    #[strum(serialize = "json")]
    Json,
    #[strum(serialize = "pg:export")]
    PostgresExport,
    #[strum(serialize = "pg:export:columns")]
    PostgresExportColumns,
    #[strum(serialize = "bq:schema:temp")]
    BigQuerySchemaTemp,
    #[strum(serialize = "bq:schema")]
    BigQuerySchema,
    #[strum(serialize = "bq:import")]
    BigQueryImport,
}

/// Schema conversion arguments.
#[derive(Debug, StructOpt)]
pub(crate) struct Opt {
    /// The URL of the database, followed by '#table_name' (as a URL fragment).
    /// If this URL is omitted, read a JSON table schema from stdin.
    url: Option<Url>,

    /// Rename the table.
    #[structopt(short = "t", long = "rename-table")]
    rename_table: Option<String>,

    /// Add a `LIMIT` clause to export SQL. Does not affect other output
    /// formats.
    #[structopt(long = "export-limit")]
    export_limit: Option<u64>,

    /// The input format to use if no URL is specified.
    #[structopt(short = "I", long = "input-format", default_value = "json")]
    input_format: InputFormat,

    /// The output format to use.
    #[structopt(short = "O", long = "output-format", default_value = "json")]
    output_format: OutputFormat,
}

pub(crate) fn run(opt: &Opt) -> Result<()> {
    // If we have a database URL, read our schema from the database.
    let mut table = if let Some(url) = &opt.url {
        let mut base_url = url.clone();
        base_url.set_fragment(None);
        let table_name = url.fragment().ok_or_else(|| {
            format_err!("Database URL must include table name after `#`")
        })?;
        PostgresDriver::fetch_from_url(&base_url, &table_name)?
    } else {
        let stdin = stdin();
        let mut input = stdin.lock();
        let mut text = String::new();
        input
            .read_to_string(&mut text)
            .context("error reading from stdin")?;
        match opt.input_format {
            InputFormat::Json => {
                serde_json::from_str(&text).context("error parsing JSON")?
            }
            InputFormat::Postgres => parse_create_table(&text)?,
        }
    };

    // Apply any requested transformations to our table schema.
    if let Some(rename_table) = &opt.rename_table {
        table.name = rename_table.to_owned();
    }

    // Output our table schema.
    let stdout = stdout();
    let mut out = stdout.lock();
    match opt.output_format {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut out, &table)?;
        }
        OutputFormat::PostgresExport => {
            PostgresDriver::write_select(&mut out, &table, opt.export_limit)?;
        }
        OutputFormat::PostgresExportColumns => {
            PostgresDriver::write_select_args(&mut out, &table)?;
        }
        OutputFormat::BigQuerySchemaTemp => {
            BigQueryDriver::write_json(&mut out, &table, true)?;
        }
        OutputFormat::BigQuerySchema => {
            BigQueryDriver::write_json(&mut out, &table, false)?;
        }
        OutputFormat::BigQueryImport => {
            BigQueryDriver::write_import_sql(&mut out, &table)?;
        }
    }
    writeln!(&mut out)?;

    Ok(())
}
