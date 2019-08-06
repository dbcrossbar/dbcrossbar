//! Command parsing.

use dbcrossbarlib::{tokio_glue::BoxFuture, Context};
use futures::FutureExt;
//use structopt::StructOpt;
use structopt_derive::StructOpt;

use crate::logging::LogFormat;

pub(crate) mod conv;
pub(crate) mod cp;
pub(crate) mod features;

/// Command-line options, parsed using `structopt`.
#[derive(Debug, StructOpt)]
#[structopt(
    name = "dbcrossbar",
    about = "Convert schemas and data between databases."
)]
pub(crate) struct Opt {
    /// Logging format (indented, flat, json).
    #[structopt(long = "log-format", default_value = "indented")]
    pub(crate) log_format: LogFormat,

    /// A `key=value` pair to add to our logs. May be passed multiple times.
    #[structopt(long = "log-extra")]
    pub(crate) log_extra: Vec<String>,

    /// The command to run.
    #[structopt(subcommand)]
    pub(crate) cmd: Command,
}

/// The command to run.
#[derive(Debug, StructOpt)]
pub(crate) enum Command {
    /// Convert table schemas from one format to another.
    #[structopt(name = "conv")]
    #[structopt(after_help = r#"EXAMPLE LOCATORS:
    postgres-sql:table.sql
    postgres://localhost:5432/db#table
    bigquery-schema:table.json
"#)]
    Conv {
        #[structopt(flatten)]
        command: conv::Opt,
    },

    /// Copy tables from one location to another.
    #[structopt(name = "cp")]
    #[structopt(after_help = r#"EXAMPLE LOCATORS:
    postgres://localhost:5432/db#table
    bigquery:project:dataset.table
"#)]
    Cp {
        #[structopt(flatten)]
        command: cp::Opt,
    },

    /// List available drivers and supported features.
    #[structopt(name = "features")]
    Features {
        #[structopt(flatten)]
        command: features::Opt,
    },
}

pub(crate) fn run(ctx: Context, opt: Opt) -> BoxFuture<()> {
    match opt.cmd {
        Command::Conv { command } => conv::run(ctx, command).boxed(),
        Command::Cp { command } => cp::run(ctx, command).boxed(),
        Command::Features { command } => features::run(ctx, command).boxed(),
    }
}
