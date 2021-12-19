//! Command parsing.

use dbcrossbarlib::{config::Configuration, tokio_glue::BoxFuture, Context};
use futures::FutureExt;
//use structopt::StructOpt;
use structopt_derive::StructOpt;

pub(crate) mod config;
pub(crate) mod count;
pub(crate) mod cp;
pub(crate) mod features;
pub(crate) mod license;
pub(crate) mod schema;

/// Command-line options, parsed using `structopt`.
#[derive(Debug, StructOpt)]
#[structopt(
    name = "dbcrossbar",
    about = "Convert schemas and data between databases."
)]
pub(crate) struct Opt {
    /// Enable unstable, experimental features.
    #[structopt(long = "enable-unstable")]
    pub(crate) enable_unstable: bool,

    /// The command to run.
    #[structopt(subcommand)]
    pub(crate) cmd: Command,
}

/// The command to run.
#[derive(Debug, StructOpt)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Command {
    /// Update configuration.
    #[structopt(name = "config")]
    Config {
        #[structopt(flatten)]
        command: config::Opt,
    },

    /// Count records.
    #[structopt(name = "count")]
    #[structopt(after_help = r#"EXAMPLE LOCATORS:
    postgres://localhost:5432/db#table
    bigquery:project:dataset.table
"#)]
    Count {
        #[structopt(flatten)]
        command: count::Opt,
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

    /// Display license information.
    #[structopt(name = "license")]
    License {
        #[structopt(flatten)]
        command: license::Opt,
    },

    /// Schema-related commands.
    Schema {
        #[structopt(flatten)]
        command: schema::Opt,
    },
}

pub(crate) fn run(ctx: Context, config: Configuration, opt: Opt) -> BoxFuture<()> {
    match opt.cmd {
        Command::Config { command } => config::run(config, command).boxed(),

        Command::Count { command } => {
            count::run(ctx, config, opt.enable_unstable, command).boxed()
        }
        Command::Cp { command } => {
            cp::run(ctx, config, opt.enable_unstable, command).boxed()
        }
        Command::Features { command } => {
            features::run(config, opt.enable_unstable, command).boxed()
        }
        Command::License { command } => {
            license::run(config, opt.enable_unstable, command).boxed()
        }
        Command::Schema { command } => {
            schema::run(ctx, config, opt.enable_unstable, command).boxed()
        }
    }
}
