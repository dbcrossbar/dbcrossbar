//! Command parsing.

use clap::Parser;
use dbcrossbarlib::{config::Configuration, tokio_glue::BoxFuture, Context};
use futures::FutureExt;

pub(crate) mod config;
pub(crate) mod count;
pub(crate) mod cp;
pub(crate) mod features;
pub(crate) mod license;
pub(crate) mod schema;

/// Command-line options, parsed using `structopt`.
#[derive(Debug, Parser)]
#[clap(
    name = "dbcrossbar",
    author,
    version,
    about = "Convert schemas and data between databases."
)]
pub(crate) struct Opt {
    /// Enable unstable, experimental features.
    #[clap(long = "enable-unstable")]
    pub(crate) enable_unstable: bool,

    /// The command to run.
    #[clap(subcommand)]
    pub(crate) cmd: Command,
}

/// The command to run.
#[derive(Debug, Parser)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Command {
    /// Update configuration.
    #[clap(name = "config")]
    Config {
        #[clap(flatten)]
        command: config::Opt,
    },

    /// Count records.
    #[clap(name = "count")]
    #[clap(after_help = r#"EXAMPLE LOCATORS:
    postgres://localhost:5432/db#table
    bigquery:project:dataset.table
"#)]
    Count {
        #[clap(flatten)]
        command: count::Opt,
    },

    /// Copy tables from one location to another.
    #[clap(name = "cp")]
    #[clap(after_help = r#"EXAMPLE LOCATORS:
    postgres://localhost:5432/db#table
    bigquery:project:dataset.table
"#)]
    Cp {
        #[clap(flatten)]
        command: cp::Opt,
    },

    /// List available drivers and supported features.
    #[clap(name = "features")]
    Features {
        #[clap(flatten)]
        command: features::Opt,
    },

    /// Display license information.
    #[clap(name = "license")]
    License {
        #[clap(flatten)]
        command: license::Opt,
    },

    /// Schema-related commands.
    Schema {
        #[clap(flatten)]
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
