use clap::Parser;
use futures::FutureExt;

use crate::{config::Configuration, tokio_glue::BoxFuture, Context};

pub(crate) mod conv;

/// Commands related to schemas.
#[derive(Debug, Parser)]
pub(crate) struct Opt {
    /// The command to run.
    #[clap(subcommand)]
    pub(crate) command: Cmd,
}

/// Schema-related commands.
#[derive(Debug, Parser)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Cmd {
    /// Convert table schemas from one format to another.
    #[clap(name = "conv")]
    #[clap(after_help = r#"EXAMPLE LOCATORS:
    postgres-sql:table.sql
    postgres://localhost:5432/db#table
    bigquery-schema:table.json
"#)]
    Conv {
        #[structopt(flatten)]
        command: conv::Opt,
    },
}

pub(crate) fn run(
    ctx: Context,
    config: Configuration,
    enable_unstable: bool,
    opt: Opt,
) -> BoxFuture<()> {
    match opt {
        Opt {
            command: Cmd::Conv { command },
        } => conv::run(ctx, config, enable_unstable, command).boxed(),
    }
}
