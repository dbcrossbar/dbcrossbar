use dbcrossbarlib::{config::Configuration, tokio_glue::BoxFuture, Context};
use futures::FutureExt;
use structopt_derive::StructOpt;

pub(crate) mod conv;

/// Schema-related commands.
#[derive(Debug, StructOpt)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Opt {
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
}

pub(crate) fn run(
    ctx: Context,
    config: Configuration,
    enable_unstable: bool,
    opt: Opt,
) -> BoxFuture<()> {
    match opt {
        Opt::Conv { command } => {
            conv::run(ctx, config, enable_unstable, command).boxed()
        }
    }
}
