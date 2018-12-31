//! Command parsing.

//use structopt::StructOpt;
use structopt_derive::StructOpt;

use crate::Result;

pub(crate) mod conv;
pub(crate) mod cp;

/// Command-line options, parsed using `structopt`.
#[derive(Debug, StructOpt)]
#[structopt(name = "dbcrossbar", about = "Convert schemas and data between databases.")]
pub(crate) enum Opt {
    #[structopt(name = "conv")]
    #[structopt(after_help = r#"EXAMPLE LOCATORS:
    postgres.sql:table.sql
    postgres://localhost:5432/db#table
    bigquery.json:table.json
"#)]
    Conv {
        #[structopt(flatten)]
        command: conv::Opt,
    },

    #[structopt(name = "cp")]
    #[structopt(after_help = r#"EXAMPLE LOCATORS:
    postgres://localhost:5432/db#table
    bigquery:project:dataset.table
"#)]
    Cp {
        #[structopt(flatten)]
        command: cp::Opt,
    }
}

pub(crate) fn run(opt: &Opt) -> Result<()> {
    match opt {
        Opt::Conv { command } => conv::run(command),
        Opt::Cp { command } => cp::run(command),
    }
}
