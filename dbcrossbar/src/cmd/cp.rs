//! The `cp` subcommand.

use common_failures::Result;
use dbcrossbarlib::{BoxLocator, IfExists, tokio_glue::tokio_fut};
use failure::format_err;
use structopt::{self, StructOpt};
use tokio;

/// Schema conversion arguments.
#[derive(Debug, StructOpt)]
pub(crate) struct Opt {
    /// One of `error`, `overrwrite` or `append`.
    #[structopt(long = "if-exists", default_value = "error")]
    if_exists: IfExists,

    /// The schema to use (defaults to input table schema).
    #[structopt(long = "schema")]
    schema: Option<BoxLocator>,

    /// The input table.
    from_locator: BoxLocator,

    /// The output table.
    to_locator: BoxLocator,
}

/// Perform our schema conversion.
pub(crate) fn run(opt: Opt) -> Result<()> {
    let schema_locator = opt.schema.as_ref().unwrap_or(&opt.from_locator);
    let schema = schema_locator.schema()?.ok_or_else(|| {
        format_err!("don't know how to read schema from {}", opt.from_locator)
    })?;

    let mut runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
    runtime.block_on(tokio_fut(
        async move {
            let data = await!(opt.from_locator.local_data())?.ok_or_else(|| {
                format_err!("don't know how to read data from {}", opt.to_locator)
            })?;
            await!(opt.to_locator
                .write_local_data(schema, data, opt.if_exists))?;
            Ok(())
        }
    ))?;
    Ok(())
}
