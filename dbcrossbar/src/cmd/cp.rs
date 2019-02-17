//! The `cp` subcommand.

use common_failures::Result;
use dbcrossbarlib::{Context, tokio_glue::tokio_fut, BoxLocator, IfExists};
use failure::format_err;
use structopt::{self, StructOpt};
use tokio::{self, prelude::*};

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
    // Figure out what table schema to use.
    let schema_locator = opt.schema.as_ref().unwrap_or(&opt.from_locator);
    let schema = schema_locator.schema()?.ok_or_else(|| {
        format_err!("don't know how to read schema from {}", opt.from_locator)
    })?;

    // Set up an execution context for our background workers, if any. The `ctx`
    // must be passed to all our background operations. The `worker_fut` will
    // return either success when all background workers have finished, or an
    // error as soon as one fails.
    let (ctx, worker_fut) = Context::create();

    // Copy data from input to output.
    let copy_fut = async move {
        let data = await!(opt.from_locator.local_data(ctx.clone()))?.ok_or_else(|| {
            format_err!("don't know how to read data from {}", opt.to_locator)
        })?;
        await!(opt.to_locator.write_local_data(ctx.clone(), schema, data, opt.if_exists))?;
        Ok(())
    };

    // Wait for both `worker_fut` and `copy_fut` to finish, but bail out as soon
    // as either returns an error. This involves some pretty deep `tokio` magic:
    // If a background worker fails, then `copy_fut` will be automatically
    // dropped, or vice vera.
    let combined_fut = async move {
        await!(tokio_fut(copy_fut).join(worker_fut))?;
        Ok(())
    };

    // Pass `combined_fut` to our `tokio` runtime, and wait for it to finish.
    let mut runtime =
        tokio::runtime::Runtime::new().expect("Unable to create a runtime");
    runtime.block_on(tokio_fut(combined_fut))?;
    Ok(())
}
