//! The `cp` subcommand.

use common_failures::Result;
use dbcrossbarlib::{BoxLocator, Context, IfExists};
use failure::format_err;
use slog::o;
use structopt::{self, StructOpt};
use tokio::prelude::*;

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
pub(crate) async fn run(ctx: Context, opt: Opt) -> Result<()> {
    // Figure out what table schema to use.
    let schema = {
        let schema_locator = opt.schema.as_ref().unwrap_or(&opt.from_locator);
        schema_locator.schema(&ctx)?.ok_or_else(|| {
            format_err!("don't know how to read schema from {}", opt.from_locator)
        })
    }?;

    // Read data from input.
    let input_ctx = ctx.child(o!("from_locator" => opt.from_locator.to_string()));
    let data = await!(opt.from_locator.local_data(input_ctx))?.ok_or_else(|| {
        format_err!("don't know how to read data from {}", opt.to_locator)
    })?;

    // Write data to output.
    let output_ctx = ctx.child(o!("to_locator" => opt.to_locator.to_string()));
    let result_stream = await!(opt.to_locator.write_local_data(
        output_ctx,
        schema,
        data,
        opt.if_exists
    ))?;

    // Consume the stream of futures produced by `write_local_data`, allowing a
    // certain degree of parallelism. This is where all the actual work happens,
    // and this what controls how many "input driver" -> "output driver"
    // connections are running at any given time.
    await!(result_stream.buffered(4).collect())?;

    Ok(())
}
