//! The `conv` subcommand.

use anyhow::{format_err, Result};
use clap::Parser;
use tracing::{field, instrument, Span};

use crate::{config::Configuration, Context, IfExists, UnparsedLocator};

/// Schema conversion arguments.
#[derive(Debug, Parser)]
pub(crate) struct Opt {
    /// One of `error`, `overrwrite` or `append`.
    #[clap(long = "if-exists", default_value = "error")]
    if_exists: IfExists,

    /// The input schema.
    from_locator: UnparsedLocator,

    /// The output schema.
    to_locator: UnparsedLocator,
}

/// Perform our schema conversion.
#[instrument(level = "debug", name = "conv", skip_all, fields(from, to))]
pub(crate) async fn run(
    ctx: Context,
    _config: Configuration,
    enable_unstable: bool,
    opt: Opt,
) -> Result<()> {
    let from_locator = opt.from_locator.parse(enable_unstable)?;
    let to_locator = opt.to_locator.parse(enable_unstable)?;

    // Fill in our span fields.
    let span = Span::current();
    span.record("from", &field::display(&from_locator));
    span.record("to", &field::display(&to_locator));

    let schema = from_locator.schema(ctx.clone()).await?.ok_or_else(|| {
        format_err!("don't know how to read schema from {}", from_locator)
    })?;
    to_locator.write_schema(ctx, schema, opt.if_exists).await?;
    Ok(())
}
