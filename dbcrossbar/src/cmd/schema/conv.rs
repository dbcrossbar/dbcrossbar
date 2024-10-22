//! The `conv` subcommand.

use anyhow::{format_err, Result};
use clap::Parser;
use tracing::{field, instrument, Span};

use crate::{
    config::Configuration, Context, DestinationArguments, DriverArguments, IfExists,
    SourceArguments, UnparsedLocator,
};

/// Schema conversion arguments.
#[derive(Debug, Parser)]
pub(crate) struct Opt {
    /// One of `error`, `overrwrite` or `append`.
    #[clap(long = "if-exists", default_value = "error")]
    if_exists: IfExists,

    /// Pass an extra argument of the form `key=value` to the source driver.
    #[structopt(long = "from-arg")]
    from_args: Vec<String>,

    /// Pass an extra argument of the form `key=value` to the destination
    /// driver.
    #[structopt(long = "to-arg")]
    to_args: Vec<String>,

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
    span.record("from", field::display(&from_locator));
    span.record("to", field::display(&to_locator));

    // Build our source arguments.
    let from_args = DriverArguments::from_cli_args(&opt.from_args)?;
    let source_args = SourceArguments::new(from_args, None, None);
    let to_args = DriverArguments::from_cli_args(&opt.to_args)?;
    let dest_args = DestinationArguments::new(to_args, None, IfExists::default());

    let schema = from_locator
        .schema(ctx.clone(), source_args)
        .await?
        .ok_or_else(|| {
            format_err!("don't know how to read schema from {}", from_locator)
        })?;
    to_locator
        .write_schema(ctx, schema, opt.if_exists, dest_args)
        .await?;
    Ok(())
}
