//! The `cp` subcommand.

use common_failures::Result;
use dbcrossbarlib::{
    BoxLocator, ConsumeWithParallelism, Context, DestinationArguments,
    DriverArguments, IfExists, SharedArguments, SourceArguments, TemporaryStorage,
};
use failure::format_err;
use slog::{debug, o};
use structopt::{self, StructOpt};

/// Schema conversion arguments.
#[derive(Debug, StructOpt)]
pub(crate) struct Opt {
    /// One of `error`, `overrwrite` or `append`.
    #[structopt(long = "if-exists", default_value = "error")]
    if_exists: IfExists,

    /// The schema to use (defaults to input table schema).
    #[structopt(long = "schema")]
    schema: Option<BoxLocator>,

    /// Temporary directories, cloud storage buckets, datasets to use during
    /// transfer (can be repeated).
    #[structopt(long = "temporary")]
    temporaries: Vec<String>,

    /// Pass an extra argument of the form `key=value` to the source driver.
    #[structopt(long = "from-arg")]
    from_args: Vec<String>,

    /// Pass an extra argument of the form `key=value` to the destination
    /// driver.
    #[structopt(long = "to-arg")]
    to_args: Vec<String>,

    /// SQL where clause specifying rows to use.
    #[structopt(long = "where")]
    where_clause: Option<String>,

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

    // Build our shared arguments.
    let temporary_storage = TemporaryStorage::new(opt.temporaries.clone());
    let shared_args = SharedArguments::new(schema, temporary_storage);

    // Build our source arguments.
    let from_args = DriverArguments::from_cli_args(&opt.from_args)?;
    let source_args = SourceArguments::new(from_args, opt.where_clause.clone());

    // Build our destination arguments.
    let to_args = DriverArguments::from_cli_args(&opt.to_args)?;
    let dest_args = DestinationArguments::new(to_args, opt.if_exists);

    // Can we short-circuit this particular copy using special features of the
    // the source and destination, or do we need to pull the data down to the
    // local machine?
    let to_locator = opt.to_locator;
    let from_locator = opt.from_locator;
    let _dests = if to_locator.supports_write_remote_data(from_locator.as_ref()) {
        // Build a logging context.
        let ctx = ctx.child(o!(
            "from_locator" => from_locator.to_string(),
            "to_locator" => to_locator.to_string(),
        ));

        // Perform a remote transfer.
        debug!(ctx.log(), "performing remote data transfer");
        to_locator
            .write_remote_data(ctx, from_locator, shared_args, source_args, dest_args)
            .await?
    } else {
        // We have to transfer the data via the local machine, so read data from
        // input.
        debug!(ctx.log(), "performing local data transfer");

        let input_ctx = ctx.child(o!("from_locator" => from_locator.to_string()));
        let data = from_locator
            .local_data(input_ctx, shared_args.clone(), source_args)
            .await?
            .ok_or_else(|| {
                format_err!("don't know how to read data from {}", from_locator)
            })?;

        // Write data to output.
        let output_ctx = ctx.child(o!("to_locator" => to_locator.to_string()));
        let result_stream = to_locator
            .write_local_data(output_ctx, data, shared_args, dest_args)
            .await?;

        // Consume the stream of futures produced by `write_local_data`, allowing a
        // certain degree of parallelism. This is where all the actual work happens,
        // and this what controls how many "input driver" -> "output driver"
        // connections are running at any given time.
        result_stream.consume_with_parallelism(4).await?
    };

    // TODO: Decide when and how to display `dests`.

    Ok(())
}
