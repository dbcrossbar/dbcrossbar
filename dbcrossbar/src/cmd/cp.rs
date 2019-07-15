//! The `cp` subcommand.

use common_failures::Result;
use dbcrossbarlib::{BoxLocator, Context, IfExists, Query, TemporaryStorage};
use failure::format_err;
use futures::compat::Future01CompatExt;
use slog::{debug, o};
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

    /// Temporary directories, cloud storage buckets, datasets to use during
    /// transfer (can be repeated).
    #[structopt(long = "temporary")]
    temporaries: Vec<String>,

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

    // Get our query details.
    let mut query = Query::default();
    query.where_clause = opt.where_clause.clone();

    // Build a `TemporaryStorage` object to keep track of places that we can put
    // temporary data.
    let temporary_storage = TemporaryStorage::new(opt.temporaries.clone());

    // Can we short-circuit this particular copy using special features of the
    // the source and destination, or do we need to pull the data down to the
    // local machine?
    if opt
        .to_locator
        .supports_write_remote_data(opt.from_locator.as_ref())
    {
        // Build a logging context.
        let ctx = ctx.child(o!(
            "from_locator" => opt.from_locator.to_string(),
            "to_locator" => opt.to_locator.to_string(),
        ));

        // Perform a remote transfer.
        debug!(ctx.log(), "performing remote data transfer");
        opt.to_locator
            .write_remote_data(
                ctx,
                schema,
                opt.from_locator,
                temporary_storage,
                opt.if_exists,
            )
            .compat()
            .await?
    } else {
        // We have to transfer the data via the local machine, so read data from
        // input.
        debug!(ctx.log(), "performaning local data transfer");

        let input_ctx = ctx.child(o!("from_locator" => opt.from_locator.to_string()));
        let data = opt
            .from_locator
            .local_data(input_ctx, schema.clone(), query, temporary_storage.clone())
            .compat()
            .await?
            .ok_or_else(|| {
                format_err!("don't know how to read data from {}", opt.from_locator)
            })?;

        // Write data to output.
        let output_ctx = ctx.child(o!("to_locator" => opt.to_locator.to_string()));
        let result_stream = opt
            .to_locator
            .write_local_data(
                output_ctx,
                schema,
                data,
                temporary_storage,
                opt.if_exists,
            )
            .compat()
            .await?;

        // Consume the stream of futures produced by `write_local_data`, allowing a
        // certain degree of parallelism. This is where all the actual work happens,
        // and this what controls how many "input driver" -> "output driver"
        // connections are running at any given time.
        result_stream.buffered(4).collect().compat().await?;
    }

    Ok(())
}
