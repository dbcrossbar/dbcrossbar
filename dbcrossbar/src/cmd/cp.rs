//! The `cp` subcommand.

use anyhow::{format_err, Context as _, Result};
use clap::Parser;
use futures::{pin_mut, stream, FutureExt, StreamExt, TryStreamExt};
use humanize_rs::bytes::Bytes as HumanizedBytes;
use opinionated_telemetry::tracing::{field, Span};
use tokio::io;
use tokio_util::codec::{FramedWrite, LinesCodec};

use crate::{
    common::*, config::Configuration, rechunk::rechunk_csvs, tokio_glue::try_forward,
    Context, DataFormat, DestinationArguments, DisplayOutputLocators, DriverArguments,
    IfExists, SharedArguments, SourceArguments, TemporaryStorage, UnparsedLocator,
};

/// Schema conversion arguments.
#[derive(Debug, Parser)]
pub(crate) struct Opt {
    /// One of `error`, `overwrite`, `append` or `upsert-on:COL`.
    #[clap(long = "if-exists", default_value = "error")]
    if_exists: IfExists,

    /// The schema to use (defaults to input table schema).
    #[clap(long = "schema")]
    schema: Option<UnparsedLocator>,

    /// Temporary directories, cloud storage buckets, datasets to use during
    /// transfer (can be repeated).
    #[clap(long = "temporary")]
    temporaries: Vec<String>,

    /// Specify the approximate size of the CSV streams manipulated by
    /// `dbcrossbar`. This can be used to split a large input into multiple
    /// smaller outputs. Actual data streams may be bigger or smaller depending
    /// on a number of factors. Examples: "100000", "1Gb".
    #[clap(long = "stream-size")]
    stream_size: Option<HumanizedBytes>, // usize

    /// Pass an extra argument of the form `key=value` to the source driver.
    #[clap(long = "from-arg")]
    from_args: Vec<String>,

    /// For directory- and file-like data sources, the format to assume. If not
    /// specified, `dbcrossbar` will use the file extension to guess the format.
    #[clap(long = "from-format")]
    from_format: Option<DataFormat>,

    /// Pass an extra argument of the form `key=value` to the destination
    /// driver.
    #[clap(long = "to-arg")]
    to_args: Vec<String>,

    /// For directory-like data destinations, the format to use. If not
    /// specified, `dbcrossbar` will use the destination file extension (if
    /// provided) or `csv`.
    #[clap(long = "to-format", short = 'F')]
    to_format: Option<DataFormat>,

    /// SQL where clause specifying rows to use.
    #[clap(long = "where")]
    where_clause: Option<String>,

    /// How many data streams should we attempt to copy in parallel?
    #[clap(long = "max-streams", short = 'J', default_value = "4")]
    max_streams: usize,

    /// Display where we wrote our output data.
    #[clap(long = "display-output-locators")]
    display_output_locators: bool,

    /// The input table.
    from_locator: UnparsedLocator,

    /// The output table.
    to_locator: UnparsedLocator,
}

/// Perform our schema conversion.
#[instrument(level = "debug", name = "cp", skip_all, fields(from, to))]
pub(crate) async fn run(
    ctx: Context,
    config: Configuration,
    enable_unstable: bool,
    opt: Opt,
) -> Result<()> {
    let schema_opt = opt.schema.map(|s| s.parse(enable_unstable)).transpose()?;
    let from_locator = opt.from_locator.parse(enable_unstable)?;
    let to_locator = opt.to_locator.parse(enable_unstable)?;

    // Fill in our span fields.
    let span = Span::current();
    span.record("from", &field::display(&from_locator));
    span.record("to", &field::display(&to_locator));

    // Figure out what table schema to use.
    let schema = {
        let schema_locator = schema_opt.as_ref().unwrap_or(&from_locator);
        schema_locator
            .schema(ctx.clone())
            .await
            .with_context(|| format!("error reading schema from {}", schema_locator))?
            .ok_or_else(|| {
                format_err!("don't know how to read schema from {}", schema_locator)
            })
    }?;

    // Build our shared arguments.
    let temporaries = opt.temporaries.clone();
    let temporary_storage = TemporaryStorage::with_config(temporaries, &config)?;
    let shared_args = SharedArguments::new(schema, temporary_storage, opt.max_streams);

    // Build our source arguments.
    let from_args = DriverArguments::from_cli_args(&opt.from_args)?;
    let source_args = SourceArguments::new(
        from_args,
        opt.from_format.clone(),
        opt.where_clause.clone(),
    );

    // Build our destination arguments.
    let to_args = DriverArguments::from_cli_args(&opt.to_args)?;
    let dest_args =
        DestinationArguments::new(to_args, opt.to_format.clone(), opt.if_exists);

    // Can we short-circuit this particular copy using special features of the
    // the source and destination, or do we need to pull the data down to the
    // local machine?
    let should_use_remote = opt.stream_size.is_none()
        && to_locator.supports_write_remote_data(from_locator.as_ref());
    let dests = if should_use_remote {
        // Perform a remote transfer.
        debug!("performing remote data transfer");
        let dests = to_locator
            .write_remote_data(ctx, from_locator, shared_args, source_args, dest_args)
            .await?;

        // Convert our list of output locators into a stream.
        stream::iter(dests).map(Ok).boxed()
    } else {
        // We have to transfer the data via the local machine, so read data from
        // input.
        debug!("performing local data transfer");

        let mut data = from_locator
            .local_data(ctx.clone(), shared_args.clone(), source_args)
            .await?
            .ok_or_else(|| {
                format_err!("don't know how to read data from {}", from_locator)
            })?;

        // Honor --stream-size if passed.
        if let Some(stream_size) = opt.stream_size {
            let stream_size = stream_size.size();
            data = rechunk_csvs(ctx.clone(), stream_size, data)?;
        }

        // Write data to output.
        let result_stream = to_locator
            .write_local_data(ctx.clone(), data, shared_args.clone(), dest_args)
            .await?;

        // Consume the stream of futures produced by `write_local_data`, allowing a
        // certain degree of parallelism. This is where all the actual work happens,
        // and this what controls how many "input driver" -> "output driver"
        // connections are running at any given time.
        result_stream
            .try_buffer_unordered(shared_args.max_streams())
            .boxed()
    };

    // Optionally display `dests`, depending on a combination of
    // `--display-output-locators` and the defaults for `to_locator`.
    let display_output_locators = match (
        opt.display_output_locators,
        to_locator.display_output_locators(),
    ) {
        // The user passed `--display-output-locators`, but displaying them is
        // forbidden (probably because we wrote actual data to standard output).
        (true, DisplayOutputLocators::Never) => {
            return Err(format_err!(
                "cannot use --display-output-locators with {}",
                to_locator
            ))
        }

        // We want to display our actual output locators.
        (true, _) | (false, DisplayOutputLocators::ByDefault) => true,

        // We don't want to display our output locators.
        (false, _) => false,
    };

    // Print our destination
    if display_output_locators {
        // Display our output locators incrementally on standard output using
        // `LinesCodec` to insert newlines.
        let stdout_sink = FramedWrite::new(io::stdout(), LinesCodec::new());
        let dest_strings = dests.and_then(|dest| {
            async move {
                let dest_str = dest.to_string();
                if dest_str.contains('\n') || dest_str.contains('\r') {
                    // If we write out this locator, it would be split between
                    // lines, causing an ambiguity for any parsing program.
                    Err(format_err!(
                        "cannot output locator with newline: {:?}",
                        dest_str
                    ))
                } else {
                    Ok(dest_str)
                }
            }
        });
        pin_mut!(dest_strings);
        try_forward(dest_strings, stdout_sink).await?;
    } else {
        // Just collect our results and ignore
        let dests = dests.try_collect::<Vec<_>>().boxed().await?;
        debug!("destination locators: {:?}", dests);
    }
    Ok(())
}
