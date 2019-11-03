//! The `count` subcommand.

use common_failures::Result;
use dbcrossbarlib::{
    BoxLocator, Context, DriverArguments, SharedArguments, SourceArguments,
    TemporaryStorage,
};
use failure::{format_err, ResultExt};
use structopt::{self, StructOpt};

/// Count arguments.
#[derive(Debug, StructOpt)]
pub(crate) struct Opt {
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

    /// SQL where clause specifying rows to use.
    #[structopt(long = "where")]
    where_clause: Option<String>,

    /// The locator specifying the records to count.
    locator: BoxLocator,
}

/// Count records.
pub(crate) async fn run(ctx: Context, opt: Opt) -> Result<()> {
    // Figure out what table schema to use.
    let schema = {
        let schema_locator = opt.schema.as_ref().unwrap_or(&opt.locator);
        schema_locator
            .schema(ctx.clone())
            .await
            .with_context(|_| format!("error reading schema from {}", opt.locator))?
            .ok_or_else(|| {
                format_err!("don't know how to read schema from {}", opt.locator)
            })
    }?;

    // Build our shared arguments. Specify 1 for `max_streams` until we actually
    // implement local counting.
    let temporary_storage = TemporaryStorage::new(opt.temporaries.clone());
    let shared_args = SharedArguments::new(schema, temporary_storage, 1);

    // Build our source arguments.
    let from_args = DriverArguments::from_cli_args(&opt.from_args)?;
    let source_args = SourceArguments::new(from_args, opt.where_clause.clone());

    let count = opt
        .locator
        .count(ctx.clone(), shared_args, source_args)
        .await?;
    println!("{}", count);
    Ok(())
}
