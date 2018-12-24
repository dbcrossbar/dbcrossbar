//! Print all the shards of a table in a Citus database.
//!
//! Run as:
//!
//! ```sh
//! env DATABASE_URL=... citus_shards $TABLE
//! ```

use diesel::{pg::PgConnection, prelude::*};
use failure::{bail, format_err, ResultExt};
use dbcrossbarlib::{drivers::citus::citus_shards, Result};
use std::env;
use url::Url;

fn main() -> Result<()> {
    // Get our DATABASE_URL from the environment.
    let database_url: Url = env::var("DATABASE_URL")
        .map_err(|_| format_err!("no DATABASE_URL found in environment"))?
        .parse()?;

    // Get our table name from the CLI.
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 1 || args[0].starts_with('-') {
        bail!("Usage: citus_shards <table_name>");
    }
    let table_name = &args[0];

    // Look up our database shards.
    let conn = PgConnection::establish(database_url.as_str())
        .context("could not connect to database")?;
    let shards = citus_shards(table_name, &conn)?;

    // Print information about each shard.
    for shard in shards {
        let name = shard.name()?;
        let url = shard.url(&database_url)?;
        println!("{} {}", name, url);
    }

    Ok(())
}
