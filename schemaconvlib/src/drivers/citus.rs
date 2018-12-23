//! Tools for interfacing with PostgreSQL Citus workers.
//!
//! Based on [`fdy-citus`][fdy-citus].
//!
//! [fdy-citus]: https://github.com/faradayio/fdy-citus/blob/master/src/placements.js

// See https://github.com/diesel-rs/diesel/issues/1785
#![allow(missing_docs, proc_macro_derive_resolution_fallback)]

use diesel::{pg::PgConnection, prelude::*, sql_query, sql_types::*, QueryableByName};
use failure::{format_err, ResultExt};
use std::result;
use try_from::TryInto;
use url::Url;

use crate::Result;

/// Information about a single shard of a table.
///
/// We derive `QueryableByName` and declare `sql_type` on each field because
/// this doesn't correspond to an underlying table. It's just the result of a
/// raw SQL query using `sql_query`.
#[derive(Debug, QueryableByName)]
pub struct ShardInfo {
    #[sql_type = "Bigint"]
    shardid: i64,

    #[sql_type = "Nullable<Text>"]
    shardname: Option<String>,

    #[sql_type = "Nullable<Text>"]
    nodename: Option<String>,

    #[sql_type = "Nullable<Integer>"]
    nodeport: Option<i32>,
}

impl ShardInfo {
    /// Find a human-readable name for this shard.
    pub fn name(&self) -> Result<&str> {
        self.shardname
            .as_ref()
            .map(|n| &n[..])
            .ok_or_else(|| format_err!("missing shard name for {:?}", self))
    }

    /// Build a URL which provides direct access to this shard, using the
    /// database URL of the controller node.
    pub fn url(&self, controller_url: &Url) -> Result<Url> {
        match (&self.nodename, self.nodeport) {
            (Some(nodename), Some(nodeport)) => {
                // Make sure the port is actually a `u16`. This requires some
                // manual type annotations.
                let port_result: result::Result<u16, _> = nodeport.try_into();
                let port = port_result.context("shard port is out of range")?;

                // Copy our Citus controller URL and modify the necessary fields
                // to get a shard URL.
                let mut url = controller_url.to_owned();
                url.set_host(Some(nodename))
                    .context("could not set shard host")?;
                url.set_port(Some(port))
                    .map_err(|_| format_err!("could not set shard port"))?;
                Ok(url)
            }
            _ => Err(format_err!("missing data about shard {:?}", self)),
        }
    }
}

/// SQL used to query for shard information.
///
/// We use a raw SQL query because there are too many things like `regclass` and
/// `shard_name` that aren't directly supported by `diesel` (AFAIK), and it
/// would be too much work to define custom wrappers for everything.
///
/// This is adapted directly from previously internal code by @madd512.
const SHARDS_FOR_TABLE_SQL: &str = r#"
SELECT
    shard.shardid AS shardid,
    shard_name(shard.logicalrelid, shard.shardid) AS shardname,
    placement.nodename AS nodename,
    placement.nodeport AS nodeport
  FROM pg_dist_shard shard
  INNER JOIN pg_dist_shard_placement placement
  ON (shard.shardid = placement.shardid)
  WHERE shard.logicalrelid::regclass = $1::regclass
"#;

/// Fetch the shards for the named table.
pub fn citus_shards(table: &str, dconn: &PgConnection) -> Result<Vec<ShardInfo>> {
    Ok(sql_query(SHARDS_FOR_TABLE_SQL)
        .bind::<Text, _>(table)
        .get_results::<ShardInfo>(dconn)
        .context("error querying for Citus shard placement")?)
}
