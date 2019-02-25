//! Driver for working with BigQuery schemas.

use lazy_static::lazy_static;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use regex::Regex;
use std::{fmt, iter, str::FromStr};

use crate::common::*;
use crate::drivers::gs::GsLocator;

mod write_remote_data;

use self::write_remote_data::write_remote_data_helper;

/// URL scheme for `BigQueryLocator`.
pub(crate) const BIGQUERY_SCHEME: &str = "bigquery:";

/// A locator for a BigQuery table.
#[derive(Debug, Clone)]
pub struct BigQueryLocator {
    /// The name of the Google Cloud project.
    pub project: String,
    /// The BigQuery dataset.
    pub dataset: String,
    /// The table.
    pub table: String,
}

impl BigQueryLocator {
    /// Return the full name of table pointed to by this locator.
    fn to_full_table_name(&self) -> String {
        format!("{}:{}.{}", self.project, self.dataset, self.table)
    }

    /// Construct a temporary table name based on our regular table name.
    ///
    /// TODO: We place this in the same data set as the original table, which
    /// may cause problems for people using wildcard table names. I think we may
    /// want some way for users to specify a temporary table name.
    fn temp_table_name(&self) -> String {
        let mut rng = thread_rng();
        let tag = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(5)
            .collect::<String>();
        format!(
            "{}:{}.temp_{}_{}",
            self.project, self.dataset, self.table, tag
        )
    }
}

impl fmt::Display for BigQueryLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bigquery:{}:{}.{}",
            self.project, self.dataset, self.table
        )
    }
}

impl FromStr for BigQueryLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        lazy_static! {
            static ref RE: Regex = Regex::new("^bigquery:([^:.]+):([^:.]+).([^:.]+)$")
                .expect("could not parse built-in regex");
        }
        let cap = RE
            .captures(s)
            .ok_or_else(|| format_err!("could not parse locator: {:?}", s))?;
        let (project, dataset, table) = (&cap[1], &cap[2], &cap[3]);
        Ok(BigQueryLocator {
            project: project.to_string(),
            dataset: dataset.to_string(),
            table: table.to_string(),
        })
    }
}

impl Locator for BigQueryLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn supports_write_remote_data(&self, source: &dyn Locator) -> bool {
        // We can only do `write_remote_data` if `source` is a `GsLocator`.
        // Otherwise, we need to do `write_local_data` like normal.
        source.as_any().is::<GsLocator>()
    }

    fn write_remote_data(
        &self,
        ctx: Context,
        schema: Table,
        source: BoxLocator,
        if_exists: IfExists,
    ) -> BoxFuture<()> {
        write_remote_data_helper(ctx, schema, source, self.to_owned(), if_exists)
            .into_boxed()
    }
}
