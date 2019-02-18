//! Driver for working with BigQuery schemas.

use lazy_static::lazy_static;
use regex::Regex;
use std::{fmt, str::FromStr};

use crate::common::*;

mod write_schema;

/// URL scheme for `BigQueryLocator`.
pub(crate) const BIGQUERY_SCHEME: &str = "bigquery:";

/// A locator for a BigQuery table.
#[derive(Debug)]
pub struct BigQueryLocator {
    /// The name of the Google Cloud project.
    pub project: String,
    /// The BigQuery dataset.
    pub dataset: String,
    /// The table.
    pub table: String,
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

impl Locator for BigQueryLocator {}

/// URL scheme for `PostgresSqlLocator`.
pub(crate) const BIGQUERY_SCHEMA_SCHEME: &str = "bigquery-schema:";

/// A JSON file containing BigQuery table schema.
#[derive(Debug)]
pub struct BigQuerySchemaLocator {
    path: PathOrStdio,
}

impl fmt::Display for BigQuerySchemaLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(BIGQUERY_SCHEMA_SCHEME, f)
    }
}

impl FromStr for BigQuerySchemaLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(BIGQUERY_SCHEMA_SCHEME, s)?;
        Ok(BigQuerySchemaLocator { path })
    }
}

impl Locator for BigQuerySchemaLocator {
    fn write_schema(
        &self,
        ctx: &Context,
        table: &Table,
        if_exists: IfExists,
    ) -> Result<()> {
        let mut f = self.path.create_sync(ctx, if_exists)?;
        write_schema::write_json(&mut f, table, false)
    }
}
