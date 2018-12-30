//! Driver for working with BigQuery schemas.

use failure::format_err;
use lazy_static::lazy_static;
use regex::Regex;
use serde_json;
use std::{fmt, io::Write, path::PathBuf, str::FromStr};

use crate::{Error, Locator, Result};
use crate::path_or_stdio::PathOrStdio;
use crate::schema::Table;

mod schema;

/// URL scheme for `BigQueryLocator`.
pub(crate) const BIGQUERY_SCHEME: &str = "bigquery:";

/// A locator for a BigQuery table.
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
pub(crate) const BIGQUERY_JSON_SCHEME: &str = "bigquery.json:";

/// A JSON file containing BigQuery table schema.
pub struct BigQueryJsonLocator {
    path: PathOrStdio,
}

impl fmt::Display for BigQueryJsonLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt_locator_helper(BIGQUERY_JSON_SCHEME, f)
    }
}

impl FromStr for BigQueryJsonLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let path = PathOrStdio::from_str_locator_helper(BIGQUERY_JSON_SCHEME, s)?;
        Ok(BigQueryJsonLocator { path })
    }
}

impl Locator for BigQueryJsonLocator {
    fn write_schema(&self, table: &Table) -> Result<()> {
        self.path.create(|f| {
            schema::BigQueryDriver::write_json(f, table, false)
        })
    }
}
