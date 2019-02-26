//! BigQuery table names.

use lazy_static::lazy_static;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use regex::Regex;
use std::{fmt, iter, str::FromStr};

use crate::common::*;

/// A BigQuery table name of the form `"project:dataset.table"`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableName {
    /// The name of the Google Cloud project.
    project: String,
    /// The BigQuery dataset.
    dataset: String,
    /// The table.
    table: String,
}

impl TableName {
    /// Return a value which will be formatted as `"project.dataset.table"`.
    ///
    /// This form of the name is used in BigQuery "standard SQL".
    pub(crate) fn dotted(&self) -> DottedTableName {
        DottedTableName(self)
    }

    /// Create a temporary table name based on this table name.
    pub(crate) fn temporary_table_name(&self) -> TableName {
        let mut rng = thread_rng();
        let tag = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(5)
            .collect::<String>();
        TableName {
            project: self.project.clone(),
            dataset: self.dataset.clone(),
            // TODO: Do we really want to put the temp table in the
            // same dataset? Or would it be safer to use a decidated dataset
            // for temporary tables?
            table: format!("temp_{}_{}", self.table, tag),
        }
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}.{}", self.project, self.dataset, self.table)
    }
}

impl FromStr for TableName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        lazy_static! {
            static ref RE: Regex = Regex::new("^([^:.]+):([^:.]+).([^:.]+)$")
                .expect("could not parse built-in regex");
        }
        let cap = RE.captures(s).ok_or_else(|| {
            format_err!("could not parse BigQuery table name: {:?}", s)
        })?;
        let (project, dataset, table) = (&cap[1], &cap[2], &cap[3]);
        Ok(TableName {
            project: project.to_string(),
            dataset: dataset.to_string(),
            table: table.to_string(),
        })
    }
}

/// A short-lived wrapped type which displays a BigQuery table name as
/// `"project.dataset.table"`.
///
/// This form of the name is used in BigQuery "standard SQL".
pub(crate) struct DottedTableName<'a>(&'a TableName);

impl<'a> fmt::Display for DottedTableName<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.0.project, self.0.dataset, self.0.table)
    }
}
