//! BigQuery table names.

use lazy_static::lazy_static;
use regex::Regex;
use std::{fmt, str::FromStr};

use crate::common::*;
use crate::drivers::bigquery::BigQueryLocator;

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
    /// Return the name of the table's project.
    pub(crate) fn project(&self) -> &str {
        &self.project
    }

    /// Return the name of the table's dataset.
    pub(crate) fn dataset(&self) -> &str {
        &self.dataset
    }

    /// Return the bare table name itself, without project or dataset.
    pub(crate) fn table(&self) -> &str {
        &self.table
    }

    /// Return a value which will be formatted as
    /// `"\`project\`.\`dataset\`.\`table\`"`, with "backtick" quoting.
    ///
    /// This form of the name is used in BigQuery "standard SQL".
    pub(crate) fn dotted_and_quoted(&self) -> DottedTableName<'_> {
        DottedTableName(self)
    }

    /// Create a temporary table name based on this table name.
    pub(crate) fn temporary_table_name(
        &self,
        temporary_storage: &TemporaryStorage,
    ) -> Result<TableName> {
        lazy_static! {
            static ref DATASET_RE: Regex =
                Regex::new("^([^:.]+):([^:.]+)$").expect("invalid regex in source");
        }

        // Decide on what project and dataset to use.
        let temp = temporary_storage.find_scheme(BigQueryLocator::scheme());
        let (project, dataset) = if let Some(temp) = temp {
            // We have a `--temporary=bigquery:...` argument, so extract a project
            // and dataset name.
            let cap = DATASET_RE
                .captures(&temp[BigQueryLocator::scheme().len()..])
                .ok_or_else(|| {
                    format_err!("could not parse BigQuery dataset name: {:?}", temp)
                })?;
            (cap[1].to_owned(), cap[2].to_owned())
        } else {
            // We don't have a `--temporary=bigquery:...` argument, so just pick
            // something.
            (self.project.clone(), self.dataset.clone())
        };

        let tag = TemporaryStorage::random_tag();
        let table = format!("temp_{}_{}", self.table, tag);
        Ok(TableName {
            project,
            dataset,
            table,
        })
    }
}

#[test]
fn temporary_table_name() {
    let table_name = "project:dataset.table".parse::<TableName>().unwrap();

    // Construct a temporary table name without a `--temporary` argument.
    let default_temp_name = table_name
        .temporary_table_name(&TemporaryStorage::new(vec![]))
        .unwrap()
        .to_string();
    assert!(default_temp_name.starts_with("project:dataset.temp_table_"));

    // Now try it with a `--temporary` argument.
    let temporary_storage =
        TemporaryStorage::new(vec!["bigquery:project2:temp".to_owned()]);
    let temp_name = table_name
        .temporary_table_name(&temporary_storage)
        .unwrap()
        .to_string();
    assert!(temp_name.starts_with("project2:temp.temp_table_"));
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
            static ref RE: Regex = Regex::new("^([^:.`]+):([^:.`]+).([^:.`]+)$")
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
/// `"\`project\`.\`dataset\`.\`table\`"`, with "backtick" quoting.
///
/// This form of the name is used in BigQuery "standard SQL".
pub(crate) struct DottedTableName<'a>(&'a TableName);

impl fmt::Display for DottedTableName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            Ident(&self.0.project),
            Ident(&self.0.dataset),
            Ident(&self.0.table),
        )
    }
}

/// A BigQuery identifier, for formatting purposes.
pub(crate) struct Ident<'a>(pub(crate) &'a str);

impl fmt::Display for Ident<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.contains('`') {
            // We can't output identifiers containing backticks.
            Err(fmt::Error)
        } else {
            write!(f, "`{}`", self.0)
        }
    }
}
