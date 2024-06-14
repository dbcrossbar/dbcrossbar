//! Information about Trino's backend storage "catalogs" and the features they
//! support.

//! Trino connector types.

use std::{fmt, str::FromStr};

use crate::common::*;

/// What type of Trino data storage connector are we working with? We need to
/// know this so that we can generate `NOT NULL` and similar SQL features for
/// backends that support, while leaving them out where they'd cause an error.
/// `dbcrossbar`'s goal is always to produce the best representation it can, but
/// there's no one perfect answer for all Trino connectors.
#[allow(missing_docs)]
pub enum TrinoConnectorType {
    Hive,
    Iceberg,
    Memory,
    Postgresql,
    /// Unknown type. Assume worst-case version of all features.
    Other(String),
}

impl TrinoConnectorType {
    /// Do we know that this backend supports `NOT NULL`?
    pub(crate) fn supports_not_null_constraint(&self) -> bool {
        match self {
            TrinoConnectorType::Memory => false,
            TrinoConnectorType::Postgresql => true,
            _ => false,
        }
    }

    /// Do we know that this backend supports `OR REPLACE`?
    pub(crate) fn supports_replace_table(&self) -> bool {
        match self {
            TrinoConnectorType::Memory => false,
            TrinoConnectorType::Postgresql => false,
            _ => false,
        }
    }
}

impl FromStr for TrinoConnectorType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "hive" => TrinoConnectorType::Hive,
            "iceberg" => TrinoConnectorType::Iceberg,
            "memory" => TrinoConnectorType::Memory,
            "postgresql" => TrinoConnectorType::Postgresql,
            other => TrinoConnectorType::Other(other.to_owned()),
        })
    }
}

impl fmt::Display for TrinoConnectorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TrinoConnectorType::Hive => "hive".fmt(f),
            TrinoConnectorType::Iceberg => "iceberg".fmt(f),
            TrinoConnectorType::Memory => "memory".fmt(f),
            TrinoConnectorType::Postgresql => "postgresql".fmt(f),
            TrinoConnectorType::Other(other) => other.fmt(f),
        }
    }
}
