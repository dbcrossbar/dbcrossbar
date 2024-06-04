//! A Trino-compatible `CREATE TABLE` statement.

use std::fmt;

use crate::common::*;

use super::{TrinoDataType, TrinoIdent, TrinoTableName};

/// A Trino-compatible `CREATE TABLE` statement.
#[derive(Clone, Debug)]
pub struct TrinoCreateTable {
    name: TrinoTableName,
    columns: Vec<TrinoColumn>,
}

impl TrinoCreateTable {
    /// Parse from an SQL string. `path` is used for error messages.
    pub fn parse(path: &str, sql: &str) -> Result<Self> {
        todo!()
    }

    /// Create from a table name and a portable schema.
    pub fn from_schema_and_name(
        schema: &Schema,
        name: &TrinoTableName,
    ) -> Result<Self> {
        todo!()
    }

    /// Convert to a portable schema.
    pub fn to_schema(&self) -> Result<Schema> {
        todo!()
    }
}

impl fmt::Display for TrinoCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TABLE {} (\n    ", self.name)?;
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ",\n    ")?;
            }
            write!(f, "{}", column)?;
        }
        write!(f, "\n);")
    }
}

/// A Trino column.
#[derive(Clone, Debug)]
pub struct TrinoColumn {
    name: TrinoIdent,
    data_type: TrinoDataType,
    is_nullable: bool,
}

impl fmt::Display for TrinoColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if !self.is_nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}
