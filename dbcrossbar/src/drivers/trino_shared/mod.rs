//! Common Trino-related types, used by both [`super::trino_sql`] and
//! [`super::trino`] drivers.

use std::fmt;

pub use dbcrossbar_trino::{ConnectorType as TrinoConnectorType, Ident as TrinoIdent};
#[cfg(test)]
use proptest_derive::Arbitrary;

use crate::common::*;

mod create_table;
mod data_type;
mod driver_args;
mod pretty;

pub use create_table::{parse_data_type, TrinoColumn, TrinoCreateTable};
pub use data_type::{TrinoDataType, TrinoField};
pub use driver_args::TrinoDriverArguments;
pub(crate) use pretty::WIDTH as PRETTY_WIDTH;

/// A Trino string literal. Used for formatting only.
pub(crate) struct TrinoStringLiteral<'a>(pub(crate) &'a str);

impl<'a> fmt::Display for TrinoStringLiteral<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // If the string contains single quotes, double them.
        if self.0.contains('\'') {
            write!(f, "'{}'", self.0.replace('\'', "''"))
        } else {
            write!(f, "'{}'", self.0)
        }
    }
}

#[test]
fn test_trino_string_literal() {
    assert_eq!(TrinoStringLiteral("foo").to_string(), "'foo'");
    assert_eq!(TrinoStringLiteral("foo'bar").to_string(), "'foo''bar'");
}

/// A Trino table name. May include catalog and schema.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(Arbitrary))]
pub enum TrinoTableName {
    /// Just a table name, like `my_table`.
    Table(TrinoIdent),
    /// A table name with a schema, like `my_schema.my_table`.
    Schema(TrinoIdent, TrinoIdent),
    /// A table name with a catalog, like `my_catalog.my_schema.my_table`.
    Catalog(TrinoIdent, TrinoIdent, TrinoIdent),
}

impl TrinoTableName {
    /// Create a new `TrinoTableName` from a table.
    pub fn new(table: &str) -> Result<Self> {
        Ok(Self::Table(TrinoIdent::new(table)?))
    }

    /// Create a new `TrinoTableName` from a schema and table.
    pub fn with_schema(schema: &str, table: &str) -> Result<Self> {
        Ok(Self::Schema(
            TrinoIdent::new(schema)?,
            TrinoIdent::new(table)?,
        ))
    }

    /// Create a new `TrinoTableName` from a catalog, schema, and table.
    pub fn with_catalog(catalog: &str, schema: &str, table: &str) -> Result<Self> {
        Ok(Self::Catalog(
            TrinoIdent::new(catalog)?,
            TrinoIdent::new(schema)?,
            TrinoIdent::new(table)?,
        ))
    }

    /// Get the table name.
    pub fn table(&self) -> &TrinoIdent {
        match self {
            TrinoTableName::Table(table) => table,
            TrinoTableName::Schema(_, table) => table,
            TrinoTableName::Catalog(_, _, table) => table,
        }
    }

    /// Get the schema name, if any.
    pub fn schema(&self) -> Option<&TrinoIdent> {
        match self {
            TrinoTableName::Table(_) => None,
            TrinoTableName::Schema(schema, _) => Some(schema),
            TrinoTableName::Catalog(_, schema, _) => Some(schema),
        }
    }

    /// Get the catalog name, if any.
    pub fn catalog(&self) -> Option<&TrinoIdent> {
        match self {
            TrinoTableName::Table(_) => None,
            TrinoTableName::Schema(_, _) => None,
            TrinoTableName::Catalog(catalog, _, _) => Some(catalog),
        }
    }
}

impl fmt::Display for TrinoTableName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TrinoTableName::Table(table) => write!(f, "{}", table),
            TrinoTableName::Schema(schema, table) => write!(f, "{}.{}", schema, table),
            TrinoTableName::Catalog(catalog, schema, table) => {
                write!(f, "{}.{}.{}", catalog, schema, table)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trino_ident() {
        assert_eq!(TrinoIdent::new("foo").unwrap().to_string(), r#""foo""#);
        assert_eq!(
            TrinoIdent::new("foo\"bar").unwrap().to_string(),
            r#""foo""bar""#
        );
    }

    #[test]
    fn test_trino_table_name() {
        assert_eq!(TrinoTableName::new("foo").unwrap().to_string(), r#""foo""#);
        assert_eq!(
            TrinoTableName::with_schema("bar", "foo")
                .unwrap()
                .to_string(),
            r#""bar"."foo""#,
        );
        assert_eq!(
            TrinoTableName::with_catalog("baz", "bar", "fo o")
                .unwrap()
                .to_string(),
            r#""baz"."bar"."fo o""#,
        );
    }
}
