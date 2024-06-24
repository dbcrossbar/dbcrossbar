//! Common Trino-related types, used by both [`super::trino_sql`] and
//! [`super::trino`] drivers.

use std::fmt;

#[cfg(test)]
use proptest_derive::Arbitrary;

use crate::common::*;

mod ast;
mod connector_type;
mod create_table;
mod data_type;
mod driver_args;
mod pretty;

pub use connector_type::TrinoConnectorType;
pub use create_table::{parse_data_type, TrinoColumn, TrinoCreateTable};
pub use data_type::{TrinoDataType, TrinoField};
pub use driver_args::TrinoDriverArguments;

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

/// A Trino identifier, which [may need to be quoted][idents], depending on
/// contents.
///
/// > Identifiers must start with a letter, and subsequently include
/// > alphanumeric characters and underscores. Identifiers with other characters
/// > must be delimited with double quotes ("). When delimited with double
/// > quotes, identifiers can use any character. Escape a " with another
/// > preceding double quote in a delimited identifier.
/// >
/// > Identifiers are not treated as case sensitive.
///
/// We store identifiers as ASCII lowercase. It's unclear how we should handle
/// Unicode identifiers, so we leave them unchanged for now.
///
/// [idents]: https://trino.io/docs/current/language/reserved.html#language-identifiers
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TrinoIdent(String);

impl TrinoIdent {
    /// Create a new `TrinoIdent`.
    pub fn new(ident: &str) -> Result<Self> {
        if ident.is_empty() {
            Err(format_err!("Trino identifiers cannot be the empty string"))
        } else {
            Ok(Self(ident.to_ascii_lowercase()))
        }
    }

    /// Get the underlying string.
    pub fn as_unquoted_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TrinoIdent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.0.contains('"') {
            // Double any double quotes in the identifier.
            let escaped = self.0.replace('"', r#""""#);
            write!(f, r#""{}""#, escaped)
        } else {
            write!(f, r#""{}""#, self.0)
        }
    }
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
    use proptest::prelude::*;

    use super::*;

    // Only generate identifiers with at least one character.
    impl Arbitrary for TrinoIdent {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: ()) -> Self::Strategy {
            ".+".prop_map(|s| TrinoIdent::new(&s).unwrap()).boxed()
        }
    }

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
