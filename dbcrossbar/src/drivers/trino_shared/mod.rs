//! Common Trino-related types, used by both [`super::trino_sql`] and
//! [`super::trino`] drivers.

use std::fmt;

use lazy_static::lazy_static;
use regex::Regex;

use crate::common::*;

mod create_table;
mod data_type;

pub use create_table::TrinoCreateTable;
pub use data_type::TrinoDataType;

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
        Ok(Self(ident.to_ascii_lowercase()))
    }

    /// Get the underlying string.
    pub fn as_unquoted_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TrinoIdent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if is_valid_bare_ident(&self.0) {
            write!(f, "{}", self.0)
        } else {
            // Double any double quotes in the identifier.
            let escaped = self.0.replace('"', r#""""#);
            write!(f, r#""{}""#, escaped)
        }
    }
}

/// Is `s` a valid bare identifier?
///
/// TODO: Handle reserved words. Should we just quote everything, always?
fn is_valid_bare_ident(s: &str) -> bool {
    lazy_static! {
        // Note that we don't allow a leading `_`. That's what the docs say.
        static ref RE: Regex = Regex::new(r"^[a-zA-Z][a-zA-Z0-9_]*$").unwrap();
    }
    RE.is_match(s)
}

/// A Trino table name. May include catalog and schema.
#[derive(Clone, Debug)]
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
    pub fn table(table: &str) -> Result<Self> {
        Ok(Self::Table(TrinoIdent::new(table)?))
    }

    /// Create a new `TrinoTableName` from a schema and table.
    pub fn schema(schema: &str, table: &str) -> Result<Self> {
        Ok(Self::Schema(
            TrinoIdent::new(schema)?,
            TrinoIdent::new(table)?,
        ))
    }

    /// Create a new `TrinoTableName` from a catalog, schema, and table.
    pub fn catalog(catalog: &str, schema: &str, table: &str) -> Result<Self> {
        Ok(Self::Catalog(
            TrinoIdent::new(catalog)?,
            TrinoIdent::new(schema)?,
            TrinoIdent::new(table)?,
        ))
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
