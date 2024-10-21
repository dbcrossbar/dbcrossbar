//! Options for creating a table in Trino.

use std::{collections::HashMap, fmt};

use crate::{Ident, QuotedString};

/// The `WITH (...)` clause of a `CREATE TABLE` statement.
///
/// The internal representation is public, because this is mostly just a
/// formatting wrapper, and users may want to add their own options.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct TableOptions(pub HashMap<Ident, TableOptionValue>);

impl fmt::Display for TableOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }

        write!(f, " WITH (")?;
        let mut first = true;
        for (ident, value) in &self.0 {
            if first {
                first = false;
            } else {
                write!(f, ", ")?;
            }
            write!(f, "{} = {}", ident, value)?;
        }
        write!(f, ")")
    }
}

/// A table option value.
///
/// This could be replaced with [`crate::Value`], but that pulls in a lot
/// of dependencies we don't otherwise need.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum TableOptionValue {
    /// A string value.
    String(String),
    /// A boolean value.
    Boolean(bool),
}

impl fmt::Display for TableOptionValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableOptionValue::String(s) => write!(f, "{}", QuotedString(s)),
            TableOptionValue::Boolean(b) => write!(f, "{}", b),
        }
    }
}
