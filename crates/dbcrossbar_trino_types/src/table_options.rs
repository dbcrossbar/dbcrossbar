//! Options for creating a table in Trino.

use std::{collections::HashMap, fmt};

use crate::{QuotedString, TrinoIdent};

/// Table options.
#[derive(Debug, Clone, PartialEq)]
pub struct TableOptions(pub HashMap<TrinoIdent, TableOptionValue>);

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
/// This could be replaced with [`crate::test::TrinoValue`], but that pulls in
/// a lot of dependencies we don't otherwise need.
#[derive(Debug, Clone, PartialEq)]
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
