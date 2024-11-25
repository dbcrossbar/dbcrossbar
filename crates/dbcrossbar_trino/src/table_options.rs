//! Options for creating a table in Trino.

use std::{collections::HashMap, fmt};

use crate::{pretty::ast::SimpleValue, Ident};

/// The `WITH (...)` clause of a `CREATE TABLE` statement.
///
/// The internal representation is public, because this is mostly just a
/// formatting wrapper, and users may want to add their own options.
///
/// Note that we use [`SimpleValue`], which is available even when the full
/// [`crate::Value`] type is not enabled.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct TableOptions(pub HashMap<Ident, SimpleValue>);

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
