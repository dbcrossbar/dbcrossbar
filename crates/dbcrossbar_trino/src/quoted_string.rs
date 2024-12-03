//! Formatting wrapper for quoted strings.

use std::fmt;

/// Formatting wrapper for quoted strings.
pub struct QuotedString<'a>(pub &'a str);

impl fmt::Display for QuotedString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "'{}'", self.0.replace("'", "''"))
    }
}
