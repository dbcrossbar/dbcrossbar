//! Code shared between various PostgreSQL-related drivers.

use std::fmt;

use crate::common::*;

mod catalog;
mod column;
mod data_type;
mod table;

pub(crate) use self::column::PgColumn;
pub(crate) use self::data_type::{PgDataType, PgScalarDataType};
pub(crate) use self::table::{CheckCatalog, PgCreateTable};

/// Escape and quote a PostgreSQL string literal. See the [docs][]. We need this
/// because PostgreSQL doesn't accept `$1`-style escapes in certain places in
/// its SQL grammar.
///
/// [docs]: https://www.postgresql.org/docs/9.2/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE
pub(crate) fn pg_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[test]
fn pg_quote_doubles_single_quotes() {
    let examples = &[
        ("", "''"),
        ("a", "'a'"),
        ("'", "''''"),
        ("'hello'", "'''hello'''"),
    ];
    for &(input, expected) in examples {
        assert_eq!(pg_quote(input), expected);
    }
}

/// A PostgreSQL identifier. This will be printed with quotes as necessary to
/// prevent clashes with keywords.
pub(crate) struct Ident<'a>(pub(crate) &'a str);

impl<'a> fmt::Display for Ident<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "\"")?;
        write!(f, "{}", self.0.replace('"', "\"\""))?;
        write!(f, "\"")?;
        Ok(())
    }
}

/// A PostgreSQL table name, including a possible namespace. This will be
/// formatted with correct quotes.
pub(crate) struct TableName<'a>(pub(crate) &'a str);

impl<'a> TableName<'a> {
    /// Split this `TableName` into an optional namespace and an actual table
    /// name.
    pub(crate) fn split(&self) -> Result<(Option<&str>, &str)> {
        let components = self.0.splitn(2, '.').collect::<Vec<_>>();
        match components.len() {
            1 => Ok((None, components[0])),
            2 => Ok((Some(components[0]), components[1])),
            _ => Err(format_err!("cannot parse table name {:?}", self.0)),
        }
    }
}

impl<'a> fmt::Display for TableName<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let components = self.split().map_err(|err| {
            // TODO: This should use a log, but we can't get access to our
            // logger here without switching away from `slog` or jumping through
            // tons of hoops.
            eprintln!("{}", err);
            fmt::Error
        })?;
        match components {
            (Some(namespace), table) => {
                write!(f, "{}.{}", Ident(namespace), Ident(table))?
            }
            (None, table) => write!(f, "{}", Ident(table))?,
        }
        Ok(())
    }
}

#[test]
fn table_name_is_quoted_correctly() {
    let formatted = format!("{}", TableName("testme1.lat-\"lon"));
    assert_eq!(formatted, "\"testme1\".\"lat-\"\"lon\"");
}
