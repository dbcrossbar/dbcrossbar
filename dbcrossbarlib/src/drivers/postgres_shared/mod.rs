//! Code shared between various PostgreSQL-related drivers.

use std::fmt;

mod column;
mod data_type;
mod table;

pub(crate) use self::column::PgColumn;
pub(crate) use self::data_type::{PgDataType, PgScalarDataType};
pub(crate) use self::table::PgCreateTable;

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
        if self.0.contains('.') {
            // Unfortunately, we have no good way to report a more detailed
            // error from here. But we do not way to see "." in names until
            // we're sure that `TableName` is used everywhere it should be.
            //
            // We could take this out later, if we're sure we trust people to
            // always use `TableName` when required.
            return Err(fmt::Error);
        }
        write!(f, "\"")?;
        write!(f, "{}", self.0.replace('"', "\"\""))?;
        write!(f, "\"")?;
        Ok(())
    }
}

/// A PostgreSQL table name, including a possible namespace. This will be
/// formatted with correct quotes.
pub(crate) struct TableName<'a>(pub(crate) &'a str);

impl<'a> fmt::Display for TableName<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (idx, component) in self.0.splitn(2, '.').enumerate() {
            if idx != 0 {
                write!(f, ".")?;
            }
            write!(f, "{}", Ident(component))?;
        }
        Ok(())
    }
}

#[test]
fn table_name_is_quoted_correctly() {
    let formatted = format!("{}", TableName("testme1.lat-\"lon"));
    assert_eq!(formatted, "\"testme1\".\"lat-\"\"lon\"");
}
