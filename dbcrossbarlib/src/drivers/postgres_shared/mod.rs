//! Code shared between various PostgreSQL-related drivers.

mod column;
mod data_type;
mod table;

pub(crate) use self::column::{Ident, PgColumn};
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
