//! Parser for PostgreSQL `CREATE TABLE` declarations.

use failure::ResultExt;

use crate::table::Table;
use crate::Result;

/// Include our `rust-peg` grammar.
///
/// We disable lots of clippy warnings because this is machine-generated code.
#[allow(clippy::style, clippy::complexity, clippy::perf)]
mod grammar {
    include!(concat!(env!("OUT_DIR"), "/postgres.rs"));
}

/// Parse a PostgreSQL `CREATE TABLE` statement and return a `Table`.
pub fn parse_create_table(input: &str) -> Result<Table> {
    Ok(grammar::create_table(input)
        .context("error parsing Postgres `CREATE TABLE`")?)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_table() {
        let input = include_str!("postgres_example.sql");
        parse_create_table(input).unwrap();
    }
}
