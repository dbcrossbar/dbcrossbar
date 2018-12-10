//! Parser for PostgreSQL `CREATE TABLE` declarations.

use crate::Result;
use crate::table::Table;

/// Include our `rust-peg` grammar.
mod grammar {
    include!(concat!(env!("OUT_DIR"), "/postgres.rs"));
}

/// Parse a PostgreSQL `CREATE TABLE` statement and return a `Table`.
pub fn parse_create_table(input: &str) -> Result<Table> {
    Ok(grammar::create_table(input)?)
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
