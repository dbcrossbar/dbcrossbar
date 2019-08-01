//! A PostgreSQL `CREATE TABLE` declaration.

use std::{fmt, str::FromStr};

use super::{Ident, PgColumn};
use crate::common::*;
use crate::schema::Column;

/// A PostgreSQL table declaration.
///
/// This is marked as `pub` and not `pub(crate)` because of a limitation of the
/// `peg` crate, which can only declare regular `pub` functions, which aren't
/// allowed to expose `pub(crate)` types. But we don't actually want to export
/// this outside of our crate, so we mark it `pub` here but take care to not
/// export it from a `pub` module anywhere.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgCreateTable {
    /// The name of the table.
    pub(crate) name: String,
    /// The columns in the table.
    pub(crate) columns: Vec<PgColumn>,
    /// Only create the table if it doesn't already exist.
    pub(crate) if_not_exists: bool,
}

impl PgCreateTable {
    /// Given a table name and a list of portable columns, construct a
    /// corresponding `PgCreateTable`.
    ///
    /// We don't take a portable `Table` as an argument, because the `name`
    /// contained in the `Table` might be an input table name, something from a
    /// schema, etc., and it's usually a mistake to use it directly without
    /// thinking things through first.
    ///
    /// We set `if_not_exists` to false, but the caller can change this directly
    /// once once the `PgCreateTable` has been created.
    pub(crate) fn from_name_and_columns(
        name: String,
        columns: &[Column],
    ) -> Result<PgCreateTable> {
        let pg_columns = columns
            .iter()
            .map(|c| PgColumn::from_column(c))
            .collect::<Result<Vec<PgColumn>>>()?;
        Ok(PgCreateTable {
            name,
            columns: pg_columns,
            if_not_exists: false,
        })
    }

    /// Given a `PgCreateTable`, convert it to a portable `Table`.
    pub(crate) fn to_table(&self) -> Result<Table> {
        let columns = self
            .columns
            .iter()
            .map(|c| c.to_column())
            .collect::<Result<Vec<Column>>>()?;
        Ok(Table {
            name: self.name.clone(),
            columns,
        })
    }

    /// Write a `COPY (SELECT ...) TO STDOUT ...` statement for this table.
    pub(crate) fn write_export_sql(
        &self,
        f: &mut dyn Write,
        query: &Query,
    ) -> Result<()> {
        write!(f, "COPY (")?;
        self.write_export_select_sql(f, query)?;
        write!(f, ") TO STDOUT WITH CSV HEADER")?;
        Ok(())
    }

    /// Write a `SELECT ...` statement for this table.
    pub(crate) fn write_export_select_sql(
        &self,
        f: &mut dyn Write,
        query: &Query,
    ) -> Result<()> {
        write!(f, "SELECT ")?;
        let mut first: bool = true;
        for col in &self.columns {
            if first {
                first = false;
            } else {
                write!(f, ",")?;
            }
            col.write_export_select_expr(f)?;
        }
        write!(f, " FROM {:?}", self.name)?;
        if let Some(where_clause) = &query.where_clause {
            write!(f, " WHERE ({})", where_clause)?;
        }
        Ok(())
    }
}

impl fmt::Display for PgCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE TABLE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        writeln!(f, " {} (", Ident(&self.name))?;
        for (idx, col) in self.columns.iter().enumerate() {
            write!(f, "    {}", col)?;
            if idx + 1 == self.columns.len() {
                writeln!(f)?;
            } else {
                writeln!(f, ",")?;
            }
        }
        writeln!(f, ");")?;
        Ok(())
    }
}

/// Include our `rust-peg` grammar.
///
/// We disable lots of clippy warnings because this is machine-generated code.
#[allow(clippy::all, rust_2018_idioms, elided_lifetimes_in_paths)]
mod grammar {
    include!(concat!(env!("OUT_DIR"), "/create_table_sql.rs"));
}

impl FromStr for PgCreateTable {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(grammar::create_table(s)
            .context("error parsing Postgres `CREATE TABLE`")?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::schema::{Column, DataType, Srid};

    use std::str;

    #[test]
    fn simple_table() {
        let input = include_str!("create_table_sql_example.sql");
        let pg_table: PgCreateTable = input.parse().unwrap();
        let table = pg_table.to_table().unwrap();
        let expected = Table {
            name: "example".to_string(),
            columns: vec![
                Column {
                    name: "a".to_string(),
                    is_nullable: true,
                    data_type: DataType::Text,
                    comment: None,
                },
                Column {
                    name: "b".to_string(),
                    is_nullable: true,
                    data_type: DataType::Int32,
                    comment: None,
                },
                Column {
                    name: "c".to_string(),
                    is_nullable: false,
                    data_type: DataType::Uuid,
                    comment: None,
                },
                Column {
                    name: "d".to_string(),
                    is_nullable: true,
                    data_type: DataType::Date,
                    comment: None,
                },
                Column {
                    name: "e".to_string(),
                    is_nullable: true,
                    data_type: DataType::Float64,
                    comment: None,
                },
                Column {
                    name: "f".to_string(),
                    is_nullable: true,
                    data_type: DataType::Array(Box::new(DataType::Text)),
                    comment: None,
                },
                Column {
                    name: "g".to_string(),
                    is_nullable: true,
                    data_type: DataType::Array(Box::new(DataType::Int32)),
                    comment: None,
                },
                Column {
                    name: "h".to_string(),
                    is_nullable: true,
                    data_type: DataType::GeoJson(Srid::wgs84()),
                    comment: None,
                },
                Column {
                    name: "i".to_string(),
                    is_nullable: true,
                    data_type: DataType::GeoJson(Srid::new(3857)),
                    comment: None,
                },
                Column {
                    name: "j".to_string(),
                    is_nullable: true,
                    data_type: DataType::Int16,
                    comment: None,
                },
                Column {
                    name: "k".to_string(),
                    is_nullable: true,
                    data_type: DataType::TimestampWithoutTimeZone,
                    comment: None,
                },
            ],
        };
        assert_eq!(table, expected);

        // Now try writing and re-reading.
        let mut out = vec![];
        write!(&mut out, "{}", &pg_table).expect("error writing table");
        let pg_parsed_again: PgCreateTable = str::from_utf8(&out)
            .unwrap()
            .parse()
            .expect("error parsing table");
        let parsed_again = pg_parsed_again.to_table().unwrap();
        assert_eq!(parsed_again, expected);
    }
}
