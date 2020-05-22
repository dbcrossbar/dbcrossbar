//! A PostgreSQL `CREATE TABLE` declaration.

use std::{collections::HashMap, fmt, iter::FromIterator, sync::Arc};

use super::{catalog, PgColumn, TableName};
use crate::common::*;
use crate::parse_error::{Annotation, FileInfo, ParseError};
use crate::schema::Column;
use crate::separator::Separator;

mod create_table_sql;

/// Should we check the PostgreSQL catalog for a schema, or just use the one we
/// were given?
///
/// This is basically a fancy boolean that exists in order to make the related
/// logic clear at a glance, and easy to verify.
pub(crate) enum CheckCatalog {
    /// Check the PostgreSQL catalog for an existing schema.
    Yes,
    /// Always use the schema given by the user.
    No,
}

impl From<&IfExists> for CheckCatalog {
    fn from(if_exists: &IfExists) -> CheckCatalog {
        match if_exists {
            IfExists::Error | IfExists::Overwrite => CheckCatalog::No,
            IfExists::Append | IfExists::Upsert(_) => CheckCatalog::Yes,
        }
    }
}

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
    /// Create a temporary table local to a specific client session.
    pub(crate) temporary: bool,
}

impl PgCreateTable {
    /// Parse a source file containing a PostgreSQL `CREATE TABLE` statement.
    pub(crate) fn parse(
        file_name: String,
        file_contents: String,
    ) -> Result<Self, ParseError> {
        let file_info = Arc::new(FileInfo::new(file_name, file_contents));
        create_table_sql::parse(&file_info.contents).map_err(|err| {
            ParseError::new(
                file_info,
                vec![Annotation::primary(
                    err.location.offset,
                    format!("expected {}", err.expected),
                )],
                "error parsing Postgres CREATE TABLE",
            )
        })
    }

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
            temporary: false,
        })
    }

    /// Look up `full_table_name` in the database, and return a new
    /// `PgCreateTable` based on what we find in `pg_catalog`.
    ///
    /// Returns `None` if no matching table exists.
    pub(crate) async fn from_pg_catalog(
        database_url: &UrlWithHiddenPassword,
        full_table_name: &str,
    ) -> Result<Option<PgCreateTable>> {
        let database_url = database_url.to_owned();
        let full_table_name = full_table_name.to_owned();
        spawn_blocking(move || {
            catalog::fetch_from_url(&database_url, &full_table_name)
        })
        .await
    }

    /// Look up `full_table_name` in the database, and return a new
    /// `PgCreateTable` based on what we find in `pg_catalog`.
    ///
    /// If this fails, use `full_table_name` and `default` to construct a new
    /// table.
    pub(crate) async fn from_pg_catalog_or_default(
        check_catalog: CheckCatalog,
        database_url: &UrlWithHiddenPassword,
        full_table_name: &str,
        default: &Table,
    ) -> Result<PgCreateTable> {
        // If we can't find a catalog in the database, use this one.
        let default_dest_table = PgCreateTable::from_name_and_columns(
            full_table_name.to_owned(),
            &default.columns,
        )?;

        // Should we check the catalog to see if the table schema exists?
        match check_catalog {
            // Nope, we just want to use the default.
            CheckCatalog::No => Ok(default_dest_table),

            // See if the table is listed in the catalog.
            CheckCatalog::Yes => {
                let opt_dest_table =
                    PgCreateTable::from_pg_catalog(database_url, full_table_name)
                        .await?;
                Ok(match opt_dest_table {
                    Some(dest_table) => {
                        dest_table.aligned_with(&default_dest_table)?
                    }
                    None => default_dest_table,
                })
            }
        }
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

    /// Create a new table based on this table, but with columns matching the
    /// the names and order of the columns in `other_table`. This is useful if
    /// we want to insert from `other_table` into `self`.
    ///
    /// Hypothetically, we could also check for compatibility between column
    /// types in the two tables, but for now, we're happy to let the database
    /// verify all that for us.
    pub(crate) fn aligned_with(
        &self,
        other_table: &PgCreateTable,
    ) -> Result<PgCreateTable> {
        let column_map = HashMap::<&str, &PgColumn>::from_iter(
            self.columns.iter().map(|c| (&c.name[..], c)),
        );
        Ok(PgCreateTable {
            name: self.name.clone(),
            columns: other_table
                .columns
                .iter()
                .map(|c| {
                    if let Some(&col) = column_map.get(&c.name[..]) {
                        Ok(col.to_owned())
                    } else {
                        Err(format_err!(
                            "could not find column {} in destination table",
                            c.name
                        ))
                    }
                })
                .collect::<Result<Vec<_>>>()?,
            if_not_exists: self.if_not_exists,
            temporary: self.temporary,
        })
    }

    /// Write a `COPY (SELECT ...) TO STDOUT ...` statement for this table.
    pub(crate) fn write_export_sql(
        &self,
        f: &mut dyn Write,
        source_args: &SourceArguments<Verified>,
    ) -> Result<()> {
        write!(f, "COPY (")?;
        self.write_export_select_sql(f, source_args)?;
        write!(f, ") TO STDOUT WITH CSV HEADER")?;
        Ok(())
    }

    /// Write a `SELECT ...` statement for this table.
    pub(crate) fn write_export_select_sql(
        &self,
        f: &mut dyn Write,
        source_args: &SourceArguments<Verified>,
    ) -> Result<()> {
        write!(f, "SELECT ")?;
        if self.columns.is_empty() {
            return Err(format_err!("cannot export 0 columns"));
        }
        let mut sep = Separator::new(",");
        for col in &self.columns {
            write!(f, "{}", sep.display())?;
            col.write_export_select_expr(f)?;
        }
        write!(f, " FROM {}", TableName(&self.name))?;
        if let Some(where_clause) = source_args.where_clause() {
            write!(f, " WHERE ({})", where_clause)?;
        }
        Ok(())
    }

    /// Write a `SELECT COUNT(*) ...` statement for this table.
    pub(crate) fn write_count_sql(
        &self,
        f: &mut dyn Write,
        source_args: &SourceArguments<Verified>,
    ) -> Result<()> {
        writeln!(f, "SELECT COUNT(*)")?;
        writeln!(f, " FROM {}", TableName(&self.name))?;
        if let Some(where_clause) = source_args.where_clause() {
            writeln!(f, " WHERE ({})", where_clause)?;
        }
        Ok(())
    }
}

impl fmt::Display for PgCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE")?;
        if self.temporary {
            write!(f, " TEMPORARY")?;
        }
        write!(f, " TABLE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        writeln!(f, " {} (", TableName(&self.name))?;
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::schema::{Column, DataType, Srid};

    #[test]
    fn simple_table() {
        let input = include_str!("create_table_sql_example.sql");
        let pg_table =
            PgCreateTable::parse("test.sql".to_owned(), input.to_owned()).unwrap();
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
        let pg_parsed_again = PgCreateTable::parse(
            "test.sql".to_owned(),
            String::from_utf8(out).unwrap(),
        )
        .expect("error re-parsing table");
        let parsed_again = pg_parsed_again.to_table().unwrap();
        assert_eq!(parsed_again, expected);
    }
}
