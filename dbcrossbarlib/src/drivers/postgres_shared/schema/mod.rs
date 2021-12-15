//! PostgreSQL schemas.

use std::{fmt, sync::Arc};

use super::{catalog, CheckCatalog, PgCreateTable, PgCreateType, PgName};
use crate::common::*;
use crate::parse_error::{Annotation, FileInfo, ParseError};

mod schema_sql;

/// A collection of PostgreSQL `CREATE TABLE` and `CREATE TYPE` defintions.
///
/// **This does _not_ correspond to what PostgreSQL calls a "schema"!** A
/// PostgreSQL "schema" is a namespace. This is what `dbcrossbar` calls a
/// schema, which is a collection of named tables and types.
#[derive(Clone, Debug)]
pub(crate) struct PgSchema {
    pub(crate) types: Vec<PgCreateType>,
    pub(crate) tables: Vec<PgCreateTable>,
}

impl PgSchema {
    /// Parse a source file containing PostgreSQL `CREATE TABLE` and `CREATE
    /// TYPE` statements.
    pub(crate) fn parse(
        file_name: String,
        file_contents: String,
    ) -> Result<Self, ParseError> {
        let file_info = Arc::new(FileInfo::new(file_name, file_contents));
        schema_sql::parse(&file_info.contents).map_err(|err| {
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

    /// Look up `full_table_name` in the database, and return a new
    /// `PgCreateTable` based on what we find in `pg_catalog`.
    ///
    /// Returns `None` if no matching table exists.
    pub(crate) async fn from_pg_catalog(
        ctx: &Context,
        database_url: &UrlWithHiddenPassword,
        table_name: &PgName,
    ) -> Result<Option<Self>> {
        catalog::fetch_from_url(ctx, database_url, table_name).await
    }

    /// Look up `full_table_name` in the database, and return a new
    /// `PgCreateTable` based on what we find in `pg_catalog`.
    ///
    /// If this fails, use `full_table_name` and `default` to construct a new
    /// table.
    pub(crate) async fn from_pg_catalog_or_default(
        ctx: &Context,
        check_catalog: CheckCatalog,
        database_url: &UrlWithHiddenPassword,
        table_name: &PgName,
        default: &Schema,
    ) -> Result<Self> {
        // If we can't find a catalog in the database, use this one.
        let default_dest_schema =
            Self::from_schema_and_name(ctx, default, table_name)?;

        // Should we check the catalog to see if the table schema exists?
        match check_catalog {
            // Nope, we just want to use the default.
            CheckCatalog::No => Ok(default_dest_schema),

            // See if the table is listed in the catalog.
            CheckCatalog::Yes => {
                let opt_dest_schema =
                    Self::from_pg_catalog(ctx, database_url, table_name).await?;
                Ok(match opt_dest_schema {
                    Some(dest_schema) => {
                        dest_schema.aligned_with(&default_dest_schema)?
                    }
                    None => default_dest_schema,
                })
            }
        }
    }

    /// Construct a PostgreSQL schema from a portable schema and a table name
    /// (which will be used instead of the name in the schema).
    pub(crate) fn from_schema_and_name(
        _ctx: &Context,
        schema: &Schema,
        name: &PgName,
    ) -> Result<Self> {
        let types = schema
            .named_data_types
            .values()
            .map(PgCreateType::from_named_data_type)
            .collect::<Result<Vec<_>>>()?;
        let tables = vec![PgCreateTable::from_name_and_columns(
            schema,
            name.to_owned(),
            &schema.table.columns,
        )?];
        Ok(PgSchema { types, tables })
    }

    /// Convert to a portable schema.
    pub(crate) fn to_schema(&self) -> Result<Schema> {
        if self.tables.len() != 1 {
            Err(format_err!("Postgres schema must contain exactly 1 table"))
        } else {
            let types = self
                .types
                .iter()
                .map(|ty| ty.to_named_data_type())
                .collect::<Result<Vec<_>>>()?;
            let table = self.tables[0].to_table()?;
            Schema::from_types_and_table(types, table)
        }
    }

    /// Return either the sole table associated with this schema, or an error.
    pub(crate) fn table(&self) -> Result<&PgCreateTable> {
        if self.tables.len() != 1 {
            Err(format_err!(
                "expected PostgreSQL schema to contain only one table"
            ))
        } else {
            Ok(&self.tables[0])
        }
    }

    /// Return either the sole table associated with this schema, or an error.
    pub(crate) fn table_mut(&mut self) -> Result<&mut PgCreateTable> {
        if self.tables.len() != 1 {
            Err(format_err!(
                "expected PostgreSQL schema to contain only one table"
            ))
        } else {
            Ok(&mut self.tables[0])
        }
    }

    /// Create a new scehma based on this schema, but with columns matching the
    /// the names and order of the columns in `other_table`. This is useful if
    /// we want to insert from `other_schema`'s table into `self`'s table.
    pub(crate) fn aligned_with(&self, other_schema: &PgSchema) -> Result<PgSchema> {
        // Get the table from each schema, erroring out if we have more than one
        // at this point, and align them.
        let self_table = self.table()?;
        let other_table = other_schema.table()?;
        let aligned_table = self_table.aligned_with(other_table)?;
        Ok(PgSchema {
            tables: vec![aligned_table],
            types: self.types.clone(),
        })
    }

    /// Write a `COPY (SELECT ...) TO STDOUT ...` statement for this schema's
    /// table.
    pub(crate) fn write_export_sql(
        &self,
        f: &mut dyn Write,
        source_args: &SourceArguments<Verified>,
    ) -> Result<()> {
        self.table()?.write_export_sql(f, source_args)
    }

    /// Write a `SELECT ...` statement for this schema's table.
    pub(crate) fn write_export_select_sql(
        &self,
        f: &mut dyn Write,
        source_args: &SourceArguments<Verified>,
    ) -> Result<()> {
        self.table()?.write_export_select_sql(f, source_args)
    }

    /// Write a `SELECT COUNT(*) ...` statement for this schema's table.
    pub(crate) fn write_count_sql(
        &self,
        f: &mut dyn Write,
        source_args: &SourceArguments<Verified>,
    ) -> Result<()> {
        self.table()?.write_count_sql(f, source_args)
    }
}

impl fmt::Display for PgSchema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for ty in &self.types {
            writeln!(f, "{}", ty)?;
        }
        for tb in &self.tables {
            write!(f, "{}", tb)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    use super::*;
    use crate::schema::{Column, DataType, NamedDataType, Srid};

    #[test]
    fn simple_table() {
        let input = include_str!("schema_sql_example.sql");
        let pg_schema =
            PgSchema::parse("test.sql".to_owned(), input.to_owned()).unwrap();
        let table = pg_schema.to_schema().unwrap();
        let mut expected_named_data_types = HashMap::new();
        expected_named_data_types.insert(
            "color".to_owned(),
            NamedDataType {
                name: "color".to_owned(),
                data_type: DataType::OneOf(vec![
                    "red".to_owned(),
                    "green".to_owned(),
                    "blue".to_owned(),
                ]),
            },
        );
        // mood
        expected_named_data_types.insert(
            "mood".to_owned(),
            NamedDataType {
                name: "mood".to_owned(),
                data_type: DataType::OneOf(vec![
                    "happy".to_owned(),
                    "sad".to_owned(),
                    "amused".to_owned(),
                ]),
            },
        );
        let expected = Schema {
            named_data_types: expected_named_data_types,
            table: Table {
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
                    Column {
                        name: "l".to_string(),
                        is_nullable: true,
                        data_type: DataType::Named("color".to_owned()),
                        comment: None,
                    },
                    Column {
                        name: "m".to_string(),
                        is_nullable: true,
                        data_type: DataType::Named("mood".to_owned()),
                        comment: None,
                    },
                ],
            },
        };
        assert_eq!(table, expected);

        // Now try writing and re-reading.
        let mut out = vec![];
        write!(&mut out, "{}", &pg_schema).expect("error writing table");
        let pg_parsed_again =
            PgSchema::parse("test.sql".to_owned(), String::from_utf8(out).unwrap())
                .expect("error re-parsing table");
        let parsed_again = pg_parsed_again.to_schema().unwrap();
        assert_eq!(parsed_again, expected);
    }
}
