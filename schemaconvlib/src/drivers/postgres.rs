//! Driver for working with PostgreSQL schemas.

// See https://github.com/diesel-rs/diesel/issues/1785
#![allow(missing_docs, proc_macro_derive_resolution_fallback)]

use diesel::{pg::PgConnection, prelude::*};
use failure::ResultExt;
use std::io::Write;
use url::Url;

use Result;
use table::{Column, Table};

table! {
    // https://www.postgresql.org/docs/10/static/infoschema-columns.html
    information_schema.columns (table_catalog, table_schema, table_name, column_name) {
        table_catalog -> VarChar,
        table_schema -> VarChar,
        table_name -> VarChar,
        column_name -> VarChar,
        ordinal_position -> Integer,
        is_nullable -> VarChar,
        data_type -> VarChar,
    }
}

#[derive(Queryable, Insertable)]
#[table_name = "columns"]
struct PgColumn {
    table_catalog: String,
    table_schema: String,
    table_name: String,
    column_name: String,
    ordinal_position: i32,
    is_nullable: String,
    data_type: String,
}

/// A driver for working with PostgreSQL.
pub struct PostgresDriver;

impl PostgresDriver {
    /// Fetch information about a table from the database.
    pub fn fetch_from_url(database_url: &Url, table: &str) -> Result<Table> {
        let conn = PgConnection::establish(database_url.as_str())
            .context("error connecting to PostgreSQL")?;
        // TODO: Break table into table_schema and table_name, and query for
        // both.
        let pg_columns = columns::table
            .filter(columns::table_name.eq(table))
            .load::<PgColumn>(&conn)?;

        let mut columns = Vec::with_capacity(pg_columns.len());
        for pg_col in pg_columns {
            columns.push(Column {
                name: pg_col.column_name,
                // TODO: This is wrong, especially for arrays and custom types
                // like geometry.
                data_type: pg_col.data_type.parse()?,
                is_nullable: match pg_col.is_nullable.as_str() {
                    "YES" => true,
                    "NO" => false,
                    value => {
                        return Err(format_err!(
                            "Unexpected is_nullable value: {:?}", value,
                        ))
                    }
                },
                comment: None,
            })
        }

        Ok(Table { name: table.to_owned(), columns })
    }

    /// Write out a table's column names as `SELECT` arguments.
    pub fn write_select_args(f: &mut Write, table: &Table) -> Result<()> {
        let mut first: bool = true;
        for col in &table.columns {
            if first {
                first = false;
            } else {
                write!(f, ",")?;
            }
            write!(f, "{:?}", col.name)?;
        }
        Ok(())
    }
}

