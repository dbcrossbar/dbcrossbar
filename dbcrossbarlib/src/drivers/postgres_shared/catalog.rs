//! Reading schemas from a PostgreSQL server.
//!
//! TODO: This code needs a fairly thorough overhaul, and careful attention to
//! SRID handling. It should also be converted to use `PgCreateTable`, etc.
//! Basically, this is old `schemaconv` code that we imported with minimal
//! changes.

use diesel::{
    dsl::count_star,
    pg::PgConnection,
    prelude::*,
    sql_function,
    sql_types::{Integer, Text},
};
use std::collections::HashMap;

use super::{PgColumn, PgCreateTable, PgDataType, PgScalarDataType};
use crate::common::*;
use crate::schema::Srid;

sql_function! {
    /// Given the PostgreSQL schema name, table name and column name of a
    /// PostGIS geoemtry column (which must have been correctly set up using
    /// `AddGeometryColumns`), return the SRID used by that column.
    fn find_srid(schema_name: Text, table_name: Text, column_name: Text) -> Integer
}

table! {
    // https://www.postgresql.org/docs/10/infoschema-tables.html
    information_schema.tables (table_catalog, table_schema, table_name) {
        table_catalog -> VarChar,
        table_schema -> VarChar,
        table_name -> VarChar,
    }
}

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
        udt_schema -> VarChar,
        udt_name -> VarChar,
    }
}

#[derive(Queryable)]
#[allow(dead_code)]
struct PgColumnSchema {
    table_catalog: String,
    table_schema: String,
    table_name: String,
    column_name: String,
    ordinal_position: i32,
    is_nullable: String,
    data_type: String,
    udt_schema: String,
    udt_name: String,
}

impl PgColumnSchema {
    /// Get the data type for a column.
    fn data_type(&self) -> Result<PgDataType> {
        pg_data_type(&self.data_type, &self.udt_schema, &self.udt_name)
    }
}

/// Fetch information about a table from the database.
///
/// Returns `None` if no matching table exists.
pub(crate) fn fetch_from_url(
    database_url: &Url,
    full_table_name: &str,
) -> Result<Option<PgCreateTable>> {
    let conn = PgConnection::establish(database_url.as_str())
        .context("error connecting to PostgreSQL")?;
    let (table_schema, table_name) = parse_full_table_name(full_table_name);

    // Check to see if we have a table with this name.
    let table_count = tables::table
        .select(count_star())
        .filter(tables::table_schema.eq(table_schema))
        .filter(tables::table_name.eq(table_name))
        .first::<i64>(&conn)?;
    if table_count == 0 {
        return Ok(None);
    }

    // Look up column information.
    let pg_columns = columns::table
        .filter(columns::table_schema.eq(table_schema))
        .filter(columns::table_name.eq(table_name))
        .order(columns::ordinal_position)
        .load::<PgColumnSchema>(&conn)?;

    // Do we have any PostGIS geometry columns?
    let need_srids = pg_columns
        .iter()
        .any(|c| c.data_type == "USER-DEFINED" && c.udt_name == "geometry");

    // Look up SRIDs for our geometry columns.
    let srid_map = if need_srids {
        columns::table
            .filter(columns::table_schema.eq(table_schema))
            .filter(columns::table_name.eq(table_name))
            .filter(columns::data_type.eq("USER-DEFINED"))
            .filter(columns::udt_name.eq("geometry"))
            .select((
                columns::column_name,
                find_srid(
                    columns::table_schema,
                    columns::table_name,
                    columns::column_name,
                ),
            ))
            .load::<(String, i32)>(&conn)?
            // Now do some Rust iterator magic to construct `Srid` objects, to
            // handle integer range errors, and to convert everything into a
            // nice `HashMap`.
            .into_iter()
            .map(|(name, srid)| Ok((name, Srid::new(cast::u32(srid)?))))
            .collect::<Result<HashMap<String, Srid>>>()?
    } else {
        // If we don't have any geometry columns, then we don't want to run the
        // query, because it's possible that PostGIS isn't installed and we have
        // no `find_srid` function.
        HashMap::new()
    };

    let mut columns = Vec::with_capacity(pg_columns.len());
    for pg_col in pg_columns {
        // Get the data type for our column.
        let data_type = if let Some(srid) = srid_map.get(&pg_col.column_name) {
            PgDataType::Scalar(PgScalarDataType::Geometry(*srid))
        } else {
            pg_col.data_type()?
        };

        // Build our column.
        columns.push(PgColumn {
            name: pg_col.column_name,
            data_type,
            is_nullable: match pg_col.is_nullable.as_str() {
                "YES" => true,
                "NO" => false,
                value => {
                    return Err(format_err!(
                        "Unexpected is_nullable value: {:?}",
                        value,
                    ));
                }
            },
        })
    }

    Ok(Some(PgCreateTable {
        name: full_table_name.to_owned(),
        columns,
        temporary: false,
        if_not_exists: false,
    }))
}

/// Given a name of the form `mytable` or `myschema.mytable`, split it into
/// a `table_schema` and `table_name`.
fn parse_full_table_name(full_table_name: &str) -> (&str, &str) {
    if let Some(pos) = full_table_name.find('.') {
        (&full_table_name[..pos], &full_table_name[pos + 1..])
    } else {
        ("public", full_table_name)
    }
}

#[test]
fn parsing_full_table_name() {
    assert_eq!(parse_full_table_name("mytable"), ("public", "mytable"));
    assert_eq!(parse_full_table_name("other.mytable"), ("other", "mytable"));
}

/// Choose an appropriate `DataType`.
fn pg_data_type(
    data_type: &str,
    _udt_schema: &str,
    udt_name: &str,
) -> Result<PgDataType> {
    if data_type == "ARRAY" {
        // Array element types have their own naming convention, which appears
        // to be "_" followed by the internal udt_name version of PostgreSQL's
        // base types.
        let element_type = match udt_name {
            "_bool" => PgScalarDataType::Boolean,
            "_date" => PgScalarDataType::Date,
            "_float4" => PgScalarDataType::Real,
            "_float8" => PgScalarDataType::DoublePrecision,
            "_int2" => PgScalarDataType::Smallint,
            "_int4" => PgScalarDataType::Int,
            "_int8" => PgScalarDataType::Bigint,
            "_text" => PgScalarDataType::Text,
            "_timestamp" => PgScalarDataType::TimestampWithoutTimeZone,
            "_timestamptz" => PgScalarDataType::TimestampWithTimeZone,
            "_uuid" => PgScalarDataType::Uuid,
            _ => return Err(format_err!("unknown array element {:?}", udt_name)),
        };
        Ok(PgDataType::Array {
            // TODO: Do we actually check the `dimension_count`?
            dimension_count: 1,
            ty: element_type,
        })
    } else if data_type == "USER-DEFINED" {
        match udt_name {
            "citext" => Ok(PgDataType::Scalar(PgScalarDataType::Text)),
            "geometry" => Err(format_err!(
                "cannot extract SRID for geometry columns without database connection"
            )),
            other => Err(format_err!("unknown user-defined data type {:?}", other)),
        }
    } else {
        let ty = match data_type {
            "bigint" => Ok(PgScalarDataType::Bigint),
            "boolean" => Ok(PgScalarDataType::Boolean),
            "character" => Ok(PgScalarDataType::Text),
            "character varying" => Ok(PgScalarDataType::Text),
            "date" => Ok(PgScalarDataType::Date),
            "double precision" => Ok(PgScalarDataType::DoublePrecision),
            "integer" => Ok(PgScalarDataType::Int),
            "json" => Ok(PgScalarDataType::Json),
            "jsonb" => Ok(PgScalarDataType::Jsonb),
            "numeric" => Ok(PgScalarDataType::Numeric),
            "real" => Ok(PgScalarDataType::Real),
            "smallint" => Ok(PgScalarDataType::Smallint),
            "text" => Ok(PgScalarDataType::Text),
            "timestamp with time zone" => Ok(PgScalarDataType::TimestampWithTimeZone),
            "timestamp without time zone" => {
                Ok(PgScalarDataType::TimestampWithoutTimeZone)
            }
            "uuid" => Ok(PgScalarDataType::Uuid),
            other => Err(format_err!("unknown data type {:?}", other)),
        }?;
        Ok(PgDataType::Scalar(ty))
    }
}

#[test]
fn parsing_pg_data_type() {
    let array = |ty| PgDataType::Array {
        dimension_count: 1,
        ty,
    };
    let examples = &[
        // Basic types.
        (
            ("bigint", "pg_catalog", "int8"),
            PgDataType::Scalar(PgScalarDataType::Bigint),
        ),
        (
            ("boolean", "pg_catalog", "bool"),
            PgDataType::Scalar(PgScalarDataType::Boolean),
        ),
        (
            ("character varying", "pg_catalog", "varchar"),
            PgDataType::Scalar(PgScalarDataType::Text),
        ),
        (
            ("date", "pg_catalog", "date"),
            PgDataType::Scalar(PgScalarDataType::Date),
        ),
        (
            ("double precision", "pg_catalog", "float8"),
            PgDataType::Scalar(PgScalarDataType::DoublePrecision),
        ),
        (
            ("integer", "pg_catalog", "int4"),
            PgDataType::Scalar(PgScalarDataType::Int),
        ),
        (
            ("json", "pg_catalog", "json"),
            PgDataType::Scalar(PgScalarDataType::Json),
        ),
        (
            ("jsonb", "pg_catalog", "jsonb"),
            PgDataType::Scalar(PgScalarDataType::Jsonb),
        ),
        (
            ("real", "pg_catalog", "float4"),
            PgDataType::Scalar(PgScalarDataType::Real),
        ),
        (
            ("smallint", "pg_catalog", "int2"),
            PgDataType::Scalar(PgScalarDataType::Smallint),
        ),
        (
            ("text", "pg_catalog", "text"),
            PgDataType::Scalar(PgScalarDataType::Text),
        ),
        (
            ("timestamp without time zone", "pg_catalog", "timestamp"),
            PgDataType::Scalar(PgScalarDataType::TimestampWithoutTimeZone),
        ),
        // Array types.
        (
            ("ARRAY", "pg_catalog", "_bool"),
            array(PgScalarDataType::Boolean),
        ),
        (
            ("ARRAY", "pg_catalog", "_date"),
            array(PgScalarDataType::Date),
        ),
        (
            ("ARRAY", "pg_catalog", "_float4"),
            array(PgScalarDataType::Real),
        ),
        (
            ("ARRAY", "pg_catalog", "_float8"),
            array(PgScalarDataType::DoublePrecision),
        ),
        (
            ("ARRAY", "pg_catalog", "_int2"),
            array(PgScalarDataType::Smallint),
        ),
        (
            ("ARRAY", "pg_catalog", "_int4"),
            array(PgScalarDataType::Int),
        ),
        (
            ("ARRAY", "pg_catalog", "_int8"),
            array(PgScalarDataType::Bigint),
        ),
        (
            ("ARRAY", "pg_catalog", "_text"),
            array(PgScalarDataType::Text),
        ),
        (
            ("ARRAY", "pg_catalog", "_timestamp"),
            array(PgScalarDataType::TimestampWithoutTimeZone),
        ),
        (
            ("ARRAY", "pg_catalog", "_timestamptz"),
            array(PgScalarDataType::TimestampWithTimeZone),
        ),
        (
            ("ARRAY", "pg_catalog", "_uuid"),
            array(PgScalarDataType::Uuid),
        ),
    ];
    for ((data_type, udt_schema, udt_name), expected) in examples {
        assert_eq!(
            &pg_data_type(data_type, udt_schema, udt_name).unwrap(),
            expected,
        );
    }
}
