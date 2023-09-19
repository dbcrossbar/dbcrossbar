//! Reading schemas from a PostgreSQL server.
//!
//! TODO: This code needs a fairly thorough overhaul, and careful attention to
//! SRID handling. It should also be converted to use `PgCreateTable`, etc.
//! Basically, this is old `schemaconv` code that we imported with minimal
//! changes.

use std::collections::HashMap;

use tokio_postgres::Client;

use super::{
    connect, PgColumn, PgCreateTable, PgCreateType, PgCreateTypeDefinition,
    PgDataType, PgName, PgScalarDataType, PgSchema,
};
use crate::common::*;
use crate::schema::Srid;

/*
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
*/

struct PgColumnSchema {
    column_name: String,
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
#[instrument(level = "trace", skip(ctx))]
pub(crate) async fn fetch_from_url(
    ctx: &Context,
    database_url: &UrlWithHiddenPassword,
    table_name: &PgName,
) -> Result<Option<PgSchema>> {
    let client = connect(ctx, database_url).await?;
    let schema = table_name.schema_or_public();
    let table = table_name.name();

    // Check to see if we have a table with this name.
    let count_matching_tables_sql = r#"
SELECT COUNT(*) AS count
FROM information_schema.tables
WHERE
    table_schema = $1 AND
    table_name = $2
"#;
    let row = client
        .query_one(count_matching_tables_sql, &[&schema, &table])
        .await?;
    let table_count: i64 = row.get("count");
    if table_count == 0 {
        return Ok(None);
    }

    // Look up column information.
    let columns_sql = r#"
SELECT column_name, is_nullable, data_type, udt_schema, udt_name
FROM information_schema.columns
WHERE
    table_schema = $1 AND
    table_name = $2
ORDER BY ordinal_position
"#;
    let rows = client.query(columns_sql, &[&schema, &table]).await?;
    let pg_columns = rows
        .into_iter()
        .map(|row| PgColumnSchema {
            column_name: row.get("column_name"),
            is_nullable: row.get("is_nullable"),
            data_type: row.get("data_type"),
            udt_schema: row.get("udt_schema"),
            udt_name: row.get("udt_name"),
        })
        .collect::<Vec<PgColumnSchema>>();

    // Do we have any PostGIS geometry columns?
    let need_srids = pg_columns
        .iter()
        .any(|c| c.data_type == "USER-DEFINED" && c.udt_name == "geometry");

    // Look up SRIDs for our geometry columns.
    let srid_map = if need_srids {
        // This SQL will fail if `Find_SRID` isn't defined. But `Find_SRID` is
        // part of the PostGIS extension, and we've confirmed that we have
        // geometry columns, so we should be fine.
        let srid_sql = r#"
SELECT
    column_name,
    Find_SRID(
        table_schema::TEXT,
        table_name::TEXT,
        column_name::TEXT
    ) AS srid
FROM information_schema.columns
WHERE
    table_schema = $1 AND
    table_name = $2 AND
    data_type = 'USER-DEFINED' AND
    udt_name = 'geometry'
"#;
        let rows = client.query(srid_sql, &[&schema, &table]).await?;
        rows.into_iter()
            .map(|row| {
                let name = row.get("column_name");
                let srid: i32 = row.get("srid");
                Ok((name, Srid::new(u32::try_from(srid)?)))
            })
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

    // Look up any types used by the table.
    let mut types = vec![];
    for col in &columns {
        if let PgDataType::Scalar(PgScalarDataType::Named(type_name)) = &col.data_type
        {
            let pg_create_type = fetch_create_type(&client, type_name)
                .await?
                .ok_or_else(|| {
                    format_err!(
                        "cannot find definiton of user-defined type {} (perhaps it isn't supported?)",
                        type_name.unquoted(),)
                })?;
            types.push(pg_create_type);
        }
    }

    // Build our schema.
    let pg_create_table = PgCreateTable {
        name: table_name.to_owned(),
        columns,
        temporary: false,
        if_not_exists: false,
    };
    let pg_schema = PgSchema {
        types,
        tables: vec![pg_create_table],
    };
    Ok(Some(pg_schema))
}

/// Choose an appropriate `DataType`.
fn pg_data_type(
    data_type: &str,
    udt_schema: &str,
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
            // We don't actually know what this is, so let's just create a
            // `Named` placeholder and let other code figure out if there's an
            // appropriate `PgCreateType` value later.
            _ => Ok(PgDataType::Scalar(PgScalarDataType::Named(PgName::new(
                udt_schema.to_owned(),
                udt_name.to_owned(),
            )))),
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
            "time without time zone" => Ok(PgScalarDataType::TimeWithoutTimeZone),
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
        (
            ("time without time zone", "pg_catalog", "time"),
            PgDataType::Scalar(PgScalarDataType::TimeWithoutTimeZone),
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

/// Look up `type_name`.
///
/// - If it is not defined, return `None`.
/// - If it is defined as an enum, return the enum.
/// - Otherwise, return an error.
pub(crate) async fn fetch_create_type(
    client: &Client,
    type_name: &PgName,
) -> Result<Option<PgCreateType>> {
    let schema = type_name.schema_or_public();
    let base_name = type_name.name();

    // Check to see if a user-defined type of this name exists, and figure out
    // what kind of type it might be.
    //
    // https://www.postgresql.org/docs/9.2/catalog-pg-type.html
    let typtype_sql = "\
SELECT TEXT(t.typtype)
FROM pg_type t
    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = $1 AND t.typname = $2 AND t.typtype = 'e'
";
    trace!(
        "checking for type {}: {}",
        type_name.unquoted(),
        typtype_sql
    );
    let typtypes = client.query(typtype_sql, &[&schema, &base_name]).await?;
    if typtypes.is_empty() {
        // No matching type exists.
        return Ok(None);
    } else if typtypes.len() > 1 {
        return Err(format_err!(
            "found multiple types with name {}",
            type_name.unquoted(),
        ));
    } else if typtypes[0].get::<_, &str>(0) != "e" {
        return Err(format_err!(
            "found unsupported custom type {}",
            type_name.unquoted(),
        ));
    }

    // We know we have an `enum`, so fetch the values.
    let enum_values_sql = "\
SELECT e.enumlabel AS value
FROM pg_type t
    JOIN pg_enum e ON t.oid = e.enumtypid
    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = $1 AND t.typname = $2
ORDER BY e.enumsortorder
";
    trace!(
        "fetching enum values {}: {}",
        type_name.unquoted(),
        enum_values_sql
    );
    let enum_values = client
        .query(enum_values_sql, &[&schema, &base_name])
        .await?
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>();
    let pg_create_type = PgCreateType {
        name: type_name.to_owned(),
        definition: PgCreateTypeDefinition::Enum(enum_values),
    };
    trace!("looked up type definition {:?}", pg_create_type);
    Ok(Some(pg_create_type))
}
