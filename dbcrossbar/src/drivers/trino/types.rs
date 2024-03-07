//! Trino data types, and converting to and from `dbcrossbar` data types.

use std::{collections::HashMap, fmt, str::FromStr};

use prusto::{Client, Presto};

use crate::{
    common::*,
    drivers::trino::errors::abbreviate_trino_error,
    schema::{Column, DataType, Srid, StructField},
};

/// A table declaration.
pub(crate) struct TrinoTable {
    pub(crate) name: TrinoTableName,
    pub(crate) columns: Vec<TrinoColumn>,
}

impl TrinoTable {
    /// Read information about a table from the database.
    pub(crate) async fn from_database(
        client: &Client,
        table_name: TrinoTableName,
    ) -> Result<TrinoTable> {
        #[derive(Debug, Presto)]
        #[allow(non_snake_case)]
        struct Col {
            col: String,
            ty: String,
            is_nullable: bool,
        }

        let sql = format!(
            "SELECT column_name AS col, data_type AS ty, (is_nullable = 'YES') AS is_nullable
            FROM memory.information_schema.columns
            WHERE table_catalog = {catalog} AND table_schema = {schema} AND table_name = {table_name}",
            catalog = TrinoString(&table_name.catalog),
            schema = TrinoString(&table_name.schema),
            table_name = TrinoString(&table_name.table)
        );
        let dataset = retry_trino_error! {
            client.get_all::<Col>(sql.clone()).await
        }
        .map_err(|err| abbreviate_trino_error(&sql, err))
        .with_context(|| {
            format!("Failed to get columns for table: {}", table_name.table)
        })?;
        let columns = dataset
            .into_vec()
            .into_iter()
            .map(|c| {
                Ok(TrinoColumn {
                    name: ColumnName::new(c.col),
                    is_nullable: c.is_nullable,
                    data_type: c.ty.parse()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(TrinoTable {
            name: table_name,
            columns,
        })
    }

    /// Convert to a portable table schema.
    pub(crate) fn to_schema(&self) -> Result<Schema> {
        // What we're building.
        //
        // pub struct Schema {
        //     pub(crate) named_data_types: HashMap<String, NamedDataType, RandomState>,
        //     pub(crate) table: Table,
        // }
        //
        // /// Information about a table.
        // #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
        // #[serde(deny_unknown_fields)]
        // pub struct Table {
        //     /// The name of the table.
        //     pub name: String,
        //
        //     /// Information about the table's columns.
        //     pub columns: Vec<Column>,
        // }

        let columns = self
            .columns
            .iter()
            .map(|c| Column {
                name: c.name.unescaped().to_owned(),
                is_nullable: c.is_nullable,
                data_type: c.data_type.to_data_type(),
                comment: None,
            })
            .collect();

        let table = Table {
            name: self.name.unquoted(),
            columns,
        };

        Ok(Schema {
            named_data_types: HashMap::default(),
            table,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TrinoTableName {
    pub(crate) catalog: String,
    pub(crate) schema: String,
    pub(crate) table: String,
}

impl TrinoTableName {
    /// Return as an unquoted table name.
    pub(crate) fn unquoted(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// A column declartion.
pub(crate) struct TrinoColumn {
    pub(crate) name: ColumnName,
    pub(crate) data_type: TrinoDataType,
    pub(crate) is_nullable: bool,
}

/// A column name. This is wrapped in its own type to prevent people from
/// interpolating it into SQL strings, which would be a SQL injection
/// vulnerability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnName(String);

impl ColumnName {
    /// Create a new column name.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Unnescaped column name.
    pub fn unescaped(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ColumnName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.contains('`') {
            // TODO: Can we escape this?
            return fmt::Result::Err(fmt::Error);
        }
        write!(f, "`{}`", self.0)
    }
}

/// Trino data types. This only contains types that correspond to a `dbcrossbar`
/// type, not other types like `P4HyperLogLog`.
///
/// See [the Trino
/// documentation](https://trino.io/docs/current/language/types.html).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TrinoDataType {
    Boolean,

    SmallInt,
    Integer,
    BigInt,

    Real,
    Double,

    Decimal {
        precision: u32,
        scale: u32,
    },

    Varchar,
    //Varbinary,
    Json,

    Date,
    Timestamp {
        scale: u32,
    },
    TimestampWithTimeZone {
        scale: u32,
    },

    Array(Box<TrinoDataType>),
    Row(Vec<TrinoStructField>),
    Uuid,

    /// Trino's spherical geography type. There's also a `Geometry` type. This
    /// stuff is barely documented (but see [the `ST_` function
    /// docs](https://github.com/trinodb/docs.trino.io/blob/master/313/_sources/functions/geospatial.rst.txt).
    SphericalGeography,
}

impl TrinoDataType {
    /// Construct a `TrinoDataType` from a `DataType`.
    pub fn for_data_type(schema: &Schema, data_type: &DataType) -> Result<Self> {
        match data_type {
            DataType::Array(elem_ty) => Ok(Self::Array(Box::new(
                TrinoDataType::for_data_type(schema, elem_ty)?,
            ))),
            DataType::Bool => Ok(Self::Boolean),
            DataType::Date => Ok(Self::Date),
            DataType::Decimal => Ok(Self::Decimal {
                precision: 38,
                scale: 9,
            }),
            DataType::Float32 => Ok(Self::Real),
            DataType::Float64 => Ok(Self::Double),
            // Trino does not seem to document their SRID handling, or really
            // much else about their GIS stuff. So let's be conservative, and
            // only map WGS84 to `SphericalGeography`.
            //
            // TODO: Consult a GIS expert.
            DataType::GeoJson(srid) if *srid == Srid::wgs84() => {
                Ok(Self::SphericalGeography)
            }
            DataType::GeoJson(_) => Ok(Self::Json),
            DataType::Int16 => Ok(Self::SmallInt),
            DataType::Int32 => Ok(Self::Integer),
            DataType::Int64 => Ok(Self::BigInt),
            DataType::Json => Ok(Self::Json),
            DataType::Named(name) => {
                let dt = schema.data_type_for_name(name);
                TrinoDataType::for_data_type(schema, dt)
            }
            DataType::OneOf(_) => Ok(Self::Varchar),
            DataType::Struct(rows) => {
                let mut fields = Vec::new();
                for field in rows {
                    fields.push(TrinoStructField {
                        name: Some(ColumnName::new(&field.name)),
                        data_type: TrinoDataType::for_data_type(
                            schema,
                            &field.data_type,
                        )?,
                    });
                }
                Ok(Self::Row(fields))
            }
            DataType::Text => Ok(Self::Varchar),
            DataType::TimestampWithoutTimeZone => Ok(Self::Timestamp { scale: 3 }),
            DataType::TimestampWithTimeZone => {
                Ok(Self::TimestampWithTimeZone { scale: 3 })
            }
            DataType::Uuid => Ok(Self::Uuid),
        }
    }

    /// Convert this `TrinoDataType` into a `DataType`.
    pub fn to_data_type(&self) -> DataType {
        match self {
            TrinoDataType::Boolean => DataType::Bool,
            TrinoDataType::SmallInt => DataType::Int16,
            TrinoDataType::Integer => DataType::Int32,
            TrinoDataType::BigInt => DataType::Int64,
            TrinoDataType::Real => DataType::Float32,
            TrinoDataType::Double => DataType::Float64,
            TrinoDataType::Decimal { .. } => DataType::Decimal,
            TrinoDataType::Varchar => DataType::Text,
            TrinoDataType::Json => DataType::Json,
            TrinoDataType::Date => DataType::Date,
            TrinoDataType::Timestamp { .. } => DataType::TimestampWithoutTimeZone,
            TrinoDataType::TimestampWithTimeZone { .. } => {
                DataType::TimestampWithTimeZone
            }
            TrinoDataType::Array(elem_ty) => {
                DataType::Array(Box::new(elem_ty.to_data_type()))
            }
            TrinoDataType::Row(fields) => {
                let mut rows = Vec::new();
                for (idx, field) in fields.iter().enumerate() {
                    let name = field
                        .name
                        .clone()
                        .unwrap_or_else(|| ColumnName(format!("_field{}", idx)));

                    rows.push(StructField {
                        name: name.unescaped().to_owned(),
                        data_type: field.data_type.to_data_type(),
                        is_nullable: true,
                    });
                }
                DataType::Struct(rows)
            }
            TrinoDataType::Uuid => DataType::Uuid,
            TrinoDataType::SphericalGeography => DataType::GeoJson(Srid::wgs84()),
        }
    }
}

impl fmt::Display for TrinoDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrinoDataType::Boolean => write!(f, "BOOLEAN"),
            TrinoDataType::SmallInt => write!(f, "SMALLINT"),
            TrinoDataType::Integer => write!(f, "INTEGER"),
            TrinoDataType::BigInt => write!(f, "BIGINT"),
            TrinoDataType::Real => write!(f, "REAL"),
            TrinoDataType::Double => write!(f, "DOUBLE"),
            TrinoDataType::Decimal { precision, scale } => {
                write!(f, "DECIMAL({},{})", precision, scale)
            }
            TrinoDataType::Varchar => write!(f, "VARCHAR"),
            TrinoDataType::Json => write!(f, "JSON"),
            TrinoDataType::Date => write!(f, "DATE"),
            TrinoDataType::Timestamp { scale } => write!(f, "TIMESTAMP({})", scale),
            TrinoDataType::TimestampWithTimeZone { scale } => {
                write!(f, "TIMESTAMP({}) WITH TIME ZONE", scale)
            }
            TrinoDataType::Array(inner) => write!(f, "ARRAY({})", inner),
            TrinoDataType::Row(fields) => {
                write!(f, "ROW(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field)?;
                }
                write!(f, ")")
            }
            TrinoDataType::Uuid => write!(f, "UUID"),
            TrinoDataType::SphericalGeography => write!(f, "SphericalGeography"),
        }
    }
}

impl FromStr for TrinoDataType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        trino_type::ty(s)
            .map_err(|e| format_err!("could not parse Trino type in {:?}: {}", s, e))
    }
}

// A `peg` grammar for parsing Trino types.
peg::parser! {
    grammar trino_type() for str {
        // Match a case-insensitive keyword, which must never be followed by more
        // valid identifier characters.
        rule k(s: &'static str)
            = i:$(['a'..='z' | 'A'..='Z' | '_']+) !['a'..='z' | 'A'..='Z' | '_'] {?
                if i.to_ascii_uppercase() == s {
                    Ok(())
                } else {
                    Err(s)
                }
            }

        // Match simple whitespace.
        rule _() = [' ' | '\t' | '\n' | '\r']+

        // Match a Trino type.
        pub rule ty() -> TrinoDataType
            = k("BOOLEAN") { TrinoDataType::Boolean }
            / k("SMALLINT") { TrinoDataType::SmallInt }
            / k("INTEGER") { TrinoDataType::Integer }
            / k("BIGINT") { TrinoDataType::BigInt }
            / k("REAL") { TrinoDataType::Real }
            / k("DOUBLE") { TrinoDataType::Double }
            / k("DECIMAL") _? "(" _? p:uint() _? "," _? s:uint() _? ")" { TrinoDataType::Decimal { precision: p, scale: s } }
            / k("VARCHAR") { TrinoDataType::Varchar }
            / k("JSON") { TrinoDataType::Json }
            / k("DATE") { TrinoDataType::Date }
            / k("TIMESTAMP") _? "(" _? s:uint() _? ")" _? k("WITH") _? k("TIME") _? k("ZONE") { TrinoDataType::TimestampWithTimeZone { scale: s } }
            / k("TIMESTAMP") _? "(" _? s:uint() _? ")" { TrinoDataType::Timestamp { scale: s } }
            / k("ARRAY") _? "(" _? ty:ty() _? ")" { TrinoDataType::Array(Box::new(ty)) }
            / k("ROW") _? "(" _? fields:field() ** ( _? "," _?) _? ")" { TrinoDataType::Row(fields) }
            / k("UUID") { TrinoDataType::Uuid }
            / k("SPHERICALGEOGRAPHY") { TrinoDataType::SphericalGeography }

        rule field() -> TrinoStructField
            = name:column_name() _ ty:ty() { TrinoStructField { name: Some(name), data_type: ty } }
            / ty:ty() { TrinoStructField { name: None, data_type: ty } }

        rule uint() -> u32
            = n:$(['0'..='9']+) {
                n.parse().expect("parser should ensure this never happens")
            }

        // A column name in a ROW definition. Unfortunately, Trino does not
        // seem to output quoted identifiers even when they are needed, so we
        // don't try to parse them here.
        rule column_name() -> ColumnName
            = i:$(['a'..='z' | 'A'..='Z' | '_']+) { ColumnName(i.to_owned()) }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrinoStructField {
    name: Option<ColumnName>,
    data_type: TrinoDataType,
}

impl fmt::Display for TrinoStructField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{} ", name)?;
        }
        write!(f, "{}", self.data_type)
    }
}

/// Quote `s` for Trino, surrounding it with `'` and escaping special
/// characters as needed.
fn trino_quote_fmt(s: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if s.chars().all(|c| c.is_ascii_graphic() || c == ' ') {
        write!(f, "'")?;
        for c in s.chars() {
            match c {
                '\'' => write!(f, "''")?,
                _ => write!(f, "{}", c)?,
            }
        }
        write!(f, "'")
    } else {
        write!(f, "U&'")?;
        for c in s.chars() {
            match c {
                '\'' => write!(f, "''")?,
                '\\' => write!(f, "\\\\")?,
                _ if c.is_ascii_graphic() || c == ' ' => write!(f, "{}", c)?,
                _ if c as u32 <= 0xFFFF => write!(f, "\\{:04x}", c as u32)?,
                _ => write!(f, "\\+{:06x}", c as u32)?,
            }
        }
        write!(f, "'")
    }
}

/// Formatting wrapper for strings quoted with single quotes.
pub struct TrinoString<'a>(pub &'a str);

impl fmt::Display for TrinoString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        trino_quote_fmt(self.0, f)
    }
}
