//! A Trino data type.

use std::fmt;

use crate::{
    common::*,
    schema::{DataType, StructField},
};

use super::{
    ast::{ident, Expr},
    TrinoIdent,
};

/// A Trino data type.
///
/// If you add new types here, be sure to also add them to our
/// [`proptest::Arbitrary`] implementation.
#[derive(Clone, Debug, PartialEq)]
pub enum TrinoDataType {
    /// A boolean value.
    Boolean,
    /// An 8-bit signed integer value.
    TinyInt,
    /// A 16-bit signed integer value.
    SmallInt,
    /// A 32-bit signed integer value.
    Int,
    /// A 64-bit signed integer value.
    BigInt,
    /// A 32-bit floating-point value.
    Real,
    /// A 64-bit floating-point value.
    Double,
    /// A fixed-point decimal value.
    Decimal {
        /// The total number of digits in the decimal value.
        precision: u32,
        /// The number of digits after the decimal point. Defaults to 0.
        scale: u32,
    },
    /// Variable-length character data.
    Varchar {
        /// The maximum number of characters in the string.
        length: Option<u32>,
    },
    /// Fixed-length character data.
    Char {
        /// The number of characters in the string. Defaults to 1.
        length: u32,
    },
    /// Variable-length binary data.
    Varbinary,
    /// JSON data.
    Json,
    /// A calendar date (year, month, day), with no time zone.
    Date,
    /// A time of day (hour, minute, second), with no time zone.
    Time {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// A time (hour, minute, second, milliseconds), with a time zone.
    TimeWithTimeZone {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// Calendar date and time, with no time zone.
    Timestamp {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// Calendar date and time, with a time zone.
    TimestampWithTimeZone {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// A time interval involving days, hours, minutes, and seconds.
    IntervalDayToSecond,
    /// A time interval involving years and months.
    IntervalYearToMonth,
    /// An array of values.
    Array(Box<TrinoDataType>),
    /// A map of keys to values.
    Map {
        /// The data type of the keys.
        key_type: Box<TrinoDataType>,
        /// The data type of the values.
        value_type: Box<TrinoDataType>,
    },
    /// A row of fields.
    Row(Vec<TrinoField>),
    /// A UUID.
    Uuid,
    /// A spherical geographic value. This isn't documented in the official list
    /// of Trino types, but it's mentioned in [their geospatial
    /// documentation](https://trino.io/docs/current/functions/geospatial.html).
    SphericalGeography,
    // Left out for now: IP address, HyperLogLog, digests, etc.
}

impl TrinoDataType {
    pub(crate) fn bigquery_sized_decimal() -> Self {
        TrinoDataType::Decimal {
            precision: 38,
            scale: 9,
        }
    }

    pub(crate) fn varchar() -> Self {
        TrinoDataType::Varchar { length: None }
    }

    pub(crate) fn timestamp() -> Self {
        TrinoDataType::Timestamp { precision: 3 }
    }

    pub(crate) fn timestamp_with_time_zone() -> Self {
        TrinoDataType::TimestampWithTimeZone { precision: 3 }
    }

    /// Given a `DataType`, try to find a corresponding `TrinoDataType`.
    pub(crate) fn from_data_type(
        schema: &Schema,
        ty: &DataType,
    ) -> Result<TrinoDataType> {
        match ty {
            DataType::Array(ty) => Ok(TrinoDataType::Array(Box::new(
                Self::from_data_type(schema, ty)?,
            ))),
            DataType::Bool => Ok(TrinoDataType::Boolean),
            DataType::Date => Ok(TrinoDataType::Date),
            // TODO: Document `DataType::Decimal` as having some limited
            // precision and scale?
            DataType::Decimal => Ok(Self::bigquery_sized_decimal()),
            DataType::Float32 => Ok(TrinoDataType::Real),
            DataType::Float64 => Ok(TrinoDataType::Double),
            // Map all SRIDs to spherical geography. You're responsible for
            // remembering what SRIDs you're using. Unlike BigQuery, Trino doesn't
            // seem to have an expected SRID, and unlike Postgres, it doesn't
            // record the SRID in the column type.
            DataType::GeoJson(_srid) => Ok(TrinoDataType::SphericalGeography),
            DataType::Int16 => Ok(TrinoDataType::SmallInt),
            DataType::Int32 => Ok(TrinoDataType::Int),
            DataType::Int64 => Ok(TrinoDataType::BigInt),
            DataType::Json => Ok(TrinoDataType::Json),
            DataType::Named(name) => {
                let ty = schema.data_type_for_name(name);
                Self::from_data_type(schema, ty)
            }
            // Enums/categoricals become strings.
            DataType::OneOf(_) => Ok(TrinoDataType::varchar()),
            DataType::Struct(fields) => Ok(TrinoDataType::Row(
                fields
                    .iter()
                    .map(|field| TrinoField::from_struct_field(schema, field))
                    .collect::<Result<Vec<_>>>()?,
            )),
            DataType::Text => Ok(TrinoDataType::varchar()),
            DataType::TimestampWithoutTimeZone => Ok(TrinoDataType::timestamp()),
            DataType::TimestampWithTimeZone => {
                Ok(TrinoDataType::timestamp_with_time_zone())
            }
            DataType::Uuid => Ok(TrinoDataType::Uuid),
        }
    }

    /// Convert this `PgDataType` to a portable `DataType`.
    pub(crate) fn to_data_type(&self) -> Result<DataType> {
        match self {
            TrinoDataType::Boolean => Ok(DataType::Bool),
            // We don't support 8-bit ints in our portable schema, so promote
            // them.
            TrinoDataType::TinyInt | TrinoDataType::SmallInt => Ok(DataType::Int16),
            TrinoDataType::Int => Ok(DataType::Int32),
            TrinoDataType::BigInt => Ok(DataType::Int64),
            TrinoDataType::Real => Ok(DataType::Float32),
            TrinoDataType::Double => Ok(DataType::Float64),
            TrinoDataType::Decimal { .. } => Ok(DataType::Decimal),
            TrinoDataType::Varchar { .. } | TrinoDataType::Char { .. } => {
                Ok(DataType::Text)
            }
            TrinoDataType::Varbinary => Err(format_err!(
                "VARBINARY is not yet supported in portable schemas"
            )),
            TrinoDataType::Json => Ok(DataType::Json),
            TrinoDataType::Date => Ok(DataType::Date),
            TrinoDataType::Time { .. } => {
                Err(format_err!("TIME is not yet supported in portable schemas"))
            }
            TrinoDataType::TimeWithTimeZone { .. } => Err(format_err!(
                "TIME WITH TIME ZONE is not yet supported in portable schemas"
            )),
            TrinoDataType::Timestamp { .. } => Ok(DataType::TimestampWithoutTimeZone),
            TrinoDataType::TimestampWithTimeZone { .. } => {
                Ok(DataType::TimestampWithTimeZone)
            }
            TrinoDataType::IntervalDayToSecond
            | TrinoDataType::IntervalYearToMonth => Err(format_err!(
                "INTERVAL types are not supported in portable schemas"
            )),
            TrinoDataType::Array(elem_ty) => {
                Ok(DataType::Array(Box::new(elem_ty.to_data_type()?)))
            }
            TrinoDataType::Map { key_type, .. } => {
                // Try to convert maps to JSON.
                let key_type = key_type.to_data_type()?;
                if key_type == DataType::Text {
                    Ok(DataType::Json)
                } else {
                    Err(format_err!("MAP key type must be TEXT"))
                }
            }
            TrinoDataType::Row(fields) => {
                let fields = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| field.to_struct_field(idx))
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(fields))
            }
            TrinoDataType::Uuid => Ok(DataType::Uuid),
            // We don't know the SRID for a spherical geography, so we can't
            // map it to [`DataType::GeoJson(srid)`]. So just export it as JSON.
            TrinoDataType::SphericalGeography => Ok(DataType::Json),
        }
    }

    /// Generate SQL to import `value` as a value of type `self`, assuming that
    /// `name` is represented as a string.
    pub(super) fn string_import_expr(&self, value: &Expr) -> Result<Expr> {
        match self {
            // Nothing to do for these types.
            TrinoDataType::Varchar { .. } | TrinoDataType::Char { .. } => {
                Ok(value.to_owned())
            }

            // Types which can imported by CAST from a string.
            TrinoDataType::Boolean
            | TrinoDataType::TinyInt
            | TrinoDataType::SmallInt
            | TrinoDataType::Int
            | TrinoDataType::BigInt
            | TrinoDataType::Real
            | TrinoDataType::Double
            | TrinoDataType::Decimal { .. }
            | TrinoDataType::Date
            | TrinoDataType::Uuid => Ok(Expr::cast(value.to_owned(), self.clone())),

            // Parse JSON values.
            TrinoDataType::Json => {
                Ok(Expr::func("JSON_PARSE", vec![value.to_owned()]))
            }

            // We need to parse the timestamp as if it had a time zone, then
            // strip the time zone. This seems to be simpler than using one
            // of the several pattern-based parsing functions.
            TrinoDataType::Timestamp { .. } => Ok(Expr::cast(
                Expr::func(
                    "FROM_ISO8601_TIMESTAMP",
                    vec![Expr::func("CONCAT", vec![value.to_owned(), Expr::str("Z")])],
                ),
                self.clone(),
            )),

            TrinoDataType::TimestampWithTimeZone { .. } => {
                Ok(Expr::func("FROM_ISO8601_TIMESTAMP", vec![value.to_owned()]))
            }

            TrinoDataType::Array(_) => {
                // Figure out the closest type we can convert to using `CAST`.
                let casted_ty = self.cast_parsed_json_as()?;
                let cast_expr = Expr::cast(
                    Expr::func("JSON_PARSE", vec![value.to_owned()]),
                    casted_ty.clone(),
                );
                if self == &casted_ty {
                    Ok(cast_expr)
                } else {
                    self.json_import_expr(&cast_expr)
                }
            }

            TrinoDataType::Row(_) => todo!(),

            // TODO: This is importing as
            //
            // ```
            // "{""type"":""Point"",""coordinates"":[-71,42],""crs"":{""type"":""name"",""properties"":{""name"":""EPSG:0""}}}"
            // ```
            //
            // Figure out what's up with ESPG.
            TrinoDataType::SphericalGeography => {
                Ok(Expr::func("FROM_GEOJSON_GEOMETRY", vec![value.to_owned()]))
            }

            // Types we can't import.
            TrinoDataType::Varbinary
            | TrinoDataType::Time { .. }
            | TrinoDataType::TimeWithTimeZone { .. }
            | TrinoDataType::IntervalDayToSecond
            | TrinoDataType::IntervalYearToMonth
            | TrinoDataType::Map { .. } => {
                Err(format_err!("cannot import values of type {}", self))
            }
        }
    }

    /// When importing, cast a parsed JSON value to this type.
    fn cast_parsed_json_as(&self) -> Result<Self> {
        match self {
            // Cast these types to themselves.
            TrinoDataType::Boolean
            | TrinoDataType::TinyInt
            | TrinoDataType::SmallInt
            | TrinoDataType::Int
            | TrinoDataType::BigInt
            | TrinoDataType::Real
            | TrinoDataType::Double
            | TrinoDataType::Varchar { .. }
            | TrinoDataType::Char { .. }
            | TrinoDataType::Json => Ok(self.clone()),

            // Cast these to VARCHAR. We will then parse them to the correct
            // type.
            TrinoDataType::Decimal { .. }
            | TrinoDataType::Date
            | TrinoDataType::Timestamp { .. }
            | TrinoDataType::TimestampWithTimeZone { .. }
            | TrinoDataType::Uuid => Ok(TrinoDataType::varchar()),

            // We we need to convert the GeoJSON to a string, then parse it.
            TrinoDataType::SphericalGeography => Ok(TrinoDataType::Json),

            // Handle array element types recursively.
            TrinoDataType::Array(elem_ty) => {
                let elem_ty = elem_ty.cast_parsed_json_as()?;
                Ok(TrinoDataType::Array(Box::new(elem_ty)))
            }

            // Handle row field types recursively.
            TrinoDataType::Row(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        field.data_type.cast_parsed_json_as().map(|data_type| {
                            TrinoField {
                                name: field.name.clone(),
                                data_type,
                            }
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TrinoDataType::Row(fields))
            }

            // Types we can't import.
            TrinoDataType::Varbinary
            | TrinoDataType::Time { .. }
            | TrinoDataType::TimeWithTimeZone { .. }
            | TrinoDataType::IntervalDayToSecond
            | TrinoDataType::IntervalYearToMonth
            | TrinoDataType::Map { .. } => {
                // We can't cast these types directly from JSON.
                Err(format_err!("cannot import columns of type {}", self))
            }
        }
    }

    /// When this type is represented as a JSON value, do we need to do any
    /// further conversion to import it?
    fn imported_json_needs_conversion(&self) -> Result<bool> {
        let casted_ty = self.cast_parsed_json_as()?;
        Ok(self != &casted_ty)
    }

    /// Write the SQL to import `name` (of type `JSON`) as a value of type `self`.
    fn json_import_expr(&self, value: &Expr) -> Result<Expr> {
        match self {
            // Types represented as themselves in JSON.
            TrinoDataType::Boolean
            | TrinoDataType::TinyInt
            | TrinoDataType::SmallInt
            | TrinoDataType::Int
            | TrinoDataType::BigInt
            | TrinoDataType::Real
            | TrinoDataType::Double
            | TrinoDataType::Varchar { .. }
            | TrinoDataType::Char { .. }
            | TrinoDataType::Json => Ok(value.to_owned()),

            // Types represented as strings in JSON.
            TrinoDataType::Decimal { .. }
            | TrinoDataType::Date
            | TrinoDataType::Timestamp { .. }
            | TrinoDataType::TimestampWithTimeZone { .. }
            | TrinoDataType::Uuid => self.string_import_expr(value),

            // More complex types that still don't require any conversion.
            _ if !self.imported_json_needs_conversion()? => Ok(value.to_owned()),

            // We know this needs further conversion, so process it recursively.
            TrinoDataType::Array(elem_ty) => {
                let elem = ident("elem");
                Ok(Expr::func(
                    "TRANSFORM",
                    vec![
                        value.to_owned(),
                        Expr::lambda(
                            elem.clone(),
                            elem_ty.json_import_expr(&Expr::Var(elem))?,
                        ),
                    ],
                ))
            }

            TrinoDataType::Row(_) => todo!("json_import_expr Row"),

            // This is bit messy, because we need to convert it from JSON back to string,
            // then parse it.
            TrinoDataType::SphericalGeography => {
                let str_expr = Expr::func("JSON_FORMAT", vec![value.to_owned()]);
                self.string_import_expr(&str_expr)
            }

            // Types that don't exist in our portable schema and that we can't
            // import.
            TrinoDataType::Varbinary
            | TrinoDataType::Time { .. }
            | TrinoDataType::TimeWithTimeZone { .. }
            | TrinoDataType::IntervalDayToSecond
            | TrinoDataType::IntervalYearToMonth
            | TrinoDataType::Map { .. } => {
                Err(format_err!("cannot import data of type {} from JSON", self))
            }
        }
    }

    /// Generate SQL to export `value`, assuming it has type `self`.
    pub(super) fn string_export_expr(&self, value: &Expr) -> Result<Expr> {
        match self {
            // These will will do the right thing when our caller uses `CAST(..
            // AS VARCHAR)`.
            TrinoDataType::TinyInt
            | TrinoDataType::SmallInt
            | TrinoDataType::Int
            | TrinoDataType::BigInt
            | TrinoDataType::Real
            | TrinoDataType::Double
            | TrinoDataType::Decimal { .. }
            | TrinoDataType::Varchar { .. }
            | TrinoDataType::Char { .. }
            | TrinoDataType::Date
            | TrinoDataType::Uuid => Ok(value.to_owned()),

            // Use our canonical representation for boolean values.
            TrinoDataType::Boolean => Ok(Expr::case_match(
                value.to_owned(),
                vec![
                    (Expr::bool(true), Expr::str("t")),
                    (Expr::bool(false), Expr::str("f")),
                ],
                Expr::str(""),
            )),

            // Convert to ISO8601 format, stripping any trailing ".0+" for
            // consistency with other dbcrossbar drivers.
            TrinoDataType::Timestamp { .. } => Ok(Expr::func(
                "REGEXP_REPLACE",
                vec![
                    Expr::func("TO_ISO8601", vec![value.to_owned()]),
                    Expr::str(".0+$"),
                    Expr::str(""),
                ],
            )),
            TrinoDataType::TimestampWithTimeZone { .. } => Ok(Expr::func(
                "REGEXP_REPLACE",
                vec![
                    Expr::func("TO_ISO8601", vec![value.to_owned()]),
                    Expr::str(".0+Z$"),
                    Expr::str("Z"),
                ],
            )),

            // Serialize JSON to a string. We have accept that this may use
            // various whitespace and ordering conventions. `dbcrossbar` doesn't
            // make any promises about the exact format of JSON output.
            TrinoDataType::Json => Ok(Expr::json_to_string(value.to_owned())),

            // "Trivial" ARRAY and ROW types can be serialized as JSON without any
            // further processing.
            TrinoDataType::Array(_) | TrinoDataType::Row { .. }
                if !self.exported_json_needs_conversion()? =>
            {
                Ok(Expr::json_to_string_with_cast(value.to_owned()))
            }

            TrinoDataType::Array(_) => Ok(Expr::json_to_string_with_cast(
                self.json_export_expr(value)?,
            )),

            TrinoDataType::Row { .. } => todo!("string_export_expr Row"),

            // Serialize as GeoJSON.
            TrinoDataType::SphericalGeography => Ok(Expr::func(
                // This returns VARCHAR, not a Trino `JSON` value.
                "TO_GEOJSON_GEOMETRY",
                vec![value.to_owned()],
            )),

            // These types are not directly supported.
            TrinoDataType::Varbinary
            | TrinoDataType::Time { .. }
            | TrinoDataType::TimeWithTimeZone { .. }
            | TrinoDataType::IntervalDayToSecond
            | TrinoDataType::IntervalYearToMonth
            | TrinoDataType::Map { .. } => {
                Err(format_err!("cannot export values of type {}", self))
            }
        }
    }

    /// Does our exported JSON need conversion?
    fn exported_json_needs_conversion(&self) -> Result<bool> {
        match self {
            // Types that are represented as themselves in exported JSON.
            TrinoDataType::Boolean
            | TrinoDataType::TinyInt
            | TrinoDataType::SmallInt
            | TrinoDataType::Int
            | TrinoDataType::BigInt
            | TrinoDataType::Real
            | TrinoDataType::Double
            | TrinoDataType::Varchar { .. }
            | TrinoDataType::Char { .. }
            | TrinoDataType::Json => Ok(false),

            // This isn't represented as a string, but it will
            // do the right thing even if nested somewhere deep in a `CAST(... AS JSON)`.
            TrinoDataType::Date => Ok(false),

            // Types that are represented as strings in JSON, and so require
            // conversion.
            TrinoDataType::Timestamp { .. }
            | TrinoDataType::TimestampWithTimeZone { .. }
            | TrinoDataType::Uuid => Ok(true),

            // This would naturally convert to a numeric value, I think, but we
            // want to force it to always be a string.
            TrinoDataType::Decimal { .. } => Ok(true),

            // Arrays need conversion if their elements need conversion.
            TrinoDataType::Array(elem_ty) => elem_ty.exported_json_needs_conversion(),

            // Rows need conversion if any of their fields need conversion.
            TrinoDataType::Row(fields) => {
                for field in fields {
                    if field.name.is_none()
                        || field.data_type.exported_json_needs_conversion()?
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            }

            // This is converted to inline JSON.
            TrinoDataType::SphericalGeography => Ok(true),

            // Types that don't exist in our portable schema and that we can't
            // import.
            TrinoDataType::Varbinary
            | TrinoDataType::Time { .. }
            | TrinoDataType::TimeWithTimeZone { .. }
            | TrinoDataType::IntervalDayToSecond
            | TrinoDataType::IntervalYearToMonth
            | TrinoDataType::Map { .. } => {
                Err(format_err!("cannot export data of type {} to JSON", self))
            }
        }
    }

    /// Generate SQL to export `value` as JSON, assuming it has type `self`.
    fn json_export_expr(&self, value: &Expr) -> Result<Expr> {
        match self {
            // These types can be represented directly in JSON.
            TrinoDataType::Boolean
            | TrinoDataType::TinyInt
            | TrinoDataType::SmallInt
            | TrinoDataType::Int
            | TrinoDataType::BigInt
            | TrinoDataType::Real
            | TrinoDataType::Double
            | TrinoDataType::Varchar { .. }
            | TrinoDataType::Char { .. }
            | TrinoDataType::Json => Ok(value.to_owned()),

            // Types that are represented as strings in JSON, and so require
            // conversion.
            TrinoDataType::Date
            | TrinoDataType::Timestamp { .. }
            | TrinoDataType::TimestampWithTimeZone { .. } => {
                self.string_export_expr(value)
            }

            // Force to a string now, because `CAST(value AS VARCHAR)` will work
            // but `CAST(ARRAY[value] AS JSON)` will not.
            TrinoDataType::Uuid => {
                Ok(Expr::cast(value.to_owned(), TrinoDataType::varchar()))
            }

            // Force this to a JSON string, so that it doesn't lose precision.
            //
            // TODO: Do the other drivers do this for DECIMAL? They should. Do
            // we specify it? We should.
            TrinoDataType::Decimal { .. } => {
                Ok(Expr::cast(value.to_owned(), TrinoDataType::varchar()))
            }

            // Can we end our recursion here?
            TrinoDataType::Array(_) | TrinoDataType::Row(_)
                if !self.exported_json_needs_conversion()? =>
            {
                Ok(value.to_owned())
            }

            TrinoDataType::Array(elem_ty) => {
                let elem = ident("elem");
                Ok(Expr::func(
                    "TRANSFORM",
                    vec![
                        value.to_owned(),
                        Expr::lambda(
                            elem.clone(),
                            elem_ty.json_export_expr(&Expr::Var(elem))?,
                        ),
                    ],
                ))
            }

            TrinoDataType::Row(_) => todo!("json_export_expr Row"),

            // TODO: I _think_ this is how we want to handle this? Or should the
            // GeoJSON be stored as a string inside our larger JSON object?
            TrinoDataType::SphericalGeography => Ok(Expr::func(
                "JSON_PARSE",
                vec![self.string_export_expr(value)?],
            )),

            TrinoDataType::Varbinary
            | TrinoDataType::Time { .. }
            | TrinoDataType::TimeWithTimeZone { .. }
            | TrinoDataType::IntervalDayToSecond
            | TrinoDataType::IntervalYearToMonth
            | TrinoDataType::Map { .. } => {
                Err(format_err!("cannot export values of type {}", self))
            }
        }
    }
}

impl fmt::Display for TrinoDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrinoDataType::Boolean => write!(f, "BOOLEAN"),
            TrinoDataType::TinyInt => write!(f, "TINYINT"),
            TrinoDataType::SmallInt => write!(f, "SMALLINT"),
            TrinoDataType::Int => write!(f, "INT"),
            TrinoDataType::BigInt => write!(f, "BIGINT"),
            TrinoDataType::Real => write!(f, "REAL"),
            TrinoDataType::Double => write!(f, "DOUBLE"),
            TrinoDataType::Decimal { precision, scale } => {
                write!(f, "DECIMAL({}, {})", precision, scale)
            }
            TrinoDataType::Varchar { length: None } => write!(f, "VARCHAR"),
            TrinoDataType::Varchar {
                length: Some(length),
            } => write!(f, "VARCHAR({})", length),
            TrinoDataType::Char { length: 1 } => write!(f, "CHAR"),
            TrinoDataType::Char { length } => write!(f, "CHAR({})", length),
            TrinoDataType::Varbinary => write!(f, "VARBINARY"),
            TrinoDataType::Json => write!(f, "JSON"),
            TrinoDataType::Date => write!(f, "DATE"),
            TrinoDataType::Time { precision } => write!(f, "TIME({})", precision),
            TrinoDataType::TimeWithTimeZone { precision } => {
                write!(f, "TIME({}) WITH TIME ZONE", precision)
            }
            TrinoDataType::Timestamp { precision } => {
                write!(f, "TIMESTAMP({})", precision)
            }
            TrinoDataType::TimestampWithTimeZone { precision } => {
                write!(f, "TIMESTAMP({}) WITH TIME ZONE", precision)
            }
            TrinoDataType::IntervalDayToSecond => write!(f, "INTERVAL DAY TO SECOND"),
            TrinoDataType::IntervalYearToMonth => write!(f, "INTERVAL YEAR TO MONTH"),
            TrinoDataType::Array(elem_ty) => write!(f, "ARRAY({})", elem_ty),
            TrinoDataType::Map {
                key_type,
                value_type,
            } => {
                write!(f, "MAP({}, {})", key_type, value_type)
            }
            TrinoDataType::Row(fields) => {
                write!(f, "ROW(")?;
                for (idx, field) in fields.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field)?;
                }
                write!(f, ")")
            }
            TrinoDataType::Uuid => write!(f, "UUID"),
            // This is capitalized differently in Trino's output.
            TrinoDataType::SphericalGeography => write!(f, "SphericalGeography"),
        }
    }
}

/// A field in a [`TrinoDataType::Row`] data type.
#[derive(Clone, Debug, PartialEq)]
pub struct TrinoField {
    /// The name of the field.
    name: Option<TrinoIdent>,
    /// The data type of the field.
    data_type: TrinoDataType,
}

impl TrinoField {
    /// Create an anonymous `TrinoField` with a data type.
    pub(crate) fn anonymous(data_type: TrinoDataType) -> Self {
        TrinoField {
            name: None,
            data_type,
        }
    }

    /// Create a named `TrinoField` with a data type.
    pub(crate) fn named(name: TrinoIdent, data_type: TrinoDataType) -> Self {
        TrinoField {
            name: Some(name),
            data_type,
        }
    }

    /// Given a `StructField`, try to find a corresponding `TrinoField`.
    pub(crate) fn from_struct_field(
        schema: &Schema,
        field: &StructField,
    ) -> Result<Self> {
        Ok(TrinoField {
            name: Some(TrinoIdent::new(&field.name)?),
            data_type: TrinoDataType::from_data_type(schema, &field.data_type)?,
        })
    }

    /// Convert this `TrinoField` to a portable `StructField`.
    pub(crate) fn to_struct_field(&self, idx: usize) -> Result<StructField> {
        let name = if let Some(name) = &self.name {
            name.as_unquoted_str().to_owned()
        } else {
            format!("_f{}", idx)
        };
        Ok(StructField {
            name,
            data_type: self.data_type.to_data_type()?,
            // Unless shown otherwise, assume fields are nullable.
            is_nullable: true,
        })
    }
}

impl fmt::Display for TrinoField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{} ", name)?;
        }
        write!(f, "{}", self.data_type)
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::*;

    impl Arbitrary for TrinoDataType {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        /// Generate an arbitrary [`TrinoDataType`]. We need to implement this
        /// manually because [`TrinoDataType`] is recursive, both on its own,
        /// and mututally recursive with [`TrinoField`].
        ///
        /// To learn more about this, read [the `proptest` book][proptest], and
        /// specifically the section on [recursive data][recursive].
        ///
        /// [proptest]: https://proptest-rs.github.io/proptest/intro.html
        /// [recursion]: https://proptest-rs.github.io/proptest/proptest/tutorial/recursive.html
        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            {
                // Our "leaf" types.
                let leaf = prop_oneof![
                    Just(TrinoDataType::Boolean),
                    Just(TrinoDataType::TinyInt),
                    Just(TrinoDataType::SmallInt),
                    Just(TrinoDataType::Int),
                    Just(TrinoDataType::BigInt),
                    Just(TrinoDataType::Real),
                    Just(TrinoDataType::Double),
                    (1..=38u32, 0..=9u32).prop_map(|(precision, scale)| {
                        TrinoDataType::Decimal { precision, scale }
                    }),
                    Just(TrinoDataType::Varchar { length: None }),
                    (1..=255u32).prop_map(|length| TrinoDataType::Varchar {
                        length: Some(length)
                    }),
                    Just(TrinoDataType::Char { length: 1 }),
                    (1..=255u32).prop_map(|length| TrinoDataType::Char { length }),
                    Just(TrinoDataType::Varbinary),
                    Just(TrinoDataType::Json),
                    Just(TrinoDataType::Date),
                    Just(TrinoDataType::Time { precision: 3 }),
                    (0..=9u32).prop_map(|precision| TrinoDataType::Time { precision }),
                    Just(TrinoDataType::TimeWithTimeZone { precision: 3 }),
                    (0..=9u32).prop_map(|precision| TrinoDataType::TimeWithTimeZone {
                        precision
                    }),
                    Just(TrinoDataType::Timestamp { precision: 3 }),
                    (0..=9u32)
                        .prop_map(|precision| TrinoDataType::Timestamp { precision }),
                    Just(TrinoDataType::TimestampWithTimeZone { precision: 3 }),
                    (0..=9u32).prop_map(|precision| {
                        TrinoDataType::TimestampWithTimeZone { precision }
                    }),
                    Just(TrinoDataType::IntervalDayToSecond),
                    Just(TrinoDataType::IntervalYearToMonth),
                    Just(TrinoDataType::Uuid),
                    Just(TrinoDataType::SphericalGeography),
                ];

                // Our "recursive" types.
                //
                // Pass smallish numbers to `prop_recursive` because generating
                // hugely complex types is unlikely to find additional bugs.
                leaf.prop_recursive(3, 6, 3, |inner| {
                    prop_oneof![
                        // TrinoDataType::Array.
                        inner
                            .clone()
                            .prop_map(|ty| TrinoDataType::Array(Box::new(ty))),
                        // TrinoDataType::Map.
                        (inner.clone(), inner.clone()).prop_map(
                            |(key_type, value_type)| {
                                TrinoDataType::Map {
                                    key_type: Box::new(key_type),
                                    value_type: Box::new(value_type),
                                }
                            }
                        ),
                        // TrinoDataType::Row.
                        (prop::collection::vec(
                            (any::<Option<TrinoIdent>>(), inner),
                            1..=3
                        ))
                        .prop_map(|fields| {
                            // We do this here, and not in `TrinoField`, because
                            // it's mutually recursive with `TrinoDataType`, and
                            // we want to allow `prop_recursive` to see the
                            // entire mutually recursive structure.
                            TrinoDataType::Row(
                                fields
                                    .into_iter()
                                    .map(|(name, data_type)| TrinoField {
                                        name,
                                        data_type,
                                    })
                                    .collect(),
                            )
                        }),
                    ]
                })
                .boxed()
            }
        }
    }
}
