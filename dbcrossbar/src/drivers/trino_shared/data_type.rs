//! A Trino data type.

use dbcrossbar_trino::pretty::ast::{ident, Expr};
pub use dbcrossbar_trino::{
    DataType as TrinoDataType, Field as TrinoField, Ident as TrinoIdent,
};

use crate::{
    common::*,
    schema::{DataType, Srid, StructField},
};

/// Methods we want to add to [`TrinoDataType`], but can't because it's defined
/// in another crate. So instead we define a trait and implement it for just
/// [`TrinoDataType`].
pub(crate) trait TrinoDataTypeExt {
    fn from_data_type(schema: &Schema, ty: &DataType) -> Result<TrinoDataType>;
    fn to_data_type(&self) -> Result<DataType>;
    fn string_import_expr(&self, value: &Expr) -> Result<Expr>;
    fn string_export_expr(&self, value: &Expr) -> Result<Expr>;
}

/// Like [`TrinoDataTypeExt`], but for methods that are only used in this module.
trait TrinoDataTypeExtInternal {
    fn cast_parsed_json_as(&self) -> Result<TrinoDataType>;
    fn imported_json_needs_conversion(&self) -> Result<bool>;
    fn json_import_expr(&self, value: &Expr) -> Result<Expr>;

    fn cast_exported_json_as(&self) -> Result<TrinoDataType>;
    fn exported_json_needs_conversion(&self) -> Result<bool>;
    fn json_export_expr(&self, value: &Expr) -> Result<Expr>;
}

impl TrinoDataTypeExt for TrinoDataType {
    /// Given a `DataType`, try to find a corresponding `TrinoDataType`.
    fn from_data_type(schema: &Schema, ty: &DataType) -> Result<TrinoDataType> {
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
            // Map WGS84 to a spherical coordinate system. This is consistent
            // with how we handle BigQuery, which originally only supported a
            // WGS84-based GEOGRAPHY type and no GEOMETRY type.
            //
            // PostGIS has a newer `GEOGRAPHY` type that was limited to WGS84
            // but has since been generalized to support other spherical
            // coordinate systems. But `dbcrossbar` still uses the older
            // `GEOGRAPHY(srid)` type in PostgreSQL.
            //
            // Trino's SRID handling is not particularly documented.
            DataType::GeoJson(srid) if *srid == Srid::wgs84() => {
                Ok(TrinoDataType::SphericalGeography)
            }
            // Map other GeoJSON types to JSON.
            DataType::GeoJson(_) => Ok(TrinoDataType::Json),
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
    fn to_data_type(&self) -> Result<DataType> {
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
            TrinoDataType::Varchar { .. } => Ok(DataType::Text),
            TrinoDataType::Varbinary => Err(format_err!(
                "VARBINARY is not yet supported in portable schemas"
            )),
            TrinoDataType::Json => Ok(DataType::Json),
            TrinoDataType::Date => Ok(DataType::Date),
            TrinoDataType::Time { .. } => {
                Err(format_err!("TIME is not yet supported in portable schemas"))
            }
            TrinoDataType::Timestamp { .. } => Ok(DataType::TimestampWithoutTimeZone),
            TrinoDataType::TimestampWithTimeZone { .. } => {
                Ok(DataType::TimestampWithTimeZone)
            }
            TrinoDataType::Array(elem_ty) => {
                Ok(DataType::Array(Box::new(elem_ty.to_data_type()?)))
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
            // We assume that SphericalGeography uses WGS84.
            TrinoDataType::SphericalGeography => Ok(DataType::GeoJson(Srid::wgs84())),
        }
    }

    /// Generate SQL to import `value` as a value of type `self`, assuming that
    /// `name` is represented as a string.
    ///
    /// Here, we are converting a VARCHAR column (from a CSV file mapped as a
    /// table) into the "ideal" Trino representaion of this data type in memory.
    /// We'll need to later do a _second_ conversion, from the ideal memory type
    /// to the storage type actually supported by the backend.
    fn string_import_expr(&self, value: &Expr) -> Result<Expr> {
        match self {
            // Nothing to do for these types.
            TrinoDataType::Varchar { .. } => Ok(value.to_owned()),

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

            TrinoDataType::Array(_) | TrinoDataType::Row(_) => {
                // Figure out the closest type we can convert to using `CAST`
                // from JSON.
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

            // TODO: This is eventually re-exporting as
            //
            // ```
            // "{""type"":""Point"",""coordinates"":[-71,42],""crs"":{""type"":""name"",""properties"":{""name"":""EPSG:0""}}}"
            // ```
            //
            // Figure out what's up with Trino & ESPG.
            TrinoDataType::SphericalGeography => {
                Ok(Expr::func("FROM_GEOJSON_GEOMETRY", vec![value.to_owned()]))
            }

            // Types we can't import.
            TrinoDataType::Varbinary | TrinoDataType::Time { .. } => {
                Err(format_err!("cannot import values of type {}", self))
            }
        }
    }

    /// Generate SQL to export `value`, assuming it has type `self`.
    ///
    /// Here, we are converting the "ideal" Trino representation of this data
    /// type in memory into a VARCHAR column, which can then be stored in a
    /// table backed by a CSV file. But _before_ we do this, other code will
    /// need to translate from a "storage" type to the "ideal" type.
    fn string_export_expr(&self, value: &Expr) -> Result<Expr> {
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

            TrinoDataType::Array(_) | TrinoDataType::Row { .. } => Ok(
                Expr::json_to_string_with_cast(self.json_export_expr(value)?),
            ),

            // Serialize as GeoJSON.
            TrinoDataType::SphericalGeography => Ok(Expr::func(
                // This returns VARCHAR, not a Trino `JSON` value.
                "TO_GEOJSON_GEOMETRY",
                vec![value.to_owned()],
            )),

            // These types are not directly supported.
            TrinoDataType::Varbinary | TrinoDataType::Time { .. } => {
                Err(format_err!("cannot export values of type {}", self))
            }
        }
    }
}

impl TrinoDataTypeExtInternal for TrinoDataType {
    /// When importing, cast a parsed JSON value to this type.
    fn cast_parsed_json_as(&self) -> Result<TrinoDataType> {
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
            TrinoDataType::Varbinary | TrinoDataType::Time { .. } => {
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

            TrinoDataType::Row(fields) => {
                let row = ident("row");
                let row_expr = Expr::Var(row.clone());
                Ok(Expr::bind_var(
                    row.clone(),
                    value.to_owned(),
                    Expr::row(
                        self.to_owned(),
                        fields
                            .iter()
                            .enumerate()
                            .map(|(idx, field)| {
                                // We need to use a 1-based index with Trino.
                                let idx = i64::try_from(idx)? + 1;
                                let field_value =
                                    Expr::index(row_expr.clone(), Expr::int(idx));
                                field.data_type.json_import_expr(&field_value)
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                ))
            }

            // This is bit messy, because we need to convert it from JSON back to string,
            // then parse it.
            TrinoDataType::SphericalGeography => {
                let str_expr = Expr::func("JSON_FORMAT", vec![value.to_owned()]);
                self.string_import_expr(&str_expr)
            }

            // Types that don't exist in our portable schema and that we can't
            // import.
            TrinoDataType::Varbinary | TrinoDataType::Time { .. } => {
                Err(format_err!("cannot import data of type {} from JSON", self))
            }
        }
    }

    /// Before casting to JSON, what is the type of mostly-exported value?
    /// This is needed for `CAST(... AS ROW(...))` expressions which we use
    /// to name fields while preparing to `CAST(... AS JSON)`.
    fn cast_exported_json_as(&self) -> Result<Self> {
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
            | TrinoDataType::Json => Ok(self.clone()),

            // This isn't represented as a string, but it will do the right
            // thing even if nested somewhere deep in a `CAST(... AS JSON)`.
            TrinoDataType::Date => Ok(self.clone()),

            // Types that are represented as strings in JSON, and so require
            // conversion.
            TrinoDataType::Timestamp { .. }
            | TrinoDataType::TimestampWithTimeZone { .. }
            | TrinoDataType::Uuid => Ok(TrinoDataType::varchar()),

            // This would naturally convert to a numeric value, I think, but we
            // want to force it to always be a string.
            TrinoDataType::Decimal { .. } => Ok(TrinoDataType::varchar()),

            // Arrays need conversion if their elements need conversion.
            TrinoDataType::Array(elem_ty) => Ok(TrinoDataType::Array(Box::new(
                elem_ty.cast_exported_json_as()?,
            ))),

            // Rows need conversion if any of their fields need conversion.
            TrinoDataType::Row(fields) => {
                for field in fields {
                    if field.name.is_none() {
                        return Err(format_err!(
                            "cannot export {} because it has unnamed fields",
                            self
                        ));
                    }
                }
                Ok(TrinoDataType::Row(
                    fields
                        .iter()
                        .map(|field| {
                            Ok(TrinoField {
                                name: field.name.clone(),
                                data_type: field.data_type.cast_exported_json_as()?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                ))
            }

            // This is converted to inline JSON.
            TrinoDataType::SphericalGeography => Ok(TrinoDataType::Json),

            // Types that don't exist in our portable schema and that we can't
            // import.
            TrinoDataType::Varbinary | TrinoDataType::Time { .. } => {
                Err(format_err!("cannot export data of type {} to JSON", self))
            }
        }
    }

    /// Does our exported JSON need conversion?
    fn exported_json_needs_conversion(&self) -> Result<bool> {
        let casted_ty = self.cast_exported_json_as()?;
        Ok(self != &casted_ty)
    }

    /// Generate SQL to export `value` as JSON, assuming it has type `self`.
    fn json_export_expr(&self, value: &Expr) -> Result<Expr> {
        match self {
            // These types can be represented directly in JSON.
            TrinoDataType::Boolean
            | TrinoDataType::TinyInt
            | TrinoDataType::SmallInt
            | TrinoDataType::Int
            // TODO: We may need to convert this to a string to prevent
            // overflowing JSON numbers.
            | TrinoDataType::BigInt
            | TrinoDataType::Real
            | TrinoDataType::Double
            | TrinoDataType::Varchar { .. }
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

            TrinoDataType::Row(fields) => {
                let row = ident("row");
                let row_expr = Expr::Var(row.clone());
                Ok(Expr::bind_var(
                    row.clone(),
                    value.to_owned(),
                    Expr::row(
                        self.cast_exported_json_as()?,
                        fields
                            .iter()
                            .enumerate()
                            .map(|(idx, field)| {
                                // We need to use a 1-based index with Trino.
                                let idx = i64::try_from(idx)? + 1;
                                let field_value =
                                    Expr::index(row_expr.clone(), Expr::int(idx));
                                field.data_type.json_export_expr(&field_value)
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                ))
            }

            // TODO: I _think_ this is how we want to handle this? Or should the
            // GeoJSON be stored as a string inside our larger JSON object?
            TrinoDataType::SphericalGeography => Ok(Expr::func(
                "JSON_PARSE",
                vec![self.string_export_expr(value)?],
            )),

            TrinoDataType::Varbinary
            | TrinoDataType::Time { .. } => {
                Err(format_err!("cannot export values of type {}", self))
            }
        }
    }
}

/// Add-on methods for the external [`TrinoField`] type.
pub(crate) trait TrinoFieldExt {
    fn from_struct_field(schema: &Schema, field: &StructField) -> Result<TrinoField>;
    fn to_struct_field(&self, idx: usize) -> Result<StructField>;
}

impl TrinoFieldExt for TrinoField {
    /// Given a `StructField`, try to find a corresponding `TrinoField`.
    fn from_struct_field(schema: &Schema, field: &StructField) -> Result<Self> {
        Ok(TrinoField {
            name: Some(TrinoIdent::new(&field.name)?),
            data_type: TrinoDataType::from_data_type(schema, &field.data_type)?,
        })
    }

    /// Convert this `TrinoField` to a portable `StructField`.
    fn to_struct_field(&self, idx: usize) -> Result<StructField> {
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

// #[cfg(test)]
// mod test {
//     use proptest::prelude::*;
//
//     use super::*;
// }
