//! Data types supported BigQuery.

use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use std::{collections::HashSet, fmt, result};

use super::ColumnName;
use crate::common::*;
use crate::schema::{DataType, Srid, StructField};
use crate::separator::Separator;

mod grammar;

/// Extensions to `DataType` (the portable version) to handle BigQuery-query
/// specific stuff.
pub(crate) trait DataTypeBigQueryExt {
    /// Can BigQuery import this type from a CSV file?
    fn bigquery_can_import_from_csv(&self, schema: &Schema) -> Result<bool>;
}

impl DataTypeBigQueryExt for DataType {
    fn bigquery_can_import_from_csv(&self, schema: &Schema) -> Result<bool> {
        // Convert this to the corresponding BigQuery type and check that.
        let bq_data_type = BqDataType::for_data_type(schema, self, Usage::FinalTable)?;
        Ok(bq_data_type.bigquery_can_import_from_csv())
    }
}

/// How do we intend to use a BigQuery type?
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Usage {
    /// We intend to use this type for loading from a CSV, which means we can't
    /// that certain data types will need to be treated as `STRING`.
    CsvLoad,

    /// We intend to use the type for
    FinalTable,
}

/// A BigQuery data type.
///
/// This is marked `pub` instead of `pub(crate)` because of limitations in
/// `rust-peg`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BqDataType {
    /// An array type. May not contain another directly nested array inside
    /// it. Use a nested struct with only one field instead.
    Array(BqNonArrayDataType),
    /// A non-array type.
    NonArray(BqNonArrayDataType),
}

impl BqDataType {
    /// Give a database-independent `DataType`, and the intended usage within
    /// BigQuery, map it to a corresponding `BqDataType`.
    ///
    /// See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types.
    pub(crate) fn for_data_type(
        schema: &Schema,
        data_type: &DataType,
        usage: Usage,
    ) -> Result<BqDataType> {
        match (data_type, usage) {
            // Arrays cannot be directly loaded from a CSV file, according to the
            // docs. So if we're working with CSVs, output them as STRING.
            (DataType::Array(_), Usage::CsvLoad) => {
                Ok(BqDataType::NonArray(BqNonArrayDataType::String))
            }
            (DataType::Array(nested), _) => {
                if let DataType::Json = nested.as_ref() {
                    return Err(format_err!(
                        "cannot represent arrays of JSON in BigQuery yet"
                    ));
                }
                let bq_nested =
                    BqNonArrayDataType::for_data_type(schema, nested, usage)?;
                Ok(BqDataType::Array(bq_nested))
            }
            (other, _) => {
                let bq_other =
                    BqNonArrayDataType::for_data_type(schema, other, usage)?;
                Ok(BqDataType::NonArray(bq_other))
            }
        }
    }

    /// Convert this `BqDataType` to `DataType`.
    pub(crate) fn to_data_type(&self) -> Result<DataType> {
        match self {
            BqDataType::Array(ty) => Ok(DataType::Array(Box::new(ty.to_data_type()?))),
            BqDataType::NonArray(ty) => ty.to_data_type(),
        }
    }

    /// Can BigQuery import this type from a CSV file?
    pub(crate) fn bigquery_can_import_from_csv(&self) -> bool {
        matches!(self, BqDataType::Array(_))
    }

    /// Can this type be safely represented as a JSON value?
    pub(crate) fn is_json_safe(&self) -> bool {
        match self {
            BqDataType::Array(ty) => ty.is_json_safe(),
            BqDataType::NonArray(ty) => ty.is_json_safe(),
        }
    }

    /// Given two data types the same name, use `other` to "upgrade" the
    /// information that we have about `self`, and return the result.
    ///
    /// This is used to replace `BqNonArrayDataType::String` with
    /// `BqNonArrayDataType::Stringified(_)` when we have more specific type
    /// information available.
    pub(crate) fn aligned_with(&self, other: &BqDataType) -> Result<BqDataType> {
        match (self, other) {
            (BqDataType::Array(self_nested), BqDataType::Array(other_nested)) => {
                Ok(BqDataType::Array(self_nested.aligned_with(other_nested)?))
            }
            (
                BqDataType::NonArray(self_nested),
                BqDataType::NonArray(other_nested),
            ) => Ok(BqDataType::NonArray(
                self_nested.aligned_with(other_nested)?,
            )),
            _ => Err(format_err!("cannot align types {:?} and {:?}", self, other)),
        }
    }
}

impl<'de> Deserialize<'de> for BqDataType {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let parsed = grammar::data_type(&raw).map_err(|err| {
            D::Error::custom(format!(
                "error parsing BigQuery data type {:?}: {}",
                raw, err
            ))
        })?;
        Ok(parsed)
    }
}

impl fmt::Display for BqDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BqDataType::Array(element_type) => write!(f, "ARRAY<{}>", element_type),
            BqDataType::NonArray(ty) => write!(f, "{}", ty),
        }
    }
}

impl Serialize for BqDataType {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert to a string and serialize that.
        format!("{}", self).serialize(serializer)
    }
}

/// Either a regular BigQuery non-array data type or `"RECORD"`, which appears
/// as a placeholder in BigQuery schema files, but it really a placeholder
/// telling us to construct a `STRUCT` type using the column's `"fields"`.
///
/// This is marked `pub` instead of `pub(crate)` because of limitations in
/// `rust-peg`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BqRecordOrNonArrayDataType {
    Record,
    DataType(BqNonArrayDataType),
}

impl<'de> Deserialize<'de> for BqRecordOrNonArrayDataType {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let parsed = grammar::record_or_non_array_data_type(&raw).map_err(|err| {
            D::Error::custom(format!(
                "error parsing BigQuery data type {:?}: {}",
                raw, err
            ))
        })?;
        Ok(parsed)
    }
}

impl fmt::Display for BqRecordOrNonArrayDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BqRecordOrNonArrayDataType::Record => write!(f, "RECORD"),
            BqRecordOrNonArrayDataType::DataType(ty) => write!(f, "{}", ty),
        }
    }
}

impl Serialize for BqRecordOrNonArrayDataType {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert to a string and serialize that.
        format!("{}", self).serialize(serializer)
    }
}
/// Any type except `ARRAY` (which cannot be nested in another `ARRAY`).
///
/// This should really be `pub(crate)`, see [BqDataType].
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum BqNonArrayDataType {
    Bool,
    Bytes,
    Date,
    Datetime,
    Float64,
    Geography,
    Int64,
    Numeric,
    String,
    Stringified(DataType),
    Struct(Vec<BqStructField>),
    Time,
    Timestamp,
}

impl BqNonArrayDataType {
    /// Give a database-independent `DataType`, and the intended usage within
    /// BigQuery, map it to a corresponding `BqNonArrayDataType`.
    ///
    /// If this is passed an array data type, it will do one of two things:
    ///
    /// 1. If we have `Usage::CsvLoad`, we will fail, because nested array types
    ///    should never occur in CSV mode.
    /// 2. Otherwise, we will assume we're dealing with a nested array, which
    ///    means that we need to wrap it in a single-element
    ///    `BqNonArrayDataType::Struct`, because BigQuery always needs to have
    ///    `ARRAY<STRUCT<ARRAY<...>>` instead of `ARRAY<ARRAY<...>>`.
    ///    TODO: Is this still a safe assumption?
    ///
    /// Getting (2) right is the whole reason for separating `BqDataType` and
    /// `BqNonArrayDataType`.
    fn for_data_type(
        schema: &Schema,
        data_type: &DataType,
        usage: Usage,
    ) -> Result<BqNonArrayDataType> {
        match data_type {
            // We should only be able to get here if we're nested inside another
            // `Array`, but the top-level `ARRAY` should already have been converted
            // to a `STRING`.
            DataType::Array(_) if usage == Usage::CsvLoad => Err(format_err!(
                "should never encounter nested arrays in CSV mode"
            )),
            DataType::Array(nested) => {
                let bq_nested =
                    BqNonArrayDataType::for_data_type(schema, nested, usage)?;
                let field = BqStructField {
                    name: None,
                    ty: BqDataType::Array(bq_nested),
                };
                Ok(BqNonArrayDataType::Struct(vec![field]))
            }
            DataType::Bool => Ok(BqNonArrayDataType::Bool),
            DataType::Date => Ok(BqNonArrayDataType::Date),
            DataType::Decimal => Ok(BqNonArrayDataType::Numeric),
            DataType::Float32 => Ok(BqNonArrayDataType::Float64),
            DataType::Float64 => Ok(BqNonArrayDataType::Float64),
            DataType::GeoJson(srid) if *srid == Srid::wgs84() => {
                Ok(BqNonArrayDataType::Geography)
            }
            ty @ DataType::GeoJson(_) => {
                Ok(BqNonArrayDataType::Stringified(ty.to_owned()))
            }
            DataType::Int16 => Ok(BqNonArrayDataType::Int64),
            DataType::Int32 => Ok(BqNonArrayDataType::Int64),
            DataType::Int64 => Ok(BqNonArrayDataType::Int64),
            DataType::Json => Ok(BqNonArrayDataType::Stringified(DataType::Json)),
            DataType::Named(name) => {
                let dt = schema.data_type_for_name(name);
                BqNonArrayDataType::for_data_type(schema, dt, usage)
            }
            DataType::OneOf(_) => Ok(BqNonArrayDataType::String),
            DataType::Struct(_) if usage == Usage::CsvLoad => {
                Ok(BqNonArrayDataType::String)
            }
            DataType::Struct(fields) => Ok(BqNonArrayDataType::Struct(
                fields
                    .iter()
                    .map(|f| BqStructField::for_struct_field(schema, f))
                    .collect::<Result<Vec<_>>>()?,
            )),
            DataType::Text => Ok(BqNonArrayDataType::String),
            // Timestamps without timezones will be mapped to `DATETIME`.
            DataType::TimestampWithoutTimeZone => Ok(BqNonArrayDataType::Datetime),
            // As far as I can tell, BigQuery will convert timestamps with timezones
            // to UTC.
            DataType::TimestampWithTimeZone => Ok(BqNonArrayDataType::Timestamp),
            DataType::Uuid => Ok(BqNonArrayDataType::Stringified(DataType::Uuid)),
        }
    }

    /// Convert this `BqNonArrayDataType` to a portable `DataType`.
    pub(crate) fn to_data_type(&self) -> Result<DataType> {
        match self {
            BqNonArrayDataType::Bool => Ok(DataType::Bool),
            BqNonArrayDataType::Date => Ok(DataType::Date),
            BqNonArrayDataType::Numeric => Ok(DataType::Decimal),
            BqNonArrayDataType::Float64 => Ok(DataType::Float64),
            BqNonArrayDataType::Geography => Ok(DataType::GeoJson(Srid::wgs84())),
            BqNonArrayDataType::Int64 => Ok(DataType::Int64),
            BqNonArrayDataType::String => Ok(DataType::Text),
            BqNonArrayDataType::Stringified(ty) => Ok(ty.to_owned()),
            BqNonArrayDataType::Datetime => Ok(DataType::TimestampWithoutTimeZone),
            // Our struct has a single anonymous field, so we should treat it as a transparent wrapper.
            //
            // TODO: This may require major export support.
            BqNonArrayDataType::Struct(bq_fields)
                if bq_fields.len() == 1 && bq_fields[0].name.is_none() =>
            {
                Err(format_err!(
                    "cannot yet export struct with 1 anonymous field: {}",
                    self
                ))
            }
            BqNonArrayDataType::Struct(bq_fields) => {
                // Convert our fields.
                let fields = bq_fields
                    .iter()
                    .map(BqStructField::to_struct_field)
                    .collect::<Result<Vec<StructField>>>()?;
                let mut names = HashSet::new();

                // Check for duplicate names.
                for f in &fields {
                    if !names.insert(&f.name[..]) {
                        return Err(format_err!(
                            "duplicate field name {:?} in BigQuery struct {}",
                            f.name,
                            self
                        ));
                    }
                }
                Ok(DataType::Struct(fields))
            }
            BqNonArrayDataType::Timestamp => Ok(DataType::TimestampWithTimeZone),
            BqNonArrayDataType::Bytes | BqNonArrayDataType::Time => Err(format_err!(
                "cannot convert {} to portable type (yet)",
                self,
            )),
        }
    }

    /// Can this type be safely represented as a JSON value?
    pub(crate) fn is_json_safe(&self) -> bool {
        match self {
            BqNonArrayDataType::Struct(fields) => {
                for field in fields {
                    // Only allow serializing structs with (1) named fields, not
                    // positional fields, and (2) unique names. This limit
                    // exists because `TO_JSON_STRING` will output JSON objects
                    // with key names of `""` or duplicate key names if these
                    // constraints aren't met.
                    let mut names = HashSet::new();
                    if let Some(name) = &field.name {
                        if !names.insert(name) || !field.ty.is_json_safe() {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            _ => true,
        }
    }

    /// Given two data types the same name, use `other` to "upgrade" the
    /// information that we have about `self`, and return the result.
    ///
    /// This is used to replace `BqNonArrayDataType::String` with
    /// `BqNonArrayDataType::Stringified(_)` when we have more specific type
    /// information available.
    pub(crate) fn aligned_with(
        &self,
        other: &BqNonArrayDataType,
    ) -> Result<BqNonArrayDataType> {
        match (self, other) {
            // Upgrade a bare `String` type to contain more detailed
            // information.
            (BqNonArrayDataType::String, ty @ BqNonArrayDataType::Stringified(_)) => {
                Ok(ty.to_owned())
            }

            // Align struct types recursively.
            (
                BqNonArrayDataType::Struct(self_fields),
                BqNonArrayDataType::Struct(other_fields),
            ) => {
                if self_fields.len() != other_fields.len() {
                    return Err(format_err!(
                        "cannot align types {} and {} other",
                        self,
                        other,
                    ));
                }
                let mut aligned_fields = vec![];
                for (self_f, other_f) in self_fields.iter().zip(other_fields.iter()) {
                    if self_f.name != other_f.name {
                        return Err(format_err!(
                            "cannot align STRUCT fields in {} and {}",
                            self,
                            other,
                        ));
                    }
                    let mut aligned_field = self_f.to_owned();
                    aligned_field.ty = aligned_field.ty.aligned_with(&other_f.ty)?;
                    aligned_fields.push(aligned_field);
                }
                Ok(BqNonArrayDataType::Struct(aligned_fields))
            }

            // Matching types need no further work.
            (self_ty, other_ty) if self_ty == other_ty => Ok(self_ty.to_owned()),

            // Any other combinations can't be aligned.
            (_, _) => Err(format_err!("cannot align types {} and {}", self, other)),
        }
    }
}

impl<'de> Deserialize<'de> for BqNonArrayDataType {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let parsed = grammar::non_array_data_type(&raw).map_err(|err| {
            D::Error::custom(format!(
                "error parsing BigQuery data type {:?}: {}",
                raw, err
            ))
        })?;
        Ok(parsed)
    }
}

impl fmt::Display for BqNonArrayDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BqNonArrayDataType::Bool => write!(f, "BOOL"),
            BqNonArrayDataType::Bytes => write!(f, "BYTES"),
            BqNonArrayDataType::Date => write!(f, "DATE"),
            BqNonArrayDataType::Datetime => write!(f, "DATETIME"),
            BqNonArrayDataType::Float64 => write!(f, "FLOAT64"),
            BqNonArrayDataType::Geography => write!(f, "GEOGRAPHY"),
            BqNonArrayDataType::Int64 => write!(f, "INT64"),
            BqNonArrayDataType::Numeric => write!(f, "NUMERIC"),
            BqNonArrayDataType::String | BqNonArrayDataType::Stringified(_) => {
                write!(f, "STRING")
            }
            BqNonArrayDataType::Struct(fields) => {
                write!(f, "STRUCT<")?;
                let mut sep = Separator::new(",");
                for field in fields {
                    write!(f, "{}{}", sep.display(), field)?;
                }
                write!(f, ">")
            }
            BqNonArrayDataType::Time => write!(f, "TIME"),
            BqNonArrayDataType::Timestamp => write!(f, "TIMESTAMP"),
        }
    }
}

impl Serialize for BqNonArrayDataType {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert to a string and serialize that.
        format!("{}", self).serialize(serializer)
    }
}

/// A field of a `STRUCT`.
///
/// This should really be `pub(crate)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BqStructField {
    /// An optional field name. BigQuery `STRUCT`s are basically tuples, but
    /// with optional names for each position in the tuple.
    ///
    /// We assume, with no particular documentation that we've seen, that these
    /// follow the rules from columns names and not generic BigQuery
    /// identifiers. However, they do _not_ need to be unique within a struct.
    pub(crate) name: Option<ColumnName>,
    /// The field type.
    pub(crate) ty: BqDataType,
}

impl BqStructField {
    /// Create a `BqStructField` from a portable `StructField`.
    fn for_struct_field(schema: &Schema, f: &StructField) -> Result<Self> {
        let name = ColumnName::try_from(&f.name)?;
        Ok(BqStructField {
            name: Some(name),
            ty: BqDataType::for_data_type(schema, &f.data_type, Usage::FinalTable)?,
        })
    }

    /// Convert this `BqStructField` to a portable `StructField`.
    fn to_struct_field(&self) -> Result<StructField> {
        if let Some(name) = &self.name {
            // This is guaranteed to be non-empty.
            assert!(!name.as_str().is_empty());
            Ok(StructField {
                name: name.to_portable_name(),
                is_nullable: true,
                data_type: self.ty.to_data_type()?,
            })
        } else {
            Err(format_err!(
                "cannot convert anonymous BigQuery field to portable struct"
            ))
        }
    }
}

impl fmt::Display for BqStructField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            // TODO: It's not clear whether we can/should escape this using
            // `Ident` to insert backticks.
            write!(f, "{} ", name.quoted())?;
        }
        write!(f, "{}", self.ty)
    }
}

#[test]
fn nested_arrays() {
    let schema = Schema::dummy_test_schema();
    let input = DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
        Box::new(DataType::Int32),
    )))));

    // What we expect when loading from a CSV file.
    let bq = BqDataType::for_data_type(&schema, &input, Usage::CsvLoad).unwrap();
    assert_eq!(format!("{}", bq), "STRING");

    // What we expect in the final BigQuery table.
    let bq = BqDataType::for_data_type(&schema, &input, Usage::FinalTable).unwrap();
    assert_eq!(
        format!("{}", bq),
        "ARRAY<STRUCT<ARRAY<STRUCT<ARRAY<INT64>>>>>"
    );
}

#[test]
fn parsing() {
    use std::convert::TryFrom;
    use BqDataType as DT;
    use BqNonArrayDataType as NADT;
    let examples = [
        ("BOOL", DT::NonArray(NADT::Bool)),
        // Not documented, but it exists.
        ("BOOLEAN", DT::NonArray(NADT::Bool)),
        ("BYTES", DT::NonArray(NADT::Bytes)),
        ("DATE", DT::NonArray(NADT::Date)),
        ("DATETIME", DT::NonArray(NADT::Datetime)),
        ("FLOAT64", DT::NonArray(NADT::Float64)),
        ("GEOGRAPHY", DT::NonArray(NADT::Geography)),
        ("INT64", DT::NonArray(NADT::Int64)),
        ("NUMERIC", DT::NonArray(NADT::Numeric)),
        ("STRING", DT::NonArray(NADT::String)),
        ("TIME", DT::NonArray(NADT::Time)),
        ("TIMESTAMP", DT::NonArray(NADT::Timestamp)),
        ("ARRAY<STRING>", DT::Array(NADT::String)),
        (
            "STRUCT<FLOAT64, FLOAT64>",
            DT::NonArray(NADT::Struct(vec![
                BqStructField {
                    name: None,
                    ty: DT::NonArray(NADT::Float64),
                },
                BqStructField {
                    name: None,
                    ty: DT::NonArray(NADT::Float64),
                },
            ])),
        ),
        (
            "STRUCT<x FLOAT64, y FLOAT64>",
            DT::NonArray(NADT::Struct(vec![
                BqStructField {
                    name: Some(ColumnName::try_from("x").unwrap()),
                    ty: DT::NonArray(NADT::Float64),
                },
                BqStructField {
                    name: Some(ColumnName::try_from("y").unwrap()),
                    ty: DT::NonArray(NADT::Float64),
                },
            ])),
        ),
        (
            "ARRAY<STRUCT<ARRAY<INT64>>>",
            DT::Array(NADT::Struct(vec![BqStructField {
                name: None,
                ty: DT::Array(NADT::Int64),
            }])),
        ),
    ];
    for (input, expected) in &examples {
        let quoted = format!("\"{}\"", input);
        let parsed: BqDataType = serde_json::from_str(&quoted).unwrap();
        assert_eq!(&parsed, expected);
    }
}
