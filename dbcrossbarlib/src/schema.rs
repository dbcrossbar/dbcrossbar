//! Our "interchange" format for database table schemas.
//!
//! To convert table schemas between different databases, we have a choice:
//!
//! 1. We can convert between each pair of schema formats directly, which would
//!    require `2*n*(n-1)` conversions for `n` databases.
//! 2. We can define an "interchange" format, and then build `n` input
//!    conversions and `n` output conversions. This is much simpler.
//!
//! A good interchange format should be rich enough to include the most common
//! database types, including not just obvious things like text and integers,
//! but also things like timestamps and geodata. But a good interchange format
//! should also be as simple as possible, omitting details that generally don't
//! translate well.
//!
//! Inevitably, this means that we're going to wind up with a subjective and
//! opinionated design.
//!
//! We define our format using Rust data structures, which are serialized and
//! deserialized using [`serde`](https://serde.rs/).
//!
//! ```
//! use dbcrossbarlib::schema::Table;
//! use serde_json;
//!
//! let json = r#"
//! {
//!   "name": "example",
//!   "columns": [
//!     { "name": "a", "is_nullable": true,  "data_type": "text" },
//!     { "name": "b", "is_nullable": true,  "data_type": "int32" },
//!     { "name": "c", "is_nullable": false, "data_type": "uuid" },
//!     { "name": "d", "is_nullable": true,  "data_type": "date" },
//!     { "name": "e", "is_nullable": true,  "data_type": "float64" },
//!     { "name": "f", "is_nullable": true,  "data_type": { "array": "text" } },
//!     { "name": "h", "is_nullable": true,  "data_type": { "geo_json": 4326 } },
//!     { "name": "g", "is_nullable": true,  "data_type": { "struct": [
//!       { "name": "x", "data_type": "float64", "is_nullable": false },
//!       { "name": "y", "data_type": "float64", "is_nullable": false }
//!     ] } }
//!   ]
//! }
//! "#;
//!
//! let table: Table = serde_json::from_str(json).expect("could not parse JSON");
//! ```

use serde_derive::{Deserialize, Serialize};
#[cfg(test)]
use serde_json::json;
use std::fmt;

/// Information about a table.
///
/// This is the "top level" of our JSON schema format.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    /// The name of the table.
    pub name: String,

    /// Information about the table's columns.
    pub columns: Vec<Column>,
}

/// Information about a column.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    /// The name of the column.
    pub name: String,

    /// Can this column be `NULL`?
    pub is_nullable: bool,

    /// The data type of this column.
    pub data_type: DataType,

    /// An optional comment associated with this column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// The data type of a column.
///
/// This is a rather interesting type: It only exists to provide a reasonable
/// set of "interchange" types, that we might want to preserve when moving from
/// on database to another. So it's less precise than PostgreSQL's built-in
/// types, but more precise than BigQuery's built-in types. It exists to be a
/// "happy medium"--every output driver should be able to understand every one
/// of these types meaningfully, and it should almost always be able to map it
/// to something in the local database.
///
/// Essentially, this fulfills a similar role to the standard JSON types
/// (number, string, array, map, boolean, etc.). It's an interchange format.
/// It's not supposed to cover every imaginable type. But it should at least
/// cover common, generic types that make sense to many database backends.
///
/// We represent this as a Rust `enum`, and not a class hierarchy, because:
///
/// 1. Class hierarchies provide an extensible set of _types_ (subclasses), but
///    a closed set of _operations_ (instance methods on the root class).
/// 2. Rust `enum`s provide a closed set of _types_ (`enum` variants), but an
///    open set of operations (`match` statements matching each possible
///    variant).
///
/// In this case, we will extend and change our set of _operations_ regularly,
/// as we add new input and output filters. But we will only change the possible
/// data types after careful deliberation. So `enum` is the better choice here.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    /// An array of another data type. For many output formats, it may not be
    /// possible to nest arrays.
    Array(Box<DataType>),
    /// A boolean value.
    Bool,
    /// A date, with no associated time value.
    Date,
    /// A decimal integer (can represent currency, etc., without rounding
    /// errors).
    Decimal,
    /// 4-byte float.
    Float32,
    /// 8-byte float.
    Float64,
    /// Geodata in GeoJSON format, using the specified SRID.
    GeoJson(Srid),
    /// 2-byte int.
    Int16,
    /// 4-byte integer.
    Int32,
    /// 8-byte integer.
    Int64,
    /// JSON data. This includes both Postgres `json` and `jsonb` types, the
    /// differences between which don't usually matter when converting schemas.
    Json,
    /// A text type.
    Text,
    /// A structure with a known set of named fields.
    ///
    /// Field names must be unique within a struct, and non-empty.
    Struct(Vec<StructField>),
    /// A timestamp with no timezone. Ideally, this will would be in UTC, and
    /// some systems like BigQuery may automatically assume that.
    TimestampWithoutTimeZone,
    /// A timestamp with a timezone.
    TimestampWithTimeZone,
    /// A UUID.
    Uuid,
}

impl DataType {
    /// Should we serialize values of this type as JSON in a CSV file?
    pub(crate) fn serializes_as_json_for_csv(&self) -> bool {
        match self {
            DataType::Array(_)
            | DataType::GeoJson(_)
            | DataType::Json
            | DataType::Struct(_) => true,

            DataType::Bool
            | DataType::Date
            | DataType::Decimal
            | DataType::Float32
            | DataType::Float64
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Text
            | DataType::TimestampWithoutTimeZone
            | DataType::TimestampWithTimeZone
            | DataType::Uuid => false,
        }
    }
}

/// Information about a named field.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StructField {
    /// The name of this field.
    pub name: String,

    /// Can this field be `NULL`?
    pub is_nullable: bool,

    /// The type of this field.
    pub data_type: DataType,
}

#[test]
fn data_type_serialization_examples() {
    // Our serialization format is an external format, so let's write some tests
    // to make sure we don't change it accidentally.
    let examples = &[
        (
            DataType::Array(Box::new(DataType::Text)),
            json!({"array":"text"}),
        ),
        (DataType::Bool, json!("bool")),
        (DataType::Date, json!("date")),
        (DataType::Decimal, json!("decimal")),
        (DataType::Float32, json!("float32")),
        (DataType::Float64, json!("float64")),
        (DataType::Int16, json!("int16")),
        (DataType::Int32, json!("int32")),
        (DataType::Int64, json!("int64")),
        (DataType::Json, json!("json")),
        (
            DataType::Struct(vec![StructField {
                name: "x".to_owned(),
                is_nullable: false,
                data_type: DataType::Float32,
            }]),
            json!({ "struct": [
                { "name": "x", "is_nullable": false, "data_type": "float32" },
            ] }),
        ),
        (DataType::Text, json!("text")),
        (
            DataType::TimestampWithoutTimeZone,
            json!("timestamp_without_time_zone"),
        ),
        (
            DataType::TimestampWithTimeZone,
            json!("timestamp_with_time_zone"),
        ),
        (DataType::Uuid, json!("uuid")),
    ];
    for (data_type, serialized) in examples {
        assert_eq!(&json!(data_type), serialized);
    }
}

#[test]
fn parse_schema_from_manual() {
    // We use this schema as an example in our manual, so make sure it parses.
    serde_json::from_str::<Table>(include_str!(
        "../../dbcrossbar/fixtures/dbcrossbar_schema.json"
    ))
    .unwrap();
}

#[test]
fn data_type_roundtrip() {
    let data_types = vec![
        DataType::Array(Box::new(DataType::Text)),
        DataType::Bool,
        DataType::Date,
        DataType::Decimal,
        DataType::Float32,
        DataType::Float64,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Json,
        DataType::Struct(vec![StructField {
            name: "x".to_owned(),
            is_nullable: false,
            data_type: DataType::Float32,
        }]),
        DataType::Text,
        DataType::TimestampWithoutTimeZone,
        DataType::TimestampWithTimeZone,
        DataType::Uuid,
    ];
    for data_type in &data_types {
        let serialized = serde_json::to_string(data_type).unwrap();
        println!("{:?}: {}", data_type, serialized);
        let parsed: DataType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(&parsed, data_type);
    }
}

/// An SRID number specifying how to intepret geographical coordinates.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Srid(u32);

impl Srid {
    /// Return the one true SRID (WGS84), according to our GIS folks and Google BigQuery.
    pub fn wgs84() -> Srid {
        Srid(4326)
    }

    /// Create a new `Srid` from a numeric code.
    pub fn new(srid: u32) -> Srid {
        Srid(srid)
    }

    /// Return our `Srid` as a `u32`.
    pub fn to_u32(self) -> u32 {
        self.0
    }
}

impl Default for Srid {
    /// Default to WGS84.
    fn default() -> Self {
        Self::wgs84()
    }
}

impl fmt::Display for Srid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}
