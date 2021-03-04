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
//! use dbcrossbarlib::schema::Schema;
//! use serde_json;
//!
//! let json = r#"
//! {
//!   "named_data_types": [{
//!     "name": "color",
//!     "data_type": { "one_of": ["red", "green", "blue"] }
//!   }],
//!   "tables": [{
//!     "name": "example",
//!     "columns": [
//!       { "name": "a", "is_nullable": true,  "data_type": "text" },
//!       { "name": "b", "is_nullable": true,  "data_type": "int32" },
//!       { "name": "c", "is_nullable": false, "data_type": "uuid" },
//!       { "name": "d", "is_nullable": true,  "data_type": "date" },
//!       { "name": "e", "is_nullable": true,  "data_type": "float64" },
//!       { "name": "f", "is_nullable": true,  "data_type": { "array": "text" } },
//!       { "name": "g", "is_nullable": true,  "data_type": { "geo_json": 4326 } },
//!       { "name": "h", "is_nullable": true,  "data_type": { "struct": [
//!         { "name": "x", "data_type": "float64", "is_nullable": false },
//!         { "name": "y", "data_type": "float64", "is_nullable": false }
//!       ] } },
//!       { "name": "i", "is_nullable": false, "data_type": { "named": "color" }}
//!     ]
//!   }]
//! }
//! "#;
//!
//! let schema = serde_json::from_str::<Schema>(json).expect("could not parse JSON");
//! ```

use serde::{de::Error as _, Deserialize, Serialize};
#[cfg(test)]
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use crate::{common::*, drivers::dbcrossbar_schema::external_schema::ExternalSchema};

/// Information about about a table and any supporting types. This is the "top
/// level" of our JSON schema format.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Schema {
    /// Named type aliases. This is serialized as a list.
    pub(crate) named_data_types: HashMap<String, NamedDataType>,

    /// Tables. This is serialized as a list.
    pub(crate) table: Table,
}

impl Schema {
    /// Validate this schema. At a minimum, this should detect `DataType::Named`
    /// values without a corresponding `NamedDataType`, and detect any infinite cycles.
    fn validate(&self) -> Result<()> {
        for ndt in self.named_data_types.values() {
            ndt.data_type.validate(self)?;
        }
        for col in &self.table.columns {
            col.data_type.validate(self)?;
        }
        Ok(())
    }

    /// Construct a `Schema` from a list of `NamedDataType` and a `Table`.
    pub(crate) fn from_types_and_table(
        types: Vec<NamedDataType>,
        table: Table,
    ) -> Result<Schema> {
        let named_data_types = types
            .into_iter()
            .map(|ty| (ty.name.clone(), ty))
            .collect::<HashMap<_, _>>();
        let schema = Schema {
            named_data_types,
            table,
        };
        schema.validate()?;
        Ok(schema)
    }

    /// Given a standalone table, create a new `` object containing just
    /// that table. Returns an error if the resulting `Schema` would be invalid.
    pub(crate) fn from_table(table: Table) -> Result<Schema> {
        let schema = Schema {
            named_data_types: HashMap::new(),
            table,
        };
        schema.validate()?;
        Ok(schema)
    }

    /// Look up the `DataType` associated with a name. We assume that `validate`
    /// has already been called on this schema.
    pub(crate) fn data_type_for_name(&self, name: &str) -> &DataType {
        if let Some(named_data_type) = self.named_data_types.get(name) {
            &named_data_type.data_type
        } else {
            panic!(
                "data type {:?} is not defined, and this wasn't caught by `validate`",
                name,
            );
        }
    }

    /// Create a dummy schema with a placeholder table and no named data types
    /// for test purposes.
    #[cfg(test)]
    pub(crate) fn dummy_test_schema() -> Schema {
        Schema {
            named_data_types: HashMap::new(),
            table: Table {
                name: "placeholder".to_owned(),
                columns: vec![],
            },
        }
    }
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let external = ExternalSchema::deserialize(deserializer)?;
        external.into_schema().map_err(|err| {
            D::Error::custom(format!("error validating schema: {}", err))
        })
    }
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let external = ExternalSchema::from_schema(self.to_owned());
        external.serialize(serializer)
    }
}

#[test]
fn rejects_undefined_type_names() {
    let json = r#"
    {
      "named_data_types": [],
      "table": {
        "name": "example",
        "columns": [
          { "name": "i", "is_nullable": false, "data_type": { "named": "color" }}
        ]
      }
    }
    "#;
    assert!(serde_json::from_str::<Schema>(json).is_err());
}

#[test]
fn accepts_defined_type_names() {
    let json = r#"
    {
      "named_data_types": [{
        "name": "color",
        "data_type": { "one_of": ["red", "green", "blue"] }
      }],
      "tables": [{
        "name": "example",
        "columns": [
          { "name": "i", "is_nullable": false, "data_type": { "named": "color" }}
        ]
      }]
    }
    "#;
    let schema = serde_json::from_str::<Schema>(json).expect("could not parse schema");
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
    assert_eq!(
        schema,
        Schema {
            named_data_types: expected_named_data_types,
            table: Table {
                name: "example".to_owned(),
                columns: vec![Column {
                    name: "i".to_owned(),
                    is_nullable: false,
                    data_type: DataType::Named("color".to_owned()),
                    comment: None,
                }],
            }
        }
    )
}

#[test]
fn rejects_recursive_named_types() {
    // Many recursive types are probably fine, but we haven't defined semantics
    // yet, so we return an error rather than getting into unknown territory.
    let json = r#"
    {
      "named_data_types": [{
        "name": "colors",
        "data_type": { "array": { "named": "colors" } }
      }],
      "table": {
        "name": "example",
        "columns": [
          { "name": "i", "is_nullable": false, "data_type": { "named": "colors" }}
        ]
      }
    }
    "#;
    assert!(serde_json::from_str::<Schema>(json).is_err());
}

#[test]
fn round_trip_serialization() {
    let mut named_data_types = HashMap::new();
    named_data_types.insert(
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
    let schema = Schema {
        named_data_types,
        table: Table {
            name: "example".to_owned(),
            columns: vec![Column {
                name: "i".to_owned(),
                is_nullable: false,
                data_type: DataType::Named("color".to_owned()),
                comment: None,
            }],
        },
    };
    let json = serde_json::to_string(&schema).expect("could not serialize schema");
    let parsed =
        serde_json::from_str::<Schema>(&json).expect("could not parse schema");
    assert_eq!(parsed, schema);
}

/// A named data type or type alias. This is used for things like named Postgres
/// enums.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamedDataType {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
}

/// Information about a table.
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
    /// A named data type. This should correspond to a type defined in
    /// [`Schema::named_data_types`].
    Named(String),
    /// One of a fixed list of strings. This represents an `enum` in some
    /// databases, or a `"red" | "green" | "blue"`-style union type in
    /// TypeScript, or a "categorical" value in a machine-learning system, or a
    /// `CHECK (val IN ('red', ...))` column constraint in standard SQL.
    ///
    /// We treat this separately from `Text` because it's semantically important
    /// in machine learning, and because enumeration types are an important
    /// optimization for large tables in some databases.
    OneOf(Vec<String>),
    /// A structure with a known set of named fields.
    ///
    /// Field names must be unique within a struct, and non-empty.
    Struct(Vec<StructField>),
    /// A text type.
    Text,
    /// A timestamp with no timezone. Ideally, this will would be in UTC, and
    /// some systems like BigQuery may automatically assume that.
    TimestampWithoutTimeZone,
    /// A timestamp with a timezone.
    TimestampWithTimeZone,
    /// A UUID.
    Uuid,
}

impl DataType {
    /// Is this `DataType` valid? Specifically, do all `DataType::Named` values
    /// point to a defined type, and are there no recursive types?
    fn validate(&self, schema: &Schema) -> Result<()> {
        let mut seen = HashSet::new();
        self.validate_recursive(schema, &mut seen)?;
        Ok(())
    }

    /// An internal helper function for `validate`.
    fn validate_recursive(
        &self,
        schema: &Schema,
        seen: &mut HashSet<String>,
    ) -> Result<()> {
        match self {
            DataType::Bool
            | DataType::Date
            | DataType::Decimal
            | DataType::Float32
            | DataType::Float64
            | DataType::GeoJson(_)
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Json
            | DataType::OneOf(_)
            | DataType::Text
            | DataType::TimestampWithoutTimeZone
            | DataType::TimestampWithTimeZone
            | DataType::Uuid => Ok(()),

            DataType::Array(ty) => ty.validate_recursive(schema, seen),

            DataType::Named(name) => {
                // Look up the underlying type, make sure we're not in an
                // infinitely recursive type, and validate recursively.
                if let Some(named_data_type) = schema.named_data_types.get(name) {
                    debug_assert_eq!(name, &named_data_type.name);
                    if !seen.insert(name.to_owned()) {
                        return Err(format_err!("the named type {:?} refers to itself recursively, which is not supported", name));
                    }
                    named_data_type.data_type.validate_recursive(schema, seen)?;
                    seen.remove(name);
                    Ok(())
                } else {
                    Err(format_err!(
                        "named data type {:?} is not defined anywhere",
                        name
                    ))
                }
            }

            DataType::Struct(fields) => {
                for field in fields {
                    field.data_type.validate_recursive(schema, seen)?;
                }
                Ok(())
            }
        }
    }

    /// Should we serialize values of this type as JSON in a CSV file?
    pub(crate) fn serializes_as_json_for_csv(&self, schema: &Schema) -> bool {
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
            | DataType::OneOf(_)
            | DataType::Text
            | DataType::TimestampWithoutTimeZone
            | DataType::TimestampWithTimeZone
            | DataType::Uuid => false,

            DataType::Named(name) => {
                let dt = schema.data_type_for_name(name);
                dt.serializes_as_json_for_csv(schema)
            }
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
            DataType::Named("name".to_owned()),
            json!({ "named": "name" }),
        ),
        (
            DataType::OneOf(vec!["a".to_owned()]),
            json!({ "one_of": ["a"] }),
        ),
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
    serde_json::from_str::<Schema>(include_str!(
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
        DataType::Named("name".to_owned()),
        DataType::OneOf(vec!["a".to_owned()]),
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
