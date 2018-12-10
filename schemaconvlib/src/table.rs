//! Core data types that we manipulate.

/// Information about a table.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Table {
    /// The name of the table.
    pub name: String,

    /// Information about the table's columns.
    pub columns: Vec<Column>,
}

/// Information about a column.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    /// Geodata in GeoJSON format, using SRID EPSG:4326 (aka WGS 84).
    GeoJson,
    /// 2-byte int.
    Int16,
    /// 4-byte integer.
    Int32,
    /// 8-byte integer.
    Int64,
    /// JSON data. This includes both Postgres `json` and `jsonb` types, the
    /// differences between which don't usually matter when converting schemas.
    Json,
    /// A data type which isn't in this list.
    Other(String),
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
            DataType::Other("custom".to_owned()),
            json!({"other":"custom"}),
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
fn data_type_roundtrip() {
    use serde_json;

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
        DataType::Other("custom".to_owned()),
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
