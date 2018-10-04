//! Core data types that we manipulate.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt,
    str::FromStr,
};

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
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub comment: Option<String>,
}

/// The data type of a column.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(missing_docs)]
pub enum DataType {
    /// An array of another data type.
    Array(Box<DataType>),
    Bigint,
    Boolean,
    CharacterVarying,
    Date,
    DoublePrecision,
    Integer,
    Interval,
    Json,
    Jsonb,
    Name,
    Numeric,
    /// A data type which isn't in this list.
    Other(String),
    Real,
    Smallint,
    Text,
    TimestampWithoutTimeZone,
    TimestampWithTimeZone,
    Uuid,
}

/// A type representing an error which can never happen. It's an `enum` with no
/// possible values, and it cannot be instantiated.
#[derive(Debug)]
pub enum NoError {}

impl FromStr for DataType {
    type Err = NoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.ends_with("[]") {
            let element_type = s[..s.len()-2].parse()?;
            Ok(DataType::Array(Box::new(element_type)))
        } else {
            match s {
                "bigint" => Ok(DataType::Bigint),
                "boolean" => Ok(DataType::Boolean),
                "character varying" => Ok(DataType::CharacterVarying),
                "date" => Ok(DataType::Date),
                "double precision" => Ok(DataType::DoublePrecision),
                "integer" => Ok(DataType::Integer),
                "interval" => Ok(DataType::Interval),
                "json" => Ok(DataType::Json),
                "jsonb" => Ok(DataType::Jsonb),
                "name" => Ok(DataType::Name),
                "numeric" => Ok(DataType::Numeric),
                "real" => Ok(DataType::Real),
                "smallint" => Ok(DataType::Smallint),
                "text" => Ok(DataType::Text),
                "timestamp without time zone" => Ok(DataType::TimestampWithoutTimeZone),
                "timestamp with time zone" => Ok(DataType::TimestampWithTimeZone),
                "uuid" => Ok(DataType::Uuid),
                other =>Ok(DataType::Other(other.to_owned())),
            }
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataType::Array(element_type) => write!(f, "{}[]", element_type),
            DataType::Bigint => write!(f, "bigint"),
            DataType::Boolean => write!(f, "boolean"),
            DataType::CharacterVarying => write!(f, "character varying"),
            DataType::Date => write!(f, "date"),
            DataType::DoublePrecision => write!(f, "double precision"),
            DataType::Integer => write!(f, "integer"),
            DataType::Interval => write!(f, "interval"),
            DataType::Json => write!(f, "json"),
            DataType::Jsonb => write!(f, "jsonb"),
            DataType::Name => write!(f, "name"),
            DataType::Numeric => write!(f, "numeric"),
            DataType::Other(name) => write!(f, "{}", name),
            DataType::Real => write!(f, "real"),
            DataType::Smallint => write!(f, "smallint"),
            DataType::Text => write!(f, "text"),
            DataType::TimestampWithoutTimeZone => write!(f, "timestamp without time zone"),
            DataType::TimestampWithTimeZone => write!(f, "timestamp with time zone"),
            DataType::Uuid => write!(f, "uuid"),
        }
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let name: &str = Deserialize::deserialize(deserializer)?;
        // `unwrap` is safe because `parse` returns `NoError`.
        Ok(name.parse().unwrap())
    }
}

impl Serialize for DataType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("{}", self).serialize(serializer)
    }
}

#[test]
fn data_type_roundtrip() {
    let data_types = vec![
        DataType::Array(Box::new(DataType::Text)),
        DataType::Bigint,
        DataType::Boolean,
        DataType::CharacterVarying,
        DataType::Date,
        DataType::DoublePrecision,
        DataType::Integer,
        DataType::Interval,
        DataType::Json,
        DataType::Jsonb,
        DataType::Name,
        DataType::Numeric,
        DataType::Other("custom".to_owned()),
        DataType::Real,
        DataType::Smallint,
        DataType::Text,
        DataType::TimestampWithoutTimeZone,
        DataType::TimestampWithTimeZone,
        DataType::Uuid,
    ];
    for data_type in &data_types {
        let parsed = data_type.to_string().parse::<DataType>().unwrap();
        assert_eq!(&parsed, data_type);
    }
}
