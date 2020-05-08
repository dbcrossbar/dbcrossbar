//! Conversion to and from BigML data types.

use bigml::resource::source::Optype;

use crate::common::*;
use crate::schema::DataType;

/// Local extensions to the BigQuery [`Optype`] type.
pub(crate) trait OptypeExt {
    /// Convert a portable `DateType` into a BigML-specific one.
    fn for_data_type(data_type: &DataType, optype_for_text: Optype) -> Result<Optype>;

    /// Convert a BigML-specific type into a portable one.
    fn to_data_type(&self) -> Result<DataType>;
}

impl OptypeExt for Optype {
    fn for_data_type(data_type: &DataType, optype_for_text: Optype) -> Result<Optype> {
        match data_type {
            DataType::Array(_) => Ok(Optype::Text),
            DataType::Bool => Ok(Optype::Categorical),
            DataType::Date => Ok(Optype::DateTime),
            DataType::Decimal => Ok(Optype::Numeric),
            DataType::Float32 => Ok(Optype::Numeric),
            DataType::Float64 => Ok(Optype::Numeric),
            DataType::GeoJson(_) => Ok(Optype::Text),
            DataType::Int16 => Ok(Optype::Numeric),
            DataType::Int32 => Ok(Optype::Numeric),
            DataType::Int64 => Ok(Optype::Numeric),
            DataType::Json => Ok(Optype::Text),
            DataType::Other(_) => Ok(Optype::Text),
            DataType::Struct(_) => Ok(Optype::Text),
            DataType::Text => Ok(optype_for_text),
            DataType::TimestampWithoutTimeZone => Ok(Optype::DateTime),
            DataType::TimestampWithTimeZone => Ok(Optype::DateTime),
            DataType::Uuid => Ok(Optype::Text),
        }
    }

    fn to_data_type(&self) -> Result<DataType> {
        match self {
            Optype::Categorical | Optype::DateTime | Optype::Items | Optype::Text => {
                Ok(DataType::Text)
            }
            Optype::Numeric => Ok(DataType::Float64),
            // Future versions of `bigml` may support new optypes.
            _ => Err(format_err!("unknown BigML optype {:?}", self)),
        }
    }
}
