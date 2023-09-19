//! Conversion to and from BigML data types.

use bigml::resource::source::Optype;

use crate::common::*;
use crate::schema::DataType;

/// Local extensions to the BigML [`Optype`] type.
pub(crate) trait OptypeExt {
    /// Convert a portable `DateType` into a BigML-specific one.
    fn for_data_type(
        schema: &Schema,
        data_type: &DataType,
        optype_for_text: Optype,
    ) -> Result<Optype>;

    /// Convert a BigML-specific type into a portable one.
    fn to_data_type(&self) -> Result<DataType>;
}

impl OptypeExt for Optype {
    /// Construct a BigML optype from a portable data type.
    fn for_data_type(
        schema: &Schema,
        data_type: &DataType,
        optype_for_text: Optype,
    ) -> Result<Optype> {
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
            DataType::Named(name) => {
                let ty = schema.data_type_for_name(name);
                Optype::for_data_type(schema, ty, optype_for_text)
            }
            DataType::OneOf(_) => Ok(Optype::Categorical),
            DataType::Struct(_) => Ok(Optype::Text),
            DataType::Text => Ok(optype_for_text),
            DataType::TimestampWithoutTimeZone => Ok(Optype::DateTime),
            DataType::TimestampWithTimeZone => Ok(Optype::DateTime),
            DataType::TimeWithoutTimeZone => Ok(Optype::DateTime),
            DataType::Uuid => Ok(Optype::Text),
        }
    }

    /// Convert a BigML optype to a portable data type.
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

#[test]
fn map_one_of_to_categorical() {
    use crate::schema::NamedDataType;

    let mut schema = Schema::dummy_test_schema();
    schema.named_data_types.insert(
        "cat".to_owned(),
        NamedDataType {
            name: "cat".to_owned(),
            data_type: DataType::OneOf(vec!["a".to_owned()]),
        },
    );

    let ot = Optype::for_data_type(
        &schema,
        &DataType::Named("cat".to_owned()),
        Optype::Text,
    )
    .unwrap();
    assert_eq!(ot, Optype::Categorical);
}
