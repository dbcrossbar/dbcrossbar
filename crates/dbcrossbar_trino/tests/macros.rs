//! Tests for our macros.

#[cfg(feature = "macros")]
#[test]
fn test_derive_trino_row() {
    use dbcrossbar_trino::{
        values::{DataTypeOrAny, ExpectedDataType as _, FieldWithDataTypeOrAny},
        DataType, Field, Ident, TrinoRow, Value,
    };

    #[derive(TrinoRow)]
    struct MyRow {
        a: i32,
        b: String,
    }

    let expected = DataTypeOrAny::Row(vec![
        FieldWithDataTypeOrAny {
            name: Some(Ident::new("a").unwrap()),
            data_type: DataTypeOrAny::DataType(DataType::Int),
        },
        FieldWithDataTypeOrAny {
            name: Some(Ident::new("b").unwrap()),
            data_type: DataTypeOrAny::DataType(DataType::varchar()),
        },
    ]);
    assert_eq!(MyRow::expected_data_type(), expected);

    let value = Value::Row {
        values: vec![Value::Int(1), Value::Varchar("hello".to_string())],
        literal_type: DataType::Row(vec![
            Field::named(Ident::new("a").unwrap(), DataType::Int),
            Field::named(Ident::new("b").unwrap(), DataType::varchar()),
        ]),
    };
    let row: MyRow = value.try_into().unwrap();
    assert_eq!(row.a, 1);
    assert_eq!(row.b, "hello");
}
