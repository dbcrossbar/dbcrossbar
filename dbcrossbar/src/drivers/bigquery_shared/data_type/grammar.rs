//! This file contains a [`rust-peg`][peg] grammar. A "PEG" is a "parser
//! expression grammar". It's basically similar to a regular expression,
//! except it can contain recursive rules. See the site for an overview
//! of the basic syntax.
//!
//! [peg]: https://github.com/kevinmehall/rust-peg

pub use data_type_grammar::{
    data_type, non_array_data_type, record_or_non_array_data_type,
};

peg::parser! {
    grammar data_type_grammar() for str {
        use super::super::{ColumnName, BqDataType, BqNonArrayDataType, BqRecordOrNonArrayDataType, BqStructField};

        pub rule data_type() -> BqDataType
            = array_data_type()
            / ty:non_array_data_type() { BqDataType::NonArray(ty) }

        rule array_data_type() -> BqDataType
            = "ARRAY<" ty:non_array_data_type() ">" { BqDataType::Array(ty) }

        pub rule record_or_non_array_data_type() -> BqRecordOrNonArrayDataType
            = "RECORD" { BqRecordOrNonArrayDataType::Record }
            / ty:non_array_data_type() { BqRecordOrNonArrayDataType::DataType(ty) }

        pub rule non_array_data_type() -> BqNonArrayDataType
            // BOOLEAN, FLOAT and INTEGER are undocumented but seen in `bq show --schema`
            // output. Also, longer names must go first.
            = "BOOLEAN" { BqNonArrayDataType::Bool }
            / "BOOL" { BqNonArrayDataType::Bool }
            / "BYTES" { BqNonArrayDataType::Bytes }
            / "DATETIME" { BqNonArrayDataType::Datetime }
            / "DATE" { BqNonArrayDataType::Date }
            / "FLOAT64" { BqNonArrayDataType::Float64 }
            / "FLOAT" { BqNonArrayDataType::Float64 }
            / "GEOGRAPHY" { BqNonArrayDataType::Geography }
            / "INT64" { BqNonArrayDataType::Int64 }
            / "INTEGER" { BqNonArrayDataType::Int64 }
            / "NUMERIC" { BqNonArrayDataType::Numeric }
            / "STRING" { BqNonArrayDataType::String }
            / "TIMESTAMP" { BqNonArrayDataType::Timestamp }
            / "TIME" { BqNonArrayDataType::Time }
            / struct()

        rule struct() -> BqNonArrayDataType
            = "STRUCT<" fields:(field() ++ ("," ws()?)) ">" { BqNonArrayDataType::Struct(fields) }

        rule field() -> BqStructField
            // Since data_type is often a valid field_name, try matching this case first,
            // and only try matching a bare data_type if this fails. This might not be
            // optimal.
            = name:field_name() ws()? ty:data_type() {
                BqStructField { name: Some(name), ty }
            }
            / ty:data_type() { BqStructField { name: None, ty }}

        // I can't find a syntax for field names in the docs, so let's assume
        // unquoted C identifiers for now.
        rule field_name() -> ColumnName
            // Use `quiet!` and `expected!` to give nicer parse error messages.
            = quiet! {
                name:$(
                    ['A'..='Z' | 'a'..='z' | '_']
                    ['A'..='Z' | 'a'..='z' | '_' | '0'..='9']*
                ) {
                    name.parse::<ColumnName>().expect("field name should already be valid")
                }
            }
            / expected!("struct field name")

        // One or more characters of whitespace.
        rule ws() = quiet! { [' ' | '\t' | '\r' | '\n']+ }
    }
}
