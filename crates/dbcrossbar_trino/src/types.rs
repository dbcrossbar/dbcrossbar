//! A Trino data type.

use std::{error, fmt, str::FromStr};

use pretty::RcDoc;

use crate::{
    ident::Ident,
    pretty::{comma_sep_list, parens},
};

/// A Trino data type.
#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    /// A boolean value.
    Boolean,
    /// An 8-bit signed integer value.
    TinyInt,
    /// A 16-bit signed integer value.
    SmallInt,
    /// A 32-bit signed integer value.
    Int,
    /// A 64-bit signed integer value.
    BigInt,
    /// A 32-bit floating-point value.
    Real,
    /// A 64-bit floating-point value.
    Double,
    /// A fixed-point decimal value.
    Decimal {
        /// The total number of digits in the decimal value.
        precision: u32,
        /// The number of digits after the decimal point. Defaults to 0.
        scale: u32,
    },
    /// Variable-length character data.
    Varchar {
        /// The maximum number of characters in the string.
        length: Option<u32>,
    },
    /// Variable-length binary data.
    Varbinary,
    /// JSON data.
    Json,
    /// A calendar date (year, month, day), with no time zone.
    Date,
    /// A time of day (hour, minute, second), with no time zone.
    Time {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// Calendar date and time, with no time zone.
    Timestamp {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// Calendar date and time, with a time zone.
    TimestampWithTimeZone {
        /// The number of digits in the fractional seconds. Defaults to 3.
        precision: u32,
    },
    /// An array of values.
    Array(Box<DataType>),
    /// A row of fields.
    Row(Vec<Field>),
    /// A UUID.
    Uuid,
    /// A spherical geographic value. This isn't documented in the official list
    /// of Trino types, but it's mentioned in [their geospatial
    /// documentation](https://trino.io/docs/current/functions/geospatial.html).
    SphericalGeography,
}

impl DataType {
    /// Create a decimal data type with precision and scale matching BigQuery.
    /// Trino's `DECIMAL` type has no default precision, so this is as good
    /// as any other choice.
    pub fn bigquery_sized_decimal() -> Self {
        DataType::Decimal {
            precision: 38,
            scale: 9,
        }
    }

    /// Create a `VARCHAR` data type with no length, which is Trino's default.
    pub fn varchar() -> Self {
        DataType::Varchar { length: None }
    }

    /// Create a `TIME` data type with Trino's default precision.
    pub fn time() -> Self {
        DataType::Time { precision: 3 }
    }

    /// Create a `TIMESTAMP` data type with Trino's default precision.
    pub fn timestamp() -> Self {
        DataType::Timestamp { precision: 3 }
    }

    /// Create a `TIMESTAMP WITH TIME ZONE` data type with Trino's default
    /// precision.
    pub fn timestamp_with_time_zone() -> Self {
        DataType::TimestampWithTimeZone { precision: 3 }
    }

    /// Is this a ROW type with any named fields?
    #[cfg(feature = "values")]
    pub(crate) fn is_row_with_named_fields(&self) -> bool {
        match self {
            DataType::Row(fields) => fields.iter().any(|field| field.name.is_some()),
            _ => false,
        }
    }

    /// Convert to a pretty-printable [`RcDoc`]. This is useful for complex type
    /// arguments to `CAST` expressions in [`crate::pretty::ast`].
    pub fn to_doc(&self) -> RcDoc<'static, ()> {
        match self {
            DataType::Array(elem_ty) => RcDoc::concat(vec![
                RcDoc::as_string("ARRAY"),
                parens(elem_ty.to_doc()),
            ]),

            DataType::Row(fields) => RcDoc::concat(vec![
                RcDoc::as_string("ROW"),
                parens(comma_sep_list(fields.iter().map(|field| field.to_doc()))),
            ]),

            // Types which cannot contain other types will be printed without
            // further wrapping.
            _ => RcDoc::as_string(self),
        }
    }
}

// We keep around an implementation of `fmt::Display` for [`DataType`] mostly
// for use in error messages, where we don't need fancy formatting.
impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::TinyInt => write!(f, "TINYINT"),
            DataType::SmallInt => write!(f, "SMALLINT"),
            DataType::Int => write!(f, "INT"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Real => write!(f, "REAL"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Decimal { precision, scale } => {
                write!(f, "DECIMAL({}, {})", precision, scale)
            }
            DataType::Varchar { length: None } => write!(f, "VARCHAR"),
            DataType::Varchar {
                length: Some(length),
            } => write!(f, "VARCHAR({})", length),
            DataType::Varbinary => write!(f, "VARBINARY"),
            DataType::Json => write!(f, "JSON"),
            DataType::Date => write!(f, "DATE"),
            DataType::Time { precision } => write!(f, "TIME({})", precision),
            DataType::Timestamp { precision } => {
                write!(f, "TIMESTAMP({})", precision)
            }
            DataType::TimestampWithTimeZone { precision } => {
                write!(f, "TIMESTAMP({}) WITH TIME ZONE", precision)
            }
            DataType::Array(elem_ty) => write!(f, "ARRAY({})", elem_ty),
            DataType::Row(fields) => {
                write!(f, "ROW(")?;
                for (idx, field) in fields.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field)?;
                }
                write!(f, ")")
            }
            DataType::Uuid => write!(f, "UUID"),
            // This is capitalized differently in Trino's output.
            DataType::SphericalGeography => write!(f, "SphericalGeography"),
        }
    }
}

/// A field in a [`DataType::Row`] data type.
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    /// The name of the field.
    pub name: Option<Ident>,
    /// The data type of the field.
    pub data_type: DataType,
}

impl Field {
    /// Create an anonymous [`Field`] with a data type.
    pub fn anonymous(data_type: DataType) -> Self {
        Field {
            name: None,
            data_type,
        }
    }

    /// Create a named [`Field`] with a data type.
    pub fn named(name: Ident, data_type: DataType) -> Self {
        Field {
            name: Some(name),
            data_type,
        }
    }

    /// Pretty-print this `TrinoField` as a [`RcDoc`].
    fn to_doc(&self) -> RcDoc<'static, ()> {
        if let Some(name) = &self.name {
            RcDoc::concat(vec![
                RcDoc::as_string(name),
                RcDoc::space(),
                self.data_type.to_doc(),
            ])
        } else {
            self.data_type.to_doc()
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{} ", name)?;
        }
        write!(f, "{}", self.data_type)
    }
}

impl FromStr for DataType {
    type Err = ParseError;

    fn from_str(data_type: &str) -> Result<Self, Self::Err> {
        parse_rule(trino_parser::ty, data_type, "error parsing Trino data type")
    }
}

/// An error parsing a Trino data type.
#[derive(Debug)]
#[non_exhaustive]
pub struct ParseError {
    pub error_message: String,
    pub source: String,
    pub line: usize,
    pub column: usize,
    pub offset: usize,
    pub expected: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} at {}:{}: expected {} in SQL type {:?}",
            self.error_message, self.line, self.column, self.expected, self.source
        )
    }
}

impl error::Error for ParseError {}

/// Wrap a [`peg`] parser function and convert the error to a pretty
/// [`ParseError`].
fn parse_rule<Output, ParseFn>(
    parse_fn: ParseFn,
    s: &str,
    error_message: &str,
) -> Result<Output, ParseError>
where
    ParseFn: Fn(&str) -> Result<Output, peg::error::ParseError<peg::str::LineCol>>,
{
    parse_fn(s).map_err(|err| ParseError {
        error_message: error_message.to_owned(),
        source: s.to_owned(),
        line: err.location.line,
        column: err.location.column,
        offset: err.location.offset,
        expected: err.expected.to_string(),
    })
}

// `rustpeg` grammar for parsing Trino data types.
peg::parser! {
    grammar trino_parser() for str {
        rule _ = quiet! { (
            [' ' | '\t' | '\r' | '\n']
            / "--" [^'\n']* "\n"
            / "/*" (!"*/" [_])* "*/"
        )* }

        // Case-insensitive keywords.
        rule k(kw: &'static str) -> &'static str
            = quiet! { s:$(['a'..='z' | 'A'..='Z' | '_'] ['a'..='z' | 'A'..='Z' | '_' | '0'..='9']*) {?
                if s.eq_ignore_ascii_case(kw) {
                    Ok(kw)
                } else {
                    Err(kw)
                }
            } }
            / expected!(kw)

        rule ident() -> Ident
            // Note: No leading underscores allowed.
            = quiet! {
                s:$(['a'..='z' | 'A'..='Z'] ['a'..='z' | 'A'..='Z' | '_' | '0'..='9']*) {
                    // `unwrap` is safe because the parser controls our input.
                    Ident::new(s).unwrap()
                }
                / "\"" s:$(([^ '"'] / "\"\"")+) "\"" {
                    // `unwrap` is safe because the parser controls our input.
                    Ident::new(&s.replace("\"\"", "\"")).unwrap()
                }
            } / expected!("identifier")

        // A signed integer literal.
        rule i64() -> i64
            = quiet! { n:$("-"? ['0'..='9']+) {?
                n.parse().map_err(|_| "64-bit signed integer")
            } }
            / expected!("64-bit signed integer")

        // An unsigned integer literal.
        rule u32() -> u32
            = quiet! { n:$(['0'..='9']+) {?
                n.parse().map_err(|_| "32-bit unsigned integer")
            } }
            / expected!("32-bit unsigned integer")

        // A string literal.
        rule string() -> String
            = quiet! { "\'" s:$(([^ '\''] / "''")*) "\'" {
                s.replace("''", "'")
            } }
            / expected!("string literal")

        rule size_opt() -> Option<u32>
            = _? "(" _? size:u32() _? ")" { Some(size) }
            / { None }

        rule size_default(default: u32) -> u32
            = _? "(" _? size:u32() _? ")" { size }
            / { default }

        rule boolean_ty() -> DataType
            = k("boolean") { DataType::Boolean }

        rule tinyint_ty() -> DataType
            = k("tinyint") { DataType::TinyInt }

        rule smallint_ty() -> DataType
            = k("smallint") { DataType::SmallInt }

        rule int_ty() -> DataType
            = (k("integer") / k("int")) { DataType::Int }

        rule bigint_ty() -> DataType
            = k("bigint") { DataType::BigInt }

        rule real_ty() -> DataType
            = k("real") { DataType::Real }

        rule double_ty() -> DataType
            = k("double") { DataType::Double }

        rule decimal_ty() -> DataType
            = k("decimal") _? "(" _? precision:u32() _? "," _? scale:u32() _? ")" {
                DataType::Decimal { precision, scale }
            }

        rule varchar_ty() -> DataType
            = k("varchar") length:size_opt() {
                DataType::Varchar { length }
            }

        rule char_ty() -> DataType
            = k("char") length:size_default(1) {?
                //DataType::Char { length }
                Err("Trino CHAR type is not currently supported")
            }

        rule varbinary_ty() -> DataType
            = k("varbinary") { DataType::Varbinary }

        rule json_ty() -> DataType
            = k("json") { DataType::Json }

        rule date_ty() -> DataType
            = k("date") { DataType::Date }

        rule time_ty() -> DataType
            = k("time") precision:size_default(3) {
                DataType::Time { precision }
            }

        rule time_with_time_zone_ty() -> DataType
            = k("time") precision:size_default(3) _ k("with") _ k("time") _ k("zone") {?
                //DataType::TimeWithTimeZone { precision }
                Err("Trino TIME WITH TIME ZONE type is not currently supported")
            }

        rule timestamp_ty() -> DataType
            = k("timestamp") precision:size_default(3) {
                DataType::Timestamp { precision }
            }

        rule timestamp_with_time_zone_ty() -> DataType
            = k("timestamp") precision:size_default(3) _ k("with") _ k("time") _ k("zone") {
                DataType::TimestampWithTimeZone { precision }
            }

        rule interval_day_to_second_ty() -> DataType
            = k("interval") _ k("day") _ k("to") _ k("second") {?
                //DataType::IntervalDayToSecond
                Err("Trino INTERVAL DAY TO SECOND type is not currently supported")
            }

        rule interval_year_to_month_ty() -> DataType
            = k("interval") _ k("year") _ k("to") _ k("month") {?
                //DataType::IntervalYearToMonth
                Err("Trino INTERVAL YEAR TO MONTH type is not currently supported")
            }

        rule array_ty() -> DataType
            = k("array") _? "(" _? elem_ty:ty() _? ")" {
                DataType::Array(Box::new(elem_ty))
            }

        rule map_ty() -> DataType
            = k("map") _? "(" _? key_ty:ty() _? "," _? value_ty:ty() _? ")" {?
                // DataType::Map {
                //     key_type: Box::new(key_ty),
                //     value_type: Box::new(value_ty),
                // }
                Err("Trino MAP type is not currently supported")
            }

        rule row_ty() -> DataType
            = k("row") _? "(" _? fields:(field() ++ (_? "," _?)) _? ")" {
                DataType::Row(fields)
            }

        rule field() -> Field
            = ty:ty() { Field::anonymous(ty) }
            / name:ident() _ ty:ty() { Field::named(name, ty) }

        rule uuid_ty() -> DataType
            = k("uuid") { DataType::Uuid }

        rule spherical_geography_ty() -> DataType
            = k("sphericalgeography") { DataType::SphericalGeography }

        pub rule ty() -> DataType
            = boolean_ty()
            / tinyint_ty()
            / smallint_ty()
            / int_ty()
            / bigint_ty()
            / real_ty()
            / double_ty()
            / decimal_ty()
            / varchar_ty()
            / char_ty()
            / varbinary_ty()
            / json_ty()
            / date_ty()
            // The `with_time_zone` versions must come first.
            / time_with_time_zone_ty()
            / time_ty()
            / timestamp_with_time_zone_ty()
            / timestamp_ty()
            / interval_day_to_second_ty()
            / interval_year_to_month_ty()
            / array_ty()
            / map_ty()
            / row_ty()
            / uuid_ty()
            / spherical_geography_ty()
    }
}

#[cfg(all(test, feature = "proptest"))]
mod test {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn test_print_parse_roundtrip(data_type: DataType) {
            // Make sure we can print the data type without panicking.
            let s = format!("{}", data_type);
            // Make sure we can parse the string.
            let parsed = s.parse::<DataType>().unwrap();
            // Make sure the parsed data type matches the original.
            prop_assert_eq!(data_type, parsed);
        }
    }
}
