//! Generate UDF functions to import BigQuery values from portable CSV.
//!
//! This is essentially a miniature compiler that emits JavaScript.

use std::io::Write;

use super::{indent_level::IndentLevel, BqColumn, BqDataType, BqNonArrayDataType};
use crate::common::*;
use crate::schema::DataType;

/// Given `column` and its index `idx`, generate a UDF function to deserialize
/// JSON strings and convert them to values of the appropriate type.
pub(crate) fn generate_import_udf(
    column: &BqColumn,
    idx: usize,
    f: &mut dyn Write,
) -> Result<()> {
    let ty = column.bq_data_type()?;
    write!(
        f,
        r#"CREATE TEMP FUNCTION ImportJson_{idx}(json_string STRING)
RETURNS {bq_type}
LANGUAGE js AS """
const json = JSON.parse(json_string);
return "#,
        idx = idx,
        bq_type = ty,
    )?;
    write_transform_expr("json", &ty, IndentLevel::none(), f)?;
    writeln!(
        f,
        r#";
""";
"#
    )?;
    Ok(())
}

/// Write a JavaScript expression that will transform `input_expr` into a value
/// of type `output_type`.
fn write_transform_expr(
    input_expr: &str,
    output_type: &BqDataType,
    indent: IndentLevel,
    f: &mut dyn Write,
) -> Result<()> {
    match output_type {
        BqDataType::Array(ty) => {
            writeln!(f, "{}.map(function (e) {{", input_expr)?;
            write!(f, "{}return ", indent.incr())?;
            write_non_array_transform_expr("e", ty, indent.incr(), f)?;
            writeln!(f, ";")?;
            write!(f, "{}}})", indent)?;
        }
        BqDataType::NonArray(ty) => {
            write_non_array_transform_expr(input_expr, ty, indent, f)?;
        }
    }
    Ok(())
}

/// Write a JavaScript expression that will transform `input_expr` into a value
/// of type `output_type`. Only applies to non-array types.
fn write_non_array_transform_expr(
    input_expr: &str,
    output_type: &BqNonArrayDataType,
    indent: IndentLevel,
    f: &mut dyn Write,
) -> Result<()> {
    match output_type {
        // These types can be directly converted to JSON.
        //
        // TODO: Check min and max `INT64` values, because need to be
        // represented as JSON strings, not JSON numbers.
        BqNonArrayDataType::Bool
        | BqNonArrayDataType::Float64
        | BqNonArrayDataType::Int64
        | BqNonArrayDataType::String => {
            write!(f, "{}", input_expr)?;
        }

        // These types all need to go through `Date`, even when they
        // theoretically don't involve time zones.
        //
        // TODO: We may need to handle `TIMESTAMP` microseconds explicitly.
        BqNonArrayDataType::Date | BqNonArrayDataType::Timestamp => {
            write!(f, "new Date({})", input_expr)?;
        }

        // These types appear as arbitrary inline JSON, so they need to be run
        // through `JSON.stringify` before we store them.
        BqNonArrayDataType::Stringified(DataType::GeoJson(_))
        | BqNonArrayDataType::Stringified(DataType::Json)
        | BqNonArrayDataType::Stringified(DataType::Struct(_)) => {
            write!(f, "JSON.stringify({})", input_expr)?;
        }

        // This is just converted to a string, with no special handling.
        BqNonArrayDataType::Stringified(DataType::Uuid) => {
            write!(f, "{}", input_expr)?;
        }

        // No other `DataType`s should appear as `Stringified`.
        BqNonArrayDataType::Stringified(ty) => {
            return Err(format_err!(
                "the type {:?} is not expected to be stringified in BigQuery",
                ty,
            ));
        }

        // Structs require special handling.
        BqNonArrayDataType::Struct(fields) => {
            write!(f, "{{")?;
            let mut first = true;
            for field in fields {
                if first {
                    first = false;
                } else {
                    write!(f, ",")?;
                }
                write!(f, "\n{}", indent.incr())?;
                if let Some(name) = &field.name {
                    let id = name.javascript_quoted();
                    write!(f, "{}: ", id)?;
                    let field_expr = format!("{}[{}]", input_expr, id);
                    write_transform_expr(&field_expr, &field.ty, indent.incr(), f)?;
                } else {
                    return Err(format_err!(
                        "cannot import unnamed field from {}",
                        output_type,
                    ));
                }
            }
            write!(f, "\n{}}}", indent)?;
        }

        // These types cannot yet be imported via our JavaScript UDFs. Sometimes
        // BigQuery simply does not provide any way to return the type from
        // JavaScript. Other times there might be a way, but we haven't
        // implemented it (perhaps because it can't appear in portable schemas).
        BqNonArrayDataType::Datetime
        | BqNonArrayDataType::Bytes
        | BqNonArrayDataType::Geography
        | BqNonArrayDataType::Numeric
        | BqNonArrayDataType::Time => {
            return Err(format_err!(
                "cannot import nested values of type {} into BigQuery yet",
                output_type,
            ));
        }
    }
    Ok(())
}
