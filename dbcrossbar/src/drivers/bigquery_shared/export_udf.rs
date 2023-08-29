//! Generate UDF functions to export BigQuery values to portable CSV.
//!
//! This is essentially a miniature compiler that emits JavaScript.

use std::cmp::max;

use super::{indent_level::IndentLevel, BqColumn, BqDataType, BqNonArrayDataType};
use crate::common::*;
use crate::schema::DataType;

/// When does a type require custom exporting?
///
/// This type implements `Ord`, so that `Always > OnlyInsideUdf` and
/// `OnlyInsideUdf > Never`. We use this with `max` to combine
/// `NeedsCustomJsonExport` values.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum NeedsCustomJsonExport {
    /// This type will always be correctly serialized without any custom
    /// processing, from either SQL or JavaScript.
    Never,

    /// The type can be exported by calling `TO_JSON_STRING` on a BigQuery SQL
    /// value. But once we're inside a UDF, it requires special handling.
    OnlyInsideUdf,

    /// This type can only be exported via a UDF.
    Always,
}

impl NeedsCustomJsonExport {
    /// Does this type require custom export in an SQL context?
    pub(crate) fn in_sql_code(self) -> bool {
        match self {
            NeedsCustomJsonExport::Never | NeedsCustomJsonExport::OnlyInsideUdf => {
                false
            }
            NeedsCustomJsonExport::Always => true,
        }
    }

    /// Does this type require custom export in a JavaScript UDF context?
    fn in_js_code(self) -> bool {
        match self {
            NeedsCustomJsonExport::Never => false,
            NeedsCustomJsonExport::OnlyInsideUdf | NeedsCustomJsonExport::Always => {
                true
            }
        }
    }
}

/// If `ty` appears inside a JSON value, will it need a JSON export that's
/// fancier than what's provided by `TO_JSON_STRING` on the entire value?
pub(crate) fn needs_custom_json_export(
    ty: &BqDataType,
) -> Result<NeedsCustomJsonExport> {
    match ty {
        BqDataType::Array(nested) => non_array_needs_custom_json_export(nested),
        BqDataType::NonArray(nested) => non_array_needs_custom_json_export(nested),
    }
}

/// Similar to `needs_custom_json_export`, but for `BqNonArrayDataType`.
fn non_array_needs_custom_json_export(
    ty: &BqNonArrayDataType,
) -> Result<NeedsCustomJsonExport> {
    match ty {
        // These types should all export to JSON correctly by default, I hope.
        //
        // TODO: Check min and max `INT64`.
        BqNonArrayDataType::Bool
        | BqNonArrayDataType::Float64
        | BqNonArrayDataType::Int64
        | BqNonArrayDataType::Numeric
        | BqNonArrayDataType::String
        | BqNonArrayDataType::Timestamp => Ok(NeedsCustomJsonExport::Never),

        // The SQL function `TO_JSON_STRING` handles `DATE` correctly, but it
        // gets converted to a full `Date` when passed to JavaScript. And that
        // won't automatically convert to the right thing.
        BqNonArrayDataType::Date => Ok(NeedsCustomJsonExport::OnlyInsideUdf),

        // This will also export directly.
        BqNonArrayDataType::Stringified(DataType::Uuid) => Ok(NeedsCustomJsonExport::Never),

        // These need custom conversion to JSON.
        BqNonArrayDataType::Stringified(DataType::GeoJson(_))
        | BqNonArrayDataType::Stringified(DataType::Json) => Ok(NeedsCustomJsonExport::Always),

        // Other `DataType` values should never be stringified for BigQuery.
        BqNonArrayDataType::Stringified(nested) => Err(format_err!(
            "did not expect type {:?} to be represented a stringified value in BigQuery",
            nested,
        )),

        // Check struct types recursively.
        BqNonArrayDataType::Struct(fields) => {
            // The export behavior required for a struct is the "worst" behavior
            // required by a field in that struct. So compute the `max` of the
            // values returned by `needs_custom_json_export`, aborting early on
            // errors thanks to `try_fold`. If a struct has no fields, return
            // `NeedsCustomJsonExport::Never`.
            fields.iter()
                .map(|f| needs_custom_json_export(&f.ty))
                .try_fold(
                    // Our default value.
                    NeedsCustomJsonExport::Never,
                    // Combine `acc` (our value so far) with a new `result`.
                    |acc: NeedsCustomJsonExport, result: Result<NeedsCustomJsonExport>| {
                      Ok(max(acc, result?))
                    }
                )
        }

        // We can export either just fine as long as we stay in SQL. We assume
        // that these would need custom export code inside a UDF, but we have no
        // idea whether that would work or how to do it. So we say they need
        // custom export code, but we'll error out of `generate_export_udf` if
        // anyone asks for it.
        BqNonArrayDataType::Datetime
        | BqNonArrayDataType::Geography => Ok(NeedsCustomJsonExport::OnlyInsideUdf),

        // We're not sure how to handle these yet.
        BqNonArrayDataType::Bytes
        | BqNonArrayDataType::Time => Err(format_err!("don't know how to export {}", ty)),
    }
}

/// Given `column` and its index `idx`, generate a UDF function to serialize the
/// column as JSON.
///
/// Note that we prefer to avoid calling this, because many common types can
/// exported using SQL, which is faster.
pub(crate) fn generate_export_udf(
    column: &BqColumn,
    idx: usize,
    f: &mut dyn Write,
) -> Result<()> {
    let ty = column.bq_data_type()?;
    write!(
        f,
        r#"CREATE TEMP FUNCTION ExportJson_{idx}(value {bq_type})
RETURNS STRING
LANGUAGE js AS """
"#,
        idx = idx,
        bq_type = ty,
    )?;

    let indent = IndentLevel::none();
    match ty {
        // We want to output empty arrays as empty strings, because BigQuery
        // doesn't really distinguish between empty arrays and NULL arrays, and
        // we want to represent NULL values as empty strings in CSV files.
        BqDataType::Array(_) => {
            write!(f, "const arr = ")?;
            write_transform_expr("value", &ty, indent, f)?;
            writeln!(f, ";")?;
            writeln!(
                f,
                r#"if (arr === []) {{ return ""; }} else {{ return JSON.stringify(arr); }}"#
            )?;
        }

        // Everything else is handled simply.
        BqDataType::NonArray(_) => {
            write!(f, "return JSON.stringify(")?;
            write_transform_expr("value", &ty, indent, f)?;
            writeln!(f, ");")?;
        }
    }

    writeln!(
        f,
        r#"""";
"#
    )?;
    Ok(())
}

/// Write a JavaScript expression that will transform `input_expr` of
/// `intput_type` into a `STRING` containing serialized JSON data.
fn write_transform_expr(
    input_expr: &str,
    input_type: &BqDataType,
    indent: IndentLevel,
    f: &mut dyn Write,
) -> Result<()> {
    // Check to see if anything below this level needs custom handling. This
    // check potentially requires O((depth of type)^2) to compute, but it
    // potentially saves us lots of unnecessary work on 300-3000 BigQuery CPUs.
    if needs_custom_json_export(input_type)?.in_js_code() {
        // Yup, we need to do this the hard way.
        match input_type {
            BqDataType::Array(ty) => {
                writeln!(
                    f,
                    "({ie}) == null ? null : {ie}.map(function (e) {{",
                    ie = input_expr,
                )?;
                write!(f, "{}return ", indent.incr())?;
                write_non_array_transform_expr("e", ty, indent.incr(), f)?;
                writeln!(f, ";")?;
                write!(f, "{}}})", indent)?;
            }
            BqDataType::NonArray(ty) => {
                write_non_array_transform_expr(input_expr, ty, indent, f)?;
            }
        }
    } else {
        // Nothing inside this type requires special handling.
        write!(f, "{}", input_expr)?;
    }
    Ok(())
}

/// Write a JavaScript expression that will transform `input_expr` of
/// `intput_type` into a `STRING` containing serialized JSON data.  Only applies
/// to non-array types.
fn write_non_array_transform_expr(
    input_expr: &str,
    input_type: &BqNonArrayDataType,
    indent: IndentLevel,
    f: &mut dyn Write,
) -> Result<()> {
    match input_type {
        BqNonArrayDataType::Bool
        | BqNonArrayDataType::Float64
        | BqNonArrayDataType::Int64
        | BqNonArrayDataType::Numeric
        | BqNonArrayDataType::String
        | BqNonArrayDataType::Timestamp => {
            write!(f, "{}", input_expr)?;
        }

        // BigQuery represents DATE (which has no time or timezone) as a
        // JavaScript `Date` with a timezone of `Z`. So we need to fix it.
        BqNonArrayDataType::Date => {
            write!(f, "{}.toISOString().split('T')[0]", input_expr)?;
        }

        BqNonArrayDataType::Stringified(DataType::Uuid) => {
            write!(f, "{}", input_expr)?;
        }

        // These are stored as serialized JSON strings, and we want to turn them
        // back into JSON. This is the important part!
        BqNonArrayDataType::Stringified(DataType::GeoJson(_))
        | BqNonArrayDataType::Stringified(DataType::Json) => {
            write!(f, "JSON.parse({})", input_expr)?;
        }

        BqNonArrayDataType::Stringified(nested) => return Err(format_err!(
            "did not expect type {:?} to be represented a stringified value in BigQuery",
            nested,
        )),

        BqNonArrayDataType::Struct(fields) => {
            write!(f, "({}) == null ? null : {{", input_expr)?;
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
                        "cannot export unnamed field from {}",
                        input_type,
                    ));
                }
            }
            write!(f, "\n{}}}", indent)?;
        },

        BqNonArrayDataType::Bytes
        | BqNonArrayDataType::Datetime
        | BqNonArrayDataType::Geography
        | BqNonArrayDataType::Time => return Err(format_err!(
            "don't know how to export {} inside JSON", input_type,
        )),
    }
    Ok(())
}
