//! Write a JSON value in CSV-compatible format.

use serde_json::Value;
use std::io::Write;

use crate::common::*;
use crate::schema::DataType;

/// Write a series of JSON values as a CSV file.
pub(crate) fn write_rows<W: Write>(
    wtr: &mut W,
    schema: &Table,
    rows: Vec<Value>,
    include_headers: bool,
) -> Result<()> {
    // Create a CSV writer and write our header.
    let mut wtr = csv::Writer::from_writer(wtr);
    if include_headers {
        wtr.write_record(schema.columns.iter().map(|c| &c.name))?;
    }

    // Output our rows, using `buffer` as scratch space.
    let mut buffer = Vec::with_capacity(2 * 1024);
    for row in rows {
        write_row(&mut wtr, schema, row, &mut buffer)?;
    }
    Ok(())
}

/// Write a JSON row to a CSV document.
fn write_row<W: Write>(
    wtr: &mut csv::Writer<W>,
    schema: &Table,
    row: Value,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    // Convert our row to a JSON object.
    let obj = match row {
        Value::Object(obj) => obj,
        value => return Err(format_err!("expected JSON object, found {:?}", value)),
    };

    // Look up each column and output it.
    for col in &schema.columns {
        let value = obj.get(&col.name).unwrap_or(&Value::Null);
        buffer.clear();
        write_json_value(buffer, &col.data_type, &value)?;
        if !col.is_nullable && buffer.is_empty() {
            return Err(format_err!(
                "unexpected NULL value in column {:?}",
                col.name,
            ));
        }
        wtr.write_field(&buffer)?;
    }

    // Finish our record. To do this, we need to write an empty iterator.
    let empty: &[&str] = &[];
    wtr.write_record(empty)?;
    Ok(())
}

/// Write the specified value.
fn write_json_value<W: Write>(
    wtr: &mut W,
    data_type: &DataType,
    value: &Value,
) -> Result<()> {
    if data_type.serializes_as_json_for_csv() && !value.is_null() {
        serde_json::to_writer(wtr, value)?;
    } else {
        match value {
            // Write `null` as an empty CSV field.
            Value::Null => {}

            // Write booleans using our standard convention.
            Value::Bool(true) => write!(wtr, "t")?,
            Value::Bool(false) => write!(wtr, "f")?,

            // Numbers and strings can be written as-is.
            Value::Number(n) => write!(wtr, "{}", n)?,
            Value::String(s) => write!(wtr, "{}", s)?,

            // Compound types should never make it this far.
            Value::Array(_) | Value::Object(_) => {
                return Err(format_err!(
                    "cannot serialize {} as {:?}",
                    value,
                    data_type,
                ));
            }
        }
    }
    Ok(())
}
