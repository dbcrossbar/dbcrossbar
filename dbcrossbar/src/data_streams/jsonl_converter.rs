use std::{
    io::{BufRead, BufReader, BufWriter},
    ops::RangeInclusive,
};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use csv::StringRecord;
use serde_json::{Number, Value};
use uuid::Uuid;

use super::DataFormatConverter;
use crate::{
    common::*,
    from_csv_cell::FromCsvCell,
    schema::{Column, DataType},
    transform::spawn_sync_transform,
};

/// The minimum safe integer value that can be represented in JSON, as specified
/// by [RFC 7159](https://tools.ietf.org/html/rfc7159#section-6):
///
/// > Note that when such software is used, numbers that are integers and are in
/// > the range [-(2**53)+1, (2**53)-1] are interoperable in the sense that
/// > implementations will agree exactly on their numeric values.
const JSON_SAFE_INTEGERS: RangeInclusive<i64> =
    -(2_i64.pow(53) + 1)..=2_i64.pow(53) - 1;

pub(crate) struct JsonLinesConverter;

#[async_trait]
impl DataFormatConverter for JsonLinesConverter {
    async fn data_format_to_csv(
        &self,
        ctx: &Context,
        schema: &Schema,
        data: BoxStream<BytesMut>,
    ) -> Result<BoxStream<BytesMut>> {
        // Convert our JSON Lines stream to a CSV stream.
        let transform_schema = schema.clone();
        spawn_sync_transform(
            ctx.clone(),
            "copy_jsonl_to_csv".to_owned(),
            data,
            move |_ctx, rdr, wtr| copy_jsonl_to_csv(&transform_schema, rdr, wtr),
        )
    }

    async fn csv_to_data_format(
        &self,
        ctx: &Context,
        schema: &Schema,
        data: BoxStream<BytesMut>,
    ) -> Result<BoxStream<BytesMut>> {
        // Convert our CSV stream to a JSON Lines stream.
        let transform_schema = schema.clone();
        spawn_sync_transform(
            ctx.clone(),
            "copy_csv_to_jsonl".to_owned(),
            data,
            move |_ctx, rdr, wtr| copy_csv_to_jsonl(&transform_schema, rdr, wtr),
        )
    }
}

/// Synchronously copy a CSV file to a JSON Lines file. (This is a helper for
/// `JsonLinesConverter::data_format_to_csv`.)
fn copy_jsonl_to_csv(
    schema: &Schema,
    rdr: Box<dyn Read>,
    mut wtr: Box<dyn Write>,
) -> Result<()> {
    let rdr = BufReader::new(rdr);
    let mut wtr = csv::WriterBuilder::new()
        .buffer_capacity(64 * 1024)
        .from_writer(&mut wtr);

    write_header(&mut wtr, schema)?;

    let mut buffer = Vec::with_capacity(2 * 1024);
    for line in rdr.lines() {
        let line = line?;
        let value: Value = serde_json::from_str(&line)?;
        write_row(&mut wtr, schema, value, &mut buffer)?;
    }

    Ok(())
}

/// Write a series of JSON values as a CSV file.
pub(crate) fn write_rows<W: Write>(
    wtr: &mut W,
    schema: &Schema,
    rows: Vec<Value>,
    include_headers: bool,
) -> Result<()> {
    // Create a CSV writer and write our header.
    let mut wtr = csv::Writer::from_writer(wtr);
    if include_headers {
        write_header(&mut wtr, schema)?;
    }

    // Output our rows, using `buffer` as scratch space.
    let mut buffer = Vec::with_capacity(2 * 1024);
    for row in rows {
        write_row(&mut wtr, schema, row, &mut buffer)?;
    }
    Ok(())
}

/// Write our CSV header.
fn write_header<W: Write>(
    wtr: &mut csv::Writer<&mut W>,
    schema: &Schema,
) -> Result<(), Error> {
    wtr.write_record(schema.table.columns.iter().map(|c| &c.name))?;
    Ok(())
}

/// Write a JSON row to a CSV document.
fn write_row<W: Write>(
    wtr: &mut csv::Writer<W>,
    schema: &Schema,
    row: Value,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    // Convert our row to a JSON object.
    let obj = match row {
        Value::Object(obj) => obj,
        value => return Err(format_err!("expected JSON object, found {:?}", value)),
    };

    // Look up each column and output it.
    for col in &schema.table.columns {
        let value = obj.get(&col.name).unwrap_or(&Value::Null);
        buffer.clear();
        write_json_value(buffer, schema, &col.data_type, value)?;
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
    schema: &Schema,
    data_type: &DataType,
    value: &Value,
) -> Result<()> {
    if data_type.serializes_as_json_for_csv(schema) && !value.is_null() {
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

/// Synchronously copy a CSV file to a JSON Lines file. (This is a helper for
/// `JsonLinesConverter::csv_to_data_format`.)
fn copy_csv_to_jsonl(
    schema: &Schema,
    rdr: Box<dyn Read>,
    wtr: Box<dyn Write>,
) -> Result<()> {
    let mut wtr = BufWriter::new(wtr);
    for result in csv::ReaderBuilder::new()
        .buffer_capacity(64 * 1024)
        .from_reader(rdr)
        .records()
    {
        let row = result?;
        let value = convert_csv_row_to_json(schema, &row)?;
        serde_json::to_writer(&mut wtr, &value)?;
        wtr.write_all(b"\n")?;
    }

    Ok(())
}

/// Convert a CSV row to a JSON value.
fn convert_csv_row_to_json(schema: &Schema, row: &StringRecord) -> Result<Value> {
    // Look up each column and output it.
    let mut obj = serde_json::Map::new();
    if row.len() != schema.table.columns.len() {
        return Err(format_err!(
            "expected {} columns in CSV, found {}",
            schema.table.columns.len(),
            row.len(),
        ));
    }
    for (col, value) in schema.table.columns.iter().zip(row.iter()) {
        let value = convert_csv_field_to_json(schema, col, value)?;
        obj.insert(col.name.clone(), value);
    }
    Ok(Value::Object(obj))
}

/// Convert a CSV field to a JSON value.
fn convert_csv_field_to_json(
    schema: &Schema,
    column: &Column,
    value: &str,
) -> Result<Value> {
    if column.is_nullable && value.is_empty() {
        // We have a nullable column and a blank cell. `dbcrossbar` generally
        // chooses to represent empty string values as `null` wherever `null` is
        // allowed.
        Ok(Value::Null)
    } else {
        // Convert CSV data to JSON. Note that even when we're going to
        // represent the output type as a string, we strongly prefer to parse
        // and validate the raw CSV data before passing it along. This will
        // normalize the representation of certain values, and it will prevent
        // us from passing along data that isn't what we promised.
        let data_type = &column.data_type;
        match data_type {
            _ if data_type.serializes_as_json_for_csv(schema) => {
                serde_json::from_str(value)
                    .with_context(|| format_err!("expected JSON, found {:?}", value))
            }

            DataType::Array(_)
            | DataType::GeoJson(_)
            | DataType::Json
            | DataType::Struct(_) => {
                unreachable!(
                    "compound types should have been serialized as JSON by now"
                )
            }

            DataType::Bool => {
                let value: bool = FromCsvCell::from_csv_cell(value)?;
                Ok(Value::Bool(value))
            }
            DataType::Date => {
                let value: NaiveDate = FromCsvCell::from_csv_cell(value)?;
                Ok(Value::String(value.to_string()))
            }
            DataType::Decimal => {
                // TODO: We don't have a good internal representation for
                // arbitrary precision lossless decimal values, so just pass
                // them through as strings for now. Under no circumstances
                // should we represent a `Decimal` value as a JSON float.
                Ok(Value::String(value.to_string()))
            }
            DataType::Float32 | DataType::Float64 => {
                let value: f64 = FromCsvCell::from_csv_cell(value)?;
                let number = Number::from_f64(value).ok_or_else(|| {
                    // I don't know whether this can happen, or if it does, if
                    // there's a good way to handle it. So fail for now.
                    format_err!("cannot represent {:?} as a JSON float", value)
                })?;
                Ok(Value::Number(number))
            }
            DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let value: i64 = FromCsvCell::from_csv_cell(value)?;
                if JSON_SAFE_INTEGERS.contains(&value) {
                    Ok(Value::Number(Number::from(value)))
                } else {
                    // If the integer is too large to represent as a JSON float
                    // in a safely portable fashion, then represent it as a
                    // string instead. This will break some naive JSON
                    // consumers. But at least the breakage will be obvious,
                    // instead of just silently losing precision.
                    Ok(Value::String(value.to_string()))
                }
            }
            DataType::Named(name) => {
                let dt = schema.data_type_for_name(name);
                // HACK: Create a fake column with the resolved data type. We
                // should change our arguments instead. <<<<<
                let column = Column {
                    name: column.name.clone(),
                    data_type: dt.clone(),
                    is_nullable: column.is_nullable,
                    comment: column.comment.clone(),
                };
                convert_csv_field_to_json(schema, &column, value)
            }
            DataType::OneOf(values) => {
                // TODO PERFORMANCE: This is O(C*V), where C is the number of
                // cells and V is the number of values in our enum. We could
                // build and memoize a hash set to make this O(C), but that adds
                // a bunch of complexity.
                //
                // Or we could blindly trust `value` and pass it through
                // directly. But relaxing overly-strict validation is a usually
                // a non-breaking change, but fixing overly-lax validation is
                // almost always a breaking change.
                if values.iter().any(|v| v == value) {
                    Ok(Value::String(value.to_string()))
                } else {
                    Err(format_err!(
                        "expected one of {:?}, found {:?}",
                        values,
                        value,
                    ))
                }
            }
            DataType::Text => Ok(Value::String(value.to_string())),
            DataType::TimestampWithoutTimeZone => {
                let value: NaiveDateTime = FromCsvCell::from_csv_cell(value)?;
                Ok(Value::String(
                    value.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
                ))
            }
            DataType::TimestampWithTimeZone => {
                let value: DateTime<Utc> = FromCsvCell::from_csv_cell(value)?;
                Ok(Value::String(
                    value.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string(),
                ))
            }
            DataType::Uuid => {
                let value: Uuid = FromCsvCell::from_csv_cell(value)?;
                Ok(Value::String(value.to_string()))
            }
            DataType::TimeWithoutTimeZone => {
                let value: NaiveDateTime =
                    FromCsvCell::from_csv_cell(&("2000-01-01T".to_owned() + value))?;
                Ok(Value::String(value.format("%H:%M:%S%.f").to_string()))
            }
        }
    }
}
