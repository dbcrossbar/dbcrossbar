//! Convert CSV data to PostgreSQL `BINARY` format.
//!
//! For more details, see the following:
//!
//! - https://www.postgresql.org/docs/9.4/sql-copy.html "Binary Format"
//! - https://github.com/postgres/postgres/tree/master/src/backend/utils/adt `*send` and `*recv`
//! - https://www.postgresql.org/docs/9.4/xfunc-c.html More C type into.
//! - https://github.com/sfackler/rust-postgres/blob/master/postgres-protocol/src/types.rs Rust implementations.

use byteorder::{NetworkEndian as NE, WriteBytesExt};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use geo_types::Geometry;
use serde_json::Value;
use std::{
    io::{self, prelude::*},
    str,
};
use uuid::Uuid;

use crate::common::*;
use crate::drivers::postgres_shared::{
    PgColumn, PgCreateTable, PgDataType, PgScalarDataType,
};
use crate::from_csv_cell::FromCsvCell;
use crate::from_json_value::FromJsonValue;

mod to_postgis;
mod write_binary;

use self::write_binary::{GeometryWithSrid, RawJson, RawJsonb, WriteBinary};

/// A buffered writer. Here, we know the concrete type of the outer `BufWriter`,
/// so we can make lots of small writes efficiently. But we don't know the type of
/// the underlying `dyn Write` implementation. This means that when we flush our
/// writes, we'll need to pay for dynamic dispatch.
pub(crate) type BufferedWriter = io::BufWriter<Box<dyn Write>>;

/// Read CSV data, and write PostgreSQL `FORMAT BINARY` data, using `table` to
/// figure out how to interpret the CSV data.
///
/// This is synchronous because it relies heavily on `csv::Reader`, which takes
/// a synchronous `Read` value as input. So in general, you're going to have to
/// run it in its own thread.
///
/// This function will take care of reasonable buffering for `rdr` and `wtr`.
pub(crate) fn copy_csv_to_pg_binary(
    table: &PgCreateTable,
    rdr: Box<dyn Read>,
    wtr: Box<dyn Write>,
) -> Result<()> {
    // Set up wrappers for `rdr` and `wtr`, handling CSV parsing and buffering.
    let mut rdr = csv::Reader::from_reader(rdr);
    let mut wtr = io::BufWriter::with_capacity(BUFFER_SIZE, wtr);

    // Check to make sure our CSV headers and table column names match.
    let headers = rdr.headers()?;
    if headers.len() != table.columns.len() {
        return Err(format_err!(
            "CSV file has {} columns, but schema has {}",
            headers.len(),
            table.columns.len(),
        ));
    }
    for (idx, (hdr, col)) in headers.iter().zip(table.columns.iter()).enumerate() {
        if hdr != col.name {
            return Err(format_err!(
                "CSV file has column {} at position {}, but schema has {}",
                hdr,
                idx,
                col.name,
            ));
        }
    }

    // Write out our header.
    wtr.write_all(b"PGCOPY\n")?;
    wtr.write_all(&[0o377])?;
    wtr.write_all(b"\r\n\0")?;
    wtr.write_u32::<NE>(0)?; // Flags.
    wtr.write_u32::<NE>(0)?; // Extension area length.

    // Iterate over our CSV rows.
    for (row_idx, row) in rdr.records().enumerate() {
        // Check for read errors.
        let row = row?;

        // Write our tuple field count.
        wtr.write_i16::<NE>(cast::i16(row.len())?)?;

        // Write each of our rows. Using `zip` allows Rust to omit bounds
        // checks on the `row` and `columns` arrays.
        for (cell, col) in row.iter().zip(table.columns.iter()) {
            cell_to_binary(&mut wtr, col, cell).with_context(|_| {
                format!(
                    "could not convert row {}, column {} ({:?})",
                    row_idx + 1, // Add 1 for header row.
                    col.name,
                    cell,
                )
            })?;
        }
    }

    Ok(())
}

/// Convert a cell to PostgreSQL `BINARY` format.
fn cell_to_binary(wtr: &mut BufferedWriter, col: &PgColumn, cell: &str) -> Result<()> {
    if cell.is_empty() && col.is_nullable {
        // We found an empty string in the CSV and this column is
        // nullable, so represent it as an SQL `NULL`. If the column
        // isn't nullable, then somebody else will have to figure out
        // if they can do anything with the empty string.
        wtr.write_i32::<NE>(-1)?;
    } else {
        match &col.data_type {
            PgDataType::Array {
                dimension_count,
                ty,
            } => {
                array_to_binary(wtr, *dimension_count, ty, cell)?;
            }
            PgDataType::Scalar(ty) => {
                scalar_to_binary(wtr, ty, cell)?;
            }
        }
    }
    Ok(())
}

/// Convert a JSON-syntax array (possibly nested) into a `BINARY` array.
fn array_to_binary(
    wtr: &mut BufferedWriter,
    dimension_count: i32,
    data_type: &PgScalarDataType,
    cell: &str,
) -> Result<()> {
    // TODO: For now, we can only handle single-dimensional arrays like
    // `[1,2,3]`. Multidimensional arrays would require us to figure out things
    // like `[[1,2], [3]]` and what order to serialize nested elements in.
    if dimension_count != 1 {
        return Err(format_err!(
            "arrays with {} dimensions cannot yet be written to PostgreSQL",
            dimension_count,
        ));
    }

    // Parse our cell into a JSON value.
    let json = serde_json::from_str(cell).context("cannot parse JSON")?;
    let json_array = match json {
        Value::Array(json_array) => json_array,
        other => return Err(format_err!("expected JSON array, found {}", other)),
    };

    // Write our array, using `write_value` to calculate the total length.
    let mut buffer = vec![];
    wtr.write_value(&mut buffer, |wtr| {
        // The number of dimensions in our array.
        WriteBytesExt::write_i32::<NE>(wtr, dimension_count)?;

        // Has NULL? (I'm not sure what this does, but I figure it's safer
        // to assume we might have NULLs than otherwise.)
        WriteBytesExt::write_i32::<NE>(wtr, 1)?;

        // The OID for our `data_type`, so PostgreSQL knows how to parse this.
        WriteBytesExt::write_i32::<NE>(wtr, data_type.oid()?)?;

        // Array dimension 1 of 1: Size.
        WriteBytesExt::write_i32::<NE>(wtr, cast::i32(json_array.len())?)?;

        // Array dimension 1 of 1: Lower bound. We want 1-based, because that's the default
        // in PostgreSQL.
        WriteBytesExt::write_i32::<NE>(wtr, 1)?;

        // Elements.
        for elem in &json_array {
            match elem {
                Value::Null => {
                    WriteBytesExt::write_i32::<NE>(wtr, -1)?;
                }
                other => {
                    json_to_binary(wtr, data_type, other)?;
                }
            }
        }

        Ok(())
    })?;
    Ok(())
}

/// Interpret a JSON value as `data_type` and write it out as a `BINARY` value.
fn json_to_binary<W: Write>(
    wtr: &mut W,
    data_type: &PgScalarDataType,
    json: &Value,
) -> Result<()> {
    match data_type {
        PgScalarDataType::Boolean => write_json_as_binary::<bool, W>(wtr, json),
        PgScalarDataType::Date => write_json_as_binary::<NaiveDate, W>(wtr, json),
        PgScalarDataType::Numeric => Err(format_err!(
            "cannot use `numeric` arrays with PostgreSQL yet",
        )),
        PgScalarDataType::Real => write_json_as_binary::<f32, W>(wtr, json),
        PgScalarDataType::DoublePrecision => write_json_as_binary::<f64, W>(wtr, json),
        PgScalarDataType::Geometry(srid) => {
            let geometry = Geometry::<f64>::from_json_value(json)?;
            let value = GeometryWithSrid {
                geometry: &geometry,
                srid: *srid,
            };
            value.write_binary(wtr)
        }
        PgScalarDataType::Smallint => write_json_as_binary::<i16, W>(wtr, json),
        PgScalarDataType::Int => write_json_as_binary::<i32, W>(wtr, json),
        PgScalarDataType::Bigint => write_json_as_binary::<i64, W>(wtr, json),
        PgScalarDataType::Json => Err(format_err!(
            "PostgreSQL arrays with json elements not supported (try jsonb)",
        )),
        PgScalarDataType::Jsonb => {
            let serialized = serde_json::to_string(json)?;
            RawJsonb(&serialized).write_binary(wtr)
        }
        PgScalarDataType::Text => match json {
            Value::String(s) => s.as_str().write_binary(wtr),
            _ => Err(format_err!("expected JSON string, found {}", json)),
        },
        PgScalarDataType::TimestampWithoutTimeZone => {
            write_json_as_binary::<NaiveDateTime, W>(wtr, json)
        }
        PgScalarDataType::TimestampWithTimeZone => {
            write_json_as_binary::<DateTime<Utc>, W>(wtr, json)
        }
        PgScalarDataType::Uuid => write_json_as_binary::<Uuid, W>(wtr, json),
    }
}

/// Parse a JSON value and write it out as a PostgreSQL binary value. This works
/// for any type implementing `FromJsonValue` and `WriteBinary`. More
/// complicated cases will need to do this manually.
fn write_json_as_binary<T, W>(wtr: &mut W, json: &Value) -> Result<()>
where
    T: FromJsonValue + WriteBinary,
    W: Write,
{
    let value = T::from_json_value(json)?;
    value.write_binary(wtr)
}

/// Convert a scalar value from a CSV file into a `BINARY` value.
fn scalar_to_binary(
    wtr: &mut BufferedWriter,
    data_type: &PgScalarDataType,
    cell: &str,
) -> Result<()> {
    match data_type {
        PgScalarDataType::Boolean => write_cell_as_binary::<bool>(wtr, cell),
        PgScalarDataType::Date => write_cell_as_binary::<NaiveDate>(wtr, cell),
        PgScalarDataType::Numeric => {
            // The only sensible way to make this work is to port PostgresSQL's
            // own `decimal` parser from C, because it's an unusual internal
            // format built using very complicated parsing rules (and `numeric`
            // needs to be a perfectly-accurate type).
            Err(format_err!(
                "cannot use numeric columns with PostgreSQL yet",
            ))
        }
        PgScalarDataType::Real => write_cell_as_binary::<f32>(wtr, cell),
        PgScalarDataType::DoublePrecision => write_cell_as_binary::<f64>(wtr, cell),
        PgScalarDataType::Geometry(srid) => {
            if !cell.is_empty() && cell.as_bytes()[0].is_ascii_hexdigit() {
                // We don't have valid GeoJSON, but it looks like it's hex, so
                // try to treat it as hexadecimal-serialized EWKB data, for
                // compatibility with earlier versions of `dbcrossbar`.
                let bytes = hex::decode(cell).context("not valid GeoJSON or EWKB")?;
                (&bytes[..]).write_binary(wtr)
            } else {
                // We should have correct GeoJSON data, so handle it normally.
                let geometry = Geometry::<f64>::from_csv_cell(cell)?;
                let value = GeometryWithSrid {
                    geometry: &geometry,
                    srid: *srid,
                };
                value.write_binary(wtr)
            }
        }
        PgScalarDataType::Smallint => write_cell_as_binary::<i16>(wtr, cell),
        PgScalarDataType::Int => write_cell_as_binary::<i32>(wtr, cell),
        PgScalarDataType::Bigint => write_cell_as_binary::<i64>(wtr, cell),
        PgScalarDataType::Json => {
            let value = RawJson(cell);
            value.write_binary(wtr)
        }
        PgScalarDataType::Jsonb => {
            let value = RawJsonb(cell);
            value.write_binary(wtr)
        }
        PgScalarDataType::Text => cell.write_binary(wtr),
        PgScalarDataType::TimestampWithoutTimeZone => {
            write_cell_as_binary::<NaiveDateTime>(wtr, cell)
        }
        PgScalarDataType::TimestampWithTimeZone => {
            write_cell_as_binary::<DateTime<Utc>>(wtr, cell)
        }
        PgScalarDataType::Uuid => write_cell_as_binary::<Uuid>(wtr, cell),
    }
}

#[test]
fn parse_ewkb_fallback() {
    use crate::schema::Srid;

    // It's too annoying to actually get our data back out of the
    // `BufferedWriter` to check the actual value, so just make sure it parses.
    let cell = "0101000020E61000000000806A7CC351C093985E78E32E4540";
    let mut out = BufferedWriter::new(Box::new(vec![]));
    scalar_to_binary(&mut out, &PgScalarDataType::Geometry(Srid::wgs84()), cell)
        .unwrap();
}

/// Parse a CSV cell and write it out as a PostgreSQL binary value. This works
/// for any type implementing `FromCsvCell` and `WriteBinary`. More complicated
/// cases will need to do this manually.
fn write_cell_as_binary<T: FromCsvCell + WriteBinary>(
    wtr: &mut BufferedWriter,
    cell: &str,
) -> Result<()> {
    let value = T::from_csv_cell(cell)?;
    value.write_binary(wtr)
}

/// Useful extensions to `Write`.
pub(crate) trait WriteExt {
    /// Write the length of a PostgreSQL value.
    fn write_len(&mut self, len: usize) -> Result<()>;

    /// Given a scratch `buffer` (which must be empty) and a function
    /// that writes output, run the function, collect the output, and write
    /// the output length and the output to this writer.
    ///
    /// We use an external `buffer` because this is an inner loop that will run
    /// over terabytes of data.
    fn write_value<F>(&mut self, buffer: &mut Vec<u8>, f: F) -> Result<()>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<()>;
}

impl<'a, W: Write + 'a> WriteExt for W {
    fn write_len(&mut self, len: usize) -> Result<()> {
        self.write_i32::<NE>(cast::i32(len)?)?;
        Ok(())
    }

    fn write_value<F>(&mut self, buffer: &mut Vec<u8>, f: F) -> Result<()>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<()>,
    {
        assert!(buffer.is_empty());
        let result = f(buffer);
        if result.is_ok() {
            self.write_len(buffer.len())?;
            self.write_all(buffer)?;
        }
        buffer.clear();
        result
    }
}
