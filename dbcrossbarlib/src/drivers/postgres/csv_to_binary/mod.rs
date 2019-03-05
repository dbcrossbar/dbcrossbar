//! Convert CSV data to PostgreSQL `BINARY` format.
//!
//! For more details, see the following:
//!
//! - https://www.postgresql.org/docs/9.4/sql-copy.html "Binary Format"
//! - https://github.com/postgres/postgres/tree/master/src/backend/utils/adt `*send` and `*recv`
//! - https://www.postgresql.org/docs/9.4/xfunc-c.html More C type into.
//! - https://github.com/sfackler/rust-postgres/blob/master/postgres-protocol/src/types.rs Rust implementations.

use byteorder::{LittleEndian, NetworkEndian as NE, WriteBytesExt};
use cast;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use csv;
use geo_types::Geometry;
use geojson::{conversion::TryInto, GeoJson};
use lazy_static::lazy_static;
use regex::Regex;
use std::{
    io::{self, prelude::*},
    mem::size_of,
    str,
};
use uuid::Uuid;
use wkb::geom_to_wkb;

use crate::common::*;
use crate::drivers::postgres_shared::{
    PgCreateTable, PgDataType, PgScalarDataType, Srid,
};
use crate::from_csv_cell::FromCsvCell;

mod write_binary;

use self::write_binary::{GeometryWithSrid, RawJsonb, WriteBinary};

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
    let mut wtr = io::BufWriter::new(wtr);

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
    for row in rdr.records() {
        // Check for read errors.
        let row = row?;

        // Write our tuple field count.
        wtr.write_i16::<NE>(cast::i16(row.len())?)?;

        // Write each of our rows. Using `zip` allows Rust to omit bounds
        // checks on the `row` and `columns` arrays.
        for (cell, col) in row.iter().zip(table.columns.iter()) {
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
                        array_to_binary(&mut wtr, *dimension_count, ty, cell)?;
                    }
                    PgDataType::Scalar(ty) => {
                        scalar_to_binary(&mut wtr, ty, cell)?;
                    }
                }
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
    unimplemented!()
}

/// Convert a scalar value from a CSV file into a `BINARY` value.
fn scalar_to_binary(
    wtr: &mut BufferedWriter,
    data_type: &PgScalarDataType,
    cell: &str,
) -> Result<()> {
    match data_type {
        PgScalarDataType::Boolean => boolean_to_binary(wtr, cell),
        PgScalarDataType::Date => date_to_binary(wtr, cell),
        PgScalarDataType::Numeric => Err(format_err!(
            "cannot use numeric columns with PostgreSQL yet",
        )),
        PgScalarDataType::Real => real_to_binary(wtr, cell),
        PgScalarDataType::DoublePrecision => double_precision_to_binary(wtr, cell),
        PgScalarDataType::Geometry(srid) => geometry_to_binary(wtr, *srid, cell),
        PgScalarDataType::Smallint => smallint_to_binary(wtr, cell),
        PgScalarDataType::Int => int_to_binary(wtr, cell),
        PgScalarDataType::Bigint => bigint_to_binary(wtr, cell),
        PgScalarDataType::Json => Err(format_err!(
            "PostgreSQL json columns not supported (try jsonb)",
        )),
        PgScalarDataType::Jsonb => jsonb_to_binary(wtr, cell),
        PgScalarDataType::Text => text_to_binary(wtr, cell),
        PgScalarDataType::TimestampWithoutTimeZone => timestamp_to_binary(wtr, cell),
        PgScalarDataType::TimestampWithTimeZone => {
            timestamp_with_time_zone_to_binary(wtr, cell)
        }
        PgScalarDataType::Uuid => uuid_to_binary(wtr, cell),
    }
}

/// Convert a `boolean` column to binary.
fn boolean_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let value = bool::from_csv_cell(cell)?;
    value.write_binary(wtr)
}

/// Convert a `date` column to binary.
fn date_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let value = NaiveDate::from_csv_cell(cell)?;
    value.write_binary(wtr)
}

/// Convert an `real` column to binary.
fn real_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let value = f32::from_csv_cell(cell)?;
    value.write_binary(wtr)
}

/// Convert a `double precision` column to binary.
fn double_precision_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let value = f64::from_csv_cell(cell)?;
    value.write_binary(wtr)
}

/// Convert a `geometry` column to binary.
fn geometry_to_binary(wtr: &mut BufferedWriter, srid: Srid, cell: &str) -> Result<()> {
    let geometry = Geometry::<f64>::from_csv_cell(cell)?;
    let value = GeometryWithSrid {
        geometry: &geometry,
        srid,
    };
    value.write_binary(wtr)
}

/// Convert an `smallint` column to binary.
fn smallint_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let value = i16::from_csv_cell(cell)?;
    value.write_binary(wtr)
}

/// Convert an `int` column to binary.
fn int_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let value = i32::from_csv_cell(cell)?;
    value.write_binary(wtr)
}

/// Convert an `bigint` column to binary.
fn bigint_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let value = i64::from_csv_cell(cell)?;
    value.write_binary(wtr)
}

/// Convert a `jsonb` column to binary.
fn jsonb_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let value = RawJsonb(cell);
    value.write_binary(wtr)
}

/// Convert a `text` column to binary.
fn text_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    cell.write_binary(wtr)
}

/// Convert a `timestamp` column to binary.
fn timestamp_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let value = NaiveDateTime::from_csv_cell(cell)?;
    value.write_binary(wtr)
}

/// Convert a `timestamp with time zone` column to binary.
fn timestamp_with_time_zone_to_binary(
    wtr: &mut BufferedWriter,
    cell: &str,
) -> Result<()> {
    let value = DateTime::<Utc>::from_csv_cell(cell)?;
    value.write_binary(wtr)
}

/// Convert a `uuid` column to binary.
fn uuid_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let value = Uuid::from_csv_cell(cell)?;
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
        F: FnOnce(&dyn Write) -> Result<()>;
}

impl<'a, W: Write + 'a> WriteExt for W {
    fn write_len(&mut self, len: usize) -> Result<()> {
        self.write_i32::<NE>(cast::i32(len)?)?;
        Ok(())
    }

    fn write_value<F>(&mut self, buffer: &mut Vec<u8>, f: F) -> Result<()>
    where
        F: FnOnce(&dyn Write) -> Result<()>,
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
