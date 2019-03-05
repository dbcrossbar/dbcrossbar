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
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};
use csv;
use geo_types::Geometry;
use geojson::{conversion::TryInto, GeoJson};
use lazy_static::lazy_static;
use regex::Regex;
use std::{
    io::{self, prelude::*},
    mem::size_of,
    str::{self, FromStr},
};
use uuid::Uuid;
use wkb::geom_to_wkb;

use crate::common::*;
use crate::drivers::postgres_shared::{
    PgCreateTable, PgDataType, PgScalarDataType, Srid,
};

/// A buffered writer. Here, we know the concrete type of the outer `BufWriter`,
/// so we can make lots of small writes efficiently. But we don't know the type of
/// the underlying `dyn Write` implementation. This means that when we flush our
/// writes, we'll need to pay for dynamic dispatch.
type BufferedWriter = io::BufWriter<Box<dyn Write>>;

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
    // We use the same list of boolean expressions as
    // https://github.com/kevincox/humanbool.rs, but we use regular expressions
    // and do a case-insensitive match. Is this reasonably fast? We'll probably
    // match a ton of these and we don't want to allocate memory using
    // `to_lowercase`.
    lazy_static! {
        static ref TRUE_RE: Regex = Regex::new(r"^(?i)(?:1|y|yes|on|t|true)$")
            .expect("invalid `TRUE_RE` in source");
        static ref FALSE_RE: Regex = Regex::new(r"^(?i)(?:0|n|no|off|f|false)$")
            .expect("invalid `TRUE_RE` in source");
    }

    let binary_bool = if TRUE_RE.is_match(cell) {
        1
    } else if FALSE_RE.is_match(cell) {
        0
    } else {
        return Err(format_err!("cannot parse boolean {:?}", cell));
    };
    wtr.write_len(size_of::<u8>())?;
    wtr.write_u8(binary_bool)?;
    Ok(())
}

/// Convert a `date` column to binary.
fn date_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let d = cell.parse::<NaiveDate>()?;
    let epoch = NaiveDate::from_ymd(2000, 1, 1);
    let day_number = cast::i32((d - epoch).num_days())?;
    wtr.write_len(size_of::<i32>())?;
    wtr.write_i32::<NE>(day_number)?;
    Ok(())
}

/// Convert an `real` column to binary.
fn real_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let f = cell.parse::<f32>()?;
    wtr.write_len(size_of::<f32>())?;
    wtr.write_f32::<NE>(f)?;
    Ok(())
}

/// Convert a `double precision` column to binary.
fn double_precision_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let f = cell.parse::<f64>()?;
    wtr.write_len(size_of::<f64>())?;
    wtr.write_f64::<NE>(f)?;
    Ok(())
}

/// Convert a `geometry` column to binary.
fn geometry_to_binary(wtr: &mut BufferedWriter, srid: Srid, cell: &str) -> Result<()> {
    // Convert our GeoJSON into standard WKB format. Unfortunately, this
    // allocates memory, which tends to be expensive in our inner loop.
    let geojson = cell.parse::<GeoJson>()?;
    let wkb = if let GeoJson::Geometry(geojson_geometry) = geojson {
        let geometry: Geometry<f64> = geojson_geometry
            .value
            .try_into()
            .map_err(|e| format_err!("couldn't convert point: {}", e))?;
        geom_to_wkb(&geometry)
    } else {
        panic!("expected geometry");
    };

    // Patch up our `wkb` value to use EWKB format with a SRID, which PostGIS
    // requires. See
    // http://trac.osgeo.org/postgis/browser/trunk/doc/ZMSgeoms.txt for details.
    wtr.write_len(wkb.len() + 4)?;
    wtr.write_all(&wkb[0..4])?; // These header bytes are OK.
    wtr.write_u8(wkb[4] | 0x20)?; // Set SRID present flag.
    wtr.write_u32::<LittleEndian>(srid.to_u32())?; // Splice in our SRID.
    wtr.write_all(&wkb[5..])?; // And write the rest.
    Ok(())
}

/// Convert an `smallint` column to binary.
fn smallint_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let i = cell.parse::<i16>()?;
    wtr.write_len(size_of::<i16>())?;
    wtr.write_i16::<NE>(i)?;
    Ok(())
}

/// Convert an `int` column to binary.
fn int_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let i = cell.parse::<i32>()?;
    wtr.write_len(size_of::<i32>())?;
    wtr.write_i32::<NE>(i)?;
    Ok(())
}

/// Convert an `bigint` column to binary.
fn bigint_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let i = cell.parse::<i64>()?;
    wtr.write_len(size_of::<i64>())?;
    wtr.write_i64::<NE>(i)?;
    Ok(())
}

/// Convert a `jsonb` column to binary.
fn jsonb_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    wtr.write_len(1 + cell.len())?;
    wtr.write_u8(1)?; // jsonb format tag.
    wtr.write_all(cell.as_bytes())?;
    Ok(())
}

/// Convert a `text` column to binary.
fn text_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    wtr.write_len(cell.len())?;
    wtr.write_all(cell.as_bytes())?;
    Ok(())
}

/// Convert a `timestamp` column to binary.
fn timestamp_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let timestamp = parse_timestamp(cell)?;
    let epoch = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);
    let duration = timestamp - epoch;
    let microseconds = duration
        .num_microseconds()
        .ok_or_else(|| format_err!("date math overflow"))?;
    wtr.write_len(size_of::<i64>());
    wtr.write_i64::<NE>(microseconds)?;
    Ok(())
}

/// Convert a `timestamp with time zone` column to binary.
fn timestamp_with_time_zone_to_binary(
    wtr: &mut BufferedWriter,
    cell: &str,
) -> Result<()> {
    let timestamp = parse_timestamp_with_time_zone(cell)?;
    let epoch = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
    let duration = timestamp - epoch;
    let microseconds = duration
        .num_microseconds()
        .ok_or_else(|| format_err!("date math overflow"))?;
    wtr.write_len(size_of::<i64>());
    wtr.write_i64::<NE>(microseconds)?;
    Ok(())
}

/// Convert a `uuid` column to binary.
fn uuid_to_binary(wtr: &mut BufferedWriter, cell: &str) -> Result<()> {
    let uuid = cell.parse::<Uuid>()?;
    wtr.write_len(uuid.as_bytes().len())?;
    wtr.write_all(uuid.as_bytes())?;
    Ok(())
}

/// Parse a timestamp without a time zone.
fn parse_timestamp(s: &str) -> Result<NaiveDateTime> {
    Ok(NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .context("error parsing timestamp")?)
}

#[test]
fn parses_timestamp() {
    let examples = &[
        (
            "1969-07-20 20:17:39",
            NaiveDate::from_ymd(1969, 7, 20).and_hms(20, 17, 39),
        ),
        (
            "1969-07-20 20:17:39.0",
            NaiveDate::from_ymd(1969, 7, 20).and_hms(20, 17, 39),
        ),
    ];
    for (s, expected) in examples {
        let parsed = parse_timestamp(s).unwrap();
        assert_eq!(&parsed, expected);
    }
}

/// Parse a timestamp with a time zone.
fn parse_timestamp_with_time_zone(s: &str) -> Result<DateTime<Utc>> {
    let timestamp = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
        .context("error parsing timestamp")?;
    Ok(timestamp.with_timezone(&Utc))
}

#[test]
fn parses_timestamp_with_time_zone() {
    let examples = &[
        (
            "1969-07-20 20:17:39+00",
            Utc.ymd(1969, 7, 20).and_hms(20, 17, 39),
        ),
        (
            "1969-07-20 19:17:39.0-0100",
            Utc.ymd(1969, 7, 20).and_hms(20, 17, 39),
        ),
        (
            "1969-07-20 21:17:39.0+01:00",
            Utc.ymd(1969, 7, 20).and_hms(20, 17, 39),
        ),
    ];
    for (s, expected) in examples {
        let parsed = parse_timestamp_with_time_zone(s).unwrap();
        assert_eq!(&parsed, expected);
    }
}

/// Useful extensions to `Write`.
trait WriteExt {
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
