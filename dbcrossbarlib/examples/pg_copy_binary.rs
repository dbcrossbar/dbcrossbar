//! A short spike to see if we can generate working `FORMAT BINARY` for
//! PostgreSQL.
//!
//! See:
//!
//! - https://www.postgresql.org/docs/9.4/sql-copy.html "Binary Format"
//! - https://github.com/postgres/postgres/tree/master/src/backend/utils/adt `*send` and `*recv`
//! - https://www.postgresql.org/docs/9.4/xfunc-c.html More C type into.
//! - https://github.com/sfackler/rust-postgres/blob/master/postgres-protocol/src/types.rs Rust implementations.
//!
//! ```sh
//! createdb -h localhost -U postgres -w dbcrossbar_test
//! cargo run -p dbcrossbarlib --example pg_copy_binary > pg_copy_binary.bin
//! psql postgres://postgres@localhost/dbcrossbar_test -f dbcrossbarlib/examples/pg_copy_binary.sql
//! ```

use byteorder::{LittleEndian, NetworkEndian, WriteBytesExt};
use chrono::{TimeZone, Utc};
use geo_types::Geometry;
use geojson::{conversion::TryInto, GeoJson};
use std::{
    error::Error,
    io::{self, prelude::*},
    mem::size_of,
};
use uuid::Uuid;
use wkb::geom_to_wkb;

type NE = NetworkEndian;

#[rustfmt::skip]
fn main() -> Result<(), Box<Error>> {
    let stdout = io::stdout();
    let mut out = stdout.lock();

    // Header.
    out.write_all(b"PGCOPY\n")?;
    out.write_all(&[0o377])?;
    out.write_all(b"\r\n\0")?;

    // Flags.
    out.write_u32::<NE>(0)?;

    // Header extension area length.
    out.write_u32::<NE>(0)?;

    // Tuple field count.
    out.write_i16::<NE>(15)?;

    // Field: NULL.
    out.write_i32::<NE>(-1)?;

    // Field: Array of Int32.
    out.write_i32::<NE>(8*4)?;                      // Array value length.
    out.write_i32::<NE>(1)?;                        // # of dimensions.
    out.write_i32::<NE>(1)?;                        // Has NULL?
    out.write_i32::<NE>(23)?;                       // Elem type.
    // For elem types, see `SELECT typname,oid FROM pg_type;`

    out.write_i32::<NE>(2)?;                        // Dim 1.
    out.write_i32::<NE>(1)?;                        // Dim 1 lower bound, 1-based.

    out.write_i32::<NE>(-1)?;                       // Item 1: NULL.
    out.write_i32::<NE>(size_of::<i32>() as i32)?;  // Item 2: length.
    out.write_i32::<NE>(32)?;                       // Item 2: value.

    // Field: Bool.
    out.write_i32::<NE>(size_of::<u8>() as i32)?;
    out.write_u8(1)?;

    // Field: Date.
    //
    // Julian day relative to 01 Jan 2000. Below, we use 20 July 1969.
    out.write_i32::<NE>(size_of::<i32>() as i32)?;
    out.write_i32::<NE>(-11122)?;

    // Field: Decimal. (**Punt** for now. Probably the best solution is a straight
    // port of PostgreSQL's own C parsing code to Rust.)
    //
    // See https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/numeric.c#L5620

    // Field: Float32.
    out.write_i32::<NE>(size_of::<f32>() as i32)?;
    out.write_f32::<NE>(32.0)?;

    // Field: Float64.
    out.write_i32::<NE>(size_of::<f64>() as i32)?;
    out.write_f64::<NE>(64.0)?;

    // Field: GeoJson.
    //
    // See http://trac.osgeo.org/postgis/browser/trunk/doc/ZMSgeoms.txt for
    // a discussion of EWKB + SRID format.
    let geojson = r#"{
  "type": "Point",
  "coordinates": [
    -71.05446875095367,
    42.36631683939881
  ]
}"#.parse::<GeoJson>().expect("invalid GeoJSON");
    let mut wkb = if let GeoJson::Geometry(geometry) = geojson {
        let point: Geometry<f64> = geometry.value.try_into()
            .expect("couldn't convert point");
        geom_to_wkb(&point)
    } else {
        panic!("expected geometry");
    };
    wkb[4] |= 0x20; // Set SRID present flag.
    let mut srid = Vec::with_capacity(4);
    srid.write_u32::<LittleEndian>(4326)?;
    wkb.splice(5..5, srid); // Splice in SRID.
    out.write_i32::<NE>(wkb.len() as i32)?;
    out.write_all(&wkb)?;

    // Field: Int16.
    out.write_i32::<NE>(size_of::<i16>() as i32)?;
    out.write_i16::<NE>(16)?;

    // Field: Int32.
    out.write_i32::<NE>(size_of::<i32>() as i32)?;
    out.write_i32::<NE>(32)?;

    // Field: Int64.
    out.write_i32::<NE>(size_of::<i64>() as i32)?;
    out.write_i64::<NE>(64)?;

    // Field: JSON.
    let json = r#"{ "x": 2 }"#;
    out.write_i32::<NE>((json.len() + 1) as i32)?;
    out.write_u8(1)?; // jsonb format.
    out.write_all(json.as_bytes())?;

    // Field: Text.
    let text = "Hello";
    out.write_i32::<NE>(text.len() as i32)?;
    out.write_all(text.as_bytes())?;

    // Field: Timestamp without time zone.
    let j_epoch = Utc.ymd(2000, 01, 01).and_hms(0, 0, 0);
    let date_time = Utc.ymd(1969, 07, 20).and_hms(20, 17, 39);
    let duration = date_time - j_epoch;
    let microseconds = duration.num_microseconds()
        .expect("date math overflow");
    out.write_i32::<NE>(size_of::<i64>() as i32)?;
    out.write_i64::<NE>(microseconds)?;

    // Field: Timestamp with time zone.
    out.write_i32::<NE>(size_of::<i64>() as i32)?;
    out.write_i64::<NE>(microseconds)?;

    // Field: UUID.
    let uuid = "fc438a75-3bf8-4941-8751-9208526e9516".parse::<Uuid>()?;
    out.write_i32::<NE>(uuid.as_bytes().len() as i32)?;
    out.write_all(uuid.as_bytes())?;

    // File trailer.
    out.write_i16::<NE>(-1)?;

    Ok(())
}
