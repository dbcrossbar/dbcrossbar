//! Write data values in PostgreSQL `BINARY` format.

use byteorder::{LittleEndian, NetworkEndian as NE, WriteBytesExt};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use geo_types::Geometry;
use std::mem::{size_of, size_of_val};
use uuid::Uuid;
use wkb::geom_to_wkb;

use super::WriteExt;
use crate::common::*;
use crate::drivers::postgres_shared::Srid;

/// A JSON string that we want to serialize as `jsonb` format 1.
pub(crate) struct RawJsonb<'a>(pub(crate) &'a str);

/// A geometry with an attached [`SRID`].
pub(crate) struct GeometryWithSrid<'a> {
    /// Our geometry.
    pub(crate) geometry: &'a Geometry<f64>,
    /// The coordinate system for interpreting our geometry.
    pub(crate) srid: Srid,
}

/// Write a value in PostgreSQL `BINARY` format.
pub(crate) trait WriteBinary {
    /// Write this value to `f` in PostgreSQL `BINARY` format.
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()>;
}

impl WriteBinary for bool {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        wtr.write_len(size_of::<u8>())?;
        wtr.write_u8(match self {
            true => 1,
            false => 0,
        })?;
        Ok(())
    }
}

impl WriteBinary for NaiveDate {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        let epoch = NaiveDate::from_ymd(2000, 1, 1);
        let day_number = cast::i32((*self - epoch).num_days())?;
        wtr.write_len(size_of_val(&day_number))?;
        wtr.write_i32::<NE>(day_number)?;
        Ok(())
    }
}

impl WriteBinary for f32 {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        wtr.write_len(size_of_val(self))?;
        wtr.write_f32::<NE>(*self)?;
        Ok(())
    }
}

impl WriteBinary for f64 {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        wtr.write_len(size_of_val(self))?;
        wtr.write_f64::<NE>(*self)?;
        Ok(())
    }
}

impl<'a> WriteBinary for GeometryWithSrid<'a> {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        // Serialize our geometry in `wkb` format. Unfortunately,
        // this allocates.
        let wkb = geom_to_wkb(&self.geometry);

        // Patch up our `wkb` value to use EWKB format with a SRID, which PostGIS
        // requires. See
        // http://trac.osgeo.org/postgis/browser/trunk/doc/ZMSgeoms.txt for details.
        wtr.write_len(wkb.len() + 4)?;
        wtr.write_all(&wkb[0..4])?; // These header bytes are OK.
        wtr.write_u8(wkb[4] | 0x20)?; // Set SRID present flag.
        wtr.write_u32::<LittleEndian>(self.srid.to_u32())?; // Splice in our SRID.
        wtr.write_all(&wkb[5..])?; // And write the rest.
        Ok(())
    }
}

impl WriteBinary for i16 {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        wtr.write_len(size_of_val(self))?;
        wtr.write_i16::<NE>(*self)?;
        Ok(())
    }
}

impl WriteBinary for i32 {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        wtr.write_len(size_of_val(self))?;
        wtr.write_i32::<NE>(*self)?;
        Ok(())
    }
}

impl WriteBinary for i64 {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        wtr.write_len(size_of_val(self))?;
        wtr.write_i64::<NE>(*self)?;
        Ok(())
    }
}

impl<'a> WriteBinary for RawJsonb<'a> {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        wtr.write_len(1 + self.0.len())?;
        wtr.write_u8(1)?; // jsonb format tag.
        wtr.write_all(self.0.as_bytes())?;
        Ok(())
    }
}

impl<'a> WriteBinary for &'a str {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        wtr.write_len(self.len())?;
        wtr.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl<'a> WriteBinary for NaiveDateTime {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        let epoch = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);
        let duration = *self - epoch;
        let microseconds = duration
            .num_microseconds()
            .ok_or_else(|| format_err!("date math overflow"))?;
        wtr.write_len(size_of::<i64>())?;
        wtr.write_i64::<NE>(microseconds)?;
        Ok(())
    }
}

impl<'a> WriteBinary for DateTime<Utc> {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        let epoch = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let duration = *self - epoch;
        let microseconds = duration
            .num_microseconds()
            .ok_or_else(|| format_err!("date math overflow"))?;
        wtr.write_len(size_of::<i64>())?;
        wtr.write_i64::<NE>(microseconds)?;
        Ok(())
    }
}

impl WriteBinary for Uuid {
    fn write_binary<W: Write>(&self, wtr: &mut W) -> Result<()> {
        wtr.write_len(self.as_bytes().len())?;
        wtr.write_all(self.as_bytes())?;
        Ok(())
    }
}
