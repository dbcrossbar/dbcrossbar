//! Convert geometries to PostGIS format.

use geo_types as gt;
use postgis::ewkb;
use std::convert::TryInto;

use super::write_binary::GeometryWithSrid;
use crate::common::*;

impl<'a> GeometryWithSrid<'a> {
    /// Convert a `GeometryWithSrid` into the corresponding
    /// [`postgis::ewkb::Geometry`], failing if the SRID cannot be converted to
    /// an i32 value.
    pub(crate) fn try_to_postgis(&self) -> Result<ewkb::Geometry> {
        // Convert our underlying value without an SRID.
        let mut converted = self.geometry.to_postgis();

        // Insert the SRID into our converted value.
        let new_srid = Some(
            self.srid
                .to_u32()
                .try_into()
                .map_err(|_| format_err!("out of range SRID: {}", self.srid))?,
        );
        match &mut converted {
            ewkb::GeometryT::Point(ewkb::Point { srid, .. }) => *srid = new_srid,
            ewkb::GeometryT::LineString(ewkb::LineStringT { srid, .. }) => {
                *srid = new_srid
            }
            ewkb::GeometryT::Polygon(ewkb::PolygonT { srid, .. }) => *srid = new_srid,
            ewkb::GeometryT::MultiPoint(ewkb::MultiPointT { srid, .. }) => {
                *srid = new_srid
            }
            ewkb::GeometryT::MultiLineString(ewkb::MultiLineStringT {
                srid, ..
            }) => *srid = new_srid,
            ewkb::GeometryT::MultiPolygon(ewkb::MultiPolygonT { srid, .. }) => {
                *srid = new_srid
            }
            ewkb::GeometryT::GeometryCollection(ewkb::GeometryCollectionT {
                srid,
                ..
            }) => *srid = new_srid,
        }
        Ok(converted)
    }
}

/// Implement this trait to convert a value into the corresponding `postgis`
/// geometry type.
pub(crate) trait ToPostgis {
    /// The `postgis` type corresponding to this type.
    type PostgisType;

    /// Convert this value to `Self::PostgisType`.
    fn to_postgis(&self) -> Self::PostgisType;
}

impl ToPostgis for gt::Geometry<f64> {
    type PostgisType = ewkb::Geometry;

    fn to_postgis(&self) -> Self::PostgisType {
        match self {
            gt::Geometry::Point(point) => ewkb::GeometryT::Point(point.to_postgis()),
            gt::Geometry::Line(line) => ewkb::GeometryT::LineString(line.to_postgis()),
            gt::Geometry::LineString(line_string) => {
                ewkb::GeometryT::LineString(line_string.to_postgis())
            }
            gt::Geometry::Polygon(polygon) => {
                ewkb::GeometryT::Polygon(polygon.to_postgis())
            }
            gt::Geometry::Rect(rect) => ewkb::GeometryT::Polygon(rect.to_postgis()),
            gt::Geometry::Triangle(tri) => ewkb::GeometryT::Polygon(tri.to_postgis()),
            gt::Geometry::MultiPoint(multi_point) => {
                ewkb::GeometryT::MultiPoint(multi_point.to_postgis())
            }
            gt::Geometry::MultiLineString(multi_line_string) => {
                ewkb::GeometryT::MultiLineString(multi_line_string.to_postgis())
            }
            gt::Geometry::MultiPolygon(multi_polygon) => {
                ewkb::GeometryT::MultiPolygon(multi_polygon.to_postgis())
            }
            gt::Geometry::GeometryCollection(geometry_collection) => {
                ewkb::GeometryT::GeometryCollection(geometry_collection.to_postgis())
            }
        }
    }
}

impl ToPostgis for gt::Coord<f64> {
    type PostgisType = ewkb::Point;

    fn to_postgis(&self) -> Self::PostgisType {
        ewkb::Point {
            x: self.x,
            y: self.y,
            srid: None,
        }
    }
}

impl ToPostgis for gt::Point<f64> {
    type PostgisType = ewkb::Point;

    fn to_postgis(&self) -> Self::PostgisType {
        self.0.to_postgis()
    }
}

impl ToPostgis for gt::Line<f64> {
    type PostgisType = ewkb::LineString;

    fn to_postgis(&self) -> Self::PostgisType {
        ewkb::LineString {
            points: vec![self.start.to_postgis(), self.end.to_postgis()],
            srid: None,
        }
    }
}

impl ToPostgis for gt::LineString<f64> {
    type PostgisType = ewkb::LineString;

    fn to_postgis(&self) -> Self::PostgisType {
        ewkb::LineString {
            points: self.points().map(|coord| coord.to_postgis()).collect(),
            srid: None,
        }
    }
}

impl ToPostgis for gt::Polygon<f64> {
    type PostgisType = ewkb::Polygon;

    fn to_postgis(&self) -> Self::PostgisType {
        let mut rings = Vec::with_capacity(1 + self.interiors().len());
        rings.push(self.exterior().to_postgis());
        rings.extend(self.interiors().iter().map(|i| i.to_postgis()));
        ewkb::Polygon { rings, srid: None }
    }
}

impl ToPostgis for gt::Rect<f64> {
    type PostgisType = ewkb::Polygon;

    fn to_postgis(&self) -> Self::PostgisType {
        // We might be able to convert this faster if did it directly instead of
        // going through a polygon, but then we'd have to test it more.
        self.to_polygon().to_postgis()
    }
}

impl ToPostgis for gt::Triangle<f64> {
    type PostgisType = ewkb::Polygon;

    fn to_postgis(&self) -> Self::PostgisType {
        // We might be able to convert this faster if did it directly instead of
        // going through a polygon, but then we'd have to test it more.
        self.to_polygon().to_postgis()
    }
}

impl ToPostgis for gt::MultiPoint<f64> {
    type PostgisType = ewkb::MultiPoint;

    fn to_postgis(&self) -> Self::PostgisType {
        ewkb::MultiPoint {
            points: self.0.iter().map(|p| p.to_postgis()).collect(),
            srid: None,
        }
    }
}

impl ToPostgis for gt::MultiLineString<f64> {
    type PostgisType = ewkb::MultiLineString;

    fn to_postgis(&self) -> Self::PostgisType {
        ewkb::MultiLineString {
            lines: self.0.iter().map(|p| p.to_postgis()).collect(),
            srid: None,
        }
    }
}

impl ToPostgis for gt::MultiPolygon<f64> {
    type PostgisType = ewkb::MultiPolygon;

    fn to_postgis(&self) -> Self::PostgisType {
        ewkb::MultiPolygon {
            polygons: self.0.iter().map(|p| p.to_postgis()).collect(),
            srid: None,
        }
    }
}

impl ToPostgis for gt::GeometryCollection<f64> {
    type PostgisType = ewkb::GeometryCollection;

    fn to_postgis(&self) -> Self::PostgisType {
        ewkb::GeometryCollection {
            geometries: self.0.iter().map(|p| p.to_postgis()).collect(),
            srid: None,
        }
    }
}
