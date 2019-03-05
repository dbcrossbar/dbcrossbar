//! Parsing values found in CSV cells.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use geo_types::Geometry;
use geojson::{conversion::TryInto, GeoJson};
use lazy_static::lazy_static;
use regex::Regex;
use serde_json;
use uuid::Uuid;

use crate::common::*;

/// Parse a value found in a CSV cell. This is analogous to Rust's built-in
/// [`FromStr`] trait, but it follws the rules of our CSV interchange format.
pub(crate) trait FromCsvCell: Sized {
    /// Given the contents of a CSV cell, parse it as a value and return it.
    ///
    /// This function is not responsible for translating `""` to an SQL `NULL`
    /// value. That should be handled before calling this, if desired.
    fn from_csv_cell(cell: &str) -> Result<Self>;
}

impl FromCsvCell for bool {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        // We use the same list of boolean expressions as
        // https://github.com/kevincox/humanbool.rs, but we use regular
        // expressions and do a case-insensitive match. Is this reasonably fast?
        // We'll probably match a ton of these and we don't want to allocate
        // memory using `to_lowercase`.
        lazy_static! {
            static ref TRUE_RE: Regex = Regex::new(r"^(?i)(?:1|y|yes|on|t|true)$")
                .expect("invalid `TRUE_RE` in source");
            static ref FALSE_RE: Regex = Regex::new(r"^(?i)(?:0|n|no|off|f|false)$")
                .expect("invalid `TRUE_RE` in source");
        }

        if TRUE_RE.is_match(cell) {
            Ok(true)
        } else if FALSE_RE.is_match(cell) {
            Ok(false)
        } else {
            Err(format_err!("cannot parse boolean {:?}", cell))
        }
    }
}

#[test]
fn parse_bool() {
    let examples = &[
        // True.
        ("1", true),
        ("y", true),
        ("Y", true),
        ("yes", true),
        ("YES", true),
        ("Yes", true),
        ("on", true),
        ("ON", true),
        ("On", true),
        ("t", true),
        ("T", true),
        // False.
        ("0", false),
        ("n", false),
        ("N", false),
        ("no", false),
        ("NO", false),
        ("No", false),
        ("off", false),
        ("OFF", false),
        ("Off", false),
        ("f", false),
        ("F", false),
    ];
    for (s, expected) in examples {
        let parsed = bool::from_csv_cell(s).unwrap();
        assert_eq!(parsed, *expected);
    }
    assert!(bool::from_csv_cell("10").is_err());
}

impl FromCsvCell for NaiveDate {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        Ok(cell
            .parse::<NaiveDate>()
            .with_context(|_| format!("cannot parse {:?} as date", cell))?)
    }
}

impl FromCsvCell for f32 {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        Ok(cell
            .parse::<f32>()
            .with_context(|_| format!("cannot parse {:?} as f32", cell))?)
    }
}

impl FromCsvCell for f64 {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        Ok(cell
            .parse::<f64>()
            .with_context(|_| format!("cannot parse {:?} as f64", cell))?)
    }
}

impl FromCsvCell for Geometry<f64> {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        let geojson = cell
            .parse::<GeoJson>()
            .with_context(|_| format!("cannot parse {:?} as GeoJSON", cell))?;
        if let GeoJson::Geometry(geojson_geometry) = geojson {
            let geometry: Geometry<f64> =
                geojson_geometry.value.try_into().map_err(|e| {
                    format_err!("couldn't convert GeoJSON {:?}: {}", cell, e)
                })?;
            Ok(geometry)
        } else {
            Err(format_err!("expected GeoJSON {:?} to be a geometry", cell))
        }
    }
}

#[test]
fn parse_geometry() {
    use geo_types::Point;
    use serde_json::json;

    let geojson_value = json!({
        "type": "Point",
        "coordinates": [-71, 42],
    });
    let geojson = serde_json::to_string(&geojson_value).unwrap();
    let geometry = Geometry::<f64>::from_csv_cell(&geojson).unwrap();
    let expected = Geometry::Point(Point::new(-71.0, 42.0));
    assert_eq!(geometry, expected);
}

impl FromCsvCell for i16 {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        Ok(cell
            .parse::<i16>()
            .with_context(|_| format!("cannot parse {:?} as i16", cell))?)
    }
}

impl FromCsvCell for i32 {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        Ok(cell
            .parse::<i32>()
            .with_context(|_| format!("cannot parse {:?} as i64", cell))?)
    }
}

impl FromCsvCell for i64 {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        Ok(cell
            .parse::<i64>()
            .with_context(|_| format!("cannot parse {:?} as i64", cell))?)
    }
}

impl FromCsvCell for serde_json::Value {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        Ok(serde_json::from_str(cell)
            .with_context(|_| format!("cannot parse {:?} as JSON", cell))?)
    }
}

impl FromCsvCell for NaiveDateTime {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        Ok(NaiveDateTime::parse_from_str(cell, "%Y-%m-%d %H:%M:%S%.f")
            .with_context(|_| format!("cannot parse {:?} as timestamp", cell))?)
    }
}

#[test]
fn parse_naive_date_time() {
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
        let parsed = NaiveDateTime::from_csv_cell(s).unwrap();
        assert_eq!(&parsed, expected);
    }
}

impl FromCsvCell for DateTime<FixedOffset> {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        let parsed = DateTime::parse_from_str(cell, "%Y-%m-%d %H:%M:%S%.f%#z")
            .with_context(|_| {
                format!("cannot parse {:?} as timestamp with time zone", cell)
            })?;
        Ok(parsed)
    }
}

impl FromCsvCell for DateTime<Utc> {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        let timestamp = DateTime::<FixedOffset>::from_csv_cell(cell)?;
        Ok(timestamp.with_timezone(&Utc))
    }
}

#[test]
fn parse_utc_timestamp() {
    use chrono::TimeZone;
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
        let parsed = DateTime::<Utc>::from_csv_cell(s).unwrap();
        assert_eq!(&parsed, expected);
    }
}

impl FromCsvCell for Uuid {
    fn from_csv_cell(cell: &str) -> Result<Self> {
        Ok(cell
            .parse::<Uuid>()
            .with_context(|_| format!("cannot parse {:?} as UUID", cell))?)
    }
}
