use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

use crate::common::*;

/// Implemented by types that can be written to BigQuery SQL.
pub(crate) trait WriteBigQuerySql<W: Write> {
    /// Write `self` to `wtr` as a BigQuery SQL literal.
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error>;
}

impl<W: Write> WriteBigQuerySql<W> for bool {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        if *self {
            write!(sql, "TRUE")
        } else {
            write!(sql, "FALSE")
        }
    }
}

impl<W: Write> WriteBigQuerySql<W> for &'_ str {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        // I _think_ this is correct. See
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#string_and_bytes_literals
        write!(sql, "'")?;
        for c in self.chars() {
            match c {
                '\'' | '\\' => write!(sql, "\\{}", c),
                '\r' => write!(sql, "\\r"),
                '\n' => write!(sql, "\\n"),
                _ if c.is_ascii_graphic() => write!(sql, "{}", c),
                _ => write!(sql, "\\U{:08x}", u32::from(c)),
            }?;
        }
        write!(sql, "'")?;
        Ok(())
    }
}

/// Convenience wrapper to write a `&str` as a byte literal. We use this instead
/// of `&[u8]` because our data is already known to be `&str`.
pub(crate) struct BytesLiteral<'a>(pub(crate) &'a str);

impl<W: Write> WriteBigQuerySql<W> for BytesLiteral<'_> {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        write!(sql, "B")?;
        self.0.write_bigquery_sql(sql)
    }
}

impl<W: Write> WriteBigQuerySql<W> for f64 {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        write!(sql, "{}", *self)
    }
}

pub(crate) struct ExpNotation(pub f64);

impl<W: Write> WriteBigQuerySql<W> for ExpNotation {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        write!(sql, "{:e}", self.0)
    }
}

impl<W: Write> WriteBigQuerySql<W> for i64 {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        write!(sql, "{}", *self)
    }
}

/// Convenience wrapper to write a `&str` as a NUMERIC literal.
pub(crate) struct NumericLiteral<'a>(pub(crate) &'a str);

impl<W: Write> WriteBigQuerySql<W> for NumericLiteral<'_> {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        write!(sql, "NUMERIC ")?;
        self.0.write_bigquery_sql(sql)
    }
}

// Geography literals need to be wrapped in ST_GEOGFROMGEOJSON.
pub(crate) struct GeographyLiteral<'a>(pub(crate) &'a str);

impl<W: Write> WriteBigQuerySql<W> for GeographyLiteral<'_> {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        write!(sql, "ST_GEOGFROMGEOJSON(")?;
        self.0.write_bigquery_sql(sql)?;
        write!(sql, ")")
    }
}

impl<W: Write, Elem: WriteBigQuerySql<W>> WriteBigQuerySql<W> for &'_ [Elem] {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        // TODO: Remember, `ARRAY[ARRAY[...]]` does not work! We need to
        // generate `ARRAY[STRUCT(ARRAY[...])]`.
        write!(sql, "ARRAY[")?;
        for (idx, elem) in self.iter().enumerate() {
            if idx != 0 {
                write!(sql, ",")?;
            }
            elem.write_bigquery_sql(sql)?;
        }
        write!(sql, "]")
    }
}

impl<W: Write> WriteBigQuerySql<W> for NaiveDate {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        // BigQuery: DATETIME 'YYYY-[M]M-[D]D'
        // Rust Chrono: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        write!(sql, "DATE '{}'", self.format("%Y-%m-%d"))
    }
}

impl<W: Write> WriteBigQuerySql<W> for NaiveDateTime {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        // BigQuery: DATETIME 'YYYY-[M]M-[D]D( |T)[[H]H:[M]M:[S]S[.DDDDDD]]'
        // Rust Chrono: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        write!(sql, "DATETIME '{}'", self.format("%Y-%m-%dT%H:%M:%S%.f"))
    }
}

impl<W: Write> WriteBigQuerySql<W> for DateTime<Utc> {
    fn write_bigquery_sql(&self, sql: &mut W) -> Result<(), io::Error> {
        // BigQuery: TIMESTAMP 'YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.F]]
        // [time_zone]'
        // (but time_zone can be an offset instead).
        //
        // Rust Chrono:
        // https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        write!(
            sql,
            "TIMESTAMP '{}'",
            self.format("%Y-%m-%dT%H:%M:%S%.f%:z")
        )
    }
}
