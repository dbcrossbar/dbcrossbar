//! BigQuery columns.

use serde_derive::Serialize;
use std::{fmt, io::Write};

use super::{BqDataType, DataTypeBigQueryExt, Usage};
use crate::common::*;
use crate::schema::Column;

/// Extensions to `Column` (the portable version) to handle BigQuery-query
/// specific stuff.
pub(crate) trait ColumnBigQueryExt {
    /// Can BigQuery import this column from a CSV file without special
    /// processing?
    fn bigquery_can_import_from_csv(&self) -> Result<bool>;
}

impl ColumnBigQueryExt for Column {
    fn bigquery_can_import_from_csv(&self) -> Result<bool> {
        self.data_type.bigquery_can_import_from_csv()
    }
}

/// A BigQuery column declaration.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub(crate) struct BqColumn {
    /// An optional description of the BigQuery column.
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,

    /// The name of the BigQuery column.
    name: String,

    /// The type of the BigQuery column.
    #[serde(rename = "type")]
    ty: BqDataType,

    // The mode of the column: Is it nullable?
    mode: Mode,
}

impl BqColumn {
    /// Given a portable `Column`, and an intended usage, return a corresponding
    /// `BqColumn`.
    pub(crate) fn for_column(col: &Column, usage: Usage) -> Result<BqColumn> {
        Ok(BqColumn {
            name: col.name.to_owned(),
            description: None,
            ty: BqDataType::for_data_type(&col.data_type, usage)?,
            mode: if col.is_nullable {
                Mode::Nullable
            } else {
                Mode::Required
            },
        })
    }

    /// Output JavaScript UDF for importing a column (if necessary). This can be
    /// used to patch up types that can't be loaded directly from a CSV.
    pub(crate) fn write_import_udf(
        &self,
        f: &mut dyn Write,
        idx: usize,
    ) -> Result<()> {
        if let BqDataType::Array(_) = &self.ty {
            write!(
                f,
                r#"CREATE TEMP FUNCTION ImportJson_{idx}(input STRING)
RETURNS {bq_type}
LANGUAGE js AS """
return JSON.parse(input);
""";

"#,
                idx = idx,
                bq_type = self.ty,
            )?;
        }
        Ok(())
    }

    /// Output the SQL expression used in the `SELECT` clause of our table
    /// import statement.
    pub(crate) fn write_import_select_expr(
        &self,
        f: &mut dyn Write,
        idx: usize,
    ) -> Result<()> {
        let ident = Ident(&self.name);
        if let BqDataType::Array(_) = &self.ty {
            write!(
                f,
                "ImportJson_{idx}({ident}) AS {ident}",
                idx = idx,
                ident = ident,
            )?;
        } else {
            write!(f, "{}", ident)?;
        }
        Ok(())
    }
}

/// A column mode.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Mode {
    /// This column is `NOT NULL`.
    Required,

    /// This column can contain `NULL` values.
    Nullable,
}

/// A BigQuery identifier, for formatting purposes.
pub(crate) struct Ident<'a>(pub(crate) &'a str);

impl<'a> fmt::Display for Ident<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.contains('`') {
            // We can't output identifiers containing backticks.
            Err(fmt::Error)
        } else {
            write!(f, "`{}`", self.0)
        }
    }
}
