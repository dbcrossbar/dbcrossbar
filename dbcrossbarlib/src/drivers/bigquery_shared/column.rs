//! BigQuery columns.

use serde_derive::{Deserialize, Serialize};

use super::{
    BqDataType, BqNonArrayDataType, BqRecordOrNonArrayDataType, BqStructField,
    ColumnName, DataTypeBigQueryExt, Usage,
};
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct BqColumn {
    /// An optional description of the BigQuery column.
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,

    /// The name of the BigQuery column.
    pub name: ColumnName,

    /// The type of the BigQuery column.
    #[serde(rename = "type")]
    ty: BqRecordOrNonArrayDataType,

    /// The mode of the column: Is it nullable?
    ///
    /// This can be omitted in certain output from `bq show --schema`, in which
    /// case it appears to correspond to `NULLABLE`.
    #[serde(default)]
    mode: Mode,

    /// If `ty` is `BqRecordOrNonArrayDataType::Record`, this will contain the fields
    /// we need to construct a struct.
    ///
    /// TODO: We don't even attempt to handle anonymous fields yet, because they
    /// can't be exported as valid JSON in any case.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    fields: Vec<BqColumn>,
}

impl BqColumn {
    /// Given a portable `Column`, and an intended usage, return a corresponding
    /// `BqColumn`.
    ///
    /// Note that dashes and spaces are replaced with underscores to satisfy BigQuery naming rules.
    pub(crate) fn for_column(
        name: ColumnName,
        col: &Column,
        usage: Usage,
    ) -> Result<BqColumn> {
        let bq_data_type = BqDataType::for_data_type(&col.data_type, usage)?;
        let (ty, mode): (BqNonArrayDataType, Mode) = match bq_data_type {
            BqDataType::Array(ty) => (ty, Mode::Repeated),
            BqDataType::NonArray(ref ty) if col.is_nullable => {
                (ty.to_owned(), Mode::Nullable)
            }
            BqDataType::NonArray(ty) => (ty, Mode::Required),
        };
        Ok(BqColumn {
            name,
            description: None,
            ty: BqRecordOrNonArrayDataType::DataType(ty),
            mode,
            fields: vec![],
        })
    }

    /// Given a `BqColumn`, construct a portable `Column`.
    pub(crate) fn to_column(&self) -> Result<Column> {
        Ok(Column {
            name: self.name.to_portable_name(),
            data_type: self.bq_data_type()?.to_data_type()?,
            is_nullable: match self.mode {
                // I'm not actually sure about how to best map `Repeated`, so
                // let's make it nullable for now.
                Mode::Nullable | Mode::Repeated => true,
                Mode::Required => false,
            },
            comment: self.description.clone(),
        })
    }

    /// Can we MERGE on this column? True is this column is `NOT NULL`.
    pub(crate) fn can_be_merged_on(&self) -> bool {
        match self.mode {
            Mode::Required => true,
            Mode::Repeated | Mode::Nullable => false,
        }
    }

    /// Get the BigQuery data type for this column, taking into account
    /// shenanigans like `RECORD` and `REPEATED`.
    pub(crate) fn bq_data_type(&self) -> Result<BqDataType> {
        self.ty.to_bq_data_type(self.mode, &self.fields)
    }

    /// Should this column be declared as `NOT NULL` when generating a `CREATE TABLE`?
    pub(crate) fn is_not_null(&self) -> bool {
        match &self.mode {
            Mode::Required => true,
            Mode::Repeated | Mode::Nullable => false,
        }
    }

    /// Convert this column into a struct field. We use this to implement
    /// `RECORD` column parsing.
    pub(crate) fn to_struct_field(&self) -> Result<BqStructField> {
        Ok(BqStructField {
            name: Some(self.name.clone()),
            ty: self.bq_data_type()?,
        })
    }

    /// Output JavaScript UDF for importing a column (if necessary). This can be
    /// used to patch up types that can't be loaded directly from a CSV.
    pub(crate) fn write_import_udf(
        &self,
        f: &mut dyn Write,
        idx: usize,
    ) -> Result<()> {
        match self.bq_data_type()? {
            // JavaScript UDFs can't return `DATETIME` yet, so we need a fairly
            // elaborate workaround.
            BqDataType::Array(elem_ty @ BqNonArrayDataType::Datetime) => {
                writeln!(
                    f,
                    r#"CREATE TEMP FUNCTION ImportJsonHelper_{idx}(input STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
return JSON.parse(input);
""";

CREATE TEMP FUNCTION ImportJson_{idx}(input STRING)
RETURNS ARRAY<{bq_type}>
AS ((
    SELECT ARRAY_AGG(
        COALESCE(
            SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S', e),
            PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*S', e)
        )
    )
    FROM UNNEST(ImportJsonHelper_{idx}(input)) AS e
));
"#,
                    idx = idx,
                    bq_type = elem_ty,
                )?;
            }

            // Most kinds of arrays can be handled with JavaScript. But some
            // of these might be faster as SQL UDFs.
            BqDataType::Array(elem_ty) => {
                write!(
                    f,
                    r#"CREATE TEMP FUNCTION ImportJson_{idx}(input STRING)
RETURNS ARRAY<{bq_type}>
LANGUAGE js AS """
return "#,
                    idx = idx,
                    bq_type = elem_ty,
                )?;
                self.write_import_udf_body_for_array(f, &elem_ty)?;
                writeln!(
                    f,
                    r#";
""";
"#
                )?;
            }

            // No special import required for any of these types yet.
            BqDataType::NonArray(_) => {}
        }
        Ok(())
    }

    /// Write the actual import JavaScript for an array of the specified type.
    fn write_import_udf_body_for_array(
        &self,
        f: &mut dyn Write,
        elem_ty: &BqNonArrayDataType,
    ) -> Result<()> {
        match elem_ty {
            // These types can be converted directly from JSON.
            BqNonArrayDataType::Bool
            | BqNonArrayDataType::Float64
            | BqNonArrayDataType::String => {
                write!(f, "JSON.parse(input)")?;
            }

            // These types all need to go through `Date`, even when they
            // theoretically don't involve time zones.
            //
            // TODO: We may need to handle `TIMESTAMP` microseconds explicitly.
            BqNonArrayDataType::Date
            | BqNonArrayDataType::Datetime
            | BqNonArrayDataType::Timestamp => {
                write!(
                    f,
                    "JSON.parse(input).map(function (d) {{ return new Date(d); }})",
                )?;
            }

            // This is tricky, because not all 64-bit integers can be exactly
            // represented as JSON.
            BqNonArrayDataType::Int64 => {
                write!(f, "JSON.parse(input)")?;
            }

            // Unsupported types. Some of these aren't actually supported by our
            // portable schema, so we should never see them. Others can occur in
            // real data.
            BqNonArrayDataType::Bytes
            | BqNonArrayDataType::Geography
            | BqNonArrayDataType::Numeric
            | BqNonArrayDataType::Time
            | BqNonArrayDataType::Struct(_) => {
                return Err(format_err!(
                    "cannot import `ARRAY<{}>` into BigQuery yet",
                    elem_ty,
                ));
            }
        }
        Ok(())
    }

    /// Output an SQL expression that returns the converted form of a column.
    ///
    /// The optional `table_prefix` must end with a `.` and it will not be
    /// wrapped in `Ident`, so it can't be a string we got from the user. So we
    /// declare it `'static` as a hack to more or less enforce this.
    ///
    /// This should never fail when writing output to a `Vec<u8>`.
    pub(crate) fn write_import_expr(
        &self,
        f: &mut dyn Write,
        idx: usize,
        table_prefix: Option<&'static str>,
    ) -> Result<()> {
        let table_prefix = table_prefix.unwrap_or("");
        assert!(table_prefix == "" || table_prefix.ends_with('.'));
        if self.mode == Mode::Repeated {
            write!(
                f,
                "ImportJson_{idx}({table_prefix}{name})",
                idx = idx,
                table_prefix = table_prefix,
                name = self.name.quoted(),
            )?;
        } else {
            write!(
                f,
                "{table_prefix}{name}",
                table_prefix = table_prefix,
                name = self.name.quoted(),
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
        self.write_import_expr(f, idx, None)?;
        write!(f, " AS {name}", name = self.name.quoted())?;
        Ok(())
    }

    /// Output the SQL expression used in the `SELECT` clause of our table
    /// export statement.
    pub(crate) fn write_export_select_expr(&self, f: &mut dyn Write) -> Result<()> {
        match self.bq_data_type()? {
            // We export arrays of structs as JSON arrays of objects.
            BqDataType::NonArray(ty)
            | BqDataType::Array(ty @ BqNonArrayDataType::Struct(_)) => {
                self.write_export_select_expr_for_non_array(&ty, f)
            }
            BqDataType::Array(ty) => self.write_export_select_expr_for_array(&ty, f),
        }
    }

    /// Output a `SELECT`-clause expression for an `ARRAY<...>` column.
    fn write_export_select_expr_for_array(
        &self,
        data_type: &BqNonArrayDataType,
        f: &mut dyn Write,
    ) -> Result<()> {
        write!(f, "NULLIF(TO_JSON_STRING(")?;
        match data_type {
            // We can safely convert arrays of these types directly to JSON.
            BqNonArrayDataType::Bool
            | BqNonArrayDataType::Date
            | BqNonArrayDataType::Float64
            | BqNonArrayDataType::Int64
            | BqNonArrayDataType::Numeric
            | BqNonArrayDataType::String => {
                write!(f, "{}", self.name.quoted())?;
            }

            BqNonArrayDataType::Datetime => {
                write!(
                    f,
                    "(SELECT ARRAY_AGG(FORMAT_DATETIME(\"%Y-%m-%dT%H:%M:%E*S\", {name})) FROM UNNEST({name}) AS {name})",
                    name = self.name.quoted(),
                )?;
            }

            BqNonArrayDataType::Geography => {
                write!(
                    f,
                    "(SELECT ARRAY_AGG(ST_ASGEOJSON({name})) FROM UNNEST({name}) AS {name})",
                    name = self.name.quoted(),
                )?;
            }

            BqNonArrayDataType::Timestamp => {
                write!(
                    f,
                    "(SELECT ARRAY_AGG(FORMAT_TIMESTAMP(\"%Y-%m-%dT%H:%M:%E*SZ\", {name}, \"+0\")) FROM UNNEST({name}) AS {name})",
                    name = self.name.quoted(),
                )?;
            }

            // These we don't know how to output at all. (We don't have a
            // portable type for most of these.)
            BqNonArrayDataType::Bytes
            | BqNonArrayDataType::Struct(_)
            | BqNonArrayDataType::Time => {
                return Err(format_err!(
                    "can't output {} columns yet",
                    self.bq_data_type()?,
                ));
            }
        }
        write!(f, "), '[]') AS {name}", name = self.name.quoted())?;
        Ok(())
    }

    /// Output a `SELECT`-clause expression for a non-`ARRAY<...>` column.
    pub(crate) fn write_export_select_expr_for_non_array(
        &self,
        data_type: &BqNonArrayDataType,
        f: &mut dyn Write,
    ) -> Result<()> {
        match data_type {
            // We trust BigQuery to output these directly.
            BqNonArrayDataType::Date
            | BqNonArrayDataType::Float64
            | BqNonArrayDataType::Int64
            | BqNonArrayDataType::Numeric
            | BqNonArrayDataType::String => {
                write!(f, "{}", self.name.quoted())?;
            }

            // BigQuery outputs "true" and "false" by default, but let's make it
            // look like PostgreSQL, so that CSV import drivers don't get too
            // confused. This particularly affects BigML CSV import, because it
            // treats booleans as string values.
            BqNonArrayDataType::Bool => {
                // `IF` treats NULL as false, so use `CASE`.
                write!(
                    f,
                    "(CASE {name} WHEN TRUE THEN \"t\" WHEN FALSE THEN \"f\" ELSE NULL END) AS {name}",
                    name = self.name.quoted(),
                )?;
            }

            BqNonArrayDataType::Datetime => {
                write!(
                    f,
                    "FORMAT_DATETIME(\"%Y-%m-%dT%H:%M:%E*S\", {name}) AS {name}",
                    name = self.name.quoted()
                )?;
            }

            BqNonArrayDataType::Geography => {
                write!(
                    f,
                    "ST_ASGEOJSON({name}) AS {name}",
                    name = self.name.quoted()
                )?;
            }

            struct_ty @ BqNonArrayDataType::Struct(_) => {
                if struct_ty.is_json_safe() {
                    write!(
                        f,
                        "TO_JSON_STRING({name}) AS {name}",
                        name = self.name.quoted()
                    )?;
                } else {
                    return Err(format_err!("cannot serialize {} as JSON", struct_ty));
                }
            }

            BqNonArrayDataType::Timestamp => {
                write!(
                    f,
                    "FORMAT_TIMESTAMP(\"%Y-%m-%dT%H:%M:%E*SZ\", {name}, \"+0\") AS {name}",
                    name = self.name.quoted(),
                )?;
            }

            // These we don't know how to output at all. (We don't have a
            // portable type for most of these.)
            BqNonArrayDataType::Bytes | BqNonArrayDataType::Time => {
                return Err(format_err!(
                    "can't output {} columns yet",
                    self.bq_data_type()?,
                ));
            }
        }
        Ok(())
    }
}

#[test]
fn column_without_mode() {
    let json = r#"{"type":"STRING","name":"state"}"#;
    let col: BqColumn = serde_json::from_str(json).unwrap();
    assert_eq!(col.mode, Mode::Nullable);
}

/// A column mode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum Mode {
    /// This column is `NOT NULL`.
    Required,

    /// This column can contain `NULL` values.
    Nullable,

    /// (Undocumented.) This column is actually an `ARRAY` column,
    /// but the `type` doesn't actually mention that. This is an undocumented
    /// value that we see in the output of `bq show --schema`.
    Repeated,
}

impl Default for Mode {
    /// The `mode` field appears to default to `NULLABLE` in `bq show --schema`
    /// output, so use that as our default.
    fn default() -> Self {
        Mode::Nullable
    }
}
