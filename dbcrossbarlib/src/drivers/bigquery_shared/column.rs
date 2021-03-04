//! BigQuery columns.

use serde_derive::{Deserialize, Serialize};

use super::{
    export_udf::{generate_export_udf, needs_custom_json_export},
    import_udf::generate_import_udf,
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
    fn bigquery_can_import_from_csv(&self, schema: &Schema) -> Result<bool>;
}

impl ColumnBigQueryExt for Column {
    fn bigquery_can_import_from_csv(&self, schema: &Schema) -> Result<bool> {
        self.data_type.bigquery_can_import_from_csv(schema)
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
        schema: &Schema,
        name: ColumnName,
        col: &Column,
        usage: Usage,
    ) -> Result<BqColumn> {
        let bq_data_type = BqDataType::for_data_type(schema, &col.data_type, usage)?;
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
        let ty = match &self.ty {
            BqRecordOrNonArrayDataType::Record => {
                let fields = self
                    .fields
                    .iter()
                    .map(|f| f.to_struct_field())
                    .collect::<Result<Vec<_>>>()?;
                BqNonArrayDataType::Struct(fields)
            }
            BqRecordOrNonArrayDataType::DataType(ty) => ty.to_owned(),
        };
        match self.mode {
            Mode::Repeated => Ok(BqDataType::Array(ty)),
            Mode::Nullable | Mode::Required => Ok(BqDataType::NonArray(ty)),
        }
    }

    /// Should this column be declared as `NOT NULL` when generating a `CREATE TABLE`?
    pub(crate) fn is_not_null(&self) -> bool {
        match &self.mode {
            Mode::Required => true,
            Mode::Repeated | Mode::Nullable => false,
        }
    }

    /// Given two columns with the same name, use `other` to "upgrade" the
    /// information that we have about `self`, and return the result.
    ///
    /// This is used to replace `BqNonArrayDataType::String` with
    /// `BqNonArrayDataType::Stringified(_)` when we have more specific type
    /// information available in `other` than we have in `self`. We can't just
    /// use `other` directly, because it may be less _accurate_ than what we
    /// have in `self`, and we need accurate types to export correctly.
    pub(crate) fn aligned_with(&self, other: &BqColumn) -> Result<BqColumn> {
        // Check to make sure that our columns have the same name. (Should be
        // guaranteed by our caller.)
        if self.name != other.name {
            return Err(format_err!(
                "cannot align columns {} and {}",
                self.name.quoted(),
                other.name.quoted()
            ));
        };

        // Get the actual type of this column, and align it.
        let self_ty = self.bq_data_type()?;
        let other_ty = other.bq_data_type()?;
        let aligned_ty = self_ty.aligned_with(&other_ty)?;

        // Reconstruct our column using the new type. This is unnecessarily
        // annoying because of how BigQuery represents structs and arrays. This
        // will also end up replacing `RECORD` with a `STRUCT` type, just to make things
        // easier.
        match (self.mode, aligned_ty) {
            (Mode::Repeated, BqDataType::Array(nested)) => Ok(Self {
                description: self.description.clone(),
                name: self.name.clone(),
                ty: BqRecordOrNonArrayDataType::DataType(nested),
                mode: self.mode,
                fields: vec![],
            }),
            (Mode::Repeated, _) => {
                unreachable!("should never have REPEATED without ARRAY")
            }
            (_, BqDataType::Array(_)) => {
                unreachable!("should never have ARRAY without REPEATED")
            }
            (_, BqDataType::NonArray(nested)) => Ok(Self {
                description: self.description.clone(),
                name: self.name.clone(),
                ty: BqRecordOrNonArrayDataType::DataType(nested),
                mode: self.mode,
                fields: vec![],
            }),
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

            BqDataType::Array(_)
            | BqDataType::NonArray(BqNonArrayDataType::Struct(_)) => {
                generate_import_udf(self, idx, f)?;
            }

            // No special import required for any of these types yet.
            BqDataType::NonArray(_) => {}
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
        assert!(table_prefix.is_empty() || table_prefix.ends_with('.'));
        match self.bq_data_type()? {
            BqDataType::Array(_)
            | BqDataType::NonArray(BqNonArrayDataType::Struct(_)) => {
                write!(
                    f,
                    "ImportJson_{idx}({table_prefix}{name})",
                    idx = idx,
                    table_prefix = table_prefix,
                    name = self.name.quoted(),
                )?;
            }
            _ => {
                write!(
                    f,
                    "{table_prefix}{name}",
                    table_prefix = table_prefix,
                    name = self.name.quoted(),
                )?;
            }
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

    /// Write an an export UDF function if we need one.
    pub(crate) fn write_export_udf(
        &self,
        f: &mut dyn Write,
        idx: usize,
    ) -> Result<()> {
        if needs_custom_json_export(&self.bq_data_type()?)?.in_sql_code() {
            generate_export_udf(self, idx, f)?;
        }
        Ok(())
    }

    /// Output the SQL expression used in the `SELECT` clause of our table
    /// export statement.
    pub(crate) fn write_export_select_expr(
        &self,
        f: &mut dyn Write,
        idx: usize,
    ) -> Result<()> {
        match &self.bq_data_type()? {
            // Some types need a custom export function.
            ty if needs_custom_json_export(&ty)?.in_sql_code() => {
                write!(
                    f,
                    "ExportJson_{idx}({name}) AS {name}",
                    idx = idx,
                    name = self.name.quoted(),
                )?;
                Ok(())
            }
            // We export arrays of structs as JSON arrays of objects.
            BqDataType::NonArray(ty) => {
                self.write_export_select_expr_for_non_array(ty, f)
            }
            BqDataType::Array(ty) => self.write_export_select_expr_for_array(ty, f),
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

            // These types can only make it here if they contain nothing that
            // needs a special export routine.
            BqNonArrayDataType::Stringified(_) | BqNonArrayDataType::Struct(_) => {
                write!(f, "{}", self.name.quoted())?;
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
            | BqNonArrayDataType::String
            | BqNonArrayDataType::Stringified(_) => {
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
