//! Table-related support for BigQuery.

use itertools::Itertools;
use serde_json;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    iter::FromIterator,
};

use super::{BqColumn, ColumnBigQueryExt, ColumnName, TableName, Usage};
use crate::clouds::gcloud::bigquery;
use crate::common::*;
use crate::schema::{Column, Table};

/// Extensions to `Column` (the portable version) to handle BigQuery-query
/// specific stuff.
pub(crate) trait TableBigQueryExt {
    /// Can we import data into this table directly from a CSV file?
    fn bigquery_can_import_from_csv(&self) -> Result<bool>;
}

impl TableBigQueryExt for Table {
    fn bigquery_can_import_from_csv(&self) -> Result<bool> {
        for col in &self.columns {
            if !col.bigquery_can_import_from_csv()? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// A BigQuery table schema.
pub(crate) struct BqTable {
    /// The BigQuery name of this table.
    pub(crate) name: TableName,
    /// The columns of this table.
    pub(crate) columns: Vec<BqColumn>,
}

impl BqTable {
    /// Give a BigQuery `TableName`, a database-independent list of `Columns`,
    /// and the intended usage within BigQuery, map them to a corresponding
    /// `BqTable`.
    ///
    /// We require the BigQuery `TableName` to be passed in separately, because
    /// using the table name from the database-independent `Table` has tended to
    /// be a source of bugs in the past.
    pub(crate) fn for_table_name_and_columns(
        name: TableName,
        columns: &[Column],
        usage: Usage,
    ) -> Result<BqTable> {
        let mut col_names = HashSet::<ColumnName>::new();
        let columns = columns
            .iter()
            .map(move |c| {
                let col_name = ColumnName::try_from(&c.name)?;
                if !col_names.insert(col_name.clone()) {
                    let prev = col_names
                        .get(&col_name)
                        .expect("should already have matching column");
                    Err(format_err!(
                        "duplicate column names {:?} and {:?}",
                        prev,
                        col_name
                    ))
                } else {
                    BqColumn::for_column(col_name, c, usage)
                }
            })
            .collect::<Result<Vec<BqColumn>>>()?;
        Ok(BqTable { name, columns })
    }

    /// Given a table name, look up the schema and return a `BqTable`.
    pub(crate) async fn read_from_table(
        ctx: &Context,
        name: &TableName,
    ) -> Result<BqTable> {
        bigquery::schema(ctx, name).await
    }

    /// Create a new table based on this table, but with columns matching the
    /// the names and order of the columns in `other_table`. This is useful if
    /// we want to insert from `other_table` into `self`, or export `self` using
    /// schema of `other_table`.
    ///
    /// Hypothetically, we could also check for compatibility between column
    /// types in the two tables, but for now, we're happy to let the database
    /// verify all that for us.
    pub(crate) fn aligned_with(&self, other_table: &BqTable) -> Result<BqTable> {
        let column_map = HashMap::<&ColumnName, &BqColumn>::from_iter(
            self.columns.iter().map(|c| (&c.name, c)),
        );
        Ok(BqTable {
            name: self.name.clone(),
            columns: other_table
                .columns
                .iter()
                .map(|c| -> Result<BqColumn> {
                    if let Some(&col) = column_map.get(&c.name) {
                        Ok(col.to_owned())
                    } else {
                        Err(format_err!(
                            "could not find column {} in BigQuery table {}",
                            c.name,
                            self.name,
                        ))
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }

    /// Given a `BqTable`, convert it to a portable `Table`.
    pub(crate) fn to_table(&self) -> Result<Table> {
        let columns = self
            .columns
            .iter()
            .map(|c| c.to_column())
            .collect::<Result<Vec<Column>>>()?;
        Ok(Table {
            name: self.name.to_string(),
            columns,
        })
    }

    /// Get the BigQuery table name for this table.
    pub(crate) fn name(&self) -> &TableName {
        &self.name
    }

    /// Write out this table as a JSON schema.
    pub(crate) fn write_json_schema(&self, f: &mut dyn Write) -> Result<()> {
        serde_json::to_writer_pretty(f, &self.columns)?;
        Ok(())
    }

    /// Generate SQL which `SELECT`s from a temp table, and fixes the types
    /// of columns that couldn't be imported from CSVs.
    ///
    /// This `BqTable` should have been created with `Usage::FinalTable`.
    pub(crate) fn write_import_sql(
        &self,
        source_table_name: &TableName,
        f: &mut dyn Write,
    ) -> Result<()> {
        for (i, col) in self.columns.iter().enumerate() {
            col.write_import_udf(f, i)?;
        }
        write!(f, "SELECT ")?;
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            col.write_import_select_expr(f, i)?;
        }
        write!(f, " FROM {}", source_table_name.dotted_and_quoted())?;
        Ok(())
    }

    /// Generate a `MERGE INTO` statement using the specified columns.
    pub(crate) fn write_merge_sql(
        &self,
        source_table_name: &TableName,
        merge_keys: &[String],
        f: &mut dyn Write,
    ) -> Result<()> {
        // Convert `merge_keys` into actual column values for consistency.
        let mut column_map = HashMap::new();
        for col in &self.columns {
            column_map.insert(&col.name, col);
        }
        let merge_keys = merge_keys
            .iter()
            .map(|key| -> Result<&BqColumn> {
                let col_name = ColumnName::try_from(key)?;
                Ok(column_map.get(&col_name).ok_or_else(|| {
                    format_err!("upsert key {} is not in table", key)
                })?)
            })
            .collect::<Result<Vec<&BqColumn>>>()?;

        // As discussed at https://github.com/faradayio/dbcrossbar/issues/43,
        // it's not obvious how to `MERGE` on columns that might be `NULL`.
        // Until we have a solution that we like, fail with an error.
        for merge_key in &merge_keys {
            if !merge_key.can_be_merged_on() {
                return Err(format_err!(
                    "BigQuery cannot upsert on {:?} because it is not REQUIRED (aka NOT NULL)",
                    merge_key.name,
                ));
            }
        }

        // Build a table when we can check for merge keys by name.
        let merge_key_table =
            merge_keys.iter().map(|c| &c.name).collect::<HashSet<_>>();

        // Write out any helper functions we'll need to transform data.
        for (idx, col) in self.columns.iter().enumerate() {
            col.write_import_udf(f, idx)?;
        }

        // A helper function to generate import SQL for a column.
        let col_import_expr = |c: &BqColumn, idx: usize| -> String {
            let mut buf = vec![];
            c.write_import_expr(&mut buf, idx, Some("temp."))
                .expect("should always be able to write col_import_expr");
            String::from_utf8(buf).expect("col_import_expr should be UTF-8")
        };

        // Generate our actual SQL.
        write!(
            f,
            r#"
MERGE INTO {dest_table} AS dest
USING {temp_table} AS temp
ON
    {key_comparisons}
WHEN MATCHED THEN UPDATE SET
    {updates}
WHEN NOT MATCHED THEN INSERT (
    {columns}
) VALUES (
    {values}
)"#,
            dest_table = self.name().dotted_and_quoted(),
            temp_table = source_table_name.dotted_and_quoted(),
            key_comparisons = merge_keys
                .iter()
                .enumerate()
                .map(|(idx, c)| format!(
                    "dest.{col} = {expr}",
                    col = c.name,
                    expr = col_import_expr(c, idx),
                ))
                .join(" AND\n    "),
            updates = self
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, c)| if merge_key_table.contains(&c.name) {
                    None
                } else {
                    Some(format!(
                        "{col} = {expr}",
                        col = c.name,
                        expr = col_import_expr(c, idx),
                    ))
                })
                .join(",\n    "),
            columns = self.columns.iter().map(|c| &c.name).join(",\n    "),
            values = self
                .columns
                .iter()
                .enumerate()
                .map(|(idx, c)| col_import_expr(c, idx))
                .join(",\n    "),
        )?;
        Ok(())
    }

    /// Generate SQL which `SELECT`s from a table, producing something we can
    /// export to CSV.
    pub(crate) fn write_export_sql(
        &self,
        source_args: &SourceArguments<Verified>,
        f: &mut dyn Write,
    ) -> Result<()> {
        write!(f, "SELECT ")?;
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            col.write_export_select_expr(f)?;
        }
        write!(f, " FROM {}", self.name.dotted_and_quoted())?;
        if let Some(where_clause) = source_args.where_clause() {
            write!(f, " WHERE ({})", where_clause)?;
        }
        Ok(())
    }

    pub(crate) fn write_count_sql(
        &self,
        source_args: &SourceArguments<Verified>,
        f: &mut dyn Write,
    ) -> Result<()> {
        write!(f, "SELECT COUNT(*) AS `count`")?;
        write!(f, " FROM {}", self.name.dotted_and_quoted())?;
        if let Some(where_clause) = source_args.where_clause() {
            write!(f, " WHERE ({})", where_clause)?;
        }

        Ok(())
    }
}
