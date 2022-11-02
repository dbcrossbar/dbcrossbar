//! Driver for working with CSV files.

use std::{fmt, io::Cursor, str::FromStr};

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

use crate::{
    clouds::gcloud::bigquery,
    common::*,
    concat::concatenate_csv_streams,
    drivers::bigquery_shared::{BqTable, Usage},
    from_csv_cell::FromCsvCell,
    from_json_value::FromJsonValue,
};

use super::{
    bigquery::BigQueryLocator,
    bigquery_shared::{
        BqDataType, BqNonArrayDataType, BytesLiteral, ExpNotation, GeographyLiteral,
        NumericLiteral, WriteBigQuerySql,
    },
};

/// The maximum allowable CSV file size for us to try the "VIEW" trick.
///
/// "Maximum length of a standard SQL query used to define a view" [is defined
/// as](https://cloud.google.com/bigquery/quotas#view_limits) "256 K
/// characters".  We are cutting this a little close so failures may be possible
/// particularly where there are large numbers of columns with single character
/// strings.
const MAX_CSV_SIZE_FOR_VIEW: usize = 128 * 1024;

/// A version of `BigQueryLocator` which is optimized for creating small,
/// read-only "tables" using various tricks. This should normally be used to
/// create input "tables" for BigQuery SQL testing.
///
/// It does sneaky things like building views with built-in data. But it creates
/// those views 13-15x faster than we can create real BigQuery tables.
#[derive(Clone, Debug)]
pub(crate) struct BigQueryTestFixtureLocator {
    /// A wrapped `BigQueryLocator`, which we'll defer many operations to.
    bigquery: BigQueryLocator,
}

impl fmt::Display for BigQueryTestFixtureLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bigquery-test-fixture:{}", self.bigquery.as_table_name())
    }
}

impl FromStr for BigQueryTestFixtureLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let as_bigquery = s.replace(
            BigQueryTestFixtureLocator::scheme(),
            BigQueryLocator::scheme(),
        );
        Ok(BigQueryTestFixtureLocator {
            bigquery: BigQueryLocator::from_str(&as_bigquery)?,
        })
    }
}

impl Locator for BigQueryTestFixtureLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self, ctx: Context) -> BoxFuture<Option<Schema>> {
        self.bigquery.schema(ctx)
    }

    fn count(
        &self,
        ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<usize> {
        self.bigquery.count(ctx, shared_args, source_args)
    }

    fn local_data(
        &self,
        ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        self.bigquery.local_data(ctx, shared_args, source_args)
    }

    fn display_output_locators(&self) -> DisplayOutputLocators {
        self.bigquery.display_output_locators()
    }

    fn write_local_data(
        &self,
        ctx: Context,
        data: BoxStream<CsvStream>,
        shared_args: SharedArguments<Unverified>,
        dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<BoxStream<BoxFuture<BoxLocator>>> {
        write_local_data_helper(ctx, self.to_owned(), data, shared_args, dest_args)
            .boxed()
    }

    fn supports_write_remote_data(&self, source: &dyn Locator) -> bool {
        self.bigquery.supports_write_remote_data(source)
    }

    fn write_remote_data(
        &self,
        ctx: Context,
        source: BoxLocator,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
        dest_args: DestinationArguments<Unverified>,
    ) -> BoxFuture<Vec<BoxLocator>> {
        self.bigquery.write_remote_data(
            ctx,
            source,
            shared_args,
            source_args,
            dest_args,
        )
    }
}

/// Cached type information about a column.
struct BqColumnTypeInfo {
    is_not_null: bool,
    bq_data_type: BqDataType,
}

/// Actual implementation of `write_local_data
#[instrument(
    level = "debug",
    name = "bigquery_test_fixture::write_local_data",
    skip_all,
    fields(dest = %dest)
)]
async fn write_local_data_helper(
    ctx: Context,
    dest: BigQueryTestFixtureLocator,
    data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    // Concatenate all our CSV streams together, removing duplicate headers.
    let csv_stream = concatenate_csv_streams(ctx.clone(), data)?;
    let csv_data = csv_stream.into_bytes().await?;

    // Check our data size. If it's too big, fall back to our regular BigQuery
    // driver. (Assuming we didn't already crash by trying to read a 60 GB CSV
    // file into RAM!)
    if csv_data.len() > MAX_CSV_SIZE_FOR_VIEW {
        debug!(
            "bigquery-test-fixture data is too big ({} bytes), loading the slow way",
            csv_data.len()
        );
        let csv_stream = CsvStream::from_bytes(csv_data).await;
        let data = box_stream_once(Ok(csv_stream));
        return dest
            .bigquery
            .write_local_data(ctx, data, shared_args, dest_args)
            .await;
    }

    // Validate our driver arguments.
    let shared_args = shared_args.verify(BigQueryTestFixtureLocator::features())?;
    let dest_args = dest_args.verify(BigQueryTestFixtureLocator::features())?;

    // Make sure we're in a supported `--if-exists` mode.
    let if_exists = dest_args.if_exists();

    // Get our BigQuery table.
    let schema = shared_args.schema();
    let bq_table = BqTable::for_table_name_and_columns(
        schema,
        dest.bigquery.as_table_name().to_owned(),
        &schema.table.columns,
        Usage::FinalTable,
    )?;

    // Collect our bq_data_types for each column.
    let bq_col_type_infos = bq_table
        .columns
        .iter()
        .map(|c| {
            Ok(BqColumnTypeInfo {
                is_not_null: c.is_not_null(),
                bq_data_type: c.bq_data_type()?,
            })
        })
        .collect::<Result<Vec<BqColumnTypeInfo>>>()?;

    // Generate SQL header.
    let mut sql = vec![];
    writeln!(&mut sql, "SELECT * FROM UNNEST(ARRAY<STRUCT<")?;
    for (idx, (col, bq_col_type_info)) in
        bq_table.columns.iter().zip(&bq_col_type_infos).enumerate()
    {
        separator_comma(&mut sql, idx)?;
        writeln!(
            &mut sql,
            "{} {}",
            col.name.quoted(),
            bq_col_type_info.bq_data_type
        )?;
    }
    writeln!(&mut sql, ">>[")?;

    // Read CSV rows and copy data into SQL body.
    let mut rdr = csv::Reader::from_reader(Cursor::new(&*csv_data));
    for (row_idx, row) in rdr.records().enumerate() {
        let row = row?;
        separator_comma(&mut sql, row_idx)?;
        write!(&mut sql, "(")?;
        for (col_idx, (bq_col_type_info, cell)) in
            bq_col_type_infos.iter().zip(row.into_iter()).enumerate()
        {
            separator_comma(&mut sql, col_idx)?;
            write_csv_cell_as_bigquery_literal(&mut sql, bq_col_type_info, cell)?;
        }
        writeln!(&mut sql, ")")?;
    }
    writeln!(&mut sql, "])")?;
    let sql =
        String::from_utf8(sql).context("CREATE VIEW SQL contained non-UTF-8 data")?;

    // Create our view. Using `bigquery::execute_query` would need to create a
    // batch job, which takes a minimum of about 2 seconds. Using
    // `bigquery::delete_table` and `bigquery::create_view` gets it down under
    // 0.7 seconds.
    debug!("import sql: {}", sql);
    if if_exists == &IfExists::Overwrite {
        bigquery::delete_table(dest.bigquery.as_table_name(), true).await?;
    }
    bigquery::create_view(dest.bigquery.as_table_name(), &sql).await?;

    // We don't need any parallelism after the BigQuery step, so just return
    // a stream containing a single future.
    let fut = async { Ok(dest.boxed()) }.boxed();
    Ok(box_stream_once(Ok(fut)))
}

/// Write a comma, but only if `idx` is not 0.
fn separator_comma<W: Write>(wtr: &mut W, idx: usize) -> Result<(), io::Error> {
    if idx != 0 {
        write!(wtr, ",")?;
    }
    Ok(())
}

/// Parse a CSV cell and write it as a BigQuery literal SQL value.
fn write_csv_cell_as_bigquery_literal<W: Write>(
    sql: &mut W,
    bq_col_type_info: &BqColumnTypeInfo,
    cell: &str,
) -> Result<()> {
    match &bq_col_type_info.bq_data_type {
        BqDataType::Array(elem_ty) => {
            let json = serde_json::Value::from_csv_cell(cell)?;
            if let serde_json::Value::Array(arr) = json {
                write!(sql, "[")?;
                for (idx, json) in arr.into_iter().enumerate() {
                    separator_comma(sql, idx)?;
                    write_json_value_as_bigquery_literal(elem_ty, &json, sql)?;
                }
                write!(sql, "]")?;
            } else {
                return Err(format_err!("expected JSON array, found {:?}", cell));
            }
        }
        BqDataType::NonArray(ty) => {
            // If this column is nullable, and the cell is empty, always make it
            // `NULL`. This matches the behavior of BigQuery's native CSV
            // import, as well as our own `BigQueryLocator` driver. For example,
            // see https://stackoverflow.com/q/38014288/12089.
            if !bq_col_type_info.is_not_null && cell.is_empty() {
                write!(sql, "NULL")?;
                return Ok(());
            }

            match ty {
                BqNonArrayDataType::Bool => {
                    let value: bool = FromCsvCell::from_csv_cell(cell)?;
                    value.write_bigquery_sql(sql)?;
                }
                BqNonArrayDataType::Bytes => {
                    BytesLiteral(cell).write_bigquery_sql(sql)?;
                }
                BqNonArrayDataType::Date => {
                    let value: NaiveDate = FromCsvCell::from_csv_cell(cell)?;
                    value.write_bigquery_sql(sql)?;
                }
                BqNonArrayDataType::Datetime => {
                    let value: NaiveDateTime = FromCsvCell::from_csv_cell(cell)?;
                    value.write_bigquery_sql(sql)?;
                }
                BqNonArrayDataType::Float64 => {
                    let value: f64 = FromCsvCell::from_csv_cell(cell)?;
                    // BigQuery will accept `1e37`, but not "1" followed by 37 zeroes.
                    // Go figure.
                    ExpNotation(value).write_bigquery_sql(sql)?;
                }
                BqNonArrayDataType::Geography => {
                    GeographyLiteral(cell).write_bigquery_sql(sql)?;
                }
                BqNonArrayDataType::Int64 => {
                    let value: i64 = FromCsvCell::from_csv_cell(cell)?;
                    value.write_bigquery_sql(sql)?;
                }
                BqNonArrayDataType::Numeric => {
                    NumericLiteral(cell).write_bigquery_sql(sql)?;
                }
                BqNonArrayDataType::String | BqNonArrayDataType::Stringified(_) => {
                    cell.write_bigquery_sql(sql)?;
                }
                BqNonArrayDataType::Timestamp => {
                    let value: DateTime<Utc> = FromCsvCell::from_csv_cell(cell)?;
                    value.write_bigquery_sql(sql)?;
                }
                // Time types cannot occur naturally in the current dbcrossbar
                // interchange types. Structs do occur, and we could support
                // them, but we haven't done the work yet.
                BqNonArrayDataType::Time | BqNonArrayDataType::Struct(_) => {
                    return Err(format_err!(
                        "cannot ingest data of type {} using {}, use {} instead",
                        ty,
                        BigQueryTestFixtureLocator::scheme(),
                        BigQueryLocator::scheme(),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Take a `serde_json::Value` and write it out as a BigQuery SQL literal. We
/// use this to handle elements in array literals.
fn write_json_value_as_bigquery_literal<W: Write>(
    elem_ty: &BqNonArrayDataType,
    json: &serde_json::Value,
    sql: &mut W,
) -> Result<()> {
    match elem_ty {
        BqNonArrayDataType::Bool => {
            let value: bool = FromJsonValue::from_json_value(json)?;
            value.write_bigquery_sql(sql)?;
        }
        BqNonArrayDataType::Bytes => {
            let value: String = FromJsonValue::from_json_value(json)?;
            BytesLiteral(&value).write_bigquery_sql(sql)?;
        }
        BqNonArrayDataType::Date => {
            let value: NaiveDate = FromJsonValue::from_json_value(json)?;
            value.write_bigquery_sql(sql)?;
        }
        BqNonArrayDataType::Datetime => {
            let value: NaiveDateTime = FromJsonValue::from_json_value(json)?;
            value.write_bigquery_sql(sql)?;
        }
        BqNonArrayDataType::Float64 => {
            let value: f64 = FromJsonValue::from_json_value(json)?;
            // BigQuery will accept `1e37`, but not "1" followed by 37 zeroes.
            // Go figure.
            ExpNotation(value).write_bigquery_sql(sql)?;
        }
        BqNonArrayDataType::Geography => {
            let value: String = FromJsonValue::from_json_value(json)?;
            GeographyLiteral(&value).write_bigquery_sql(sql)?;
        }
        BqNonArrayDataType::Int64 => {
            let value: i64 = FromJsonValue::from_json_value(json)?;
            value.write_bigquery_sql(sql)?;
        }
        BqNonArrayDataType::Numeric => {
            let value: String = FromJsonValue::from_json_value(json)?;
            NumericLiteral(&value).write_bigquery_sql(sql)?;
        }
        BqNonArrayDataType::String | BqNonArrayDataType::Stringified(_) => {
            let value: String = FromJsonValue::from_json_value(json)?;
            (&value[..]).write_bigquery_sql(sql)?;
        }
        BqNonArrayDataType::Timestamp => {
            let value: DateTime<Utc> = FromJsonValue::from_json_value(json)?;
            value.write_bigquery_sql(sql)?;
        }
        // Time types cannot occur naturally in the current dbcrossbar
        // interchange types. Structs do occur, and we could support
        // them, but we haven't done the work yet.
        BqNonArrayDataType::Time | BqNonArrayDataType::Struct(_) => {
            return Err(format_err!(
                "cannot ingest data of type {} using {}, use {} instead",
                elem_ty,
                BigQueryTestFixtureLocator::scheme(),
                BigQueryLocator::scheme(),
            ));
        }
    };
    Ok(())
}

impl LocatorStatic for BigQueryTestFixtureLocator {
    fn scheme() -> &'static str {
        "bigquery-test-fixture:"
    }

    fn features() -> Features {
        // We suppor the same features as BigQuery, except our `--if-exists`
        // options are a lot more limited.
        let mut result = BigQueryLocator::features();
        result.dest_if_exists = IfExistsFeatures::Overwrite | IfExistsFeatures::Error;
        result
    }
}
