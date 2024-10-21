//! Read data from Trino and return it as a stream of streams of CSV data.

use crate::{
    common::*, drivers::s3::find_s3_temp_dir, transform::spawn_sync_transform,
};

use super::TrinoLocator;

/// Implementation of [`TrinoLocator::local_data`], but as a real `async`
/// function.
#[instrument(
    level = "debug",
    name = "TrinoLocator::local_data",
    skip_all,
    fields(source = %source)
)]
pub(super) async fn local_data_helper(
    ctx: Context,
    source: TrinoLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<BoxStream<CsvStream>>> {
    // Build a temporary location.
    let shared_args_v = shared_args.clone().verify(TrinoLocator::features())?;
    let s3_temp = find_s3_temp_dir(shared_args_v.temporary_storage())?;
    let s3_dest_args = DestinationArguments::for_temporary();
    let s3_source_args = SourceArguments::for_temporary();

    // Make a copy of `shared_args` with a schema that uses all lower-case
    // column names. We need this because Trino->S3 export doesn't honor case.
    // We'll correct it once we download the CSV files.
    let modified_shared_args = shared_args.with_modified_schema(|mut schema| {
        for col in &mut schema.table.columns {
            col.name = col.name.to_lowercase();
        }
        schema
    });

    // Extract from Trino to s3://.
    s3_temp
        .write_remote_data(
            ctx.clone(),
            Box::new(source),
            modified_shared_args,
            source_args,
            s3_dest_args,
        )
        .instrument(trace_span!("extract_to_s3_tmp", url = %s3_temp))
        .await?;

    // Copy from a temporary s3:// location.
    let opt_streams = s3_temp
        .local_data(ctx.clone(), shared_args, s3_source_args)
        .instrument(debug_span!("stream_from_s3", url = %s3_temp))
        .await?;

    // Apply normalization to each `CsvStream`.
    Ok(opt_streams.map(move |streams| {
        streams
            .map(move |result_stream| {
                let ctx = ctx.clone();
                let shared_args_v = shared_args_v.clone();
                result_stream.and_then(|stream| {
                    normalize_trino_csv_stream(ctx, &shared_args_v, stream)
                })
            })
            .boxed()
    }))

    // TODO: We've never really had a plan for cleaning up S3 temporaries or
    // Trino wrapper tables.
}

/// Normalize a `CsvStream` to match the column name in `shared_args`, and to use
/// standard CSV quoting.
fn normalize_trino_csv_stream(
    ctx: Context,
    shared_args: &SharedArguments<Verified>,
    mut stream: CsvStream,
) -> Result<CsvStream> {
    let schema = shared_args.schema().to_owned();
    let transformed = spawn_sync_transform(
        ctx,
        "normalize_trino_csv_stream".to_string(),
        stream.data,
        move |ctx, rdr, wtr| normalize_trino_csv_stream_helper(ctx, rdr, wtr, schema),
    )?;
    stream.data = transformed;
    Ok(stream)
}

/// Helper function for `normalize_trino_csv_stream`. Called in a separate
/// worker thread.
fn normalize_trino_csv_stream_helper(
    _ctx: Context,
    rdr: Box<dyn Read + Send + 'static>,
    wtr: Box<dyn Write + Send + 'static>,
    schema: Schema,
) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(rdr);
    let mut wtr = csv::WriterBuilder::new().flexible(true).from_writer(wtr);

    // Read the header row, and create a new one with the original column names
    // from the schema.
    let headers = rdr.headers()?;
    let columns = &schema.table.columns;
    if headers.len() != columns.len() {
        return Err(format_err!(
            "expected {} columns, got {}",
            columns.len(),
            headers.len()
        ));
    }
    let new_headers = columns.iter().map(|col| &col.name[..]).collect::<Vec<_>>();

    // Write the header row.
    wtr.write_record(&new_headers)?;

    // Read and write the rest of the rows. This has a side effect of
    // normalizing quotes.
    for result in rdr.byte_records() {
        let record = result?;
        wtr.write_byte_record(&record)?;
    }

    Ok(())
}
