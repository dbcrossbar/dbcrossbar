//! Implementation of `write_local_data` for BigQuery.

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::{fmt, iter, str::FromStr};

use crate::common::*;
use crate::drivers::{
    bigquery::BigQueryLocator,
    bigquery_shared::TableName,
    gs::{GsLocator, GS_SCHEME},
};

/// Implementation of `write_local_data`, but as a real `async` function.
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    dest: BigQueryLocator,
    schema: Table,
    data: BoxStream<CsvStream>,
    temporaries: Vec<String>,
    if_exists: IfExists,
) -> Result<BoxStream<BoxFuture<()>>> {
    // Build a temporary location.
    let gs_temp = find_gs_temp_dir(&temporaries)?;

    // Copy to a temporary gs:// location.
    let to_temp_ctx = ctx.child(o!("to_temp" => gs_temp.to_string()));
    let result_stream = await!(gs_temp.write_local_data(
        to_temp_ctx,
        schema.clone(),
        data,
        temporaries.clone(),
        IfExists::Overwrite,
    ))?;

    // Wait for all gs:// uploads to finish with controllable parallelism.
    //
    // TODO: This duplicates our top-level `cp` code and we need to implement the
    // same rules for picking a good argument to `buffered` and not just hard code
    // our parallelism.
    await!(result_stream.buffered(4).collect())?;

    // Load from gs:// to BigQuery.
    let from_temp_ctx = ctx.child(o!("from_temp" => gs_temp.to_string()));
    await!(dest.write_remote_data(
        from_temp_ctx,
        schema,
        Box::new(gs_temp),
        if_exists
    ))?;

    // We don't need any parallelism after the BigQuery step, so just return
    // a stream containing a single future.
    let fut = Ok(()).into_boxed_future();
    Ok(box_stream_once(Ok(fut)))
}

fn find_gs_temp_dir(temporaries: &[String]) -> Result<GsLocator> {
    for temp in temporaries {
        let mut temp = temp.to_owned();
        if temp.starts_with(GS_SCHEME) {
            if !temp.ends_with('/') {
                temp.push_str("/");
            }
            let mut rng = thread_rng();
            let subdir = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .take(10)
                .collect::<String>();
            temp.push_str(&subdir);
            temp.push_str("/");
            return GsLocator::from_str(&temp);
        }
    }
    Err(format_err!("need `--temporary=gs://...` argument"))
}
