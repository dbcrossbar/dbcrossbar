//! Tools for transforming data streams.

use crate::common::*;
use crate::tokio_glue::{
    run_sync_fn_in_background, SyncStreamReader, SyncStreamWriter,
};

/// Run a synchronous transform in a separate thread.
///
/// Given a synchronous function `transform` that reads data from an
/// implementation of `Read`, transforms it, and writes it to an implementation
/// of `Write`, spawn a background thread to run the transform.
pub(crate) fn spawn_sync_transform<T>(
    ctx: Context,
    name: String,
    input: BoxStream<BytesMut>,
    transform: T,
) -> Result<BoxStream<BytesMut>>
where
    T: (FnOnce(
            Context,
            Box<dyn Read + Send + 'static>,
            Box<dyn Write + Send + 'static>,
        ) -> Result<()>)
        + Send
        + 'static,
{
    let ctx = ctx.child(o!("transform" => name.clone()));

    let rdr_ctx = ctx.child(o!("mode" => "input"));
    let rdr = SyncStreamReader::new(rdr_ctx, input);
    let wtr_ctx = ctx.child(o!("mode" => "output"));
    let (wtr, output) = SyncStreamWriter::pipe(wtr_ctx);

    let transform_ctx = ctx.clone();
    let transform_fut = run_sync_fn_in_background(name, move || -> Result<()> {
        transform(transform_ctx, Box::new(rdr), Box::new(wtr))
    });
    ctx.spawn_worker(tokio_fut(transform_fut));

    Ok(Box::new(output))
}
