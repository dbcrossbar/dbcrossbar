//! Tools for transforming data streams.

use crate::common::*;
use crate::tokio_glue::{SyncStreamReader, SyncStreamWriter};

/// Run a synchronous transform in a separate thread.
///
/// Given a synchronous function `transform` that reads data from an
/// implementation of `Read`, transforms it, and writes it to an implementation
/// of `Write`, spawn a background thread to run the transform.
#[instrument(level = "debug", skip(ctx, input, transform))]
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
    let rdr = SyncStreamReader::new(input);
    let (wtr, output) = SyncStreamWriter::pipe();

    let transform_ctx = ctx.clone();
    let transform_fut = spawn_blocking(move || -> Result<()> {
        transform(transform_ctx, Box::new(rdr), Box::new(wtr))
    });
    ctx.spawn_worker(
        debug_span!("sync_transform", name = ?name),
        transform_fut.boxed(),
    );

    Ok(output.boxed())
}
