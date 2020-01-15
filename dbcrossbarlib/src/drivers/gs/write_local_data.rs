//! Writing data to Google Cloud Storage.

use std::process::Stdio;
use tokio::process::Command;

use super::{prepare_as_destination_helper, GsLocator};
use crate::common::*;
use crate::tokio_glue::copy_stream_to_writer;

/// Implementation of `write_local_data`, but as a real `async` function.
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    url: Url,
    data: BoxStream<CsvStream>,
    shared_args: SharedArguments<Unverified>,
    dest_args: DestinationArguments<Unverified>,
) -> Result<BoxStream<BoxFuture<BoxLocator>>> {
    let _shared_args = shared_args.verify(GsLocator::features())?;
    let dest_args = dest_args.verify(GsLocator::features())?;

    // Delete the existing output, if it exists.
    let if_exists = dest_args.if_exists().to_owned();
    prepare_as_destination_helper(ctx.clone(), url.clone(), if_exists).await?;

    // Spawn our uploader processes.
    let written = data.map_ok(move |stream| {
        let url = url.clone();
        let ctx = ctx.clone();
        async move {
            let url = url.join(&format!("{}.csv", stream.name))?;
            let ctx = ctx
                .child(o!("stream" => stream.name.clone(), "url" => url.to_string()));

            // Run `gsutil cp - $URL` as a background process.
            debug!(ctx.log(), "uploading stream to gsutil");
            let mut child = Command::new("gsutil")
                .args(&["cp", "-", url.as_str()])
                .stdin(Stdio::piped())
                // Throw away stdout so it doesn't corrupt our output.
                .stdout(Stdio::null())
                .spawn()
                .context("error running gsutil")?;
            let child_stdin = child.stdin().take().expect("child should have stdin");

            // Copy data to our child process.
            copy_stream_to_writer(ctx.clone(), stream.data, child_stdin)
                .await
                .context("error copying data to gsutil")?;

            // Wait for `gsutil` to finish.
            let status = child
                .await
                .with_context(|_| format!("error finishing upload to {}", url))?;
            if status.success() {
                Ok(GsLocator { url }.boxed())
            } else {
                Err(format_err!("gsutil returned error: {}", status))
            }
        }
        .boxed()
    });

    Ok(written.boxed())
}
