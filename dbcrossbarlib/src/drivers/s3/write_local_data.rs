//! Writing data to AWS S3.

use std::process::{Command, Stdio};
use tokio_process::CommandExt;

use super::{prepare_as_destination_helper, S3Locator};
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
    let _shared_args = shared_args.verify(S3Locator::features())?;
    let dest_args = dest_args.verify(S3Locator::features())?;

    // Look up our arguments.
    let if_exists = dest_args.if_exists().to_owned();

    // Delete the existing output, if it exists.
    prepare_as_destination_helper(ctx.clone(), url.clone(), if_exists).await?;

    // Spawn our uploader threads.
    let written = data.map(move |stream| {
        let url = url.clone();
        let ctx = ctx.clone();
        async move {
            let url = url.join(&format!("{}.csv", stream.name))?;
            let ctx = ctx
                .child(o!("stream" => stream.name.clone(), "url" => url.to_string()));

            // Run `aws cp - $URL` as a background process.
            debug!(ctx.log(), "uploading stream to `aws s3`");
            let mut child = Command::new("aws")
                .args(&["s3", "cp", "-", url.as_str()])
                .stdin(Stdio::piped())
                // Throw away std output because it's spammy.
                .stdout(Stdio::null())
                .spawn_async()
                .context("error running `aws s3`")?;
            let child_stdin = child.stdin().take().expect("child should have stdin");

            // Copy data to our child process.
            copy_stream_to_writer(ctx.clone(), stream.data, child_stdin)
                .await
                .context("error copying data to `aws s3`")?;

            // Wait for `aws s3` to finish.
            let status = child
                .compat()
                .await
                .with_context(|_| format!("error finishing upload to {}", url))?;
            if status.success() {
                Ok(S3Locator { url }.boxed())
            } else {
                Err(format_err!("`aws s3` returned error: {}", status))
            }
        }
            .boxed()
    });

    Ok(Box::new(written) as BoxStream<BoxFuture<BoxLocator>>)
}
