//! Writing data to Google Cloud Storage.

use std::process::{Command, Stdio};
use tokio_process::CommandExt;

use crate::common::*;
use crate::tokio_glue::copy_stream_to_writer;

/// Implementation of `write_local_data`, but as a real `async` function.
pub(crate) async fn write_local_data_helper(
    ctx: Context,
    url: Url,
    _schema: Table,
    data: BoxStream<CsvStream>,
    if_exists: IfExists,
) -> Result<BoxStream<BoxFuture<()>>> {
    // Delete the existing output, if it exists.
    if if_exists == IfExists::Overwrite {
        // Delete all the files under `self.url`, but be careful not to
        // delete the entire bucket. See `gsutil rm --help` for details.
        debug!(ctx.log(), "deleting existing {}", url);
        assert!(url.path().ends_with('/'));
        let delete_url = url.join("**")?;
        let status = Command::new("gsutil")
            .args(&["rm", "-f", delete_url.as_str()])
            .status_async()
            .context("error running gsutil")?;
        if !await!(status)?.success() {
            warn!(
                ctx.log(),
                "can't delete contents of {}, possibly because it doesn't exist", url
            );
        }
    } else {
        return Err(format_err!(
            "must specify `overwrite` for gs:// destination"
        ));
    }

    // Spawn our uploader threads.
    let written = data.map(move |stream| {
        let url = url.clone();
        let ctx = ctx.clone();
        tokio_fut(
            async move {
                let url = url.join(&format!("{}.csv", stream.name))?;
                let ctx = ctx.child(
                    o!("stream" => stream.name.clone(), "url" => url.to_string()),
                );

                // Run `gsutil cp - $URL` as a background process.
                debug!(ctx.log(), "uploading stream to gsutil");
                let mut child = Command::new("gsutil")
                    .args(&["cp", "-", url.as_str()])
                    .stdin(Stdio::piped())
                    .spawn_async()
                    .context("error running gsutil")?;
                let child_stdin =
                    child.stdin().take().expect("child should have stdin");

                // Copy data to our child process.
                await!(copy_stream_to_writer(ctx.clone(), stream.data, child_stdin))
                    .context("error copying data to gsutil")?;

                // Wait for `gsutil` to finish.
                let status = await!(child)
                    .with_context(|_| format!("error finishing upload to {}", url))?;
                if status.success() {
                    Ok(())
                } else {
                    Err(format_err!("gsutil returned error: {}", status))
                }
            },
        )
        .into_boxed()
    });

    Ok(Box::new(written) as BoxStream<BoxFuture<()>>)
}
