//! Download files from S3.

use std::process::Stdio;
use tokio::io::BufReader;

use super::aws_s3_command;
use crate::common::*;
use crate::tokio_glue::copy_reader_to_stream;

/// Download the file at the specified URL as a stream.
pub(crate) async fn download_file(
    ctx: &Context,
    file_url: &Url,
) -> Result<BoxStream<BytesMut>> {
    debug!(ctx.log(), "streaming from {} using `aws s3 cp`", file_url);
    let mut child = aws_s3_command()
        .await?
        .args(&["cp", file_url.as_str(), "-"])
        .stdout(Stdio::piped())
        .spawn()
        .context("error running `aws s3 cp`")?;
    let child_stdout = child.stdout.take().expect("child should have stdout");
    let child_stdout = BufReader::with_capacity(BUFFER_SIZE, child_stdout);
    let data = copy_reader_to_stream(ctx.clone(), child_stdout)?;
    ctx.spawn_process(format!("aws s3 cp {} -", file_url), child);
    Ok(data.boxed())
}
