//! Listing S3 files.

use lazy_static::lazy_static;
use regex::Regex;
use std::process::Stdio;
use tokio::io::BufReader;
use tokio_stream::wrappers::LinesStream;

use super::aws_s3_command;
use crate::common::*;

/// List all the files at the specified `s2://` URL, recursively.
#[instrument(level = "trace", skip(ctx))]
pub(crate) async fn ls(
    ctx: &Context,
    url: &Url,
) -> Result<impl Stream<Item = Result<Url>> + Send + Unpin + 'static> {
    // Start a child process to list files at that URL.
    debug!("listing {}", url);
    let mut child = aws_s3_command()
        .await?
        .args(&["ls", "--recursive", url.as_str()])
        .stdout(Stdio::piped())
        .spawn()
        .context("error running `aws s3 ls`")?;
    let child_stdout = child.stdout.take().expect("child should have stdout");
    ctx.spawn_process(format!("aws s3 ls {}", url), child);

    // Parse `ls` output into lines, and convert into `Url`s.
    //
    // XXX - This will fail (either silently or noisily, I'm not sure) if there
    // are 1000+ files in the S3 directory, and we can't fix this without
    // switching from `aws s3` to native S3 API calls from Rust.
    let url = url.to_owned();
    let lines =
        LinesStream::new(BufReader::with_capacity(BUFFER_SIZE, child_stdout).lines())
            .map_err(|e| format_err!("error reading `aws s3 ls` output: {}", e))
            .and_then(move |line| {
                let url = url.clone();
                async move {
                    trace!("`aws s3 ls` line: {}", line);
                    let bucket_url = bucket_url(&url)?;
                    let path = path_from_line(&line)?;
                    Ok(bucket_url.join(&path)?)
                }
            });

    Ok(lines.boxed())
}

/// Given an S3 URL, get the URL for just the bucket itself.
fn bucket_url(url: &Url) -> Result<Url> {
    let bucket = url
        .host()
        .ok_or_else(|| format_err!("could not find bucket name in {}", url))?;
    let bucket_url = format!("s3://{}/", bucket)
        .parse::<Url>()
        .context("could not parse S3 URL")?;
    Ok(bucket_url)
}

#[test]
fn bucket_url_extracts_bucket() {
    let examples = &[
        ("s3://bucket", "s3://bucket/"),
        ("s3://bucket/", "s3://bucket/"),
        ("s3://bucket/dir/", "s3://bucket/"),
        ("s3://bucket/dir/file.csv", "s3://bucket/"),
    ];
    for &(url, expected) in examples {
        assert_eq!(
            bucket_url(&url.parse::<Url>().unwrap()).unwrap().as_str(),
            expected,
        );
    }
}

/// Given a line of `aws s3 ls` output, extract the path.
fn path_from_line(line: &str) -> Result<String> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r#"^[-0-9]+ [:0-9]+ +[0-9]+ ([^\r\n]+)"#)
            .expect("invalid regex in source");
    }
    let cap = RE
        .captures(line)
        .ok_or_else(|| format_err!("cannot parse S3 ls output: {:?}", line))?;
    Ok(cap[1].to_owned())
}

#[test]
fn path_from_line_returns_entire_path() {
    let examples = &[
        ("2013-09-02 21:37:53         10 a.txt", "a.txt"),
        ("2013-09-02 21:37:53    2863288 foo.zip", "foo.zip"),
        (
            "2013-09-02 21:32:57         23 foo/bar/.baz/a",
            "foo/bar/.baz/a",
        ),
    ];
    for &(line, rel_path) in examples {
        assert_eq!(path_from_line(line).unwrap(), rel_path);
    }
}
