//! Reading data from AWS S3.

use lazy_static::lazy_static;
use regex::Regex;
use std::{
    io::BufReader,
    process::{Command, Stdio},
};
use tokio::io;
use tokio_process::CommandExt;

use crate::common::*;
use crate::csv_stream::csv_stream_name;
use crate::tokio_glue::copy_reader_to_stream;

/// Implementation of `local_data`, but as a real `async` function.
pub(crate) async fn local_data_helper(
    ctx: Context,
    url: Url,
    query: Query,
) -> Result<Option<BoxStream<CsvStream>>> {
    query.fail_if_query_details_provided()?;
    debug!(ctx.log(), "getting CSV files from {}", url);

    // Start a child process to list files at that URL.
    debug!(ctx.log(), "listing {}", url);
    let mut child = Command::new("aws")
        .args(&["s3", "ls", "--recursive", url.as_str()])
        .stdout(Stdio::piped())
        .spawn_async()
        .context("error running `aws s3 ls`")?;
    let child_stdout = child.stdout().take().expect("child should have stdout");
    ctx.spawn_process(format!("aws s3 ls {}", url), child);

    // Parse `ls` output into lines, and convert into `CsvStream` values lazily
    // in case there are a lot of CSV files we need to read.
    //
    // XXX - This will fail (either silently or noisily, I'm not sure) if there
    // are 1000+ files in the S3 directory, and we can't fix this without
    // switching from `aws s3` to native S3 API calls from Rust.
    let lines = io::lines(BufReader::with_capacity(BUFFER_SIZE, child_stdout))
        .map_err(|e| format_err!("error reading `aws s3 ls` output: {}", e));
    let csv_streams = lines.and_then(move |line| {
        let ctx = ctx.clone();
        let url = url.clone();
        async move {
            trace!(ctx.log(), "`aws s3 ls` line: {}", line);
            let bucket_url = bucket_url(&url)?;
            let path = path_from_line(&line)?;
            let file_url = bucket_url.join(&path)?;

            // Stream the file from the cloud.
            let name = csv_stream_name(url.as_str(), file_url.as_str())?;
            let ctx = ctx.child(
                o!("stream" => name.to_owned(), "url" => file_url.as_str().to_owned()),
            );
            debug!(ctx.log(), "streaming from {} using `aws s3 cp`", file_url);
            let mut child = Command::new("aws")
                .args(&["s3", "cp", file_url.as_str(), "-"])
                .stdout(Stdio::piped())
                .spawn_async()
                .context("error running `aws s3 cp`")?;
            let child_stdout =
                child.stdout().take().expect("child should have stdout");
            let child_stdout = BufReader::with_capacity(BUFFER_SIZE, child_stdout);
            let data = copy_reader_to_stream(ctx.clone(), child_stdout)?;
            ctx.spawn_process(format!("aws s3 cp {} -", file_url), child);

            // Assemble everything into a CSV stream.
            Ok(CsvStream {
                name: name.to_owned(),
                data: Box::new(data),
            })
        }
            .boxed()
            .compat()
    });

    Ok(Some(Box::new(csv_streams) as BoxStream<CsvStream>))
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
