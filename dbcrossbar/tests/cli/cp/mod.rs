//! Tests for the `cp` subcommand.

use cli_test_dir::*;
use std::env;

mod bigml;
mod bigquery;
mod combined;
mod csv;
mod postgres;
mod redshift;
mod s3;

/// The URL of our test database.
pub(crate) fn postgres_test_url() -> String {
    env::var("POSTGRES_TEST_URL").unwrap_or_else(|_| {
        "postgres://postgres:@localhost:5432/dbcrossbar_test".to_owned()
    })
}

/// The URL of a table in our test database.
pub(crate) fn post_test_table_url(table_name: &str) -> String {
    format!("{}#{}", postgres_test_url(), table_name)
}

/// The URL to our test `gs://` bucket and directory.
pub(crate) fn gs_url() -> String {
    env::var("GS_TEST_URL").expect("GS_TEST_URL must be set")
}

/// The URL to a subdirectory of `gs_url`.
pub(crate) fn gs_test_dir_url(dir_name: &str) -> String {
    let mut url = gs_url();
    if !url.ends_with('/') {
        url.push_str("/");
    }
    url.push_str(dir_name);
    url.push_str("/");
    url
}

/// A BigQuery table name to use for a test, including the project.
pub(crate) fn bq_temp_dataset_name() -> String {
    env::var("BQ_TEST_DATASET").expect("BQ_TEST_DATASET must be set")
}

/// Get our BigQuery test project name.
pub(crate) fn bq_project_id() -> String {
    let ds_name = bq_temp_dataset_name();
    let end = ds_name.find(':').expect("BQ_TEST_DATASET should contain :");
    ds_name[..end].to_owned()
}

/// A BigQuery table to use for a test.
pub(crate) fn bq_temp_dataset() -> String {
    format!("bigquery:{}", bq_temp_dataset_name())
}

/// A BigQuery table to use for a test.
pub(crate) fn bq_test_table(table_name: &str) -> String {
    format!("{}.{}", bq_temp_dataset(), table_name)
}

/// The URL to our test `s3://` bucket and directory.
pub(crate) fn s3_url() -> String {
    env::var("S3_TEST_URL").expect("S3_TEST_URL must be set")
}

/// The URL to a subdirectory of `gs_url`.
pub(crate) fn s3_test_dir_url(dir_name: &str) -> String {
    let mut url = s3_url();
    if !url.ends_with('/') {
        url.push_str("/");
    }
    url.push_str(dir_name);
    url.push_str("/");
    url
}

/// The URL of our Redshift test database. Optional because we're not going to
/// keep Redshift running just for unit tests, not at a minimum of $0.25/hour.
pub(crate) fn redshift_test_url() -> Option<String> {
    env::var("REDSHIFT_TEST_URL").ok()
}

/// The URL of a table in our Redshift test database.
pub(crate) fn redshift_test_table_url(table_name: &str) -> Option<String> {
    redshift_test_url().map(|url| format!("{}#{}", url, table_name))
}

#[test]
fn cp_help_flag() {
    let testdir = TestDir::new("dbcrossbar", "cp_help_flag");
    let output = testdir.cmd().args(&["cp", "--help"]).expect_success();
    assert!(output.stdout_str().contains("EXAMPLE LOCATORS:"));
}
