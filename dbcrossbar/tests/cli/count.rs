//! Tests for the `count` subcommand.

use cli_test_dir::*;
use dbcrossbar_trino::ConnectorType;

use super::cp::*;

#[test]
#[ignore]
fn count_bigquery() {
    let testdir = TestDir::new("dbcrossbar", "count_bigquery");
    let src = testdir.src_path("fixtures/posts.csv");
    let schema = testdir.src_path("fixtures/posts.sql");
    let gs_temp_dir = gs_test_dir_url("count_bigquery");
    let bq_temp_ds = bq_temp_dataset();
    let bq_table = bq_test_table("count_bigquery");

    // CSV to BigQuery.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // Count BigQuery.
    let output = testdir
        .cmd()
        .args(["count", &bq_table])
        .tee_output()
        .expect_success();

    assert_eq!(output.stdout_str().trim(), "2");
}

#[test]
#[ignore]
fn count_postgres() {
    let testdir = TestDir::new("dbcrossbar", "count_postgres");
    let src = testdir.src_path("fixtures/posts.csv");
    let schema = testdir.src_path("fixtures/posts.sql");
    let pg_table = post_test_table_url("count_postgres");

    // CSV to PostgreSQL.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &pg_table,
        ])
        .tee_output()
        .expect_success();

    // Count PostgreSQL.
    let output = testdir
        .cmd()
        .args(["count", &pg_table])
        .tee_output()
        .expect_success();

    assert_eq!(output.stdout_str().trim(), "2");
}

#[test]
#[ignore]
fn count_trino() {
    for conn in ConnectorType::all_testable() {
        let testdir = TestDir::new("dbcrossbar", "count_trino");
        let src = testdir.src_path("fixtures/posts.csv");
        let schema = testdir.src_path("fixtures/posts.sql");
        let s3_temp_dir = s3_test_dir_url("count_trino");
        let trino_table = trino_test_table(&conn, "count_trino");

        // CSV to BigQuery.
        testdir
            .cmd()
            .args([
                "cp",
                "--if-exists=overwrite",
                &format!("--temporary={}", s3_temp_dir),
                &format!("--schema=postgres-sql:{}", schema.display()),
                &format!("csv:{}", src.display()),
                &trino_table,
            ])
            .tee_output()
            .expect_success();

        // Count BigQuery.
        let output = testdir
            .cmd()
            .args(["count", &trino_table])
            .tee_output()
            .expect_success();

        assert_eq!(output.stdout_str().trim(), "2");
    }
}
