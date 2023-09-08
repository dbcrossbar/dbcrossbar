//! Tests that affect multiple backends.

use cli_test_dir::*;
use difference::assert_diff;
use opinionated_telemetry::{
    current_span_as_env, set_parent_span_from_env, AppType, TelemetryConfig,
};
use std::{fs, process::Stdio};
use tracing::info_span;

use super::*;

#[tokio::test]
#[ignore]
async fn cp_csv_to_postgres_to_gs_to_csv() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_postgres_to_gs_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let expected_schema = testdir.src_path("fixtures/many_types_expected.sql");
    let pg_table = post_test_table_url("testme1.cp_csv_to_postgres_to_gs_to_csv");
    let gs_dir = gs_test_dir_url("cp_csv_to_postgres_to_gs_to_csv");
    let bq_table = bq_test_table("cp_csv_to_postgres_to_gs_to_csv");
    let gs_dir_2 = gs_test_dir_url("cp_csv_to_postgres_to_gs_to_csv_2");
    let pg_table_2 = post_test_table_url("cp_csv_to_postgres_to_gs_to_csv_2");

    // Just for fun, set up a trace across multiple calls to `dbcrossbar`.
    let telemetry_handle = TelemetryConfig::new(
        AppType::Cli,
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
    )
    .install()
    .await
    .expect("could not install telemetry");
    let span = info_span!("cp_csv_to_postgres_to_gs_to_csv").entered();
    set_parent_span_from_env();

    // CSV to Postgres.
    testdir
        .cmd()
        .envs(current_span_as_env())
        .args([
            "cp",
            "--if-exists=overwrite",
            "--max-streams=8",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &pg_table,
        ])
        .tee_output()
        .expect_success();

    // (Check PostgreSQL schema extraction now, so we know that we aren't
    // messing up later tests.)
    testdir
        .cmd()
        .envs(current_span_as_env())
        .args(["schema", "conv", &pg_table, "postgres-sql:pg.sql"])
        .stdout(Stdio::piped())
        .tee_output()
        .expect_success();
    let postgres_sql = fs::read_to_string(expected_schema).unwrap().replace(
        "\"many_types\"",
        "\"testme1\".\"cp_csv_to_postgres_to_gs_to_csv\"",
    );
    testdir.expect_file_contents("pg.sql", postgres_sql);

    // Postgres to gs://.
    testdir
        .cmd()
        .envs(current_span_as_env())
        .args(["cp", "--if-exists=overwrite", &pg_table, &gs_dir])
        .tee_output()
        .expect_success();

    // gs:// to BigQuery.
    testdir
        .cmd()
        .envs(current_span_as_env())
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &gs_dir,
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // BigQuery to gs://.
    testdir
        .cmd()
        .envs(current_span_as_env())
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &bq_table,
            &gs_dir_2,
        ])
        .tee_output()
        .expect_success();

    // gs:// back to PostgreSQL. (Mostly because we'll need a PostgreSQL-generated
    // CSV file for the final comparison below.)
    testdir
        .cmd()
        .envs(current_span_as_env())
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &gs_dir_2,
            &pg_table_2,
        ])
        .tee_output()
        .expect_success();

    // PostgreSQL back to CSV for the final comparison below.
    testdir
        .cmd()
        .envs(current_span_as_env())
        .args([
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &pg_table_2,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&src).unwrap();
    let actual =
        fs::read_to_string(testdir.path("out/cp_csv_to_postgres_to_gs_to_csv_2.csv"))
            .unwrap();
    assert_diff!(&expected, &actual, ",", 0);

    drop(span);
    telemetry_handle.flush_and_shutdown().await;
}

#[test]
#[ignore]
fn cp_tricky_column_names_fails() {
    let testdir = TestDir::new("dbcrossbar", "cp_tricky_column_names");
    let src = testdir.src_path("fixtures/tricky_column_names.csv");
    let schema = testdir.src_path("fixtures/tricky_column_names.sql");
    let pg_table = post_test_table_url("testme1.cp_tricky_column_names");
    let bq_table = bq_test_table("cp_tricky_column_names");
    let gs_temp_dir = gs_test_dir_url("cp_from_bigquery_with_where");
    let bq_temp_ds = bq_temp_dataset();

    // CSV to Postgres.
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

    // Postgres to BigQuery.
    //
    // This is now expected to fail, because editing column names automagically
    // ends in tears and policy is now firmly against it. Instead, copy to a
    // local machine and run `scrubcsv --clean-column-names`.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &pg_table,
            &bq_table,
        ])
        .tee_output()
        .expect_failure();
}
