//! BigQuery-test-fixture specific tests.

use super::*;

#[test]
#[ignore]
fn cp_from_bigquery_test_fixture_to_exact_csv() {
    let bq_table = bq_test_table("cp_from_bigquery_test_fixture_to_exact_csv")
        .replace("bigquery:", "bigquery-test-fixture:");
    assert_cp_to_exact_csv(
        "cp_from_bigquery_test_fixture_to_exact_csv",
        &bq_table,
        TempType::Gs | TempType::Bq,
    );
}

#[test]
#[ignore]
fn cp_csv_to_bigquery_test_fixture_to_csv() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_bigquery_test_fixture_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let bq_temp_ds = bq_temp_dataset();
    let gs_temp_dir = gs_test_dir_url("cp_csv_to_bigquery_test_fixture_to_csv");
    let bq_table = bq_test_table("cp_csv_to_bigquery_test_fixture_to_csv")
        .replace("bigquery:", "bigquery-test-fixture:");

    // CSV to BigQuery.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=postgres-sql:{}", schema.display()),
            "--to-arg=job_labels[dbcrossbar_test]=true",
            &format!("csv:{}", src.display()),
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // BigQuery to CSV.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            "--from-arg=job_labels[dbcrossbar_test]=true",
            &bq_table,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();
}

#[test]
#[ignore]
fn bigquery_test_fixture_load_single_column() {
    let testdir =
        TestDir::new("dbcrossbar", "bigquery_test_fixture_load_single_column");
    let src = testdir.src_path("fixtures/bigquery_test_fixture/single_column.csv");
    let schema = testdir.src_path("fixtures/bigquery_test_fixture/single_column.sql");
    let bq_temp_ds = bq_temp_dataset();
    let gs_temp_dir = gs_test_dir_url("bigquery_test_fixture_load_single_column");
    let bq_table = bq_test_table("bigquery_test_fixture_load_single_column")
        .replace("bigquery:", "bigquery-test-fixture:");

    // CSV to BigQuery.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            "--to-arg=job_labels[dbcrossbar_test]=true",
            &format!("csv:{}", src.display()),
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // BigQuery to CSV.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            "--from-arg=job_labels[dbcrossbar_test]=true",
            &bq_table,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();
}

#[test]
#[ignore]
fn bigquery_test_fixture_load_with_empty_array() {
    let testdir =
        TestDir::new("dbcrossbar", "bigquery_test_fixture_load_with_empty_array");
    let src = testdir.src_path("fixtures/bigquery_test_fixture/empty_array.csv");
    let schema = testdir.src_path("fixtures/bigquery_test_fixture/empty_array.sql");
    let bq_temp_ds = bq_temp_dataset();
    let gs_temp_dir = gs_test_dir_url("bigquery_test_fixture_load_with_empty_array");
    let bq_table = bq_test_table("bigquery_test_fixture_load_with_empty_array")
        .replace("bigquery:", "bigquery-test-fixture:");

    // CSV to BigQuery.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            "--to-arg=job_labels[dbcrossbar_test]=true",
            &format!("csv:{}", src.display()),
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // BigQuery to CSV.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            "--from-arg=job_labels[dbcrossbar_test]=true",
            &bq_table,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();
}
