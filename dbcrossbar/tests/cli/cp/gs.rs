//! Google Cloud Storage-specific tests.

use super::*;

#[test]
#[ignore]
fn cp_from_gs_to_exact_csv() {
    let gs_dir = gs_test_dir_url("cp_from_gs_to_exact_csv");
    assert_cp_to_exact_csv("cp_from_gs_to_exact_csv", &gs_dir, Default::default());
}

#[test]
#[ignore]
fn cp_to_single_gs_csv() {
    let testdir = TestDir::new("dbcrossbar", "cp_to_single_gs_csv");
    let src = testdir.src_path("fixtures/posts.csv");
    let schema = testdir.src_path("fixtures/posts.sql");
    let gs_out_dir = gs_test_dir_url("cp_to_single_gs_csv_output");
    let gs_out_csv_file = format!("{}out.csv", gs_out_dir);

    // CSV file to Google Cloud Storage.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &gs_out_csv_file,
        ])
        .tee_output()
        .expect_success();

    // Google Cloud Storage back to a single CSV.
    testdir
        .cmd()
        .args([
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &gs_out_dir,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();

    let expected = normalize_csv_data(&fs::read_to_string(&src).unwrap());
    let actual =
        normalize_csv_data(&fs::read_to_string(testdir.path("out/out.csv")).unwrap());
    assert_diff!(&expected, &actual, ",", 0);
}

#[test]
#[ignore]
fn cp_bigquery_single_gs_csv() {
    let testdir = TestDir::new("dbcrossbar", "cp_bigquery_single_gs_csv");
    let src = testdir.src_path("fixtures/posts.csv");
    let schema = testdir.src_path("fixtures/posts.sql");
    let gs_temp_dir = gs_test_dir_url("cp_bigquery_single_gs_csv");
    let bq_temp_ds = bq_temp_dataset();
    let bq_table = bq_test_table("cp_bigquery_single_gs_csv");
    let gs_out_dir = gs_test_dir_url("cp_bigquery_single_gs_csv_output");
    let gs_out_csv_file = format!("{}out.csv", gs_out_dir);

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

    // BigQuery to a single CSV on Google Cloud Storage.
    let output = testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            "--display-output-locators",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=postgres-sql:{}", schema.display()),
            &bq_table,
            &gs_out_csv_file,
        ])
        .tee_output()
        .expect_success();
    let locators = output.stdout_str();
    assert_eq!(locators, format!("{}\n", gs_out_csv_file));

    // Google Cloud Storage back to a single CSV.
    testdir
        .cmd()
        .args([
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &gs_out_dir,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();

    let expected = normalize_csv_data(&fs::read_to_string(&src).unwrap());
    let actual =
        normalize_csv_data(&fs::read_to_string(testdir.path("out/out.csv")).unwrap());
    assert_diff!(&expected, &actual, ",", 0);
}
