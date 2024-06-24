//! Trino-related tests.

use cli_test_dir::*;
use difference::assert_diff;

use crate::cp::trino_test_url;

use super::{assert_cp_to_exact_csv, s3_test_dir_url, TempType};

/// Generate a locator for a test table in Trino.
fn trino_test_table(table_name: &str) -> String {
    format!("{}#{}", trino_test_url(), table_name)
}

#[test]
#[ignore]
fn cp_from_trino_to_exact_csv() {
    let table = trino_test_table("cp_from_trino_to_exact_csv");
    assert_cp_to_exact_csv("cp_from_trino_to_exact_csv", &table, TempType::S3.into());
}

#[test]
#[ignore]
fn cp_csv_to_trino_to_csv() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_trino_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let s3_temp_dir = s3_test_dir_url("cp_csv_to_trino_to_csv");
    let trino_table = trino_test_table("cp_csv_to_trino_to_csv");

    // CSV to Trino.
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

    // Trino to CSV.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", s3_temp_dir),
            // Including a portable schema currently helps with exporting
            // GeoJSON columns, because Trino doesn't know the SRID, and so we
            // can't necessarily choose a good portable column type.
            &format!("--schema=postgres-sql:{}", schema.display()),
            &trino_table,
            "csv:out/many_types.csv",
        ])
        .tee_output()
        .expect_success();

    // Print our output for manual inspection. Use `--nocapture` to see this.
    let out_path = testdir.path("out/many_types.csv");
    eprintln!("output:\n{}", std::fs::read_to_string(out_path).unwrap());
}

// Create table using `schema conv` and dump the schema.
#[test]
#[ignore]
fn schema_conv_on_trino_table() {
    let testdir = TestDir::new("dbcrossbar", "schema_conv_on_trino_table");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let expected = testdir.src_path("fixtures/trino/many_types_expected.sql");

    testdir
        .cmd()
        .args([
            "schema",
            "conv",
            "--if-exists=overwrite",
            &format!("postgres-sql:{}", schema.display()),
            &trino_test_table("schema_conv_on_trino_table"),
        ])
        .tee_output()
        .expect_success();

    testdir
        .cmd()
        .args([
            "schema",
            "conv",
            &trino_test_table("schema_conv_on_trino_table"),
            "postgres-sql:output.sql",
        ])
        .tee_output()
        .expect_success();
    let expected = std::fs::read_to_string(expected).unwrap();
    let output = std::fs::read_to_string(testdir.path("output.sql")).unwrap();
    assert_diff!(&expected, &output, "\n", 0);
}
