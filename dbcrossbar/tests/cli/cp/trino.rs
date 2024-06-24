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

/// Helper for copying a CSV file to Trino and back to CSV. We do not currently
/// check the output, because we are working with types that may have
/// non-deterministic representations. Although we could write a custom
/// comparator if we wanted to put in the effort.
fn cp_csv_to_trino_to_csv_helper(
    test_name: &str,
    csv_path: &str,
    schema_scheme: &str,
    schema_path: &str,
) {
    let testdir = TestDir::new("dbcrossbar", test_name);
    let src = testdir.src_path(csv_path);
    let schema = testdir.src_path(schema_path);
    let s3_temp_dir = s3_test_dir_url(test_name);
    let trino_table = trino_test_table(test_name);

    // CSV to Trino.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", s3_temp_dir),
            &format!("--schema={}:{}", schema_scheme, schema.display()),
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
            &trino_table,
            "csv:out/out.csv",
        ])
        .tee_output()
        .expect_success();

    // Print our output for manual inspection. Use `--nocapture` to see this.
    let out_path = testdir.path("out/out.csv");
    eprintln!("output:\n{}", std::fs::read_to_string(out_path).unwrap());
}

#[test]
#[ignore]
fn cp_csv_to_trino_to_csv() {
    cp_csv_to_trino_to_csv_helper(
        "cp_csv_to_trino_to_csv",
        "fixtures/many_types.csv",
        "postgres-sql",
        "fixtures/many_types.sql",
    );
}

#[test]
#[ignore]
fn cp_csv_to_trino_to_csv_complex() {
    cp_csv_to_trino_to_csv_helper(
        "cp_csv_to_trino_to_csv_complex",
        "fixtures/trino/very_complex.csv",
        "trino-sql",
        "fixtures/trino/very_complex.sql",
    );
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
