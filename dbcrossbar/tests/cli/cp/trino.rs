//! Trino-related tests.

use cli_test_dir::*;
use difference::assert_diff;

use crate::cp::trino_test_url;

use super::{assert_cp_to_exact_csv, TempType};

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
