//! Trino-related tests.

use std::fs;

use cli_test_dir::*;
use dbcrossbar_trino::ConnectorType;
use difference::assert_diff;

use crate::cp::trino_test_table;

use super::{assert_cp_to_exact_csv, s3_test_dir_url, TempType};

#[test]
#[ignore]
fn cp_from_trino_to_exact_csv() {
    for conn in ConnectorType::all_testable() {
        let table = trino_test_table(&conn, "cp_from_trino_to_exact_csv");
        assert_cp_to_exact_csv(
            "cp_from_trino_to_exact_csv",
            &table,
            TempType::S3.into(),
        );
    }
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
    for conn in ConnectorType::all_testable() {
        let testdir = TestDir::new("dbcrossbar", test_name);
        let src = testdir.src_path(csv_path);
        let schema = testdir.src_path(schema_path);
        let s3_temp_dir = s3_test_dir_url(test_name);
        let trino_table = trino_test_table(&conn, test_name);

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
        eprintln!("output:\n{}", fs::read_to_string(out_path).unwrap());
    }
}

#[test]
#[ignore]
fn cp_csv_to_trino_to_csv_many() {
    cp_csv_to_trino_to_csv_helper(
        "cp_csv_to_trino_to_csv_many",
        "fixtures/many_types.csv",
        "postgres-sql",
        "fixtures/many_types.sql",
    );
}

#[test]
#[ignore]
fn cp_csv_to_trino_to_csv_lambda_regression() {
    cp_csv_to_trino_to_csv_helper(
        "cp_csv_to_trino_to_csv_lambda_regression",
        "fixtures/trino/lambda_regression.csv",
        "trino-sql",
        "fixtures/trino/lambda_regression.sql",
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

#[test]
#[ignore]
fn cp_from_trino_with_where() {
    for conn in ConnectorType::all_testable() {
        let testdir = TestDir::new("dbcrossbar", "cp_from_trino_with_where");
        let src = testdir.src_path("fixtures/posts.csv");
        let filtered = testdir.src_path("fixtures/posts_where_author_id_1.csv");
        let schema = testdir.src_path("fixtures/posts.sql");
        let s3_temp_dir = s3_test_dir_url("cp_from_trino_with_where");
        let trino_table = trino_test_table(&conn, "cp_from_trino_with_where");

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

        // BigQuery back to CSV using --where.
        testdir
            .cmd()
            .args([
                "cp",
                &format!("--temporary={}", s3_temp_dir),
                &format!("--schema=postgres-sql:{}", schema.display()),
                "--where",
                "author_id = 1",
                &trino_table,
                "csv:out/out.csv",
            ])
            .tee_output()
            .expect_success();

        let expected = fs::read_to_string(filtered).unwrap();
        let actual = fs::read_to_string(testdir.path("out/out.csv")).unwrap();
        assert_diff!(&expected, &actual, ",", 0);
    }
}

// Create table using `schema conv` and dump the schema.
#[test]
#[ignore]
fn schema_conv_on_trino_table() {
    for conn in ConnectorType::all_testable() {
        let testdir = TestDir::new("dbcrossbar", "schema_conv_on_trino_table");
        let schema = testdir.src_path("fixtures/many_types.sql");
        let expected = testdir.src_path("fixtures/trino/many_types_expected.sql");
        let trino_table = trino_test_table(&conn, "schema_conv_on_trino_table");

        testdir
            .cmd()
            .args([
                "schema",
                "conv",
                "--if-exists=overwrite",
                &format!("postgres-sql:{}", schema.display()),
                &trino_table,
            ])
            .tee_output()
            .expect_success();

        testdir
            .cmd()
            .args(["schema", "conv", &trino_table, "postgres-sql:output.sql"])
            .tee_output()
            .expect_success();

        // "Memory" preserves most Trino types exactly, whereas other connectors
        // almost always change something.
        if conn == ConnectorType::Memory {
            let expected = std::fs::read_to_string(expected).unwrap();
            let output = std::fs::read_to_string(testdir.path("output.sql")).unwrap();
            assert_diff!(&expected, &output, "\n", 0);
        }
    }
}
