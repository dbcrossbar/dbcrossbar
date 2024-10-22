//! Tests specific to the "csv:" driver. In new code, we should prefer the
//! "file:" driver instead, but we keep this around for backwards compatibility.
//! We keep the tests to make sure we don't break anything.

use cli_test_dir::*;
use std::fs;

/// An example CSV file with columns corresponding to `EXAMPLE_SQL`.
const EXAMPLE_CSV: &str = include_str!("../../../fixtures/example.csv");

#[test]
fn cp_csv_to_csvs() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_csv");
    let src = testdir.src_path("fixtures/example.csv");
    testdir
        .cmd()
        .arg("cp")
        .arg(format!("csv:{}", src.display()))
        .arg("csv:out/")
        .expect_success();
    let expected = fs::read_to_string(&src).unwrap();
    testdir.expect_file_contents("out/example.csv", expected);
}

#[test]
fn cp_csvs_to_csv() {
    let testdir = TestDir::new("dbcrossbar", "cp_csvs_to_csv");
    let schema = testdir.src_path("fixtures/concat.sql");
    let concat_in = testdir.src_path("fixtures/concat_in");
    let concat_out = testdir.src_path("fixtures/concat_out.csv");
    testdir
        .cmd()
        .arg("cp")
        .arg(format!("--schema=postgres-sql:{}", schema.display()))
        .arg(format!("csv:{}", concat_in.display()))
        .arg("csv:out.csv")
        .expect_success();
    let expected = fs::read_to_string(concat_out).unwrap();
    testdir.expect_file_contents("out.csv", expected);
}

#[test]
fn cp_csv_to_csv_piped() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_csv");
    let schema = testdir.src_path("fixtures/example.sql");
    let output = testdir
        .cmd()
        .args([
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            "csv:-",
            "csv:-",
        ])
        .output_with_stdin(EXAMPLE_CSV)
        .expect_success();
    assert_eq!(output.stdout_str(), EXAMPLE_CSV);
}
