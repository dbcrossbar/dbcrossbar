//! Tests specific to the CSV driver.
//!
//! Note that lots of other tests use the CSV or file driver to set up inputs
//! and extract outputs, but we keep all the official file-only tests here.

use cli_test_dir::*;
use std::fs;

/// An example CSV file with columns corresponding to `EXAMPLE_SQL`.
const EXAMPLE_CSV: &str = include_str!("../../../fixtures/example.csv");

#[test]
fn cp_csv_to_csvs() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_csv");
    let schema = testdir.src_path("fixtures/concat.sql");
    let src = testdir.src_path("fixtures/example.csv");
    testdir
        .cmd()
        .arg("cp")
        .arg(&format!("--schema=postgres-sql:{}", schema.display()))
        .arg(&format!("file:{}", src.display()))
        .arg("file:out/")
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
        .arg(&format!("--schema=postgres-sql:{}", schema.display()))
        .arg(&format!("file:{}", concat_in.display()))
        .arg("file:out.csv")
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
            "file:-",
            "file:-",
        ])
        .output_with_stdin(EXAMPLE_CSV)
        .expect_success();
    assert_eq!(output.stdout_str(), EXAMPLE_CSV);
}

#[test]
fn cp_jsonl_to_csv() {
    let testdir = TestDir::new("dbcrossbar", "cp_jsonl_to_csv");
    let input = testdir.src_path("fixtures/json/input.jsonl");
    let schema = testdir.src_path("fixtures/exact_output.sql");
    let output = testdir
        .cmd()
        .args([
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("file:{}", input.display()),
            "file:-",
        ])
        .tee_output()
        .expect_success();
    let expected_path = testdir.src_path("fixtures/exact_output.csv");
    let expected = fs::read_to_string(expected_path).unwrap();
    assert_eq!(output.stdout_str(), expected);
}

#[test]
fn cp_jsonl_to_csv_piped() {
    let testdir = TestDir::new("dbcrossbar", "cp_jsonl_to_csv");
    let input_path = testdir.src_path("fixtures/json/input.jsonl");
    let input = fs::read_to_string(input_path).unwrap();
    let schema = testdir.src_path("fixtures/exact_output.sql");
    let output = testdir
        .cmd()
        .args([
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            "--from-format=jsonl",
            "file:-",
            "file:-",
        ])
        .output_with_stdin(input)
        .expect_success();
    let expected_path = testdir.src_path("fixtures/exact_output.csv");
    let expected = fs::read_to_string(expected_path).unwrap();
    assert_eq!(output.stdout_str(), expected);
}

#[test]
fn cp_csv_jsonl_fails_for_now() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_jsonl_fails_for_now");
    let input = testdir.src_path("fixtures/example.csv");
    let schema = testdir.src_path("fixtures/example.sql");
    testdir
        .cmd()
        .args([
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("file:{}", input.display()),
            "file:out.jsonl",
        ])
        .tee_output()
        .expect_failure();
}

#[test]
fn cp_csv_jsonl_dir_fails_for_now() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_jsonl_fails_for_now");
    let input = testdir.src_path("fixtures/example.csv");
    let schema = testdir.src_path("fixtures/example.sql");
    testdir
        .cmd()
        .args([
            "cp",
            "--to-format=jsonl",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("file:{}", input.display()),
            "file:out/",
        ])
        .expect_failure();
}
#[test]
fn cp_csv_jsonl_piped_fails_for_now() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_jsonl_fails_for_now");
    let input = testdir.src_path("fixtures/example.csv");
    let schema = testdir.src_path("fixtures/example.sql");
    testdir
        .cmd()
        .args([
            "cp",
            "--to-format=jsonl",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("file:{}", input.display()),
            "file:-",
        ])
        .expect_failure();
}
