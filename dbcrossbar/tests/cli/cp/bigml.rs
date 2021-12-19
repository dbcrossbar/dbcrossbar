//! BigML-specific tests.

use cli_test_dir::*;
use difference::assert_diff;
use std::fs;

use super::*;

#[test]
#[ignore]
fn cp_from_bigml_to_exact_csv() {
    assert_cp_to_exact_csv("cp_from_bigml_to_exact_csv", "bigml:dataset");
}

#[test]
#[ignore]
fn cp_csv_to_bigml_dataset_to_csv() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_bigml_dataset_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let s3_dir = s3_test_dir_url("cp_csv_to_bigml_dataset_to_csv");

    // CSV to BigML.
    let output = testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", s3_dir),
            &format!("--schema=postgres-sql:{}", schema.display()),
            "--to-arg=name=dbcrossbar test",
            "--to-arg=optype_for_text=categorical",
            "--to-arg=tags[]=dbcrossbar-test",
            "--to-arg=tags[]=dbcrossbar-temporary",
            &format!("csv:{}", src.display()),
            "bigml:dataset",
        ])
        .tee_output()
        .expect_success();
    let dataset_locator = output
        .stdout_str()
        .trim_matches(|c: char| c.is_ascii_whitespace());

    // BigML to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            dataset_locator,
            // Output as a single file to avoid weird naming conventions.
            "csv:out.csv",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&src)
        .unwrap()
        .replace(",1e+37,", ",1.0E37,");
    let actual = fs::read_to_string(testdir.path("out.csv")).unwrap();
    assert_diff!(&expected, &actual, ",", 0);

    // Verify SQL schema output contains correct column names, too.
    let output = testdir
        .cmd()
        .args(&["schema", "conv", dataset_locator, "postgres-sql:-"])
        .expect_success();
    assert!(output.stdout_str().contains("CREATE TABLE"));
    assert!(output.stdout_str().contains("test_null"));
}
