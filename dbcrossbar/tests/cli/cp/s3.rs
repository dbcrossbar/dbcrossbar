//! S3-specific tests.

use cli_test_dir::*;
use difference::assert_diff;
use std::fs;

use super::*;

#[test]
#[ignore]
fn cp_csv_to_s3_to_csv() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_s3_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let s3_dir = s3_test_dir_url("cp_csv_to_s3_to_csv");

    // CSV to S3.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &s3_dir,
        ])
        .tee_output()
        .expect_success();

    // S3 to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &s3_dir,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&src).unwrap();
    let actual = fs::read_to_string(testdir.path("out/many_types.csv")).unwrap();
    assert_diff!(&expected, &actual, ",", 0);
}
