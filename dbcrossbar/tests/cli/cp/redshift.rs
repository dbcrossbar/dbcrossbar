//! RedShift-specific tests.

use cli_test_dir::*;
use difference::assert_diff;
use std::fs;

use super::*;

// We don't test Redshift for exact output, because it's missing support for
// some of our "exact" data types, including UUIDs. And it requires special
// --to-args and --from-args just to run.

#[test]
#[ignore]
fn cp_csv_to_redshift_to_csv() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_redshift_to_csv");
    let src = testdir.src_path("fixtures/redshift_types.csv");
    let schema = testdir.src_path("fixtures/redshift_types.sql");
    let s3_dir = s3_test_dir_url("cp_csv_to_redshift_to_csv");
    let redshift_table =
        match redshift_test_table_url("public.cp_csv_to_redshift_to_csv") {
            Some(redshift_table) => redshift_table,
            None => {
                // We allow this test to be disabled by default even when --ignored
                // is passed, because Redshift is hard to set up, and it costs a
                // minimum of ~$180/month to run.
                eprintln!("SKIPPING REDSHIFT TEST - PLEASE SET `REDSHIFT_TEST_URL`!");
                return;
            }
        };
    let iam_role =
        env::var("REDSHIFT_TEST_IAM_ROLE").expect("Please set REDSHIFT_TEST_IAM_ROLE");
    let region =
        env::var("REDSHIFT_TEST_REGION").expect("Please set REDSHIFT_TEST_REGION");

    // CSV to Redshift.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", s3_dir),
            &format!("--schema=postgres-sql:{}", schema.display()),
            // --to-arg values will be converted into Redshift "credentials"
            // arguments to COPY and UNLOAD, directly.
            &format!("--to-arg=iam_role={}", iam_role),
            &format!("--to-arg=region={}", region),
            &format!("csv:{}", src.display()),
            &redshift_table,
        ])
        .tee_output()
        .expect_success();

    // Redshift to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", s3_dir),
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("--from-arg=iam_role={}", iam_role),
            &format!("--from-arg=region={}", region),
            &redshift_table,
            // Output as a single file to avoid weird naming conventions.
            "csv:out.csv",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&src).unwrap();
    let actual = fs::read_to_string(testdir.path("out.csv")).unwrap();
    assert_diff!(&expected, &actual, ",", 0);
}
