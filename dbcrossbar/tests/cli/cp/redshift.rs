//! RedShift-specific tests.

use cli_test_dir::*;
use difference::assert_diff;
use std::{fs, path::Path};

use super::*;

// We don't test Redshift for exact output, because it's missing support for
// some of our "exact" data types, including UUIDs. And it requires special
// --to-args and --from-args just to run.

#[test]
#[ignore]
fn cp_csv_to_redshift_to_csv() {
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
        .args([
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
        .args([
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

#[test]
#[ignore]
fn redshift_upsert() {
    let testdir = TestDir::new("dbcrossbar", "redshift_upsert");
    let srcs = &[
        testdir.src_path("fixtures/redshift_upsert/upsert_1.csv"),
        testdir.src_path("fixtures/redshift_upsert/upsert_2.csv"),
    ];
    let expected = testdir.src_path("fixtures/redshift_upsert/upsert_result.csv");
    let schema = testdir.src_path("fixtures/redshift_upsert/upsert.sql");
    let s3_dir = s3_test_dir_url("redshift_upsert");
    let redshift_table = match redshift_test_table_url("public.redshift_upsert") {
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

    // CSVes to Postgres.
    let mut first = true;
    for src in srcs {
        let if_exists = if first {
            first = false;
            "--if-exists=overwrite"
        } else {
            "--if-exists=upsert-on:key1,key2"
        };
        testdir
            .cmd()
            .args([
                "cp",
                if_exists,
                &format!("--temporary={}", s3_dir),
                &format!("--schema=postgres-sql:{}", schema.display()),
                concat!(
                    "--to-arg=partner=dbcrossbar test v",
                    env!("CARGO_PKG_VERSION")
                ),
                &format!("--to-arg=iam_role={}", iam_role),
                &format!("--to-arg=region={}", region),
                &format!("csv:{}", src.display()),
                &redshift_table,
            ])
            .tee_output()
            .expect_success();
    }

    // Postgres to CSV.
    testdir
        .cmd()
        .args([
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", s3_dir),
            &format!("--from-arg=iam_role={}", iam_role),
            &format!("--from-arg=region={}", region),
            concat!(
                "--from-arg=partner=dbcrossbar test v",
                env!("CARGO_PKG_VERSION")
            ),
            &redshift_table,
            "csv:out.csv",
        ])
        .tee_output()
        .expect_success();

    // We sort the lines of the CSVs because BigQuery outputs in any order, and
    // we don't want to depend on Redshift doing things any more predictably.
    // This has the side effect of putting the headers at the end.
    let normalize_csv = |path: &Path| -> String {
        let text = fs::read_to_string(path).unwrap();
        let mut lines = text.lines().collect::<Vec<_>>();
        lines.sort_unstable();
        lines.join("\n")
    };
    let expected = normalize_csv(&expected);
    let actual = normalize_csv(&testdir.path("out.csv"));
    assert_diff!(&expected, &actual, ",", 0);
}
