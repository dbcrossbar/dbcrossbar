use cli_test_dir::*;

/// Sample input SQL. We test against this, and not against a running copy of
/// PostgreSQL, because it keeps the test environment much simpler. But this
/// means we don't fully test certain modes of the CLI (though we have unit
/// tests for much of the related code).
const INPUT_SQL: &str =
    include_str!("../../dbcrossbarlib/src/drivers/postgres/postgres_example.sql");

#[test]
fn help_flag() {
    let testdir = TestDir::new("dbcrossbar", "help_flag");
    let output = testdir.cmd().arg("--help").expect_success();
    assert!(output.stdout_str().contains("dbcrossbar"));
}

#[test]
fn version_flag() {
    let testdir = TestDir::new("dbcrossbar", "version_flag");
    let output = testdir.cmd().arg("--version").expect_success();
    assert!(output.stdout_str().contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn conv_help_flag() {
    let testdir = TestDir::new("dbcrossbar", "conv_help_flag");
    let output = testdir.cmd().args(&["conv", "--help"]).expect_success();
    assert!(output.stdout_str().contains("EXAMPLE LOCATORS:"));
}

#[test]
fn conv_pg_sql_to_bq_json() {
    let testdir = TestDir::new("dbcrossbar", "conv_pg_sql_to_bq_json");
    let output = testdir
        .cmd()
        .args(&["conv", "postgres.sql:-", "bigquery.json:-"])
        .output_with_stdin(INPUT_SQL)
        .expect_success();
    assert!(output.stdout_str().contains("INT64"));
    assert!(output.stdout_str().contains("GEOGRAPHY"));
    assert!(output.stdout_str().contains("INT64"));
    assert!(output.stdout_str().contains("GEOGRAPHY"));
}

#[test]
fn cp_help_flag() {
    let testdir = TestDir::new("dbcrossbar", "cp_help_flag");
    let output = testdir.cmd().args(&["cp", "--help"]).expect_success();
    assert!(output.stdout_str().contains("EXAMPLE LOCATORS:"));
}
