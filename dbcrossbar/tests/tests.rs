use cli_test_dir::*;
use env_logger;
use std::{env, fs};

/// An example Postgres SQL `CREATE TABLE` declaration.
const EXAMPLE_SQL: &str = include_str!("../fixtures/example.sql");

// /// An example CSV file with columns corresponding to `EXAMPLE_SQL`.
// const EXAMPLE_CSV: &str = include_str!("../fixtures/example.csv");

/// Sample input SQL. We test against this, and not against a running copy of
/// PostgreSQL, because it keeps the test environment much simpler. But this
/// means we don't fully test certain modes of the CLI (though we have unit
/// tests for much of the related code).
const INPUT_SQL: &str =
    include_str!("../../dbcrossbarlib/src/drivers/postgres/postgres_example.sql");

/// The URL of our test database.
fn postgres_test_url() -> String {
    env::var("POSTGRES_TEST_URL").unwrap_or_else(|_| {
        "postgres://postgres:@localhost:5432/dbcrossbar_test".to_owned()
    })
}

/// The URL of a table in our test database.
fn post_test_table_url(table_name: &str) -> String {
    format!("{}#{}", postgres_test_url(), table_name)
}

/// The URL to our test `gs://` bucket and directory.
fn gs_url() -> String {
    env::var("GS_TEST_URL").expect("GS_TEST_URL must be set")
}

/// The URL to a subdirectory of `gs_url`.
fn gs_test_dir_url(dir_name: &str) -> String {
    let mut url = gs_url();
    if !url.ends_with('/') {
        url.push_str("/");
    }
    url.push_str(dir_name);
    url.push_str("/");
    url
}

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
fn conv_pg_sql_to_pg_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_pg_sql_to_pg_sql");
    let output = testdir
        .cmd()
        .args(&["conv", "postgres-sql:-", "postgres-sql:-"])
        .output_with_stdin(EXAMPLE_SQL)
        .expect_success();
    assert!(output.stdout_str().contains("CREATE TABLE"));
}

#[test]
fn conv_csv_to_pg_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_csv_to_pg_sql");
    let src = testdir.src_path("fixtures/example.csv");
    let output = testdir
        .cmd()
        .args(&["conv", &format!("csv:{}", src.display()), "postgres-sql:-"])
        .output()
        .expect_success();
    assert!(output.stdout_str().contains("CREATE TABLE"));
    assert!(output.stdout_str().contains("id"));
    assert!(output.stdout_str().contains("first_name"));
    assert!(output.stdout_str().contains("last_name"));
}

#[test]
fn conv_pg_sql_to_bq_schema() {
    let testdir = TestDir::new("dbcrossbar", "conv_pg_sql_to_bq_schema");
    let output = testdir
        .cmd()
        .args(&["conv", "postgres-sql:-", "bigquery-schema:-"])
        .output_with_stdin(INPUT_SQL)
        .expect_success();
    assert!(output.stdout_str().contains("GEOGRAPHY"));
    assert!(output.stdout_str().contains("ARRAY<INT64>"));
}

#[test]
fn cp_help_flag() {
    let testdir = TestDir::new("dbcrossbar", "cp_help_flag");
    let output = testdir.cmd().args(&["cp", "--help"]).expect_success();
    assert!(output.stdout_str().contains("EXAMPLE LOCATORS:"));
}

#[test]
fn cp_csv_to_csv() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_csv");
    let src = testdir.src_path("fixtures/example.csv");
    testdir
        .cmd()
        .arg("cp")
        .arg(&format!("csv:{}", src.display()))
        .arg("csv:out/")
        .expect_success();
    let expected = fs::read_to_string(&src).unwrap();
    testdir.expect_file_contents("out/example.csv", &expected);
}

#[test]
#[ignore]
fn cp_csv_to_postgres_to_gs_to_csv() {
    env_logger::init();
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_postgres_to_gs_to_csv");
    let src = testdir.src_path("fixtures/example.csv");
    let schema = testdir.src_path("fixtures/example.sql");
    let pg_table = post_test_table_url("cp_csv_to_postgres_to_gs_to_csv");
    let gs_dir = gs_test_dir_url("cp_csv_to_postgres_to_gs_to_csv");

    // CSV to Postgres.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &pg_table,
        ])
        .tee_output()
        .expect_success();

    // Postgres to gs://.
    testdir
        .cmd()
        .args(&["cp", "--if-exists=overwrite", &pg_table, &gs_dir])
        .tee_output()
        .expect_success();

    // gs:// to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &gs_dir,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&src).unwrap();
    testdir.expect_file_contents("out/cp_csv_to_postgres_to_gs_to_csv.csv", &expected);
}
