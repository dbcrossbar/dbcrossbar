use cli_test_dir::*;
use difference::assert_diff;
use env_logger;
use std::{env, fs, process::Stdio};

/// An example Postgres SQL `CREATE TABLE` declaration.
const EXAMPLE_SQL: &str = include_str!("../fixtures/example.sql");

// /// An example CSV file with columns corresponding to `EXAMPLE_SQL`.
// const EXAMPLE_CSV: &str = include_str!("../fixtures/example.csv");

/// Sample input SQL. We test against this, and not against a running copy of
/// PostgreSQL, because it keeps the test environment much simpler. But this
/// means we don't fully test certain modes of the CLI (though we have unit
/// tests for much of the related code).
const INPUT_SQL: &str = include_str!(
    "../../dbcrossbarlib/src/drivers/postgres_shared/create_table_sql_example.sql"
);

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

/// A BigQuery table to use for a test.
fn bq_test_table(table_name: &str) -> String {
    let dataset = env::var("BQ_TEST_DATASET").expect("BQ_TEST_DATASET must be set");
    format!("bigquery:{}.{}", dataset, table_name)
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
fn conv_bq_schema_to_pg_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_bq_schema_to_pg_sql");
    let input_json = testdir.src_path("fixtures/bigquery_schema.json");
    let expected_sql = testdir.src_path("fixtures/bigquery_schema_converted.sql");
    testdir
        .cmd()
        .args(&[
            "conv",
            &format!("bigquery-schema:{}", input_json.display()),
            "postgres-sql:output.sql",
        ])
        .expect_success();
    let expected = fs::read_to_string(&expected_sql).unwrap();
    testdir.expect_file_contents("output.sql", &expected);
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
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_postgres_to_gs_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let expected_schema = testdir.src_path("fixtures/many_types_expected.sql");
    let pg_table = post_test_table_url("cp_csv_to_postgres_to_gs_to_csv");
    let gs_dir = gs_test_dir_url("cp_csv_to_postgres_to_gs_to_csv");
    let bq_table = bq_test_table("cp_csv_to_postgres_to_gs_to_csv");
    let gs_dir_2 = gs_test_dir_url("cp_csv_to_postgres_to_gs_to_csv_2");
    let pg_table_2 = post_test_table_url("cp_csv_to_postgres_to_gs_to_csv_2");

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

    // (Check PostgreSQL schema extraction now, so we know that we aren't
    // messing up later tests.)
    testdir
        .cmd()
        .args(&["conv", &pg_table, "postgres-sql:pg.sql"])
        .stdout(Stdio::piped())
        .tee_output()
        .expect_success();
    let postgres_sql = fs::read_to_string(&expected_schema)
        .unwrap()
        .replace("many_types", "cp_csv_to_postgres_to_gs_to_csv");
    testdir.expect_file_contents("pg.sql", &postgres_sql);

    // Postgres to gs://.
    testdir
        .cmd()
        .args(&["cp", "--if-exists=overwrite", &pg_table, &gs_dir])
        .tee_output()
        .expect_success();

    // gs:// to BigQuery.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &gs_dir,
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // BigQuery to gs://.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &bq_table,
            &gs_dir_2,
        ])
        .tee_output()
        .expect_success();

    // gs:// back to PostgreSQL. (Mostly because we'll need a PostgreSQL-generated
    // CSV file for the final comparison below.)
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &gs_dir_2,
            &pg_table_2,
        ])
        .tee_output()
        .expect_success();

    // PostgreSQL back to CSV for the final comparison below.
    testdir
        .cmd()
        .args(&[
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &pg_table_2,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&src).unwrap();
    let actual =
        fs::read_to_string(testdir.path("out/cp_csv_to_postgres_to_gs_to_csv_2.csv"))
            .unwrap();
    assert_diff(&expected, &actual, ",", 0);
}

#[test]
#[ignore]
fn cp_csv_to_postgres_append() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_postgres_append");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let pg_table = post_test_table_url("cp_csv_to_postgres_append");

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

    // CSV to Postgres, again, but appending.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=append",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &pg_table,
        ])
        .tee_output()
        .expect_success();
}

#[test]
#[ignore]
fn cp_csv_to_bigquery_to_csv() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_bigquery_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let gs_temp_dir = gs_test_dir_url("cp_csv_to_bigquery_to_csv");
    let bq_table = bq_test_table("cp_csv_to_bigquery_to_csv");

    // CSV to BigQuery.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // BigQuery to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &bq_table,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();
}
