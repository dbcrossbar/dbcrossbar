//! Tests for the `conv` subcommand.

use cli_test_dir::*;
use difference::assert_diff;
use std::fs;

/// An example Postgres SQL `CREATE TABLE` declaration.
const EXAMPLE_SQL: &str = include_str!("../../fixtures/example.sql");

/// Sample input SQL. We test against this, and not against a running copy of
/// PostgreSQL, because it keeps the test environment much simpler. But this
/// means we don't fully test certain modes of the CLI (though we have unit
/// tests for much of the related code).
const INPUT_SQL: &str =
    include_str!("../../src/drivers/postgres_shared/schema/schema_sql_example.sql");

#[test]
fn conv_help_flag() {
    let testdir = TestDir::new("dbcrossbar", "conv_help_flag");
    let output = testdir
        .cmd()
        .args(["schema", "conv", "--help"])
        .expect_success();
    assert!(output.stdout_str().contains("EXAMPLE LOCATORS:"));
}

#[test]
fn conv_pg_sql_to_pg_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_pg_sql_to_pg_sql");
    let output = testdir
        .cmd()
        .args(["schema", "conv", "postgres-sql:-", "postgres-sql:-"])
        .output_with_stdin(EXAMPLE_SQL)
        .expect_success();
    assert!(output.stdout_str().contains("CREATE TABLE"));
}

#[test]
fn conv_pg_sql_to_dbcrossbar_schema_to_pg_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_pg_sql_to_pg_sql");
    let output1 = testdir
        .cmd()
        .args(["schema", "conv", "postgres-sql:-", "dbcrossbar-schema:-"])
        .output_with_stdin(EXAMPLE_SQL)
        .expect_success();
    let output2 = testdir
        .cmd()
        .args(["schema", "conv", "dbcrossbar-schema:-", "postgres-sql:-"])
        .output_with_stdin(output1.stdout_str())
        .expect_success();
    assert!(output2.stdout_str().contains("CREATE TABLE"));

    // And make sure it round-trips.
    let output3 = testdir
        .cmd()
        .args(["schema", "conv", "postgres-sql:-", "dbcrossbar-schema:-"])
        .output_with_stdin(output2.stdout_str())
        .expect_success();
    assert_eq!(output3.stdout_str(), output1.stdout_str());
}

#[test]
fn conv_csv_to_pg_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_csv_to_pg_sql");
    let src = testdir.src_path("fixtures/example.csv");
    let output = testdir
        .cmd()
        .args([
            "schema",
            "conv",
            &format!("csv:{}", src.display()),
            "postgres-sql:-",
        ])
        .output()
        .expect_success();
    assert!(output.stdout_str().contains("CREATE TABLE"));
    assert!(output.stdout_str().contains("id"));
    assert!(output.stdout_str().contains("first_name"));
    assert!(output.stdout_str().contains("last_name"));
}

#[test]
fn conv_file_csv_to_pg_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_file_csv_to_pg_sql");
    let src = testdir.src_path("fixtures/example.csv");
    let output = testdir
        .cmd()
        .args([
            "schema",
            "conv",
            &format!("file:{}", src.display()),
            "postgres-sql:-",
        ])
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
        .args(["schema", "conv", "postgres-sql:-", "bigquery-schema:-"])
        .output_with_stdin(INPUT_SQL)
        .expect_success();
    assert!(output.stdout_str().contains("GEOGRAPHY"));
    assert!(output.stdout_str().contains("REPEATED"));
}

#[test]
fn conv_bq_schema_to_pg_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_bq_schema_to_pg_sql");
    let input_json = testdir.src_path("fixtures/bigquery_schema.json");
    let expected_sql = testdir.src_path("fixtures/bigquery_schema_converted.sql");
    testdir
        .cmd()
        .args([
            "schema",
            "conv",
            &format!("bigquery-schema:{}", input_json.display()),
            "postgres-sql:output.sql",
        ])
        .expect_success();
    let expected = fs::read_to_string(expected_sql).unwrap();
    testdir.expect_file_contents("output.sql", expected);
}

#[test]
fn conv_ts_to_portable() {
    let testdir = TestDir::new("dbcrossbar", "conv_ts_to_portable");
    let input_ts = testdir.src_path("fixtures/dbcrossbar_ts/shapes.ts");
    let output_json = testdir.path("output.json");
    let expected_json = testdir.src_path("fixtures/dbcrossbar_ts/shapes.json");
    testdir
        .cmd()
        .args([
            "--enable-unstable",
            "schema",
            "conv",
            &format!("dbcrossbar-ts:{}#Shape", input_ts.display()),
            &format!("dbcrossbar-schema:{}", output_json.display()),
        ])
        .expect_success();
    let output = fs::read_to_string(&output_json).unwrap();
    let expected = fs::read_to_string(expected_json).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&output).unwrap(),
        serde_json::from_str::<serde_json::Value>(&expected).unwrap(),
    );
}

#[test]
fn conv_old_dbcrossbar_schema_to_new() {
    let testdir = TestDir::new("dbcrossbar", "conv_old_dbcrossbar_schema_to_new");

    static INPUT: &str = r#"
{
    "name": "images",
    "columns": [
        {
            "name": "id",
            "is_nullable": false,
            "data_type": "uuid"
        }
    ]
}
"#;

    static EXPECTED: &str = r#"
{
    "named_data_types": [],
    "tables": [{
        "name": "images",
        "columns": [
            {
                "name": "id",
                "is_nullable": false,
                "data_type": "uuid"
            }
        ]
    }]
}
"#;

    let output = testdir
        .cmd()
        .args([
            "schema",
            "conv",
            "dbcrossbar-schema:-",
            "dbcrossbar-schema:-",
        ])
        .output_with_stdin(INPUT)
        .expect_success();

    assert_eq!(
        serde_json::from_str::<serde_json::Value>(output.stdout_str()).unwrap(),
        serde_json::from_str::<serde_json::Value>(EXPECTED).unwrap(),
    );
}

#[test]
fn conv_bigquery_schema_to_trino_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_bigquery_schema_to_trino_sql");
    let input_json = testdir.src_path("fixtures/bigquery_schema.json");
    let expected_sql = testdir.src_path("fixtures/trino/from_bigquery.sql");
    testdir
        .cmd()
        .args([
            "schema",
            "conv",
            &format!("bigquery-schema:{}", input_json.display()),
            "trino-sql:output.sql",
        ])
        .expect_success();
    let expected = fs::read_to_string(expected_sql).unwrap();
    let output = fs::read_to_string(testdir.path("output.sql")).unwrap();
    assert_diff!(&expected, &output, "\n", 0);
}

#[test]
fn conv_postgres_schema_to_trino_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_postgres_schema_to_trino_sql");
    let input_sql = INPUT_SQL;
    let expected_sql = testdir.src_path("fixtures/trino/from_postgres.sql");
    testdir
        .cmd()
        .args(["schema", "conv", "postgres-sql:-", "trino-sql:output.sql"])
        .output_with_stdin(input_sql)
        .expect_success();
    let expected = fs::read_to_string(expected_sql).unwrap();
    let output = fs::read_to_string(testdir.path("output.sql")).unwrap();
    assert_diff!(&expected, &output, "\n", 0);
}

#[test]
fn conv_dbcrossbar_schema_to_trino_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_dbcrossbar_schema_to_trino_sql");
    let input_json = testdir.src_path("fixtures/dbcrossbar_schema.json");
    let expected_sql = testdir.src_path("fixtures/trino/from_dbcrossbar.sql");
    testdir
        .cmd()
        .args([
            "schema",
            "conv",
            &format!("dbcrossbar-schema:{}", input_json.display()),
            "trino-sql:output.sql",
        ])
        .expect_success();
    let expected = fs::read_to_string(expected_sql).unwrap();
    let output = fs::read_to_string(testdir.path("output.sql")).unwrap();
    assert_diff!(&expected, &output, "\n", 0);
}

#[test]
fn conv_trino_sql_to_dbcrossbar_schema() {
    let testdir = TestDir::new("dbcrossbar", "conv_trino_sql_to_dbcrossbar_schema");
    let input_sql = testdir.src_path("fixtures/trino/schema.sql");
    let output_json = testdir.path("output.json");
    let expected_json =
        testdir.src_path("fixtures/trino/dbcrossbar_schema_expected.json");
    testdir
        .cmd()
        .args([
            "schema",
            "conv",
            &format!("trino-sql:{}", input_sql.display()),
            &format!("dbcrossbar-schema:{}", output_json.display()),
        ])
        .expect_success();
    let output = fs::read_to_string(&output_json).unwrap();
    let expected = fs::read_to_string(expected_json).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&output).unwrap(),
        serde_json::from_str::<serde_json::Value>(&expected).unwrap(),
    );
}
