use cli_test_dir::*;

/// Sample input SQL. We test against this, and not against a running copy of
/// PostgreSQL, because it keeps the test environment much simpler. But this
/// means we don't fully test certain modes of the CLI (though we have unit
/// tests for much of the related code).
const INPUT_SQL: &str =
    include_str!("../../schemaconvlib/src/parsers/postgres_example.sql");

#[test]
fn help_flag() {
    let testdir = TestDir::new("schemaconv", "help_flag");
    let output = testdir.cmd().arg("--help").expect_success();
    assert!(output.stdout_str().contains("schemaconv"));
}

#[test]
fn version_flag() {
    let testdir = TestDir::new("schemaconv", "version_flag");
    let output = testdir.cmd().arg("--version").expect_success();
    assert!(output.stdout_str().contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn pg_to_json() {
    let testdir = TestDir::new("schemaconv", "pg_to_json");
    let output = testdir
        .cmd()
        .args(&["-I", "pg", "-O", "json"])
        .output_with_stdin(INPUT_SQL)
        .expect_success();
    assert!(output.stdout_str().contains("example"));
    assert!(output.stdout_str().contains("\"a\""));
}

#[test]
fn pg_to_pg_export() {
    let testdir = TestDir::new("schemaconv", "pg_to_pg_export");
    let output = testdir
        .cmd()
        .args(&["-I", "pg", "-O", "pg:export"])
        .output_with_stdin(INPUT_SQL)
        .expect_success();
    assert!(output.stdout_str().contains("COPY (SELECT"));
    assert!(output.stdout_str().contains("TO STDOUT WITH CSV HEADER"));
}

#[test]
fn pg_to_pg_export_columns() {
    let testdir = TestDir::new("schemaconv", "pg_to_pg_export_columns");
    testdir
        .cmd()
        .args(&["-I", "pg", "-O", "pg:export:columns"])
        .output_with_stdin(INPUT_SQL)
        .expect_success();
}

#[test]
fn pg_to_bq_schema_temp() {
    let testdir = TestDir::new("schemaconv", "pg_to_bq_schema_temp");
    let output = testdir
        .cmd()
        .args(&["-I", "pg", "-O", "bq:schema:temp"])
        .output_with_stdin(INPUT_SQL)
        .expect_success();
    assert!(output.stdout_str().contains("INT64"));
    assert!(output.stdout_str().contains("GEOGRAPHY"));

    // Arrays can't be directly read from a CSV file, and so they should never
    // appear in the temp schema.
    assert!(!output.stdout_str().contains("ARRAY"));
}

#[test]
fn pg_to_bq_schema() {
    let testdir = TestDir::new("schemaconv", "pg_to_bq_schema");
    let output = testdir
        .cmd()
        .args(&["-I", "pg", "-O", "bq:schema"])
        .output_with_stdin(INPUT_SQL)
        .expect_success();

    // These types should appear in the final schema, after we're run SQL on
    // BigQuery SQL to transform the data we've loaded into its final form.
    assert!(output.stdout_str().contains("ARRAY<STRING>"));
    assert!(output.stdout_str().contains("ARRAY<INT64>"));
}

#[test]
fn pg_to_bq_import() {
    let testdir = TestDir::new("schemaconv", "pg_to_bq_import");
    let output = testdir
        .cmd()
        .args(&["-I", "pg", "-O", "bq:import"])
        .output_with_stdin(INPUT_SQL)
        .expect_success();

    // We should see generated conversion functions in the output.
    assert!(output
        .stdout_str()
        .contains("CREATE TEMP FUNCTION ImportJson"));
}
