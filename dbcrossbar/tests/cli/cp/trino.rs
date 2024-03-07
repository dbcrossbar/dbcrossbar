//! Tests for our Trino driver.

use std::process::Command;

use cli_test_dir::*;

use crate::cp::random_tag;

/// Where should we put our test data?
fn trino_temp_schema_name() -> &'static str {
    "memory.default"
}

/// Construct a Trino locator.
fn trino_url(catalog_and_schema: &str) -> String {
    format!(
        "trino://anyone@localhost:8080/{}",
        catalog_and_schema.replace('.', "/"),
    )
}

#[test]
#[ignore]
fn trino_schema() {
    let testdir = TestDir::new("dbcrossbar", "trino_schema");

    let catalog_and_schema = trino_temp_schema_name();
    let table_name = format!(
        "trino_table_for_schema_{}",
        random_tag().to_ascii_lowercase()
    );
    let url = trino_url(catalog_and_schema);
    let locator = format!("{}/{}", url, table_name);

    // Create a BigQuery table containing record columns.
    let sql = format!(
        "
create table {catalog_and_schema}.{table_name} (
   
    b BOOLEAN,

    i16 SMALLINT,
    i32 INTEGER,
    i64 BIGINT,
    
    f32 REAL,
    f64 DOUBLE,
    
    dc DECIMAL(38, 9),
    
    s VARCHAR,
    --vb VARBINARY,
    j JSON,
    
    d DATE,
    t TIMESTAMP,
    t_tz TIMESTAMP WITH TIME ZONE,
    
    arr ARRAY(VARCHAR),
    r ROW(a BOOLEAN, b INTEGER),
    u UUID,
    
    geom SphericalGeography
)",
        catalog_and_schema = catalog_and_schema,
        table_name = table_name,
    );

    // Create a table with our data.
    Command::new("docker")
        .args(["exec", "trino-joinery", "trino"])
        .args(["--execute", &sql, &url])
        .expect_success();

    // Try exporting the schema.
    testdir
        .cmd()
        .args(["schema", "conv", &locator, "postgres-sql:out.sql"])
        .tee_output()
        .expect_success();
    testdir.expect_contains("out.sql", "arr");
    testdir.expect_contains("out.sql", "text[]");
}
