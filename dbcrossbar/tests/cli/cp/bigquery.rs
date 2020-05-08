//! BigQuery-specific tests.

use cli_test_dir::*;
use dbcrossbarlib::{schema::DataType, TemporaryStorage};
use difference::assert_diff;
use serde_json::json;
use std::{fs, io::Write, path::Path, process::Command};

use super::*;

#[test]
#[ignore]
fn cp_from_bigquery_to_exact_csv() {
    let pg_table = bq_test_table("cp_from_bigquery_to_exact_csv");
    assert_cp_to_exact_csv("cp_from_bigquery_to_exact_csv", &pg_table);
}

#[test]
#[ignore]
fn cp_from_bigquery_with_where() {
    let testdir = TestDir::new("dbcrossbar", "cp_from_bigquery_with_where");
    let src = testdir.src_path("fixtures/posts.csv");
    let filtered = testdir.src_path("fixtures/posts_where_author_id_1.csv");
    let schema = testdir.src_path("fixtures/posts.sql");
    let gs_temp_dir = gs_test_dir_url("cp_from_bigquery_with_where");
    let bq_temp_ds = bq_temp_dataset();
    let bq_table = bq_test_table("cp_from_bigquery_with_where");

    // CSV to BigQuery.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // BigQuery back to CSV using --where.
    testdir
        .cmd()
        .args(&[
            "cp",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=postgres-sql:{}", schema.display()),
            "--where",
            "author_id = 1",
            &bq_table,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&filtered).unwrap();
    let actual = fs::read_to_string(testdir.path("out/000000000000.csv")).unwrap();
    assert_diff!(&expected, &actual, ",", 0);
}

#[test]
#[ignore]
fn cp_csv_to_bigquery_to_csv() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_bigquery_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let bq_temp_ds = bq_temp_dataset();
    let gs_temp_dir = gs_test_dir_url("cp_csv_to_bigquery_to_csv");
    let bq_table = bq_test_table("cp_csv_to_bigquery_to_csv");

    // CSV to BigQuery.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
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
            &format!("--temporary={}", bq_temp_ds),
            &bq_table,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();
}

#[test]
#[ignore]
fn bigquery_record_columns() {
    let testdir = TestDir::new("dbcrossbar", "bigquery_record_columns");
    let bq_temp_ds = bq_temp_dataset();
    let gs_temp_dir = gs_test_dir_url("bigquery_record_columns_to_json");

    let dataset_name = bq_temp_dataset_name();
    let bare_dataset_name =
        &dataset_name[dataset_name.find(':').expect("no colon") + 1..];
    let table_name = format!("record_cols_{}", TemporaryStorage::random_tag());
    let locator = format!("bigquery:{}.{}", dataset_name, table_name);

    // Create a BigQuery table containing record columns.
    let sql = format!(
        "
create table {dataset_name}.{table_name} AS (
  select
    struct(1 as a) AS record,
    array(select struct(2 as b) union all select(struct(3 as b))) AS records
);",
        dataset_name = bare_dataset_name,
        table_name = table_name,
    );

    // Create a table with record columns.
    Command::new("bq")
        .args(&[
            "query",
            "--nouse_legacy_sql",
            "--project_id",
            &bq_project_id(),
        ])
        .arg(&sql)
        .expect_success();

    // Try exporting the schema.
    let output = testdir
        .cmd()
        .args(&["conv", &locator, "postgres-sql:out.sql"])
        .tee_output()
        .expect_success();
    output.stdout_str().contains(r#""record" jsonb"#);
    output.stdout_str().contains(r#""records" jsonb"#);

    // BigQuery to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &locator,
            "csv:out.csv",
        ])
        .expect_success();

    let expected = r#"record,records
"{""a"":1}","[{""b"":2},{""b"":3}]"
"#;
    testdir.expect_file_contents("out.csv", expected);
}

#[test]
#[ignore]
fn bigquery_upsert() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "bigquery_upsert");
    let srcs = &[
        testdir.src_path("fixtures/upsert_1.csv"),
        testdir.src_path("fixtures/upsert_2.csv"),
    ];
    let expected = testdir.src_path("fixtures/upsert_result.csv");
    let schema = testdir.src_path("fixtures/upsert.sql");
    let bq_temp_ds = bq_temp_dataset();
    let gs_temp_dir = gs_test_dir_url("bigquery_upsert");
    let bq_table = bq_test_table("bigquery_upsert");

    // CSVes to BigQuery.
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
            .args(&[
                "cp",
                if_exists,
                &format!("--temporary={}", gs_temp_dir),
                &format!("--temporary={}", bq_temp_ds),
                &format!("--schema=postgres-sql:{}", schema.display()),
                &format!("csv:{}", src.display()),
                &bq_table,
            ])
            .tee_output()
            .expect_success();
    }

    // BigQuery to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &bq_table,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();

    // We sort the lines of the CSVs because BigQuery outputs in any order.
    // This has the side effect of putting the headers at the end.
    let normalize_csv = |path: &Path| -> String {
        let text = fs::read_to_string(&path).unwrap();
        let mut lines = text.lines().collect::<Vec<_>>();
        lines.sort();
        lines.join("\n")
    };
    let expected = normalize_csv(&expected);
    let actual = normalize_csv(&testdir.path("out/000000000000.csv"));
    assert_diff!(&expected, &actual, ",", 0);
}

#[test]
#[ignore]
fn bigquery_honors_not_null_for_complex_inserts() {
    let _ = env_logger::try_init();
    let testdir =
        TestDir::new("dbcrossbar", "bigquery_honors_not_null_for_complex_inserts");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let bq_temp_ds = bq_temp_dataset();
    let gs_temp_dir = gs_test_dir_url("cp_csv_to_bigquery_to_csv");
    let bq_table = bq_test_table("cp_csv_to_bigquery_to_csv");

    // CSV to BigQuery.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // Extract the final schema.
    testdir
        .cmd()
        .args(&["conv", &bq_table, "bigquery-schema:output.json"])
        .expect_success();

    // Make sure it contains REQUIRED columns.
    testdir.expect_contains("output.json", "REQUIRED");
}

#[test]
#[ignore]
fn bigquery_roundtrips_structs() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "bigquery_roundtrips_structs");
    let raw_src_path = testdir.src_path("fixtures/structs/struct.json");
    let src = testdir.path("structs.csv");
    let raw_data_type_path =
        testdir.src_path("fixtures/structs/struct-data-type.json");
    let schema = testdir.path("structs-schema.json");
    let bq_temp_ds = bq_temp_dataset();
    let gs_temp_dir = gs_test_dir_url("bigquery_roundtrips_structs");
    let bq_table = bq_test_table("bigquery_roundtrips_structs");

    // Use our example JSON to create a CSV file with two columns: One
    // containing our struct, and the other containing a single-element array
    // containing our struct.
    let raw_src = fs::read_to_string(&raw_src_path).unwrap();
    let src_data = format!(
        r#"struct,structs
"{escaped}","[{escaped}]"
"#,
        escaped = raw_src.replace('\n', " ").replace('"', "\"\""),
    );
    let mut src_wtr = fs::File::create(&src).unwrap();
    write!(&mut src_wtr, "{}", &src_data).unwrap();
    src_wtr.flush().unwrap();
    drop(src_wtr);

    // Load our data type and use it to create our schema. This actually needs two columns.
    let schema_from_file = |path: &Path| -> serde_json::Value {
        let ty: DataType =
            serde_json::from_reader(fs::File::open(path).unwrap()).unwrap();
        json!({
            "name": "root-180513:test.bigquery_roundtrips_structs",
            "columns": [
                {
                    "name": "struct",
                    "is_nullable": true,
                    "data_type": ty
                },
                {
                    "name": "structs",
                    // TODO: Try with `is_nullable: false`.
                    "is_nullable": true,
                    "data_type": { "array": ty },
                },
            ]
        })
    };
    let schema_data = schema_from_file(&raw_data_type_path);
    let schema_wtr = fs::File::create(&schema).unwrap();
    serde_json::to_writer(schema_wtr, &schema_data).unwrap();

    // Load our data into BigQuery.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=dbcrossbar-schema:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &bq_table,
        ])
        .spawn()
        .expect_success();

    // Dump our data from BigQuery.
    let exported = testdir.path("structs.csv");
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=dbcrossbar-schema:{}", schema.display()),
            &bq_table,
            &format!("csv:{}", exported.display()),
        ])
        .spawn()
        .expect_success();

    // Compare our dumped data to what we expected, using JSON comparison to
    // ignore whitespace and ordering.
    let mut exported_rdr = ::csv::Reader::from_path(&exported).unwrap();
    let row = exported_rdr
        .records()
        .next()
        .expect("should have one row")
        .unwrap();
    let expected = serde_json::from_str::<serde_json::Value>(&raw_src).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(row.get(0).unwrap()).unwrap(),
        expected,
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(row.get(1).unwrap()).unwrap(),
        json!([expected]),
    );

    // Dump our actual schema from BigQuery.
    let exported_schema = testdir.path("structs-schema.json");
    testdir
        .cmd()
        .args(&[
            "conv",
            "--if-exists=overwrite",
            &bq_table,
            &format!("dbcrossbar-schema:{}", exported_schema.display()),
        ])
        .spawn()
        .expect_success();

    // Compare our schema data as JSON. This will contain less information than
    // the schema we originally loaded, because BigQuery can't represent all our
    // schemas perfectly.
    let expected_schema_data = schema_from_file(
        &testdir.src_path("fixtures/structs/struct-data-type-after-bq.json"),
    );
    let exported_schema: serde_json::Value =
        serde_json::from_reader(fs::File::open(&exported_schema).unwrap()).unwrap();
    assert_eq!(exported_schema, expected_schema_data);
}
