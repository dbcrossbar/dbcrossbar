//! BigQuery-specific tests.

use cli_test_dir::*;
use dbcrossbarlib::TemporaryStorage;
use difference::assert_diff;
use std::{fs, path::Path, process::Command};

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
    let mut first = false;
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
