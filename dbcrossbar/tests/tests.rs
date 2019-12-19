use cli_test_dir::*;
use dbcrossbarlib::TemporaryStorage;
use difference::assert_diff;
use env_logger;
use std::{
    env, fs,
    path::Path,
    process::{Command, Stdio},
};

/// An example Postgres SQL `CREATE TABLE` declaration.
const EXAMPLE_SQL: &str = include_str!("../fixtures/example.sql");

/// An example CSV file with columns corresponding to `EXAMPLE_SQL`.
const EXAMPLE_CSV: &str = include_str!("../fixtures/example.csv");

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

/// A BigQuery table name to use for a test, including the project.
fn bq_temp_dataset_name() -> String {
    env::var("BQ_TEST_DATASET").expect("BQ_TEST_DATASET must be set")
}

/// Get our BigQuery test project name.
fn bq_project_id() -> String {
    let ds_name = bq_temp_dataset_name();
    let end = ds_name.find(':').expect("BQ_TEST_DATASET should contain :");
    ds_name[..end].to_owned()
}

/// A BigQuery table to use for a test.
fn bq_temp_dataset() -> String {
    format!("bigquery:{}", bq_temp_dataset_name())
}

/// A BigQuery table to use for a test.
fn bq_test_table(table_name: &str) -> String {
    format!("{}.{}", bq_temp_dataset(), table_name)
}

/// The URL to our test `s3://` bucket and directory.
fn s3_url() -> String {
    env::var("S3_TEST_URL").expect("S3_TEST_URL must be set")
}

/// The URL to a subdirectory of `gs_url`.
fn s3_test_dir_url(dir_name: &str) -> String {
    let mut url = s3_url();
    if !url.ends_with('/') {
        url.push_str("/");
    }
    url.push_str(dir_name);
    url.push_str("/");
    url
}

/// The URL of our Redshift test database. Optional because we're not going to
/// keep Redshift running just for unit tests, not at a minimum of $0.25/hour.
fn redshift_test_url() -> Option<String> {
    env::var("REDSHIFT_TEST_URL").ok()
}

/// The URL of a table in our Redshift test database.
fn redshift_test_table_url(table_name: &str) -> Option<String> {
    redshift_test_url().map(|url| format!("{}#{}", url, table_name))
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
fn conv_pg_sql_to_dbcrossbar_schema_to_pg_sql() {
    let testdir = TestDir::new("dbcrossbar", "conv_pg_sql_to_pg_sql");
    let output1 = testdir
        .cmd()
        .args(&["conv", "postgres-sql:-", "dbcrossbar-schema:-"])
        .output_with_stdin(EXAMPLE_SQL)
        .expect_success();
    let output2 = testdir
        .cmd()
        .args(&["conv", "dbcrossbar-schema:-", "postgres-sql:-"])
        .output_with_stdin(output1.stdout_str())
        .expect_success();
    assert!(output2.stdout_str().contains("CREATE TABLE"));

    // And make sure it round-trips.
    let output3 = testdir
        .cmd()
        .args(&["conv", "postgres-sql:-", "dbcrossbar-schema:-"])
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
    assert!(output.stdout_str().contains("REPEATED"));
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
fn cp_csv_to_csvs() {
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
fn cp_csvs_to_csv() {
    let testdir = TestDir::new("dbcrossbar", "cp_csvs_to_csv");
    let schema = testdir.src_path("fixtures/concat.sql");
    let concat_in = testdir.src_path("fixtures/concat_in");
    let concat_out = testdir.src_path("fixtures/concat_out.csv");
    testdir
        .cmd()
        .arg("cp")
        .arg(&format!("--schema=postgres-sql:{}", schema.display()))
        .arg(&format!("csv:{}", concat_in.display()))
        .arg("csv:out.csv")
        .expect_success();
    let expected = fs::read_to_string(&concat_out).unwrap();
    testdir.expect_file_contents("out.csv", &expected);
}

#[test]
fn cp_csv_to_csv_piped() {
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_csv");
    let schema = testdir.src_path("fixtures/example.sql");
    let output = testdir
        .cmd()
        .args(&[
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            "csv:-",
            "csv:-",
        ])
        .output_with_stdin(EXAMPLE_CSV)
        .expect_success();
    assert_eq!(output.stdout_str(), EXAMPLE_CSV);
}

#[test]
#[ignore]
fn cp_csv_to_postgres_to_gs_to_csv() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_postgres_to_gs_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let expected_schema = testdir.src_path("fixtures/many_types_expected.sql");
    let pg_table = post_test_table_url("testme1.cp_csv_to_postgres_to_gs_to_csv");
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
            "--max-streams=8",
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
    let postgres_sql = fs::read_to_string(&expected_schema).unwrap().replace(
        "\"many_types\"",
        "\"testme1\".\"cp_csv_to_postgres_to_gs_to_csv\"",
    );
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
    assert_diff!(&expected, &actual, ",", 0);
}

#[test]
#[ignore]
fn cp_tricky_column_names() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "cp_tricky_column_names");
    let src = testdir.src_path("fixtures/tricky_column_names.csv");
    let expected = testdir.src_path("fixtures/tricky_column_names_expected.csv");
    let schema = testdir.src_path("fixtures/tricky_column_names.sql");
    let pg_table = post_test_table_url("testme1.cp_tricky_column_names");
    let bq_table = bq_test_table("cp_tricky_column_names");
    let gs_temp_dir = gs_test_dir_url("cp_from_bigquery_with_where");
    let bq_temp_ds = bq_temp_dataset();

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

    // Postgres to BigQuery.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &pg_table,
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // Postgres to BigQuery.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=upsert-on:person__Delivery Zone 4.14",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &pg_table,
            &bq_table,
        ])
        .tee_output()
        .expect_success();

    // BigQuery back to CSV for the final comparison below.
    testdir
        .cmd()
        .args(&[
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &bq_table,
            "csv:out.csv",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&expected).unwrap();
    let actual = fs::read_to_string(testdir.path("out.csv")).unwrap();
    assert_diff!(&expected, &actual, ",", 0);
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
fn cp_from_postgres_with_where() {
    let testdir = TestDir::new("dbcrossbar", "cp_from_postgres_with_where");
    let src = testdir.src_path("fixtures/posts.csv");
    let filtered = testdir.src_path("fixtures/posts_where_author_id_1.csv");
    let schema = testdir.src_path("fixtures/posts.sql");
    let pg_table = post_test_table_url("cp_from_postgres_with_where");

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

    // PostgreSQL back to CSV using --where.
    testdir
        .cmd()
        .args(&[
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            "--where",
            "author_id = 1",
            &pg_table,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&filtered).unwrap();
    let actual =
        fs::read_to_string(testdir.path("out/cp_from_postgres_with_where.csv"))
            .unwrap();
    assert_diff!(&expected, &actual, ",", 0);
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

#[test]
#[ignore]
fn postgres_upsert() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "postgres_upsert");
    let srcs = &[
        testdir.src_path("fixtures/upsert_1.csv"),
        testdir.src_path("fixtures/upsert_2.csv"),
    ];
    let expected = testdir.src_path("fixtures/upsert_result.csv");
    let schema = testdir.src_path("fixtures/upsert.sql");
    let pg_table = post_test_table_url("postgres_upsert");

    // CSVes to Postgres.
    let mut first = true;
    for src in srcs {
        let if_exists = if first {
            first = false;
            "--if-exists=overwrite"
        } else {
            // Make sure we have a unique index on key1,key2 first.
            Command::new("psql")
                .arg(postgres_test_url())
                .args(&[
                    "--command",
                    "CREATE UNIQUE INDEX ON postgres_upsert (key1, key2)",
                ])
                .expect_success();

            // Our `--if-exists` argument.
            "--if-exists=upsert-on:key1,key2"
        };
        testdir
            .cmd()
            .args(&[
                "cp",
                if_exists,
                &format!("--schema=postgres-sql:{}", schema.display()),
                &format!("csv:{}", src.display()),
                &pg_table,
            ])
            .tee_output()
            .expect_success();
    }

    // Postgres to CSV.
    testdir
        .cmd()
        .args(&["cp", "--if-exists=overwrite", &pg_table, "csv:out.csv"])
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
    let actual = normalize_csv(&testdir.path("out.csv"));
    assert_diff!(&expected, &actual, ",", 0);
}

#[test]
#[ignore]
fn cp_pg_append_legacy_json() {
    let testdir = TestDir::new("dbcrossbar", "cp_from_postgres_with_where");
    let src = testdir.src_path("fixtures/legacy_json.csv");
    let schema = testdir.src_path("fixtures/legacy_json.sql");
    let pg_table = post_test_table_url("legacy_json");

    // Create a database table manually, forcing the use of `json` instead of
    // `jsonb`. `dbcrossbar` silently upgrades `json` to `jsonb` under normal
    // circumstances, because it always translates column types into portable
    // types like `DataType::Json`.
    Command::new("psql")
        .arg(postgres_test_url())
        .args(&["--command", "DROP TABLE IF EXISTS legacy_json;"])
        .expect_success();
    Command::new("psql")
        .arg(postgres_test_url())
        .args(&["--command", include_str!("../fixtures/legacy_json.sql")])
        .expect_success();

    // CSV to PostgreSQL.
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

    // PostgreSQL to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &pg_table,
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
fn cp_pg_tricky_column_types() {
    let testdir = TestDir::new("dbcrossbar", "cp_pg_tricky_column_types");
    let src = testdir.src_path("fixtures/more_pg_types.csv");
    let schema = testdir.src_path("fixtures/more_pg_types.sql");
    let pg_table = post_test_table_url("more_pg_types");

    // Create a database table manually, forcing the use of the actual Postgres
    // types we want to test, and not the nearest `dbcrossbar` portable
    // equivalents.
    Command::new("psql")
        .arg(postgres_test_url())
        .args(&["--command", "DROP TABLE IF EXISTS more_pg_types;"])
        .expect_success();
    Command::new("psql")
        .arg(postgres_test_url())
        .args(&["--command", include_str!("../fixtures/more_pg_types.sql")])
        .expect_success();

    // CSV to PostgreSQL.
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

    // PostgreSQL to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &pg_table,
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
fn cp_csv_to_s3_to_csv() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_s3_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let s3_dir = s3_test_dir_url("cp_csv_to_s3_to_csv");

    // CSV to S3.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &format!("csv:{}", src.display()),
            &s3_dir,
        ])
        .tee_output()
        .expect_success();

    // S3 to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--schema=postgres-sql:{}", schema.display()),
            &s3_dir,
            "csv:out/",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&src).unwrap();
    let actual = fs::read_to_string(testdir.path("out/many_types.csv")).unwrap();
    assert_diff!(&expected, &actual, ",", 0);
}

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

#[test]
#[ignore]
fn cp_csv_to_bigml_dataset_to_csv() {
    let _ = env_logger::try_init();
    let testdir = TestDir::new("dbcrossbar", "cp_csv_to_bigml_dataset_to_csv");
    let src = testdir.src_path("fixtures/many_types.csv");
    let schema = testdir.src_path("fixtures/many_types.sql");
    let s3_dir = s3_test_dir_url("cp_csv_to_bigml_dataset_to_csv");

    // CSV to BigML.
    let output = testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", s3_dir),
            &format!("--schema=postgres-sql:{}", schema.display()),
            "--to-arg=name=dbcrossbar test",
            "--to-arg=optype_for_text=categorical",
            "--to-arg=tags[]=dbcrossbar-test",
            "--to-arg=tags[]=dbcrossbar-temporary",
            &format!("csv:{}", src.display()),
            "bigml:dataset",
        ])
        .tee_output()
        .expect_success();
    let dataset_locator = output
        .stdout_str()
        .trim_matches(|c: char| c.is_ascii_whitespace());

    // BigML to CSV.
    testdir
        .cmd()
        .args(&[
            "cp",
            "--if-exists=overwrite",
            dataset_locator,
            // Output as a single file to avoid weird naming conventions.
            "csv:out.csv",
        ])
        .tee_output()
        .expect_success();

    let expected = fs::read_to_string(&src)
        .unwrap()
        .replace(",1e+37,", ",1.0E37,");
    let actual = fs::read_to_string(testdir.path("out.csv")).unwrap();
    assert_diff!(&expected, &actual, ",", 0);

    // Verify SQL schema output contains correct column names, too.
    let output = testdir
        .cmd()
        .args(&["conv", dataset_locator, "postgres-sql:-"])
        .expect_success();
    assert!(output.stdout_str().contains("CREATE TABLE"));
    assert!(output.stdout_str().contains("test_null"));
}

#[test]
#[ignore]
fn count_bigquery() {
    let testdir = TestDir::new("dbcrossbar", "count_bigquery");
    let src = testdir.src_path("fixtures/posts.csv");
    let schema = testdir.src_path("fixtures/posts.sql");
    let gs_temp_dir = gs_test_dir_url("count_bigquery");
    let bq_temp_ds = bq_temp_dataset();
    let bq_table = bq_test_table("count_bigquery");

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

    // Count BigQuery.
    let output = testdir
        .cmd()
        .args(&["count", &bq_table])
        .tee_output()
        .expect_success();

    assert_eq!(output.stdout_str().trim(), "2");
}

#[test]
#[ignore]
fn count_postgres() {
    let testdir = TestDir::new("dbcrossbar", "count_postgres");
    let src = testdir.src_path("fixtures/posts.csv");
    let schema = testdir.src_path("fixtures/posts.sql");
    let pg_table = post_test_table_url("count_postgres");

    // CSV to PostgreSQL.
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

    // Count PostgreSQL.
    let output = testdir
        .cmd()
        .args(&["count", &pg_table])
        .tee_output()
        .expect_success();

    assert_eq!(output.stdout_str().trim(), "2");
}
