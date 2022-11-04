//! Shopify-specific tests.

use cli_test_dir::*;
use std::env;

use super::*;

#[test]
#[ignore]
fn cp_shopify_to_bigquery() {
    // Skip test if SHOPIFY_SHOP isn't set.
    if env::var("SHOPIFY_SHOP").is_err() {
        eprintln!("Skipping cp_shopify_to_bigquery because SHOPIFY_SHOP isn't set");
        return;
    }

    let testdir = TestDir::new("dbcrossbar", "cp_shopify_to_bigquery");
    let shop = env::var("SHOPIFY_SHOP").unwrap();
    let src = format!(
        "shopify://{}/admin/api/2020-04/orders.json?status=any",
        shop
    );
    let schema = testdir.src_path("fixtures/shopify.ts");
    let gs_temp_dir = gs_test_dir_url("cp_shopify_to_bigquery");
    let bq_temp_ds = bq_temp_dataset();
    let bq_table = bq_test_table("cp_shopify_to_bigquery");

    // Shopify to BigQuery.
    testdir
        .cmd()
        .args([
            "--enable-unstable",
            "cp",
            "--if-exists=overwrite",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=dbcrossbar-ts:{}#Order", schema.display()),
            &src,
            &bq_table,
        ])
        .spawn()
        .expect_success();

    // Shopify to BigQuery upsert.
    testdir
        .cmd()
        .args([
            "--enable-unstable",
            "cp",
            "--if-exists=upsert-on:id",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=dbcrossbar-ts:{}#Order", schema.display()),
            &src,
            &bq_table,
        ])
        .spawn()
        .expect_success();

    // BigQuery to CSV (make sure we aren't hitting any surprising edge cases).
    testdir
        .cmd()
        .args([
            "--enable-unstable",
            "cp",
            &format!("--temporary={}", gs_temp_dir),
            &format!("--temporary={}", bq_temp_ds),
            &format!("--schema=dbcrossbar-ts:{}#Order", schema.display()),
            &bq_table,
            "csv:out.csv",
        ])
        .spawn()
        .expect_success();
}
