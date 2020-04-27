//! This script is called before compiling this library. Its job is to generate
//! source code which will be added to the build.

fn main() {
    // Run our parser generator over our grammars.
    peg::cargo_build("src/drivers/bigquery_shared/data_type.rustpeg");
    peg::cargo_build("src/drivers/postgres_shared/create_table_sql.rustpeg");
}
