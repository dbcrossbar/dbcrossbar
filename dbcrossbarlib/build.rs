//! This script is called before compiling this library. Its job is to generate
//! source code which will be added to the build.

use peg;

fn main() {
    // Run our parser generator over our grammar.
    peg::cargo_build("src/parsers/postgres.rustpeg");
}
