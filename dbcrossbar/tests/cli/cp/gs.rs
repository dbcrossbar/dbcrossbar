//! Google Cloud Storage-specific tests.

use super::*;

#[test]
#[ignore]
fn cp_from_gs_to_exact_csv() {
    let gs_dir = gs_test_dir_url("cp_from_gs_to_exact_csv");
    assert_cp_to_exact_csv("cp_from_gs_to_exact_csv", &gs_dir);
}
