//! Getting information about `dbcrossbar`.

use cli_test_dir::*;

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
