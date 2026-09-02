use cli_test_dir::TestDir;
use std::sync::Once;

pub fn new(test_name: &str) -> TestDir {
    static PLACE_BINARY: Once = Once::new();
    PLACE_BINARY.call_once(|| {
        let destination = std::env::current_exe()
            .expect("current test executable")
            .parent()
            .expect("parent of test executable")
            .join("dbcrossbar");
        if destination.exists() {
            return;
        }
        let built_binary = env!("CARGO_BIN_EXE_dbcrossbar");
        if std::fs::hard_link(built_binary, &destination).is_err() {
            std::fs::copy(built_binary, &destination)
                .expect("could not place dbcrossbar next to CLI tests");
        }
    });
    TestDir::new("dbcrossbar", test_name)
}
