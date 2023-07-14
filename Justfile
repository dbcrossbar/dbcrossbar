# This is like a Makefile, but for the Rust tool https://github.com/casey/just.
# We use this to automated common project tasks.
#
# You can install it with `cargo install just` and run `just --list` to see
# available commands.

# Run all checks and tests that we can run without credentials.
check:
    cargo clippy -- -D warnings
    cargo fmt -- --check
    cargo deny check
    cargo test
