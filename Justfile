# This is like a Makefile, but for the Rust tool https://github.com/casey/just.
# We use this to automated common project tasks.
#
# You can install it with `cargo install just` and run `just --list` to see
# available commands.

# Look up our version using cargo.
VERSION := `cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "dbcrossbar") | .version'`

# Look up our `opinionated_telemetry` version using cargo.
OPINIONATED_TELEMETRY := `cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "opinionated_telemetry") | .version'`

# Look up our `dbcrossbar_trino` version using cargo.
DBCROSSBAR_TRINO := `cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "dbcrossbar_trino") | .version'`

# Look up our `dbcrossbar_trino_macros` version using cargo.
DBCROSSBAR_TRINO_MACROS := `cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "dbcrossbar_trino_macros") | .version'`

# Run all checks and tests that we can run without credentials.
check:
    cargo clippy -- -D warnings
    cargo fmt -- --check
    cargo deny check
    cargo test

# Check to make sure our working copy is clean.
check-clean: update-generated
    git status
    git diff-index --quiet HEAD --

# Test all drivers. Requires lots of environment variables to be set and some
# Docker containers to be running. Mostly for Faraday internal testing.
test-all:
    cargo test --all -- --include-ignored

# Update various generated files (which need to be checked in).
update-generated:
    cargo about generate --fail about.hbs -o dbcrossbar/ALL_LICENSES.html
    (cd guide/src/generated && ./generate.sh)

# Build our guide. Requires mdbook and mbook-graphviz.
guide:
    (cd guide && mdbook build)

# Install useful development tools.
install-dev-tools:
    cargo install cargo-deny cargo-about cargo-edit mdbook mdbook-graphviz
    echo "💡 You should also install graphiz and jq. 💡"

# Print the current version.
version:
    @echo "{{VERSION}}"

# Release via crates.io and GitHub.
release: check check-clean
  (cd dbcrossbar && cargo publish)
  git tag v{{VERSION}}
  git push
  git push --tags

# Print the current version of opinionated-tracing.
opinionated-telemetry-version:
    @echo "{{OPINIONATED_TELEMETRY}}"

# Release opinionated_telemetry via crates.io.
release-opinionated-telemetry: check check-clean
  (cd crates/opinionated_telemetry && cargo publish)
  git tag opinionated_telemetry_v{{OPINIONATED_TELEMETRY}}
  git push
  git push --tags

# Print the current version of dbcrossbar_trino.
dbcrossbar-trino-version:
    @echo "{{DBCROSSBAR_TRINO}}"

# Release dbcrossbar_trino via crates.io.
release-dbcrossbar-trino: check check-clean
  (cd crates/dbcrossbar_trino && cargo publish)
  git tag dbcrossbar_trino_v{{DBCROSSBAR_TRINO}}
  git push
  git push --tags

# Print the current version of dbcrossbar_trino_macros.
dbcrossbar-trino-macros-version:
    @echo "{{DBCROSSBAR_TRINO_MACROS}}"

# Release dbcrossbar_trino_macros via crates.io.
release-dbcrossbar-trino-macros: check check-clean
  (cd crates/dbcrossbar_trino_macros && cargo publish)
  git tag dbcrossbar_trino_macros_v{{DBCROSSBAR_TRINO_MACROS}}
  git push
  git push --tags
