# This is like a Makefile, but for the Rust tool https://github.com/casey/just.
# We use this to automated common project tasks.
#
# You can install it with `cargo install just` and run `just --list` to see
# available commands.

# Look up our version using cargo.
VERSION := `cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "dbcrossbar") | .version'`

# Run all checks and tests that we can run without credentials.
check:
    cargo clippy -- -D warnings
    cargo fmt -- --check
    cargo deny check
    cargo test

# Check to make sure our working copy is clean.
check-clean: update-generated
    git diff-index --quiet HEAD --

# Update various generated files (which need to be checked in).
update-generated:
    cargo about generate about.hbs -o dbcrossbar/ALL_LICENSES.html
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
  (cd dbcrossbarlib && cargo publish)
  sleep 10
  (cd dbcrossbar && cargo publish)
  git tag v{{VERSION}}
  git push
  git push --tags
