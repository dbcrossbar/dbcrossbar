# Dockerfile for building static release binaries using musl-libc.

FROM ekidd/rust-musl-builder:nightly-2019-04-25-openssl11

# We need to add the source code to the image because `rust-musl-builder`
# assumes a UID of 1000, but TravisCI has switched to 2000.
ADD . ./
RUN sudo chown -R rust:rust .

CMD cargo audit && cargo build --release
