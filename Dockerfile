# Dockerfile for building static release binaries using musl-libc.

FROM ekidd/rust-musl-builder:stable-openssl11

# We need to add the source code to the image because `rust-musl-builder`
# assumes a UID of 1000, but TravisCI has switched to 2000.
ADD . ./
RUN sudo chown -R rust:rust .

CMD cargo deny check && cargo build --release
