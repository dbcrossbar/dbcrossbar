# Installing

Pre-built binaries for `dbcrossbar` are [available on GitHub](https://github.com/dbcrossbar/dbcrossbar/releases). These currently include:

1. Fully-static Linux x86_64 binaries, which should work on any modern distribution (including Alpine Linux containers).
2. MacOS X binaries.

Windows binaries are not available at this time, but it may be possible to build them with a little work.

## Required tools

To use the S3 and RedShift drivers, you will need to install the [AWS CLI tools](https://aws.amazon.com/cli/). We plan to replace the AWS CLI tools with native Rust libraries before the 1.0 release.

## Installing using `cargo`

You can also install `dbcrossbar` using `cargo`. First, you will need to make sure you have the necessary C dependencies installed:

```sh
# Ubuntu Linux (might be incomplete).
sudo apt install build-essential libssl-dev libpq-dev

# MacOS X (might be incomplete).
brew install openssl@1.1 postgresql
```

Then, you can install using `cargo`:

```sh
cargo install dbcrossbar
```

## Building from source

The source code is available [on GitHub](https://github.com/dbcrossbar/dbcrossbar). First, install the build dependencies as described above. Then run:

```sh
git clone https://github.com/dbcrossbar/dbcrossbar.git
cd dbcrossbar
cargo build --release
```

This will create `target/release/dbcrossbar`.
