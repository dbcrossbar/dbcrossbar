# `opinionated-tracing` examples

To try these, run the following command to compile all the examples:

```sh
cargo test
```

Next, set up tracing in your environment. You'll need two different terminals, each with following:

```sh
export RUST_LOG=debug
```

In one terminal, start the server:

```sh
cargo run --example server-tracing
```

In the other terminal, run the client:

```sh
cargo run --example client-tracing
```
