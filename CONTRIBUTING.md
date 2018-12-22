# Thank you for contributing!

Thank you for contributing to our project. We really appreciate it!

Please submit your contributions as a pull request on GitHub, and we'll try to review them as soon as we can. If you've never made a pull request before, these articles might help:

- [How To Create a Pull Request on GitHub](https://www.digitalocean.com/community/tutorials/how-to-create-a-pull-request-on-github)
- [GitHub Pull Request Tutorial: Learn to submit your first PR](https://www.thinkful.com/learn/github-pull-request-tutorial/#Time-to-Submit-Your-First-PR)

By submitting your changes as a PR, you make it easy for us to read your changes, to provide feedback, and to integrate your changes into the project.

## Contribution guidelines

Our Rust projects use the standard Rust coding style, and should all have tests. Fortunately, Rust provides tools to make this easy.

### Setting up your tools

First, make sure you have an up-to-date stable Rust, with the necessary tools. You can install Rust using `rustup`:

```sh
# On Linux and MacOS. Others see https://rustup.rs/
curl https://sh.rustup.rs -sSf | sh

# Install Rust tools for code formatting and lints.
rustup component add rustfmt
rustup component add clippy
```

You may also need to install a package like Ubuntu's `build-essential`, the MacOS Xcode tools, or other basic developer tools for your platform.

For the most pleasant Rust developer experience, we recommend using either [RLS](https://github.com/rust-lang/rls) ([Visual Studio Code](https://code.visualstudio.com/) supports it nicely) or [IntelliJ Rust](https://intellij-rust.github.io/). You may also want to install `cargo-watch`:

```sh
cargo install -f cargo-watch
```

...and leave a terminal open running:

```sh
cargo watch -x test
```

### Before submitting your patch

First, read through your patch, and make sure you've removed any debugging code. Next, format your code and double-check everything using the following commands:

```sh
# Apply standard coding style automatically.
cargo fmt

# Check for any warnings, and fix them.
cargo clippy

# Compile the code.
cargo build

# Run the tests.
cargo test
```

## Code of conduct

Contributors are expected to conduct themselves in a kind and professional manner, and to refrain from discrimination.
