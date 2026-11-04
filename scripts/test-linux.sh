#!/bin/sh
# Runs all tests on Linux, inside a throwaway container. On Windows, this is the only
# way to run the tests of Unix sockets. From the root of the project:
#
#     docker run --rm -v "${PWD}:/src:ro" rust:1 sh /src/scripts/test-linux.sh
#
# The source is mounted read-only, and everything is built inside the container.
set -e

# Here the SCTP tests must run, instead of being skipped if SCTP is missing.
export LIBGLASS_REQUIRE_SCTP=1

mkdir /tmp/project
tar -C /src --exclude=./target -cf - . | tar -C /tmp/project -xf -
cd /tmp/project

rustup component add clippy
cargo clippy --all-targets --features serde -- -D warnings
cargo test --features serde
cargo run --example quicksort -- 20000 3
cargo run --example compare_transports -- --quick

cargo build
python3 bindings/python/test_libglass.py
