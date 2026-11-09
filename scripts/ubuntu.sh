#!/bin/sh
# Runs one of the test scripts in a throwaway Ubuntu container, after installing Rust in
# it. From the root of the project:
#
#     docker run --rm -v "${PWD}:/src:ro" ubuntu:24.04 sh /src/scripts/ubuntu.sh scripts/test-linux.sh
#     docker run --rm -v "${PWD}:/src:ro" -v "${PWD}/tests/render_wall/output:/out" ubuntu:24.04 \
#         sh /src/scripts/ubuntu.sh tests/render_wall/run-linux.sh
#
# The kernel, and with it SCTP, is still the one of the machine that runs Docker.
set -e

apt-get update -qq >/dev/null
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ca-certificates curl build-essential \
    pkg-config python3 >/dev/null 2>&1
curl -sSf https://sh.rustup.rs | sh -s -- -y -q --profile minimal --component clippy,rustfmt
. "$HOME/.cargo/env"

. /etc/os-release
echo "$PRETTY_NAME, $(rustc --version), Python $(python3 -c 'import platform; print(platform.python_version())')"
sh "/src/$1"
