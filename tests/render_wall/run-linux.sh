#!/bin/sh
# Runs the tests of the render_wall example on Linux, in a throwaway container, with a
# virtual screen and OpenGL in software. From the root of the project:
#
#     docker run --rm -v "${PWD}:/src:ro" -v "${PWD}/tests/render_wall/output:/out" rust:1 \
#         sh /src/tests/render_wall/run-linux.sh
#
# The source is mounted read-only. Images and logs of the run are left in /out.
set -e

apt-get update -qq >/dev/null
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq xvfb xauth xdotool libgl1 libegl1 \
    libgl1-mesa-dri libx11-xcb1 libxcursor1 libxrandr2 libxi6 libxkbcommon-x11-0 python3 \
    >/dev/null 2>&1

mkdir /tmp/project
tar -C /src --exclude=./target --exclude=./tests/render_wall/output -cf - . |
    tar -C /tmp/project -xf -
cd /tmp/project

cargo test --example render_wall
cargo build --example render_wall
LIBGL_ALWAYS_SOFTWARE=1 xvfb-run -a -s "-screen 0 1920x1080x24" \
    python3 tests/render_wall/test_render_wall.py --out /out
