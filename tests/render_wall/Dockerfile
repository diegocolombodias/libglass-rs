# The image of the containers of test_containers.py. From the root of the project:
#
#     docker build -f tests/render_wall/Dockerfile -t libglass-render-wall .
FROM rust:1 AS build
WORKDIR /src
COPY . .
RUN cargo build --release --example render_wall

FROM debian:trixie-slim
# A virtual screen, OpenGL in software, what winit loads at run time, and network delays.
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
        xvfb xauth libgl1 libegl1 libgl1-mesa-dri libx11-xcb1 libxcursor1 libxrandr2 \
        libxi6 libxkbcommon-x11-0 iproute2 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /wall
COPY --from=build /src/target/release/examples/render_wall /usr/local/bin/render_wall
ENV LIBGL_ALWAYS_SOFTWARE=1
