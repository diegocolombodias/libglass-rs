#!/usr/bin/env python3
"""Tests the render_wall example with real processes, windows and OpenGL.

It needs a display. It only uses the standard library. On Linux without a monitor:

    cargo build --example render_wall
    xvfb-run -a -s "-screen 0 1920x1080x24" python3 tests/render_wall/test_render_wall.py

With xdotool installed, which only works on X11, it also presses real keys. run-linux.sh,
next to this file, does all of this in a container.

The images it compares and the logs of the nodes are left in the output directory, which
by default is tests/render_wall/output.
"""

import argparse
import os
import shutil
import socket
import statistics
import struct
import subprocess
import sys
import time
import zlib
from pathlib import Path

TIMEOUT = 120
# Rounding may change a pixel on an edge between one tile and the whole canvas.
MAX_DIFFERENT_PIXELS = 0.001


def read_ppm(path):
    data = path.read_bytes()
    magic, size, maximum, pixels = data.split(b"\n", 3)
    assert magic == b"P6" and maximum == b"255", path
    width, height = map(int, size.split())
    assert len(pixels) == width * height * 3, path
    return width, height, pixels


def write_png(path, width, height, pixels):
    def chunk(kind, content):
        crc = zlib.crc32(kind + content)
        return struct.pack(">I", len(content)) + kind + content + struct.pack(">I", crc)

    rows = b"".join(
        b"\0" + pixels[y * width * 3 : (y + 1) * width * 3] for y in range(height)
    )
    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    path.write_bytes(
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", header)
        + chunk(b"IDAT", zlib.compress(rows, 6))
        + chunk(b"IEND", b"")
    )


def read_log(path):
    frames = {}
    for line in path.read_text().splitlines():
        frame, state, swap, x, y, scale, animation = line.split(",")
        frames[int(frame)] = {
            "hash": state,
            "swap": int(swap),
            "offset": (float(x), float(y)),
            "scale": float(scale),
            "time": float(animation),
        }
    return frames


class Wall:
    """A server, and the nodes started with `node`."""

    def __init__(self, exe, server_args):
        self.exe = exe
        self.nodes = []
        self.server = subprocess.Popen(
            [exe, "server", "127.0.0.1:0", *server_args],
            stdout=subprocess.PIPE,
            text=True,
        )
        line = self.server.stdout.readline().strip()
        prefix = "listening on "
        if not line.startswith(prefix):
            raise RuntimeError(f"unexpected output from the server: {line!r}")
        self.address = line[len(prefix) :]

    def node(self, *args):
        process = subprocess.Popen([self.exe, "node", self.address, *args])
        self.nodes.append(process)
        return process

    def wait(self, killed=()):
        for process in [*self.nodes, self.server]:
            code = process.wait(TIMEOUT)
            if process not in killed and code != 0:
                raise RuntimeError(f"{process.args} ended with code {code}")
        self.server.stdout.close()

    def stop(self):
        for process in [*self.nodes, self.server]:
            if process.poll() is None:
                process.kill()


def run_wall(exe, server_args, node_args):
    wall = Wall(exe, server_args)
    try:
        for args in node_args:
            wall.node(*args)
        wall.wait()
    finally:
        wall.stop()


def check_same_frames(logs):
    """All logs must agree on each frame that they have in common, without any gaps."""
    for log in logs:
        numbers = sorted(log)
        assert numbers == list(range(numbers[0], numbers[-1] + 1)), "a frame was skipped"
    for log in logs[1:]:
        for frame in log.keys() & logs[0].keys():
            assert log[frame]["hash"] == logs[0][frame]["hash"], f"frame {frame} differs"


def skew_of(logs):
    """How far apart in time the nodes showed each frame, in milliseconds."""
    common = set(logs[0]).intersection(*logs[1:])
    return [
        (max(log[f]["swap"] for log in logs) - min(log[f]["swap"] for log in logs)) / 1000
        for f in sorted(common)
    ]


def report_skew(name, logs):
    skew = sorted(skew_of(logs))
    p99 = skew[int(len(skew) * 0.99)]
    print(
        f"  {name}: {len(skew)} frames, difference between nodes: "
        f"median {statistics.median(skew):.2f} ms, 99% {p99:.2f} ms, worst {skew[-1]:.2f} ms"
    )


def frame_rate(log):
    frames = sorted(log)
    seconds = (log[frames[-1]]["swap"] - log[frames[0]]["swap"]) / 1e6
    return (frames[-1] - frames[0]) / seconds


def test_tiles_match_the_whole_canvas(exe, out):
    """Together, the tiles must be the same image as one window with their total size."""
    frames = [10, 60, 150, 240]
    server = ["--frames", "250", "--fixed-step", "0.02", "--fps", "0", "--autopilot", "--no-tiles"]
    dump = ["--dump-frames", ",".join(map(str, frames))]

    whole_dir = out / "whole"
    tiles_dir = out / "tiles"
    for directory in (whole_dir, tiles_dir):
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir()

    run_wall(
        exe,
        [*server, "--wait-for", "1"],
        [["--size", "800x600", "--auto-place", "--dump-dir", str(whole_dir), *dump]],
    )
    run_wall(
        exe,
        [*server, "--wait-for", "4"],
        [["--size", "400x300", "--auto-place", "--dump-dir", str(tiles_dir), *dump]] * 4,
    )

    compare_with_whole(whole_dir, tiles_dir, frames, out)


def compare_with_whole(whole_dir, tiles_dir, frames, out):
    """Joins the images of 4 tiles of 400x300, and compares them with one of 800x600."""
    for frame in frames:
        width, height, whole = read_ppm(whole_dir / f"frame{frame}_0_0.ppm")
        assert (width, height) == (800, 600), "the window did not get the size it asked for"
        joined = bytearray(len(whole))
        for row in range(2):
            for col in range(2):
                w, h, tile = read_ppm(tiles_dir / f"frame{frame}_{col}_{row}.ppm")
                assert (w, h) == (400, 300)
                for y in range(h):
                    start = ((row * 300 + y) * width + col * 400) * 3
                    joined[start : start + w * 3] = tile[y * w * 3 : (y + 1) * w * 3]

        different = sum(
            whole[i : i + 3] != joined[i : i + 3] for i in range(0, len(whole), 3)
        )
        share = different / (width * height)
        distinct_colors = len({whole[i : i + 3] for i in range(0, len(whole), 3 * 7)})
        write_png(out / f"whole_{frame}.png", width, height, whole)
        write_png(out / f"joined_{frame}.png", width, height, bytes(joined))
        print(
            f"  frame {frame}: {different} of {width * height} pixels differ, "
            f"{distinct_colors} colors in the image"
        )
        assert distinct_colors > 20, "the image is nearly empty: nothing was drawn"
        assert share <= MAX_DIFFERENT_PIXELS, f"{share:.4%} of the pixels differ"

    views = [read_ppm(whole_dir / f"frame{frame}_0_0.ppm")[2] for frame in frames]
    assert len(set(views)) == len(views), "the animation did not change the image"


def test_nodes_swap_together(exe, out):
    """All the nodes must see the same state, and show it at the same time."""
    server = ["--frames", "400", "--autopilot", "--wait-for", "4"]
    paths = [out / f"even{i}.csv" for i in range(4)]
    run_wall(exe, server, [["--size", "320x240", "--auto-place", "--log", str(p)] for p in paths])
    logs = [read_log(path) for path in paths]
    check_same_frames(logs)
    assert len(logs[0]) == 399, len(logs[0])
    report_skew("same speed", logs)
    print(f"  frame rate: {frame_rate(logs[0]):.1f} fps")


def test_slowest_node_holds_the_others(exe, out):
    """One slow node must slow all of them down, without any of them getting ahead."""
    delay = 40
    server = ["--frames", "150", "--fps", "0", "--wait-for", "4"]
    paths = [out / f"slow{i}.csv" for i in range(4)]
    args = [["--size", "320x240", "--auto-place", "--log", str(p)] for p in paths]
    args[2] += ["--delay-ms", str(delay)]
    run_wall(exe, server, args)
    logs = [read_log(path) for path in paths]
    check_same_frames(logs)
    report_skew("one slow node", logs)
    rates = [frame_rate(log) for log in logs]
    print(f"  frame rates: {', '.join(f'{rate:.1f}' for rate in rates)} fps")
    assert max(rates) < 1000 / delay * 1.05, "the others did not wait for the slow node"


def test_nodes_can_join_and_die(exe, out):
    """A node that joins late and one that is killed must not disturb the others."""
    paths = [out / f"dyn{i}.csv" for i in range(4)]
    args = [["--size", "320x240", "--auto-place", "--log", str(p)] for p in paths]
    wall = Wall(exe, ["--frames", "600", "--autopilot", "--wait-for", "3"])
    try:
        victim = wall.node(*args[0])
        wall.node(*args[1])
        wall.node(*args[2])
        time.sleep(3)
        assert wall.server.poll() is None, "the wall ended before the test"
        wall.node(*args[3])
        time.sleep(3)
        victim.kill()
        wall.wait(killed=[victim])
    finally:
        wall.stop()

    logs = [read_log(path) for path in paths]
    check_same_frames(logs)
    for log in logs[1:3]:
        assert sorted(log)[-1] == 599, "a node did not reach the end"
    assert sorted(logs[3])[-1] == 599, "the late node did not reach the end"
    assert sorted(logs[3])[0] > sorted(logs[1])[0], "it should have joined late"
    assert sorted(logs[0])[-1] < 590, "the killed node should have stopped early"
    report_skew("after the changes", logs[1:])
    print(
        f"  late node entered at frame {sorted(logs[3])[0]}, "
        f"killed node stopped at frame {sorted(logs[0])[-1]}"
    )


def test_demo_starts_and_closes_everything(exe, out):
    """The command that a person runs first."""
    subprocess.run([exe, "demo", "2", "--frames", "120", "--size", "320x240"], check=True, timeout=TIMEOUT)


def sctp_works():
    """Whether this system has SCTP. Where it must, as in run-linux.sh, missing it fails."""
    try:
        socket.socket(socket.AF_INET, socket.SOCK_STREAM, 132).close()
        return True
    except OSError:
        if os.environ.get("LIBGLASS_REQUIRE_SCTP"):
            raise
        return False


def test_wall_over_sctp(exe, out):
    """The same wall, with the server and every node on SCTP."""
    server = ["--sctp", "--frames", "300", "--autopilot", "--wait-for", "3"]
    paths = [out / f"sctp{i}.csv" for i in range(3)]
    node = ["--sctp", "--size", "320x240", "--auto-place", "--log"]
    run_wall(exe, server, [[*node, str(p)] for p in paths])
    logs = [read_log(path) for path in paths]
    check_same_frames(logs)
    assert len(logs[0]) == 299, len(logs[0])
    report_skew("over SCTP", logs)

    demo = [exe, "demo", "2", "--sctp", "--frames", "60", "--size", "320x240"]
    subprocess.run(demo, check=True, timeout=TIMEOUT)


def xdotool(*args):
    return subprocess.run(
        ["xdotool", *map(str, args)], check=True, capture_output=True, text=True
    ).stdout.strip()


def window_of(process):
    deadline = time.time() + 20
    while time.time() < deadline:
        found = subprocess.run(
            ["xdotool", "search", "--pid", str(process.pid)], capture_output=True, text=True
        ).stdout.split()
        if found:
            return found[-1]
        time.sleep(0.1)
    raise RuntimeError("the window of the node was not found")


def test_real_input_reaches_all_nodes(exe, out):
    """Keys and the mouse on one window must change what all of them show."""
    paths = [out / f"input{i}.csv" for i in range(2)]
    args = [["--size", "320x240", "--auto-place", "--log", str(p)] for p in paths]
    wall = Wall(exe, ["--wait-for", "2"])
    try:
        first = wall.node(*args[0])
        second = wall.node(*args[1])
        first_window = window_of(first)
        second_window = window_of(second)
        time.sleep(1)

        def last_view():
            logs = [read_log(path) for path in paths]
            check_same_frames(logs)
            return logs[0][max(logs[0])]

        def on(window):
            xdotool("windowfocus", "--sync", window)
            xdotool("mousemove", "--window", window, 160, 120)
            time.sleep(0.1)

        start = last_view()
        assert start["offset"] == (0.0, 0.0) and start["scale"] == 1.0, start

        # Translation with a key, on the first window.
        on(first_window)
        xdotool("keydown", "Right")
        time.sleep(0.5)
        xdotool("keyup", "Right")
        time.sleep(0.2)
        view = last_view()
        print(f"  right arrow: offset {view['offset'][0]:.3f}")
        assert 0.2 < view["offset"][0] < 1.0 and view["offset"][1] == 0.0, view

        # Scale with the wheel, on the second window.
        on(second_window)
        for _ in range(3):
            xdotool("click", 4)
        time.sleep(0.2)
        scaled = last_view()
        print(f"  wheel: scale {scaled['scale']:.3f}")
        # winit counts a synthetic click twice, on press and on release. A real wheel on X11
        # reaches it as axis motion, and its button events are ignored.
        assert abs(scaled["scale"] - 1.1**6) < 0.01, scaled

        # Translation with the mouse, on the second window. The canvas goes from -1 to 1
        # in height, and two rows split it, so 240 pixels are one unit up.
        before = scaled["offset"]
        xdotool("mousedown", 1)
        xdotool("mousemove", "--window", second_window, 160, 60)
        time.sleep(0.1)
        xdotool("mouseup", 1)
        time.sleep(0.2)
        dragged = last_view()
        moved = dragged["offset"][1] - before[1]
        print(f"  mouse drag: moved {moved:.3f}")
        assert moved > 0.1, dragged
        assert dragged["offset"][0] == before[0], dragged

        # Pause, on the first window.
        on(first_window)
        xdotool("key", "space")
        time.sleep(0.2)
        paused = last_view()
        time.sleep(0.5)
        assert last_view()["time"] == paused["time"], "the animation did not stop"
        xdotool("key", "space")
        time.sleep(0.2)
        assert last_view()["time"] > paused["time"], "the animation did not resume"
        print("  space: animation stopped and resumed")

        xdotool("key", "r")
        time.sleep(0.2)
        view = last_view()
        assert view["offset"] == (0.0, 0.0) and view["scale"] == 1.0, view
        print("  R: view was reset")

        # Closing the wall, from any window.
        xdotool("key", "Escape")
        wall.wait()
        print("  Esc: all processes ended without errors")
    finally:
        wall.stop()


def main():
    here = Path(__file__).resolve().parent
    root = here.parent.parent
    name = "render_wall.exe" if os.name == "nt" else "render_wall"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exe", default=root / "target" / "debug" / "examples" / name)
    parser.add_argument(
        "--out", type=Path, default=here / "output", help="where to leave the images and logs"
    )
    args = parser.parse_args()

    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    exe = str(args.exe)

    tests = [
        test_tiles_match_the_whole_canvas,
        test_nodes_swap_together,
        test_slowest_node_holds_the_others,
        test_nodes_can_join_and_die,
        test_demo_starts_and_closes_everything,
    ]
    if shutil.which("xdotool"):
        tests.append(test_real_input_reaches_all_nodes)
    else:
        print("without xdotool: skipping the test of keys and mouse")
    if sctp_works():
        tests.append(test_wall_over_sctp)
    else:
        print("without SCTP: skipping the wall over SCTP")

    failed = 0
    for test in tests:
        print(f"{test.__name__}")
        try:
            test(exe, out)
            print("  ok")
        except (AssertionError, RuntimeError, subprocess.SubprocessError, OSError) as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            failed += 1
    print(f"{len(tests) - failed} passed, {failed} failed; output in {out}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
