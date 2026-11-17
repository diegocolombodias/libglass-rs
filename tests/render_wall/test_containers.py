#!/usr/bin/env python3
"""Tests the render_wall example over a real network, with one container per process.

The server and each node have their own container, with their own virtual screen, and
they only see each other through a Docker network. From the root of the project:

    docker build -f tests/render_wall/Dockerfile -t libglass-render-wall .
    python tests/render_wall/test_containers.py

The containers share the clock of the machine that runs them, so the time differences
that it reports are real. Images and logs are left in tests/render_wall/output/containers.
"""

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

from test_render_wall import (
    check_same_frames,
    compare_with_whole,
    frame_rate,
    read_log,
    report_skew,
)

IMAGE = "libglass-render-wall"
NETWORK = "libglass-wall"
SERVER = "wall-server"
PORT = 9191
TIMEOUT = 180
SCREEN = "-screen 0 1024x768x24"


def docker(*args, check=True, timeout=TIMEOUT):
    result = subprocess.run(
        ["docker", *map(str, args)], capture_output=True, text=True, timeout=timeout
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"docker {args[0]} failed: {result.stderr.strip()}")
    return result


class Wall:
    """A server and its nodes, each one in its container, and all gone at the end."""

    def __init__(self, out):
        self.out = out
        self.containers = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if self.containers:
            docker("rm", "-f", *self.containers, check=False)

    def _start(self, name, command, network_delay=None):
        # --init: without it, xvfb-run as the first process may wait forever for Xvfb.
        options = ["--init", "--name", name, "--network", NETWORK, "--network-alias", name]
        if network_delay:
            # Everything that this container sends takes longer to arrive.
            options += ["--cap-add", "NET_ADMIN"]
            delay = f"tc qdisc add dev eth0 root netem delay {network_delay} && "
            command = ["sh", "-c", delay + 'exec "$@"', "sh", *command]
        docker("run", "-d", *options, IMAGE, *command)
        self.containers.append(name)

    def server(self, *args, network_delay=None):
        command = ["render_wall", "server", f"0.0.0.0:{PORT}", *args]
        self._start(SERVER, command, network_delay)

    def node(self, name, *args):
        command = ["xvfb-run", "-a", "-s", SCREEN, "render_wall", "node", f"{SERVER}:{PORT}"]
        self._start(name, [*command, *args])

    def wait(self, name, expected=0):
        code = int(docker("wait", name).stdout.strip())
        if code != expected:
            logs = docker("logs", "--tail", "20", name, check=False)
            raise RuntimeError(f"{name} ended with code {code}: {logs.stderr[-800:]}")

    def collect(self, name, directory):
        """Copies what a node has written, even if it is no longer running."""
        directory.mkdir(parents=True, exist_ok=True)
        docker("cp", f"{name}:/wall/.", directory)

    def last_frame(self, name):
        """The last frame that a node has shown."""
        log = docker("exec", name, "cat", "/wall/log.csv").stdout.splitlines()
        return int(log[-1].split(",")[0]) if log else 0


def nodes_with_logs(wall, names, *args, extra=None):
    for name in names:
        wall.node(name, "--size", "320x240", "--log", "/wall/log.csv", *args, *(extra or {}).get(name, []))


def logs_of(wall, names, directory):
    logs = []
    for name in names:
        wall.collect(name, directory / name)
        logs.append(read_log(directory / name / "log.csv"))
    return logs


def test_tiles_match_the_whole_canvas(out):
    """The tiles of 4 containers must form the same image as one window in another."""
    frames = [10, 60, 150, 240]
    server = ["--frames", "250", "--fixed-step", "0.02", "--fps", "0", "--autopilot", "--no-tiles"]
    dump = ["--dump-dir", "/wall", "--dump-frames", ",".join(map(str, frames))]
    whole_dir = out / "whole"
    tiles_dir = out / "tiles"
    for directory in (whole_dir, tiles_dir):
        shutil.rmtree(directory, ignore_errors=True)

    with Wall(out) as wall:
        wall.server(*server, "--wait-for", "1")
        wall.node("whole", "--size", "800x600", *dump)
        wall.wait("whole")
        wall.wait(SERVER)
        wall.collect("whole", whole_dir)

    names = [f"tile{i}" for i in range(4)]
    with Wall(out) as wall:
        wall.server(*server, "--wait-for", "4")
        for name in names:
            wall.node(name, "--size", "400x300", *dump)
        for name in names:
            wall.wait(name)
            wall.collect(name, tiles_dir)
        wall.wait(SERVER)

    compare_with_whole(whole_dir, tiles_dir, frames, out)


def test_nodes_swap_together(out):
    """All the containers must see the same state, and show it at the same time."""
    names = [f"even{i}" for i in range(4)]
    with Wall(out) as wall:
        wall.server("--frames", "400", "--autopilot", "--wait-for", "4")
        nodes_with_logs(wall, names)
        for name in names:
            wall.wait(name)
        logs = logs_of(wall, names, out / "even")
    check_same_frames(logs)
    assert len(logs[0]) == 399, len(logs[0])
    report_skew("same speed", logs)
    print(f"  frame rate: {frame_rate(logs[0]):.1f} fps")


def test_slowest_node_holds_the_others(out):
    """A slow container must slow all of them down."""
    delay = 40
    names = [f"slow{i}" for i in range(4)]
    with Wall(out) as wall:
        wall.server("--frames", "150", "--fps", "0", "--wait-for", "4")
        nodes_with_logs(wall, names, extra={names[2]: ["--delay-ms", str(delay)]})
        for name in names:
            wall.wait(name)
        logs = logs_of(wall, names, out / "slow")
    check_same_frames(logs)
    report_skew("one slow node", logs)
    rates = [frame_rate(log) for log in logs]
    print(f"  frame rates: {', '.join(f'{rate:.1f}' for rate in rates)} fps")
    assert max(rates) < 1000 / delay * 1.05, "the others did not wait for the slow node"


def test_containers_can_join_and_die(out):
    """A container that joins late and one that is killed must not disturb the others."""
    names = [f"dyn{i}" for i in range(4)]
    with Wall(out) as wall:
        wall.server("--frames", "900", "--autopilot", "--wait-for", "3")
        nodes_with_logs(wall, names[:3])
        time.sleep(4)
        nodes_with_logs(wall, names[3:])
        time.sleep(4)
        docker("kill", names[0])
        for name in names[1:]:
            wall.wait(name)
        wall.wait(SERVER)
        logs = logs_of(wall, names, out / "dyn")
    check_same_frames(logs)
    for log in logs[1:]:
        assert max(log) == 899, "a node did not reach the end"
    assert min(logs[3]) > min(logs[1]), "it should have joined late"
    assert max(logs[0]) < 890, "the killed node should have stopped early"
    report_skew("after the changes", logs[1:])
    print(f"  late node entered at frame {min(logs[3])}, killed node stopped at {max(logs[0])}")


def test_frozen_node_stops_the_wall_until_it_returns(out):
    """A node that freezes keeps its connection, so the others must wait for it."""
    names = [f"frozen{i}" for i in range(3)]
    with Wall(out) as wall:
        wall.server("--frames", "600", "--wait-for", "3")
        nodes_with_logs(wall, names)
        time.sleep(4)
        before_freeze = wall.last_frame(names[1])
        assert before_freeze > 0, "the wall has not started"

        docker("pause", names[0])
        time.sleep(1)
        stopped_at = wall.last_frame(names[1])
        time.sleep(3)
        assert wall.last_frame(names[1]) == stopped_at, "the wall did not wait"
        print(f"  frozen at frame {stopped_at} for 3 s")

        docker("unpause", names[0])
        for name in names:
            wall.wait(name)
        wall.wait(SERVER)
        logs = logs_of(wall, names, out / "frozen")
    check_same_frames(logs)
    assert all(max(log) == 599 for log in logs), "a node did not reach the end"
    print("  it went on from the same frame, and all reached the end")


def test_slow_network(out):
    """With delay and jitter on the network, the state must still be the same in all nodes."""
    delay = "10ms 5ms"
    names = [f"net{i}" for i in range(4)]
    with Wall(out) as wall:
        wall.server("--frames", "300", "--autopilot", "--wait-for", "4", network_delay=delay)
        nodes_with_logs(wall, names)
        for name in names:
            wall.wait(name)
        logs = logs_of(wall, names, out / "net")
    check_same_frames(logs)
    assert len(logs[0]) == 299, len(logs[0])
    report_skew(f"network with {delay} of delay", logs)
    print(f"  frame rate: {frame_rate(logs[0]):.1f} fps")


def main():
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=here / "output" / "containers")
    args = parser.parse_args()
    out = args.out
    out.mkdir(parents=True, exist_ok=True)

    docker("rm", "-f", SERVER, check=False)
    docker("network", "rm", NETWORK, check=False)
    docker("network", "create", NETWORK)

    tests = [
        test_tiles_match_the_whole_canvas,
        test_nodes_swap_together,
        test_slowest_node_holds_the_others,
        test_containers_can_join_and_die,
        test_frozen_node_stops_the_wall_until_it_returns,
        test_slow_network,
    ]
    failed = 0
    try:
        for test in tests:
            print(test.__name__, flush=True)
            try:
                test(out)
                print("  ok", flush=True)
            except (AssertionError, RuntimeError, subprocess.SubprocessError, OSError) as e:
                print(f"  FAILED: {type(e).__name__}: {e}", flush=True)
                failed += 1
    finally:
        docker("network", "rm", NETWORK, check=False)
    print(f"{len(tests) - failed} passed, {failed} failed; output in {out}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
