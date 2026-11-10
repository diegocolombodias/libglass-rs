"""Tests of the Python bindings. Run with `python bindings/python/test_libglass.py`."""

import os
import threading
import time
import unittest

import libglass

SETTLE = 0.3


def wait_until(condition, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("timed out")
        time.sleep(0.01)


class WithServer(unittest.TestCase):
    def setUp(self):
        self.server = libglass.Server()
        self.a = libglass.Client(self.server.address, "a")
        self.b = libglass.Client(self.server.address, "b")

    def tearDown(self):
        self.a.close()
        self.b.close()
        self.server.close()

    def test_clients(self):
        self.assertTrue(self.a.is_master)
        self.assertFalse(self.b.is_master)
        self.assertNotEqual(self.a.id, self.b.id)
        wait_until(lambda: self.a.total_nodes == 3)
        self.assertTrue(self.a.is_connected)

    def test_event_keeps_binary_data(self):
        with libglass.Event(self.a, "ev") as ev_a, libglass.Event(self.b, "ev") as ev_b:
            time.sleep(SETTLE)
            data = bytes(range(256))
            ev_a.enqueue(data)
            self.assertEqual(ev_b.get(), data)
            self.assertEqual(len(ev_b), 0)
            with self.assertRaises(libglass.Timeout):
                ev_b.get(timeout=0.05)

    def test_event_tells_its_sender(self):
        with libglass.Event(self.a, "ev") as ev_a, libglass.Event(self.b, "ev") as ev_b:
            time.sleep(SETTLE)
            ev_a.enqueue(b"from a")
            self.assertEqual(ev_b.get_with_sender(), (self.a.id, b"from a"))

    def test_shared(self):
        with libglass.Shared(self.a, "x", b"0") as x_a, libglass.Shared(self.b, "x", b"0") as x_b:
            time.sleep(SETTLE)
            x_a.value = b"hello"
            x_a.send_update()
            x_a.get_update()
            x_b.get_update()
            self.assertEqual(x_b.value, b"hello")

    def test_barrier(self):
        with libglass.Barrier(self.a) as barrier_a, libglass.Barrier(self.b) as barrier_b:
            time.sleep(SETTLE)
            waiting = threading.Thread(target=barrier_a.sync)
            waiting.start()
            time.sleep(0.2)
            self.assertTrue(waiting.is_alive(), "a must wait for b")
            barrier_b.sync()
            waiting.join(10)
            self.assertFalse(waiting.is_alive())

    def test_messenger(self):
        with libglass.Messenger(self.a, "m") as m_a, libglass.Messenger(self.b, "m") as m_b:
            time.sleep(SETTLE)
            m_a.send(self.b.id, b"oi")
            self.assertEqual(m_b.get(), (self.a.id, b"oi"))

    def test_remote(self):
        with libglass.Remote(self.a, "r", b"1") as r_a, libglass.Remote(self.b, "r", b"2") as r_b:
            time.sleep(SETTLE)
            self.assertEqual(r_a.get(), b"1")
            self.assertEqual(r_a.get(self.b.id), b"2")
            r_a.set(b"3", self.b.id)
            wait_until(lambda: r_b.get() == b"3")

    def test_alias(self):
        with libglass.Alias(self.a, "al") as alias_a, libglass.Alias(self.b, "al") as alias_b:
            time.sleep(SETTLE)
            self.assertIsNone(alias_b.association)
            alias_a.associate(self.b.id, "left")
            wait_until(lambda: alias_b.association == "left")

    def test_errors_become_exceptions(self):
        with libglass.Event(self.a, "dup"):
            with self.assertRaises(libglass.GlassError):
                libglass.Event(self.a, "dup")
        with self.assertRaises(libglass.GlassError):
            libglass.Client("127.0.0.1:1", "nobody")
        with self.assertRaises(libglass.GlassError):
            libglass.Server("not an address")

    def test_kill_all_stops_the_server(self):
        self.a.kill_all()
        self.server.wait()
        wait_until(lambda: not self.b.is_connected)


class Reconnect(unittest.TestCase):
    def test_client_comes_back_to_a_restarted_server(self):
        server = libglass.Server()
        address = server.address
        with libglass.Client(address, "a") as a:
            a.set_reconnect(20)
            old_id = a.id
            server.close()
            wait_until(lambda: not a.is_connected)
            with libglass.Server(address):
                wait_until(lambda: a.is_connected)
                self.assertEqual(a.id, old_id)


class WithPeers(unittest.TestCase):
    def test_global_objects_cross_peers(self):
        with libglass.Server(peer=True) as p1:
            with libglass.Server(peer=True, seed=p1.address) as p2:
                wait_until(lambda: p1.peer_count == 1)
                with libglass.Client(p1.address, "a") as a, libglass.Client(p2.address, "b") as b:
                    with libglass.Event(a, "ev", True) as ev_a, libglass.Event(b, "ev", True) as ev_b:
                        time.sleep(SETTLE)
                        ev_a.enqueue(b"across")
                        self.assertEqual(ev_b.get(), b"across")


def sctp_works():
    try:
        libglass.Server(sctp=True).close()
        return True
    except libglass.GlassError:
        if os.environ.get("LIBGLASS_REQUIRE_SCTP"):
            raise
        return False


@unittest.skipUnless(sctp_works(), "SCTP does not work here")
class WithSctp(unittest.TestCase):
    def test_event(self):
        with libglass.Server(sctp=True) as server:
            with libglass.Client(server.address, "a", sctp=True) as a:
                with libglass.Client(server.address, "b", sctp=True) as b:
                    with libglass.Event(a, "ev") as ev_a, libglass.Event(b, "ev") as ev_b:
                        time.sleep(SETTLE)
                        ev_a.enqueue(b"sctp")
                        self.assertEqual(ev_b.get(), b"sctp")

    def test_peers(self):
        with libglass.Server(peer=True, sctp=True) as p1:
            with libglass.Server(peer=True, seed=p1.address, sctp=True) as p2:
                wait_until(lambda: p1.peer_count == 1)

    def test_tcp_client_does_not_reach_it(self):
        with libglass.Server(sctp=True) as server:
            with self.assertRaises(libglass.GlassError):
                libglass.Client(server.address, "a")


if __name__ == "__main__":
    unittest.main()
