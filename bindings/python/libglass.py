"""Python bindings for libglass, through its C interface.

Build the library with `cargo build` first. The environment variable LIBGLASS_LIB
can point to it, otherwise it is searched for in the `target` directory.
"""

import ctypes
import os
import sys
from ctypes import (
    POINTER,
    byref,
    c_char_p,
    c_int,
    c_size_t,
    c_ubyte,
    c_uint16,
    c_uint32,
    c_uint64,
    c_void_p,
)
from pathlib import Path

EVERYBODY = 1

_OK = 0
_EMPTY = 1


class GlassError(Exception):
    pass


class Timeout(GlassError):
    pass


def _find_library():
    configured = os.environ.get("LIBGLASS_LIB")
    if configured:
        return configured
    names = {"win32": "libglass.dll", "darwin": "liblibglass.dylib"}
    name = names.get(sys.platform, "liblibglass.so")
    target = os.environ.get("CARGO_TARGET_DIR")
    root = Path(target) if target else Path(__file__).resolve().parents[2] / "target"
    for profile in ("release", "debug"):
        path = root / profile / name
        if path.exists():
            return str(path)
    raise GlassError(f"{name} not found in {root}: run `cargo build` first")


_lib = ctypes.CDLL(_find_library())
_Bytes = POINTER(c_ubyte)


def _declare(name, restype, *argtypes):
    function = getattr(_lib, "glass_" + name)
    function.restype = restype
    function.argtypes = argtypes


_declare("last_error", c_char_p)
_declare("free_bytes", None, _Bytes, c_size_t)
_declare("server_new", c_void_p, c_char_p)
_declare("server_new_peer", c_void_p, c_char_p, c_char_p)
_declare("server_new_sctp", c_void_p, c_char_p)
_declare("server_new_peer_sctp", c_void_p, c_char_p, c_char_p)
_declare("server_port", c_uint16, c_void_p)
_declare("server_total_nodes", c_size_t, c_void_p)
_declare("server_peer_count", c_size_t, c_void_p)
_declare("server_wait", c_int, c_void_p)
_declare("server_free", None, c_void_p)
_declare("client_connect", c_void_p, c_char_p, c_char_p)
_declare("client_connect_sctp", c_void_p, c_char_p, c_char_p)
_declare("client_set_reconnect", None, c_void_p, c_uint64)
_declare("client_id", c_uint32, c_void_p)
_declare("client_is_master", c_int, c_void_p)
_declare("client_is_connected", c_int, c_void_p)
_declare("client_total_nodes", c_size_t, c_void_p)
_declare("client_kill_all", c_int, c_void_p)
_declare("client_free", None, c_void_p)
_declare("event_new", c_void_p, c_void_p, c_char_p, c_int)
_declare("event_enqueue", c_int, c_void_p, c_char_p, c_size_t, c_uint32, c_int)
_declare("event_get", c_int, c_void_p, c_uint64, POINTER(_Bytes), POINTER(c_size_t))
_declare(
    "event_get_with_sender",
    c_int,
    c_void_p,
    c_uint64,
    POINTER(c_uint32),
    POINTER(_Bytes),
    POINTER(c_size_t),
)
_declare("event_queue_size", c_size_t, c_void_p)
_declare("event_free", None, c_void_p)
_declare("shared_new", c_void_p, c_void_p, c_char_p, c_char_p, c_size_t, c_int)
_declare("shared_set", c_int, c_void_p, c_char_p, c_size_t)
_declare("shared_get", c_int, c_void_p, POINTER(_Bytes), POINTER(c_size_t))
_declare("shared_send_update", c_int, c_void_p)
_declare("shared_get_update", c_int, c_void_p)
_declare("shared_free", None, c_void_p)
_declare("barrier_new", c_void_p, c_void_p, c_uint32, c_uint32, c_int, c_int)
_declare("barrier_sync", c_int, c_void_p)
_declare("barrier_free", None, c_void_p)
_declare("messenger_new", c_void_p, c_void_p, c_char_p, c_int)
_declare("messenger_send", c_int, c_void_p, c_uint32, c_char_p, c_size_t)
_declare("messenger_get", c_int, c_void_p, c_uint64, POINTER(c_uint32), POINTER(_Bytes), POINTER(c_size_t))
_declare("messenger_free", None, c_void_p)
_declare("remote_new", c_void_p, c_void_p, c_char_p, c_char_p, c_size_t)
_declare("remote_set", c_int, c_void_p, c_uint32, c_char_p, c_size_t)
_declare("remote_get", c_int, c_void_p, c_uint32, c_uint64, POINTER(_Bytes), POINTER(c_size_t))
_declare("remote_free", None, c_void_p)
_declare("alias_new", c_void_p, c_void_p, c_char_p, c_int)
_declare("alias_associate", c_int, c_void_p, c_uint32, c_char_p)
_declare("alias_association", c_int, c_void_p, POINTER(_Bytes), POINTER(c_size_t))
_declare("alias_free", None, c_void_p)


def _error():
    return GlassError(_lib.glass_last_error().decode("utf-8", "replace"))


def _created(pointer):
    if not pointer:
        raise _error()
    return pointer


def _check(status):
    if status < 0:
        raise _error()
    if status == _EMPTY:
        raise Timeout()


def _take(function, *args):
    """Calls a function that gives data, which is copied and released."""
    out = _Bytes()
    out_len = c_size_t()
    _check(function(*args, byref(out), byref(out_len)))
    try:
        return ctypes.string_at(out, out_len.value)
    finally:
        _lib.glass_free_bytes(out, out_len)


def _millis(timeout):
    return int(timeout * 1000)


class _Object:
    """Something from the library, released by `close` or by leaving a `with`."""

    _free = None
    _pointer = None

    def close(self):
        if self._pointer:
            self._free(self._pointer)
            self._pointer = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()


class Server(_Object):
    _free = staticmethod(_lib.glass_server_free)

    def __init__(self, addr="127.0.0.1:0", peer=False, seed=None, sctp=False):
        if peer:
            encoded = seed.encode() if seed else None
            new_peer = _lib.glass_server_new_peer_sctp if sctp else _lib.glass_server_new_peer
            self._pointer = _created(new_peer(addr.encode(), encoded))
        else:
            new = _lib.glass_server_new_sctp if sctp else _lib.glass_server_new
            self._pointer = _created(new(addr.encode()))

    @property
    def port(self):
        return _lib.glass_server_port(self._pointer)

    @property
    def address(self):
        return f"127.0.0.1:{self.port}"

    @property
    def total_nodes(self):
        return _lib.glass_server_total_nodes(self._pointer)

    @property
    def peer_count(self):
        return _lib.glass_server_peer_count(self._pointer)

    def wait(self):
        _check(_lib.glass_server_wait(self._pointer))


class Client(_Object):
    _free = staticmethod(_lib.glass_client_free)

    def __init__(self, addr, nickname="", sctp=False):
        connect = _lib.glass_client_connect_sctp if sctp else _lib.glass_client_connect
        self._pointer = _created(connect(addr.encode(), nickname.encode()))

    def set_reconnect(self, window=None):
        """Connect again when the server is lost, trying for up to `window` seconds."""
        _lib.glass_client_set_reconnect(self._pointer, _millis(window) if window else 0)

    @property
    def id(self):
        return _lib.glass_client_id(self._pointer)

    @property
    def is_master(self):
        return bool(_lib.glass_client_is_master(self._pointer))

    @property
    def is_connected(self):
        return bool(_lib.glass_client_is_connected(self._pointer))

    @property
    def total_nodes(self):
        return _lib.glass_client_total_nodes(self._pointer)

    def kill_all(self):
        _check(_lib.glass_client_kill_all(self._pointer))


class Event(_Object):
    _free = staticmethod(_lib.glass_event_free)

    def __init__(self, client, name, is_global=False):
        self._pointer = _created(_lib.glass_event_new(client._pointer, name.encode(), is_global))

    def enqueue(self, data, target=EVERYBODY, send_to_self=False):
        _check(_lib.glass_event_enqueue(self._pointer, data, len(data), target, send_to_self))

    def get(self, timeout=10.0):
        return _take(_lib.glass_event_get, self._pointer, _millis(timeout))

    def get_with_sender(self, timeout=10.0):
        """Gives the client that sent the event, and the data."""
        sender = c_uint32()
        data = _take(
            _lib.glass_event_get_with_sender, self._pointer, _millis(timeout), byref(sender)
        )
        return sender.value, data

    def __len__(self):
        return _lib.glass_event_queue_size(self._pointer)


class Shared(_Object):
    _free = staticmethod(_lib.glass_shared_free)

    def __init__(self, client, name, initial=b"", is_global=False):
        self._pointer = _created(
            _lib.glass_shared_new(client._pointer, name.encode(), initial, len(initial), is_global)
        )

    @property
    def value(self):
        return _take(_lib.glass_shared_get, self._pointer)

    @value.setter
    def value(self, data):
        _check(_lib.glass_shared_set(self._pointer, data, len(data)))

    def send_update(self):
        _check(_lib.glass_shared_send_update(self._pointer))

    def get_update(self):
        _check(_lib.glass_shared_get_update(self._pointer))


class Barrier(_Object):
    _free = staticmethod(_lib.glass_barrier_free)

    def __init__(self, client, number=0, group=0, active=True, is_global=False):
        self._pointer = _created(
            _lib.glass_barrier_new(client._pointer, number, group, active, is_global)
        )

    def sync(self):
        _check(_lib.glass_barrier_sync(self._pointer))


class Messenger(_Object):
    _free = staticmethod(_lib.glass_messenger_free)

    def __init__(self, client, name, is_global=False):
        self._pointer = _created(_lib.glass_messenger_new(client._pointer, name.encode(), is_global))

    def send(self, target, data):
        _check(_lib.glass_messenger_send(self._pointer, target, data, len(data)))

    def get(self, timeout=10.0):
        """Gives the sender and the data."""
        sender = c_uint32()
        data = _take(_lib.glass_messenger_get, self._pointer, _millis(timeout), byref(sender))
        return sender.value, data


class Remote(_Object):
    _free = staticmethod(_lib.glass_remote_free)

    def __init__(self, client, name, initial=b""):
        self._pointer = _created(
            _lib.glass_remote_new(client._pointer, name.encode(), initial, len(initial))
        )

    def set(self, data, node=0):
        _check(_lib.glass_remote_set(self._pointer, node, data, len(data)))

    def get(self, node=0, timeout=10.0):
        return _take(_lib.glass_remote_get, self._pointer, node, _millis(timeout))


class Alias(_Object):
    _free = staticmethod(_lib.glass_alias_free)

    def __init__(self, client, name, is_global=False):
        self._pointer = _created(_lib.glass_alias_new(client._pointer, name.encode(), is_global))

    def associate(self, node, association):
        _check(_lib.glass_alias_associate(self._pointer, node, association.encode()))

    @property
    def association(self):
        try:
            return _take(_lib.glass_alias_association, self._pointer).decode()
        except Timeout:
            return None
