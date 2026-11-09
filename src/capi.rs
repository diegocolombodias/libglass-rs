//! A C interface, which is what bindings for other languages are built on. See
//! `include/libglass.h` for how to use it.

use crate::wire::Bytes;
use crate::{
    Alias, Barrier, Error, Event, GlassClient, GlassServer, Messenger, Mode, NodeId, Remote,
    Result, Shared,
};
use std::cell::RefCell;
use std::ffi::{c_char, c_int, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::time::Duration;

const GLASS_OK: c_int = 0;
const GLASS_EMPTY: c_int = 1;
const GLASS_ERROR: c_int = -1;

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn set_error(message: &str) {
    let clean = message.replace('\0', " ");
    let text = CString::new(clean).unwrap_or_default();
    LAST_ERROR.with(|last| *last.borrow_mut() = text);
}

/// Runs code that gives a status. A panic must not cross into another language.
fn guard_status(f: impl FnOnce() -> Result<c_int>) -> c_int {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(status)) => status,
        Ok(Err(Error::Timeout)) => {
            set_error("timed out");
            GLASS_EMPTY
        }
        Ok(Err(e)) => {
            set_error(&e.to_string());
            GLASS_ERROR
        }
        Err(_) => {
            set_error("internal error");
            GLASS_ERROR
        }
    }
}

/// Runs code that creates an object. It gives a null pointer if it fails.
fn guard_new<T>(f: impl FnOnce() -> Result<T>) -> *mut T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => Box::into_raw(Box::new(value)),
        Ok(Err(e)) => {
            set_error(&e.to_string());
            ptr::null_mut()
        }
        Err(_) => {
            set_error("internal error");
            ptr::null_mut()
        }
    }
}

/// # Safety
/// The pointer must be null or come from this library, and not have been freed.
unsafe fn object<'a, T>(pointer: *const T) -> Result<&'a T> {
    pointer.as_ref().ok_or(Error::InvalidPacket("null pointer"))
}

/// # Safety
/// The pointer must be null or point to a NUL-terminated string.
unsafe fn text<'a>(pointer: *const c_char) -> Result<&'a str> {
    if pointer.is_null() {
        return Err(Error::InvalidName);
    }
    CStr::from_ptr(pointer)
        .to_str()
        .map_err(|_| Error::InvalidName)
}

/// # Safety
/// The pointer must be valid for `len` bytes, unless `len` is 0.
unsafe fn bytes(data: *const u8, len: usize) -> Bytes {
    if len == 0 || data.is_null() {
        Bytes(Vec::new())
    } else {
        Bytes(std::slice::from_raw_parts(data, len).to_vec())
    }
}

/// # Safety
/// The output pointers must be valid for writes.
unsafe fn give_bytes(data: Vec<u8>, out: *mut *mut u8, out_len: *mut usize) -> Result<()> {
    if out.is_null() || out_len.is_null() {
        return Err(Error::InvalidPacket("null pointer"));
    }
    *out_len = data.len();
    *out = Box::into_raw(data.into_boxed_slice()) as *mut u8;
    Ok(())
}

/// # Safety
/// `pointer` must be a NUL-terminated string, or null.
unsafe fn optional_text<'a>(pointer: *const c_char) -> Result<Option<&'a str>> {
    if pointer.is_null() {
        Ok(None)
    } else {
        text(pointer).map(Some)
    }
}

/// The SCTP functions exist everywhere, so that the header is the same on every system.
#[cfg(not(unix))]
fn no_sctp() -> Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "SCTP is not available on this system",
    )
    .into()
}

fn mode(global: c_int) -> Mode {
    if global != 0 {
        Mode::Global
    } else {
        Mode::Local
    }
}

fn millis(timeout_ms: u64) -> Duration {
    Duration::from_millis(timeout_ms)
}

/// # Safety
/// Takes back an object created by this library. It must not be used afterwards.
unsafe fn free<T>(pointer: *mut T) {
    if !pointer.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(Box::from_raw(pointer))));
    }
}

#[no_mangle]
pub extern "C" fn glass_last_error() -> *const c_char {
    LAST_ERROR.with(|last| last.borrow().as_ptr())
}

/// # Safety
/// `data` and `len` must have been given by this library, and be given back once.
#[no_mangle]
pub unsafe extern "C" fn glass_free_bytes(data: *mut u8, len: usize) {
    if !data.is_null() {
        drop(Box::from_raw(ptr::slice_from_raw_parts_mut(data, len)));
    }
}

// Servers.

/// # Safety
/// `addr` must be a NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn glass_server_new(addr: *const c_char) -> *mut GlassServer {
    guard_new(|| GlassServer::bind(text(addr)?))
}

/// # Safety
/// `addr` must be a NUL-terminated string, and `seed` as well, or null.
#[no_mangle]
pub unsafe extern "C" fn glass_server_new_peer(
    addr: *const c_char,
    seed: *const c_char,
) -> *mut GlassServer {
    guard_new(|| GlassServer::bind_peer(text(addr)?, optional_text(seed)?))
}

/// # Safety
/// `addr` must be a NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn glass_server_new_sctp(addr: *const c_char) -> *mut GlassServer {
    guard_new(|| {
        #[cfg(unix)]
        return GlassServer::bind_sctp(text(addr)?);
        #[cfg(not(unix))]
        {
            let _ = addr;
            Err(no_sctp())
        }
    })
}

/// # Safety
/// `addr` must be a NUL-terminated string, and `seed` as well, or null.
#[no_mangle]
pub unsafe extern "C" fn glass_server_new_peer_sctp(
    addr: *const c_char,
    seed: *const c_char,
) -> *mut GlassServer {
    guard_new(|| {
        #[cfg(unix)]
        return GlassServer::bind_peer_sctp(text(addr)?, optional_text(seed)?);
        #[cfg(not(unix))]
        {
            let _ = (addr, seed);
            Err(no_sctp())
        }
    })
}

/// # Safety
/// `server` must come from this library. Gives 0 if it is null.
#[no_mangle]
pub unsafe extern "C" fn glass_server_port(server: *const GlassServer) -> u16 {
    server.as_ref().map_or(0, |s| s.local_addr().port())
}

/// # Safety
/// `server` must come from this library. Gives 0 if it is null.
#[no_mangle]
pub unsafe extern "C" fn glass_server_total_nodes(server: *const GlassServer) -> usize {
    server.as_ref().map_or(0, |s| s.total_nodes())
}

/// # Safety
/// `server` must come from this library. Gives 0 if it is null.
#[no_mangle]
pub unsafe extern "C" fn glass_server_peer_count(server: *const GlassServer) -> usize {
    server.as_ref().map_or(0, |s| s.peers().len())
}

/// # Safety
/// `server` must come from this library, and not be freed during this call.
#[no_mangle]
pub unsafe extern "C" fn glass_server_wait(server: *const GlassServer) -> c_int {
    guard_status(|| {
        object(server)?.wait();
        Ok(GLASS_OK)
    })
}

/// # Safety
/// `server` must come from this library, or be null.
#[no_mangle]
pub unsafe extern "C" fn glass_server_free(server: *mut GlassServer) {
    free(server);
}

// Clients.

/// # Safety
/// The arguments must be NUL-terminated strings.
#[no_mangle]
pub unsafe extern "C" fn glass_client_connect(
    addr: *const c_char,
    nickname: *const c_char,
) -> *mut GlassClient {
    guard_new(|| GlassClient::connect(text(addr)?, text(nickname)?))
}

/// # Safety
/// The arguments must be NUL-terminated strings.
#[no_mangle]
pub unsafe extern "C" fn glass_client_connect_sctp(
    addr: *const c_char,
    nickname: *const c_char,
) -> *mut GlassClient {
    guard_new(|| {
        #[cfg(unix)]
        return GlassClient::connect_sctp(text(addr)?, text(nickname)?);
        #[cfg(not(unix))]
        {
            let _ = (addr, nickname);
            Err(no_sctp())
        }
    })
}

/// # Safety
/// `client` must come from this library. Gives 0 if it is null.
#[no_mangle]
pub unsafe extern "C" fn glass_client_id(client: *const GlassClient) -> NodeId {
    client.as_ref().map_or(0, |c| c.id())
}

/// # Safety
/// `client` must come from this library. Gives 0 if it is null.
#[no_mangle]
pub unsafe extern "C" fn glass_client_is_master(client: *const GlassClient) -> c_int {
    client.as_ref().is_some_and(|c| c.is_master()) as c_int
}

/// # Safety
/// `client` must come from this library. Gives 0 if it is null.
#[no_mangle]
pub unsafe extern "C" fn glass_client_is_connected(client: *const GlassClient) -> c_int {
    client.as_ref().is_some_and(|c| c.is_connected()) as c_int
}

/// # Safety
/// `client` must come from this library. Gives 0 if it is null.
#[no_mangle]
pub unsafe extern "C" fn glass_client_total_nodes(client: *const GlassClient) -> usize {
    client.as_ref().map_or(0, |c| c.total_nodes())
}

/// # Safety
/// `client` must come from this library.
#[no_mangle]
pub unsafe extern "C" fn glass_client_kill_all(client: *const GlassClient) -> c_int {
    guard_status(|| {
        object(client)?.kill_all()?;
        Ok(GLASS_OK)
    })
}

/// # Safety
/// `client` must come from this library, or be null. Objects created with it stay
/// valid, but stop working, and must still be freed.
#[no_mangle]
pub unsafe extern "C" fn glass_client_free(client: *mut GlassClient) {
    free(client);
}

// Events.

/// # Safety
/// `client` must come from this library, and `name` be a NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn glass_event_new(
    client: *const GlassClient,
    name: *const c_char,
    global: c_int,
) -> *mut Event<Bytes> {
    guard_new(|| Event::new(object(client)?, text(name)?, mode(global)))
}

/// # Safety
/// `event` must come from this library, and `data` be valid for `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn glass_event_enqueue(
    event: *const Event<Bytes>,
    data: *const u8,
    len: usize,
    target: NodeId,
    send_to_self: c_int,
) -> c_int {
    guard_status(|| {
        object(event)?.enqueue(&bytes(data, len), target, send_to_self != 0)?;
        Ok(GLASS_OK)
    })
}

/// Gives `GLASS_EMPTY` if nothing arrived in time.
///
/// # Safety
/// `event` must come from this library. The output pointers must be valid, and
/// what they receive must be given back with `glass_free_bytes`.
#[no_mangle]
pub unsafe extern "C" fn glass_event_get(
    event: *const Event<Bytes>,
    timeout_ms: u64,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    guard_status(|| {
        let Bytes(data) = object(event)?.get_timeout(millis(timeout_ms))?;
        give_bytes(data, out, out_len)?;
        Ok(GLASS_OK)
    })
}

/// # Safety
/// `event` must come from this library. Gives 0 if it is null.
#[no_mangle]
pub unsafe extern "C" fn glass_event_queue_size(event: *const Event<Bytes>) -> usize {
    event.as_ref().map_or(0, |e| e.queue_size())
}

/// # Safety
/// `event` must come from this library, or be null.
#[no_mangle]
pub unsafe extern "C" fn glass_event_free(event: *mut Event<Bytes>) {
    free(event);
}

// Shared variables.

/// # Safety
/// `client` must come from this library, `name` be a NUL-terminated string, and
/// `data` be valid for `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn glass_shared_new(
    client: *const GlassClient,
    name: *const c_char,
    data: *const u8,
    len: usize,
    global: c_int,
) -> *mut Shared<Bytes> {
    guard_new(|| Shared::new(object(client)?, text(name)?, bytes(data, len), mode(global)))
}

/// # Safety
/// `shared` must come from this library, and `data` be valid for `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn glass_shared_set(
    shared: *const Shared<Bytes>,
    data: *const u8,
    len: usize,
) -> c_int {
    guard_status(|| {
        object(shared)?.set(bytes(data, len));
        Ok(GLASS_OK)
    })
}

/// # Safety
/// `shared` must come from this library. The output pointers must be valid, and
/// what they receive must be given back with `glass_free_bytes`.
#[no_mangle]
pub unsafe extern "C" fn glass_shared_get(
    shared: *const Shared<Bytes>,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    guard_status(|| {
        let Bytes(data) = object(shared)?.get();
        give_bytes(data, out, out_len)?;
        Ok(GLASS_OK)
    })
}

/// # Safety
/// `shared` must come from this library.
#[no_mangle]
pub unsafe extern "C" fn glass_shared_send_update(shared: *const Shared<Bytes>) -> c_int {
    guard_status(|| {
        object(shared)?.send_update()?;
        Ok(GLASS_OK)
    })
}

/// # Safety
/// `shared` must come from this library.
#[no_mangle]
pub unsafe extern "C" fn glass_shared_get_update(shared: *const Shared<Bytes>) -> c_int {
    guard_status(|| {
        object(shared)?.get_update()?;
        Ok(GLASS_OK)
    })
}

/// # Safety
/// `shared` must come from this library, or be null.
#[no_mangle]
pub unsafe extern "C" fn glass_shared_free(shared: *mut Shared<Bytes>) {
    free(shared);
}

// Barriers.

/// # Safety
/// `client` must come from this library.
#[no_mangle]
pub unsafe extern "C" fn glass_barrier_new(
    client: *const GlassClient,
    number: u32,
    group: u32,
    active: c_int,
    global: c_int,
) -> *mut Barrier {
    guard_new(|| Barrier::new(object(client)?, number, group, active != 0, mode(global)))
}

/// # Safety
/// `barrier` must come from this library, and not be freed during this call.
#[no_mangle]
pub unsafe extern "C" fn glass_barrier_sync(barrier: *const Barrier) -> c_int {
    guard_status(|| {
        object(barrier)?.sync()?;
        Ok(GLASS_OK)
    })
}

/// # Safety
/// `barrier` must come from this library, or be null.
#[no_mangle]
pub unsafe extern "C" fn glass_barrier_free(barrier: *mut Barrier) {
    free(barrier);
}

// Messengers.

/// # Safety
/// `client` must come from this library, and `name` be a NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn glass_messenger_new(
    client: *const GlassClient,
    name: *const c_char,
    global: c_int,
) -> *mut Messenger<Bytes> {
    guard_new(|| Messenger::new(object(client)?, text(name)?, mode(global)))
}

/// # Safety
/// `messenger` must come from this library, and `data` be valid for `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn glass_messenger_send(
    messenger: *const Messenger<Bytes>,
    target: NodeId,
    data: *const u8,
    len: usize,
) -> c_int {
    guard_status(|| {
        object(messenger)?.send(target, &bytes(data, len))?;
        Ok(GLASS_OK)
    })
}

/// Gives `GLASS_EMPTY` if nothing arrived in time.
///
/// # Safety
/// `messenger` must come from this library. The output pointers must be valid,
/// and the data must be given back with `glass_free_bytes`.
#[no_mangle]
pub unsafe extern "C" fn glass_messenger_get(
    messenger: *const Messenger<Bytes>,
    timeout_ms: u64,
    from: *mut NodeId,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    guard_status(|| {
        if from.is_null() {
            return Err(Error::InvalidPacket("null pointer"));
        }
        let (sender, Bytes(data)) = object(messenger)?.get_timeout(millis(timeout_ms))?;
        give_bytes(data, out, out_len)?;
        *from = sender;
        Ok(GLASS_OK)
    })
}

/// # Safety
/// `messenger` must come from this library, or be null.
#[no_mangle]
pub unsafe extern "C" fn glass_messenger_free(messenger: *mut Messenger<Bytes>) {
    free(messenger);
}

// Remote variables.

/// # Safety
/// `client` must come from this library, `name` be a NUL-terminated string, and
/// `data` be valid for `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn glass_remote_new(
    client: *const GlassClient,
    name: *const c_char,
    data: *const u8,
    len: usize,
) -> *mut Remote<Bytes> {
    guard_new(|| Remote::new(object(client)?, text(name)?, &bytes(data, len)))
}

/// Sets the value in a node. 0 means the local value.
///
/// # Safety
/// `remote` must come from this library, and `data` be valid for `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn glass_remote_set(
    remote: *const Remote<Bytes>,
    id: NodeId,
    data: *const u8,
    len: usize,
) -> c_int {
    guard_status(|| {
        object(remote)?.set_value(id, &bytes(data, len))?;
        Ok(GLASS_OK)
    })
}

/// Gets the value in a node. 0 means the local value. Gives `GLASS_EMPTY` if there
/// was no answer in time.
///
/// # Safety
/// `remote` must come from this library. The output pointers must be valid, and
/// what they receive must be given back with `glass_free_bytes`.
#[no_mangle]
pub unsafe extern "C" fn glass_remote_get(
    remote: *const Remote<Bytes>,
    id: NodeId,
    timeout_ms: u64,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    guard_status(|| {
        let Bytes(data) = object(remote)?.get_value_timeout(id, millis(timeout_ms))?;
        give_bytes(data, out, out_len)?;
        Ok(GLASS_OK)
    })
}

/// # Safety
/// `remote` must come from this library, or be null.
#[no_mangle]
pub unsafe extern "C" fn glass_remote_free(remote: *mut Remote<Bytes>) {
    free(remote);
}

// Aliases. Values stay in the other language, which only needs the association.

/// # Safety
/// `client` must come from this library, and `name` be a NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn glass_alias_new(
    client: *const GlassClient,
    name: *const c_char,
    global: c_int,
) -> *mut Alias<()> {
    guard_new(|| Alias::new(object(client)?, text(name)?, (), mode(global)))
}

/// # Safety
/// `alias` must come from this library, and `association` be a NUL-terminated
/// string.
#[no_mangle]
pub unsafe extern "C" fn glass_alias_associate(
    alias: *const Alias<()>,
    id: NodeId,
    association: *const c_char,
) -> c_int {
    guard_status(|| {
        object(alias)?.associate(id, text(association)?)?;
        Ok(GLASS_OK)
    })
}

/// Gives `GLASS_EMPTY` if this client has no association yet.
///
/// # Safety
/// `alias` must come from this library. The output pointers must be valid, and
/// what they receive must be given back with `glass_free_bytes`.
#[no_mangle]
pub unsafe extern "C" fn glass_alias_association(
    alias: *const Alias<()>,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    guard_status(|| match object(alias)?.association() {
        Some(association) => {
            give_bytes(association.into_bytes(), out, out_len)?;
            Ok(GLASS_OK)
        }
        None => Ok(GLASS_EMPTY),
    })
}

/// # Safety
/// `alias` must come from this library, or be null.
#[no_mangle]
pub unsafe extern "C" fn glass_alias_free(alias: *mut Alias<()>) {
    free(alias);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn c(text: &str) -> CString {
        CString::new(text).unwrap()
    }

    #[test]
    fn events_go_through_the_c_interface() {
        unsafe {
            let server = glass_server_new(c("127.0.0.1:0").as_ptr());
            assert!(!server.is_null());
            let addr = c(&format!("127.0.0.1:{}", glass_server_port(server)));
            let a = glass_client_connect(addr.as_ptr(), c("a").as_ptr());
            let b = glass_client_connect(addr.as_ptr(), c("b").as_ptr());
            assert_eq!(glass_client_is_master(a), 1);
            assert_eq!(glass_client_is_master(b), 0);

            let ev_a = glass_event_new(a, c("ev").as_ptr(), 0);
            let ev_b = glass_event_new(b, c("ev").as_ptr(), 0);
            thread::sleep(Duration::from_millis(300));

            let data = [1u8, 0, 2];
            assert_eq!(
                glass_event_enqueue(ev_a, data.as_ptr(), data.len(), crate::EVERYBODY_ID, 0),
                GLASS_OK
            );
            let (mut out, mut len) = (ptr::null_mut(), 0);
            assert_eq!(glass_event_get(ev_b, 10_000, &mut out, &mut len), GLASS_OK);
            assert_eq!(std::slice::from_raw_parts(out, len), data);
            glass_free_bytes(out, len);

            assert_eq!(glass_event_get(ev_b, 10, &mut out, &mut len), GLASS_EMPTY);

            glass_event_free(ev_a);
            glass_event_free(ev_b);
            glass_client_free(a);
            glass_client_free(b);
            glass_server_free(server);
        }
    }

    #[test]
    fn errors_are_reported_instead_of_crashing() {
        unsafe {
            let bad = glass_server_new(c("not an address").as_ptr());
            assert!(bad.is_null());
            let message = CStr::from_ptr(glass_last_error()).to_str().unwrap();
            assert!(!message.is_empty());

            assert!(glass_server_new(ptr::null()).is_null());
            assert_eq!(glass_barrier_sync(ptr::null()), GLASS_ERROR);
            assert!(glass_event_new(ptr::null(), c("e").as_ptr(), 0).is_null());
            assert_eq!(glass_client_id(ptr::null()), 0);

            // Freeing nothing is harmless.
            glass_server_free(ptr::null_mut());
            glass_client_free(ptr::null_mut());
            glass_free_bytes(ptr::null_mut(), 0);
        }
    }

    #[test]
    fn empty_data_can_be_given_and_freed() {
        let (mut out, mut len) = (ptr::null_mut(), 1);
        unsafe {
            give_bytes(Vec::new(), &mut out, &mut len).unwrap();
            assert_eq!(len, 0);
            glass_free_bytes(out, len);
        }
    }
}
