//! Clients that connect again when they lose the server.

use libglass::*;
use std::io;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);
const SETTLE: Duration = Duration::from_millis(300);
const SHORT: Duration = Duration::from_millis(200);
const WINDOW: Duration = Duration::from_secs(20);

fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn reconnecting(addr: SocketAddr, nickname: &str) -> GlassClient {
    let client = GlassClient::connect(addr, nickname).unwrap();
    client.set_reconnect(Some(WINDOW));
    client
}

/// Forwards the connections it gets to another address, until they are cut.
struct Proxy {
    addr: SocketAddr,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
}

impl Proxy {
    fn new(target: SocketAddr) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let sockets = Arc::<Mutex<Vec<TcpStream>>>::default();
        let kept = sockets.clone();
        thread::spawn(move || {
            for incoming in listener.incoming() {
                let (Ok(inside), Ok(outside)) = (incoming, TcpStream::connect(target)) else {
                    return;
                };
                let mut kept = kept.lock().unwrap();
                kept.push(inside.try_clone().unwrap());
                kept.push(outside.try_clone().unwrap());
                pipe(inside.try_clone().unwrap(), outside.try_clone().unwrap());
                pipe(outside, inside);
            }
        });
        Proxy { addr, sockets }
    }

    /// Breaks the connections, as a failure of the network would, while both ends
    /// keep running.
    fn cut(&self) {
        for socket in self.sockets.lock().unwrap().drain(..) {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }
}

fn pipe(mut from: TcpStream, mut to: TcpStream) {
    thread::spawn(move || {
        let _ = io::copy(&mut from, &mut to);
        let _ = to.shutdown(Shutdown::Both);
    });
}

#[test]
fn clients_connect_again_to_a_server_that_restarts() {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let a = reconnecting(addr, "a");
    let b = reconnecting(addr, "b");
    let (id_a, id_b) = (a.id(), b.id());
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
    let barrier_a = Barrier::new(&a, 0, 0, true, Mode::Local).unwrap();
    let barrier_b = Barrier::new(&b, 0, 0, true, Mode::Local).unwrap();
    thread::sleep(SETTLE);
    ev_a.enqueue(&1, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 1);

    drop(server);
    wait_until("they notice", || !a.is_connected() && !b.is_connected());
    // While away, a wait fails instead of hanging.
    assert!(matches!(ev_b.get_timeout(SHORT), Err(Error::Disconnected)));

    let server = GlassServer::bind(addr).unwrap();
    wait_until("they are back", || a.is_connected() && b.is_connected());
    assert_eq!((a.id(), b.id()), (id_a, id_b), "they keep their ids");
    assert_eq!(server.total_nodes(), 3);

    // Their objects exist in the new server.
    wait_until("events work again", || {
        ev_a.enqueue(&2, EVERYBODY_ID, false).unwrap();
        matches!(ev_b.get_timeout(SHORT), Ok(2))
    });
    thread::scope(|scope| {
        let waiting = scope.spawn(|| barrier_a.sync().unwrap());
        thread::sleep(SHORT);
        assert!(!waiting.is_finished(), "a must wait for b");
        barrier_b.sync().unwrap();
    });
}

#[test]
fn a_client_connects_again_after_the_network_fails() {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let proxy = Proxy::new(server.local_addr());
    let a = reconnecting(proxy.addr, "a");
    let b = GlassClient::connect(server.local_addr(), "b").unwrap();
    let id_a = a.id();
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    proxy.cut();
    wait_until("events from a cross again", || {
        ev_a.enqueue(&1, EVERYBODY_ID, false).is_ok() && matches!(ev_b.get_timeout(SHORT), Ok(1))
    });
    assert_eq!(a.id(), id_a);
    ev_b.enqueue(&2, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_a.get_with_sender_timeout(TIMEOUT).unwrap(), (b.id(), 2));
    assert_eq!(server.client_list().len(), 2);
}

#[test]
fn without_asking_a_client_stays_disconnected() {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let a = GlassClient::connect(addr, "a").unwrap();
    drop(server);
    wait_until("it notices", || !a.is_connected());

    let _server = GlassServer::bind(addr).unwrap();
    thread::sleep(Duration::from_secs(1));
    assert!(!a.is_connected());
}

#[test]
fn dropping_a_client_that_is_trying_to_connect_again_is_quick() {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let a = GlassClient::connect(server.local_addr(), "a").unwrap();
    a.set_reconnect(Some(Duration::from_secs(60)));
    drop(server);
    wait_until("it notices", || !a.is_connected());
    thread::sleep(Duration::from_millis(500));

    let start = Instant::now();
    drop(a);
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "{:?}",
        start.elapsed()
    );
}

#[test]
fn udp_is_not_set_up_again_but_events_still_arrive() {
    let server = GlassServer::bind_with_udp("127.0.0.1:0").unwrap();
    let proxy = Proxy::new(server.local_addr());
    let a = GlassClient::connect_with_udp(proxy.addr, "a").unwrap();
    a.set_reconnect(Some(WINDOW));
    assert!(a.uses_udp());
    let b = GlassClient::connect(server.local_addr(), "b").unwrap();
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    proxy.cut();
    wait_until("it is back", || a.is_connected() && !a.uses_udp());
    wait_until("unreliable events go by the connection", || {
        ev_a.enqueue_unreliable(&3, EVERYBODY_ID).unwrap();
        matches!(ev_b.get_timeout(SHORT), Ok(3))
    });
}
