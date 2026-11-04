//! The library over SCTP: the same objects, with clients and servers using this protocol.
//!
//! If the kernel has no SCTP, these tests are skipped, unless the variable
//! `LIBGLASS_REQUIRE_SCTP` is set, as it is in `scripts/test-linux.sh`.

#![cfg(unix)]

use libglass::*;
use std::net::{SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);
const SETTLE: Duration = Duration::from_millis(300);
const SHORT: Duration = Duration::from_millis(200);
/// Large transfers sometimes stall for a while on loopback, in the kernel.
const LARGE_TIMEOUT: Duration = Duration::from_secs(60);

fn has_sctp() -> bool {
    match GlassServer::bind_sctp("127.0.0.1:0") {
        Ok(_) => true,
        Err(e) if std::env::var_os("LIBGLASS_REQUIRE_SCTP").is_some() => {
            panic!("SCTP is required, but it does not work here: {e}")
        }
        Err(e) => {
            eprintln!("skipped, because SCTP does not work here: {e}");
            false
        }
    }
}

fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn seed(server: &GlassServer) -> String {
    server.local_addr().to_string()
}

fn client(server: &GlassServer, nickname: &str) -> GlassClient {
    GlassClient::connect_sctp(server.local_addr(), nickname).unwrap()
}

fn wait_for_mesh(servers: &[&GlassServer]) {
    for server in servers {
        wait_until("the servers are all connected", || {
            server.peers().len() == servers.len() - 1
        });
    }
}

/// A port with nothing listening. SCTP has its own ports, but none is in use here.
fn unused_addr() -> SocketAddr {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[test]
fn objects_work_over_sctp() {
    if !has_sctp() {
        return;
    }
    let server = GlassServer::bind_sctp("127.0.0.1:0").unwrap();
    let a = client(&server, "a");
    let b = client(&server, "b");
    assert!(a.is_master());
    assert!(!a.uses_udp());
    wait_until("the server sees both", || server.total_nodes() == 3);

    let ev_a = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
    let barrier_a = Barrier::new(&a, 0, 0, true, Mode::Local).unwrap();
    let barrier_b = Barrier::new(&b, 0, 0, true, Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let start = Instant::now();
    for value in 0..100 {
        ev_a.enqueue(&value, EVERYBODY_ID, false).unwrap();
        assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), value, "in order");
    }
    // Without SCTP_NODELAY, each one waits about 200 ms for an acknowledgement.
    assert!(start.elapsed() < Duration::from_secs(5), "too slow");
    thread::scope(|scope| {
        scope.spawn(|| barrier_a.sync().unwrap());
        barrier_b.sync().unwrap();
    });
}

/// Linux refuses a single SCTP write larger than the send buffer.
#[test]
fn packets_larger_than_the_send_buffer_arrive_whole() {
    if !has_sctp() {
        return;
    }
    let server = GlassServer::bind_sctp("127.0.0.1:0").unwrap();
    let a = client(&server, "a");
    let b = client(&server, "b");
    let ev_a = Event::<Bytes>::new(&a, "big", Mode::Local).unwrap();
    let ev_b = Event::<Bytes>::new(&b, "big", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let data: Vec<u8> = (0..8_000_000u32).map(|i| (i % 251) as u8).collect();
    ev_a.enqueue(&Bytes(data.clone()), EVERYBODY_ID, false)
        .unwrap();
    assert!(ev_b.get_timeout(LARGE_TIMEOUT).unwrap().0 == data);
}

#[test]
fn stream_transfers_a_file_over_sctp() {
    if !has_sctp() {
        return;
    }
    let server = GlassServer::bind_sctp("127.0.0.1:0").unwrap();
    let a = client(&server, "a");
    let b = client(&server, "b");
    let stream_a = Stream::new(&a, "file", Mode::Local).unwrap();
    let stream_b = Stream::new(&b, "file", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 253) as u8).collect();
    let sent = stream_a.send_from(b.id(), &mut data.as_slice()).unwrap();
    assert_eq!(sent, data.len() as u64);

    let mut received = Vec::new();
    let (from, size) = stream_b.receive(&mut received, LARGE_TIMEOUT).unwrap();
    assert_eq!((from, size), (a.id(), sent));
    assert!(received == data);
}

#[test]
fn peers_form_a_group_over_sctp() {
    if !has_sctp() {
        return;
    }
    let p1 = GlassServer::bind_peer_sctp("127.0.0.1:0", None).unwrap();
    let p2 = GlassServer::bind_peer_sctp("127.0.0.1:0", Some(&seed(&p1))).unwrap();
    // The third one only knows the first, but must find the second by itself.
    let p3 = GlassServer::bind_peer_sctp("127.0.0.1:0", Some(&seed(&p1))).unwrap();
    wait_for_mesh(&[&p1, &p2, &p3]);
    assert_eq!(p1.peers()[&p2.id()].kind, LinkKind::Peer);

    let clients = [client(&p1, "a"), client(&p2, "b"), client(&p3, "c")];
    let events: Vec<Event<i32>> = clients
        .iter()
        .map(|c| Event::new(c, "ev", Mode::Global).unwrap())
        .collect();
    let barriers: Vec<Barrier> = clients
        .iter()
        .map(|c| Barrier::new(c, 0, 0, true, Mode::Global).unwrap())
        .collect();
    thread::sleep(SETTLE);

    events[0].enqueue(&7, EVERYBODY_ID, false).unwrap();
    assert_eq!(events[1].get_timeout(TIMEOUT).unwrap(), 7);
    assert_eq!(events[2].get_timeout(TIMEOUT).unwrap(), 7);
    thread::sleep(SETTLE);
    assert!(events.iter().all(|e| e.is_empty()), "exactly once");

    thread::scope(|scope| {
        for barrier in &barriers {
            scope.spawn(|| barrier.sync().unwrap());
        }
    });
}

#[test]
fn a_peer_on_all_interfaces_advertises_a_usable_address() {
    if !has_sctp() {
        return;
    }
    let p1 = GlassServer::bind_peer_sctp("0.0.0.0:0", None).unwrap();
    let port = p1.local_addr().port();
    let p2 = GlassServer::bind_peer_sctp("0.0.0.0:0", Some(&format!("127.0.0.1:{port}"))).unwrap();
    wait_for_mesh(&[&p1, &p2]);
    let advertised = &p1.peers()[&p2.id()].addr;
    assert_eq!(advertised.host, "127.0.0.1");
    assert_eq!(advertised.port, p2.local_addr().port());
}

#[test]
fn gateways_connect_groups_over_sctp() {
    if !has_sctp() {
        return;
    }
    let g1 = GlassServer::bind_remote_peer_sctp("127.0.0.1:0", None).unwrap();
    let g2 = GlassServer::bind_remote_peer_sctp("127.0.0.1:0", Some(&seed(&g1))).unwrap();
    let p1 = GlassServer::bind_peer_sctp("127.0.0.1:0", Some(&seed(&g1))).unwrap();
    let p2 = GlassServer::bind_peer_sctp("127.0.0.1:0", Some(&seed(&g2))).unwrap();
    wait_until("the gateways see the rest", || {
        g1.peers().len() == 2 && g2.peers().len() == 2
    });
    assert!(g1
        .peers()
        .values()
        .any(|info| info.kind == LinkKind::Remote));

    let a = client(&p1, "a");
    let b = client(&p2, "b");
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Global).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Global).unwrap();
    thread::sleep(SETTLE);

    ev_a.enqueue(&5, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 5);
    assert!(matches!(ev_b.get_timeout(SHORT), Err(Error::Timeout)));
}

#[test]
fn peer_with_unreachable_seed_starts_a_new_group() {
    if !has_sctp() {
        return;
    }
    let seed = unused_addr().to_string();
    let peer = GlassServer::bind_peer_sctp("127.0.0.1:0", Some(&seed)).unwrap();
    assert!(peer.peers().is_empty());
    assert!(client(&peer, "a").is_master());
}

#[test]
fn connecting_to_nothing_is_an_error() {
    if !has_sctp() {
        return;
    }
    let start = Instant::now();
    assert!(GlassClient::connect_sctp(unused_addr(), "a").is_err());
    assert!(start.elapsed() < TIMEOUT);
}

#[test]
fn tcp_clients_do_not_reach_an_sctp_server() {
    if !has_sctp() {
        return;
    }
    let server = GlassServer::bind_sctp("127.0.0.1:0").unwrap();
    assert!(GlassClient::connect(server.local_addr(), "a").is_err());
}

#[test]
fn the_end_of_a_connection_is_noticed_on_both_sides() {
    if !has_sctp() {
        return;
    }
    let p1 = GlassServer::bind_peer_sctp("127.0.0.1:0", None).unwrap();
    let p2 = GlassServer::bind_peer_sctp("127.0.0.1:0", Some(&seed(&p1))).unwrap();
    wait_for_mesh(&[&p1, &p2]);
    let a = client(&p1, "a");
    let b = client(&p1, "b");
    wait_until("the server sees both", || p1.total_nodes() == 3);

    drop(b);
    wait_until("the client leaves", || p1.total_nodes() == 2);
    drop(p2);
    wait_until("the peer leaves", || p1.peers().is_empty());
    drop(p1);
    wait_until("the client notices", || !a.is_connected());
}
