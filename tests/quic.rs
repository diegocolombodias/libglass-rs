//! The library over QUIC: the same objects, with clients and servers using this protocol.

#![cfg(feature = "quic")]

use libglass::*;
use std::net::{SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);
const SETTLE: Duration = Duration::from_millis(300);
const SHORT: Duration = Duration::from_millis(200);
const LARGE_TIMEOUT: Duration = Duration::from_secs(60);

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

fn trust(server: &GlassServer) -> QuicTrust {
    QuicTrust::Fingerprint(server.quic_fingerprint().unwrap())
}

fn client(server: &GlassServer, nickname: &str) -> GlassClient {
    GlassClient::connect_quic(server.local_addr(), nickname, trust(server)).unwrap()
}

fn wait_for_mesh(servers: &[&GlassServer]) {
    for server in servers {
        wait_until("the servers are all connected", || {
            server.peers().len() == servers.len() - 1
        });
    }
}

/// A UDP port with nothing listening.
fn unused_addr() -> SocketAddr {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[test]
fn objects_work_over_quic() {
    let server = GlassServer::bind_quic("127.0.0.1:0").unwrap();
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
    assert!(start.elapsed() < Duration::from_secs(5), "too slow");
    for value in 0..1000 {
        ev_a.enqueue(&value, EVERYBODY_ID, false).unwrap();
    }
    for value in 0..1000 {
        assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), value, "in order");
    }
    thread::scope(|scope| {
        scope.spawn(|| barrier_a.sync().unwrap());
        barrier_b.sync().unwrap();
    });
}

/// More than the flow control window of QUIC, which the writer must wait on.
#[test]
fn packets_larger_than_8_mb_arrive_whole() {
    let server = GlassServer::bind_quic("127.0.0.1:0").unwrap();
    let a = client(&server, "a");
    let b = client(&server, "b");
    let ev_a = Event::<Bytes>::new(&a, "big", Mode::Local).unwrap();
    let ev_b = Event::<Bytes>::new(&b, "big", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let data: Vec<u8> = (0..9_000_000u32).map(|i| (i % 251) as u8).collect();
    ev_a.enqueue(&Bytes(data.clone()), EVERYBODY_ID, false)
        .unwrap();
    assert!(ev_b.get_timeout(LARGE_TIMEOUT).unwrap().0 == data);
}

#[test]
fn stream_transfers_a_file_over_quic() {
    let server = GlassServer::bind_quic("127.0.0.1:0").unwrap();
    let a = client(&server, "a");
    let b = client(&server, "b");
    let stream_a = Stream::new(&a, "file", Mode::Local).unwrap();
    let stream_b = Stream::new(&b, "file", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let data: Vec<u8> = (0..5_000_000u32).map(|i| (i % 253) as u8).collect();
    let b_id = b.id();
    let (sent, (from, size, received)) = thread::scope(|scope| {
        let sender = scope.spawn(|| stream_a.send_from(b_id, &mut data.as_slice()).unwrap());
        let receiver = scope.spawn(|| {
            let mut received = Vec::new();
            let (from, size) = stream_b.receive(&mut received, LARGE_TIMEOUT).unwrap();
            (from, size, received)
        });
        (sender.join().unwrap(), receiver.join().unwrap())
    });
    assert_eq!(sent, data.len() as u64);
    assert_eq!((from, size), (a.id(), sent));
    assert!(received == data);
}

#[test]
fn peers_form_a_group_over_quic() {
    let identity = QuicIdentity::generate().unwrap();
    let p1 = GlassServer::bind_peer_quic("127.0.0.1:0", None, &identity).unwrap();
    let p2 = GlassServer::bind_peer_quic("127.0.0.1:0", Some(&seed(&p1)), &identity).unwrap();
    // The third one only knows the first, but must find the second by itself.
    let p3 = GlassServer::bind_peer_quic("127.0.0.1:0", Some(&seed(&p1)), &identity).unwrap();
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
fn a_peer_with_another_certificate_is_not_trusted() {
    let p1 = GlassServer::bind_peer_quic("127.0.0.1:0", None, &QuicIdentity::generate().unwrap())
        .unwrap();
    let other = QuicIdentity::generate().unwrap();
    let p2 = GlassServer::bind_peer_quic("127.0.0.1:0", Some(&seed(&p1)), &other).unwrap();
    thread::sleep(SETTLE);
    assert!(p1.peers().is_empty());
    assert!(p2.peers().is_empty());
}

#[test]
fn a_peer_on_all_interfaces_advertises_a_usable_address() {
    let identity = QuicIdentity::generate().unwrap();
    let p1 = GlassServer::bind_peer_quic("0.0.0.0:0", None, &identity).unwrap();
    let port = p1.local_addr().port();
    let seed = format!("127.0.0.1:{port}");
    let p2 = GlassServer::bind_peer_quic("0.0.0.0:0", Some(&seed), &identity).unwrap();
    wait_for_mesh(&[&p1, &p2]);
    let advertised = &p1.peers()[&p2.id()].addr;
    assert_eq!(advertised.host, "127.0.0.1");
    assert_eq!(advertised.port, p2.local_addr().port());
}

#[test]
fn gateways_connect_groups_over_quic() {
    let identity = QuicIdentity::generate().unwrap();
    let g1 = GlassServer::bind_remote_peer_quic("127.0.0.1:0", None, &identity).unwrap();
    let g2 =
        GlassServer::bind_remote_peer_quic("127.0.0.1:0", Some(&seed(&g1)), &identity).unwrap();
    let p1 = GlassServer::bind_peer_quic("127.0.0.1:0", Some(&seed(&g1)), &identity).unwrap();
    let p2 = GlassServer::bind_peer_quic("127.0.0.1:0", Some(&seed(&g2)), &identity).unwrap();
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
    let seed = unused_addr().to_string();
    let identity = QuicIdentity::generate().unwrap();
    let start = Instant::now();
    let peer = GlassServer::bind_peer_quic("127.0.0.1:0", Some(&seed), &identity).unwrap();
    // UDP has no refusal, so it takes the whole connect timeout.
    assert!(start.elapsed() < connect_timeout() + Duration::from_secs(2));
    assert!(peer.peers().is_empty());
    assert!(client(&peer, "a").is_master());
}

#[test]
fn connecting_to_nothing_is_an_error_within_the_connect_timeout() {
    let start = Instant::now();
    let trust = QuicTrust::AnyCertificate;
    assert!(GlassClient::connect_quic(unused_addr(), "a", trust).is_err());
    assert!(start.elapsed() < connect_timeout() + Duration::from_secs(2));
}

#[test]
fn connect_retry_quic_waits_for_the_server() {
    // A port that UDP had free a moment ago, and a certificate known in advance.
    let addr = unused_addr();
    let identity = QuicIdentity::generate().unwrap();
    let trust = QuicTrust::Fingerprint(identity.fingerprint());
    assert!(GlassClient::connect_retry_quic(addr, "a", trust, SHORT).is_err());

    let starter = thread::spawn(move || {
        thread::sleep(SHORT);
        GlassServer::bind_peer_quic(addr, None, &identity).unwrap()
    });
    let client = GlassClient::connect_retry_quic(addr, "a", trust, TIMEOUT).unwrap();
    assert!(client.is_master());
    drop(client);
    drop(starter.join().unwrap());
}

#[test]
fn clients_check_the_certificate() {
    let server = GlassServer::bind_quic("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let other = QuicTrust::Fingerprint(QuicIdentity::generate().unwrap().fingerprint());
    assert!(GlassClient::connect_quic(addr, "a", other).is_err());
    assert!(GlassClient::connect_quic(addr, "a", QuicTrust::AnyCertificate).is_ok());
    assert!(GlassClient::connect_quic(addr, "a", trust(&server)).is_ok());
}

#[test]
fn an_identity_can_be_saved_and_loaded() {
    let identity = QuicIdentity::generate().unwrap();
    let certificate = identity.certificate_der().to_vec();
    let key = identity.private_key_der().to_vec();
    let loaded = QuicIdentity::from_der(certificate.clone(), key).unwrap();
    assert_eq!(loaded.fingerprint(), identity.fingerprint());
    assert_eq!(loaded.fingerprint(), QuicFingerprint::of(&certificate));
    assert!(QuicIdentity::from_der(certificate, vec![1, 2, 3]).is_err());

    let server = GlassServer::bind_peer_quic("127.0.0.1:0", None, &loaded).unwrap();
    assert_eq!(server.quic_fingerprint(), Some(identity.fingerprint()));
    assert!(client(&server, "a").is_master());
}

#[test]
fn fingerprints_are_written_and_read_as_hexadecimal() {
    let fingerprint = QuicIdentity::generate().unwrap().fingerprint();
    let text = fingerprint.to_string();
    assert_eq!(text.len(), 64);
    assert_eq!(text.parse::<QuicFingerprint>().unwrap(), fingerprint);
    let with_colons: Vec<String> = fingerprint
        .0
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect();
    assert_eq!(
        with_colons.join(":").parse::<QuicFingerprint>().unwrap(),
        fingerprint
    );
    assert!("abc".parse::<QuicFingerprint>().is_err());
    let not_hexadecimal = format!("g{}", &text[1..]);
    assert!(not_hexadecimal.parse::<QuicFingerprint>().is_err());
}

#[test]
fn tcp_clients_do_not_reach_a_quic_server() {
    let server = GlassServer::bind_quic("127.0.0.1:0").unwrap();
    assert!(GlassClient::connect(server.local_addr(), "a").is_err());
}

#[test]
fn the_end_of_a_connection_is_noticed_on_both_sides() {
    let identity = QuicIdentity::generate().unwrap();
    let p1 = GlassServer::bind_peer_quic("127.0.0.1:0", None, &identity).unwrap();
    let p2 = GlassServer::bind_peer_quic("127.0.0.1:0", Some(&seed(&p1)), &identity).unwrap();
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

/// The last packets before a client goes away still arrive: closing a QUIC connection
/// discards what is in flight, unless it waits.
#[test]
fn what_is_sent_before_dropping_a_client_arrives() {
    let server = GlassServer::bind_quic("127.0.0.1:0").unwrap();
    let a = client(&server, "a");
    let b = client(&server, "b");
    let ev_a = Event::<Bytes>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<Bytes>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let data = Bytes(vec![7u8; 4_000_000]);
    ev_a.enqueue(&data, EVERYBODY_ID, false).unwrap();
    drop(ev_a);
    drop(a);
    assert!(ev_b.get_timeout(LARGE_TIMEOUT).unwrap() == data);
}

/// A client that loses the server connects again through QUIC, checking the same
/// certificate, which a server keeps when it starts with the same identity.
#[test]
fn a_client_connects_again_through_quic() {
    let identity = QuicIdentity::generate().unwrap();
    let server = GlassServer::bind_peer_quic("127.0.0.1:0", None, &identity).unwrap();
    let addr = server.local_addr();
    let a = client(&server, "a");
    a.set_reconnect(Some(Duration::from_secs(20)));
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
    drop(server);
    wait_until("it notices", || !a.is_connected());

    // The old connections take a moment to wind down, and hold the port meanwhile.
    let start = Instant::now();
    let server = loop {
        match GlassServer::bind_peer_quic(addr, None, &identity) {
            Ok(server) => break server,
            Err(_) if start.elapsed() < TIMEOUT => thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("the port is still busy: {e}"),
        }
    };
    wait_until("it is back", || a.is_connected());
    let b = client(&server, "b");
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);
    ev_a.enqueue(&1, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_with_sender_timeout(TIMEOUT).unwrap(), (a.id(), 1));
}
