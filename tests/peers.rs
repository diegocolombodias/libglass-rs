//! Several servers working together, based on the scenarios of the C++ suite in
//! `test/peer`: two or three peers, with clients connected to different ones.

use libglass::packet::Packet;
use libglass::protocol::{PeerAddr, RootMsg};
use libglass::*;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);
/// Time for messages to go through the servers.
const SETTLE: Duration = Duration::from_millis(300);
const SHORT: Duration = Duration::from_millis(200);

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

fn first_peer() -> GlassServer {
    GlassServer::bind_peer("127.0.0.1:0", None).unwrap()
}

fn join(group: &GlassServer) -> GlassServer {
    GlassServer::bind_peer("127.0.0.1:0", Some(&seed(group))).unwrap()
}

fn client(server: &GlassServer, nickname: &str) -> GlassClient {
    GlassClient::connect(server.local_addr(), nickname).unwrap()
}

fn wait_for_mesh(servers: &[&GlassServer]) {
    for server in servers {
        wait_until("the servers are all connected", || {
            server.peers().len() == servers.len() - 1
        });
    }
}

/// Two connected peers, each with a client.
fn two_peers() -> (GlassServer, GlassServer, GlassClient, GlassClient) {
    let p1 = first_peer();
    let p2 = join(&p1);
    wait_for_mesh(&[&p1, &p2]);
    let a = client(&p1, "a");
    let b = client(&p2, "b");
    (p1, p2, a, b)
}

#[test]
fn peers_connect_to_each_other_through_a_seed() {
    let p1 = first_peer();
    let p2 = join(&p1);
    // The third one only knows the first, but must find the second by itself.
    let p3 = join(&p1);
    wait_for_mesh(&[&p1, &p2, &p3]);

    assert!(p3.peers().contains_key(&p2.id()));
    assert!(p2.peers().contains_key(&p3.id()));
    assert_eq!(p1.peers()[&p2.id()].kind, LinkKind::Peer);
    assert_eq!(p1.peers()[&p2.id()].addr.port, p2.local_addr().port());
}

#[test]
fn peer_with_unreachable_seed_starts_a_new_group() {
    let unused = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    let peer = GlassServer::bind_peer("127.0.0.1:0", Some(&unused.to_string())).unwrap();
    assert!(peer.peers().is_empty());
    let a = client(&peer, "a");
    assert!(a.is_master());
}

/// Based on `autopeermaster.cpp`.
#[test]
fn each_server_has_its_own_clients_and_master() {
    let (p1, p2, a, b) = two_peers();
    let a2 = client(&p1, "a2");

    assert_eq!(a.id() & 0xFFFF_0000, p1.id());
    assert_eq!(b.id() & 0xFFFF_0000, p2.id());
    assert_eq!(a.server_id(), p1.id());
    assert_eq!(b.server_id(), p2.id());

    assert!(a.is_master());
    assert!(b.is_master());
    assert!(!a2.is_master());

    wait_until("a sees a2", || a.total_nodes() == 3);
    assert_eq!(b.total_nodes(), 2);
    assert_eq!(p1.client_list().len(), 2);
    assert_eq!(p2.client_list().len(), 1);
}

#[test]
fn events_tell_who_sent_them_across_peers() {
    let (p1, _p2, a, b) = two_peers();
    let a2 = client(&p1, "a2");
    let events: Vec<Event<i32>> = [&a, &b, &a2]
        .iter()
        .map(|c| Event::new(c, "ev", Mode::Global).unwrap())
        .collect();
    thread::sleep(SETTLE);

    // From another server, from the same server, and to itself.
    events[0].enqueue(&1, EVERYBODY_ID, true).unwrap();
    assert_eq!(
        events[1].get_with_sender_timeout(TIMEOUT).unwrap(),
        (a.id(), 1)
    );
    assert_eq!(
        events[2].get_with_sender_timeout(TIMEOUT).unwrap(),
        (a.id(), 1)
    );
    assert_eq!(
        events[0].get_with_sender_timeout(TIMEOUT).unwrap(),
        (a.id(), 1)
    );

    events[1].enqueue(&2, a2.id(), false).unwrap();
    assert_eq!(
        events[2].get_with_sender_timeout(TIMEOUT).unwrap(),
        (b.id(), 2)
    );
    assert!(events[0].try_get_with_sender().unwrap().is_none());
}

/// Based on `autopeereventtest.cpp`.
#[test]
fn global_events_cross_peers_and_local_ones_do_not() {
    let (_p1, _p2, a, b) = two_peers();
    let local_a = Event::<i32>::new(&a, "local", Mode::Local).unwrap();
    let local_b = Event::<i32>::new(&b, "local", Mode::Local).unwrap();
    let global_a = Event::<i32>::new(&a, "global", Mode::Global).unwrap();
    let global_b = Event::<i32>::new(&b, "global", Mode::Global).unwrap();
    thread::sleep(SETTLE);

    for value in 0..10 {
        global_a.enqueue(&value, EVERYBODY_ID, false).unwrap();
        assert_eq!(global_b.get_timeout(TIMEOUT).unwrap(), value);
    }
    global_b.enqueue(&99, EVERYBODY_ID, false).unwrap();
    assert_eq!(global_a.get_timeout(TIMEOUT).unwrap(), 99);

    local_a.enqueue(&1, EVERYBODY_ID, false).unwrap();
    assert!(matches!(local_b.get_timeout(SHORT), Err(Error::Timeout)));
}

#[test]
fn global_events_arrive_exactly_once_with_three_peers() {
    let p1 = first_peer();
    let p2 = join(&p1);
    let p3 = join(&p1);
    wait_for_mesh(&[&p1, &p2, &p3]);
    let clients = [client(&p1, "1"), client(&p2, "2"), client(&p3, "3")];
    let extra = client(&p2, "2b");

    let events: Vec<Event<i32>> = clients
        .iter()
        .chain([&extra])
        .map(|c| Event::new(c, "ev", Mode::Global).unwrap())
        .collect();
    thread::sleep(SETTLE);

    events[0].enqueue(&7, EVERYBODY_ID, false).unwrap();
    for event in &events[1..] {
        assert_eq!(event.get_timeout(TIMEOUT).unwrap(), 7);
    }
    thread::sleep(SETTLE);
    for event in &events {
        assert!(event.is_empty(), "no event may arrive twice");
    }
}

#[test]
fn event_can_be_addressed_to_a_client_of_another_peer() {
    let (p1, p2, a, b) = two_peers();
    let b2 = client(&p2, "b2");
    let a2 = client(&p1, "a2");
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Global).unwrap();
    let ev_a2 = Event::<i32>::new(&a2, "ev", Mode::Global).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Global).unwrap();
    let ev_b2 = Event::<i32>::new(&b2, "ev", Mode::Global).unwrap();
    thread::sleep(SETTLE);

    ev_a.enqueue(&1, b2.id(), false).unwrap();
    assert_eq!(ev_b2.get_timeout(TIMEOUT).unwrap(), 1);

    let targets = BTreeSet::from([a2.id(), b.id()]);
    ev_a.enqueue_many(&2, &targets, false).unwrap();
    assert_eq!(ev_a2.get_timeout(TIMEOUT).unwrap(), 2);
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 2);
    assert!(ev_b2.is_empty());
}

#[test]
fn deleted_global_event_stops_being_forwarded() {
    let (_p1, _p2, a, b) = two_peers();
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Global).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Global).unwrap();
    thread::sleep(SETTLE);
    ev_a.enqueue(&1, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 1);

    drop(ev_b);
    thread::sleep(SETTLE);
    ev_a.enqueue(&2, EVERYBODY_ID, false).unwrap();
    // Time for it to be processed, or it could arrive after the new event exists.
    thread::sleep(SETTLE);
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Global).unwrap();
    assert!(matches!(ev_b.get_timeout(SHORT), Err(Error::Timeout)));

    thread::sleep(SETTLE);
    ev_a.enqueue(&3, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 3);
}

/// The bug in the TODO of the C++ project: objects created before a peer
/// connects never reach it.
#[test]
fn peer_that_joins_later_learns_about_existing_objects() {
    let p1 = first_peer();
    let a = client(&p1, "a");
    let event_a = Event::<i32>::new(&a, "ev", Mode::Global).unwrap();
    let shared_a = Shared::new(&a, "x", 0i32, Mode::Global).unwrap();
    let barrier_a = Barrier::new(&a, 0, 0, true, Mode::Global).unwrap();
    shared_a.set(42);
    shared_a.send_update().unwrap();
    shared_a.get_update().unwrap();

    let p2 = join(&p1);
    wait_for_mesh(&[&p1, &p2]);
    thread::sleep(SETTLE);
    let b = client(&p2, "b");
    let event_b = Event::<i32>::new(&b, "ev", Mode::Global).unwrap();
    let shared_b = Shared::new(&b, "x", 0i32, Mode::Global).unwrap();
    let barrier_b = Barrier::new(&b, 0, 0, true, Mode::Global).unwrap();
    thread::sleep(SETTLE);

    event_b.enqueue(&1, EVERYBODY_ID, false).unwrap();
    assert_eq!(event_a.get_timeout(TIMEOUT).unwrap(), 1);
    event_a.enqueue(&2, EVERYBODY_ID, false).unwrap();
    assert_eq!(event_b.get_timeout(TIMEOUT).unwrap(), 2);

    shared_b.get_update().unwrap();
    assert_eq!(shared_b.get(), 42);

    thread::scope(|scope| {
        let waiting = scope.spawn(|| barrier_a.sync().unwrap());
        thread::sleep(SHORT);
        assert!(!waiting.is_finished(), "a must wait for b");
        barrier_b.sync().unwrap();
        waiting.join().unwrap();
    });
}

/// Based on `autopeersharedtest.cpp`.
#[test]
fn global_shared_variable_crosses_peers() {
    let (_p1, _p2, a, b) = two_peers();
    let local_a = Shared::new(&a, "local", 0i32, Mode::Local).unwrap();
    let local_b = Shared::new(&b, "local", 0i32, Mode::Local).unwrap();
    let global_a = Shared::new(&a, "global", 0i32, Mode::Global).unwrap();
    let global_b = Shared::new(&b, "global", 0i32, Mode::Global).unwrap();
    thread::sleep(SETTLE);

    for value in 1..=3 {
        global_a.set(value);
        global_a.send_update().unwrap();
        // This also lets others overwrite it.
        global_a.get_update().unwrap();
        wait_until("the value reaches the other peer", || {
            global_b.get_update().unwrap();
            global_b.get() == value
        });
    }

    local_a.set(5);
    local_a.send_update().unwrap();
    local_a.get_update().unwrap();
    thread::sleep(SETTLE);
    local_b.get_update().unwrap();
    assert_eq!(local_b.get(), 0);
}

#[test]
fn both_directions_work_on_a_shared_variable() {
    let (_p1, _p2, a, b) = two_peers();
    let var_a = Shared::new(&a, "x", 0i32, Mode::Global).unwrap();
    let var_b = Shared::new(&b, "x", 0i32, Mode::Global).unwrap();
    thread::sleep(SETTLE);

    var_a.set(1);
    var_a.send_update().unwrap();
    var_a.get_update().unwrap();
    wait_until("b sees 1", || {
        var_b.get_update().unwrap();
        var_b.get() == 1
    });

    var_b.set(2);
    var_b.send_update().unwrap();
    var_b.get_update().unwrap();
    wait_until("a sees 2", || {
        var_a.get_update().unwrap();
        var_a.get() == 2
    });
}

#[test]
fn peers_converge_after_concurrent_updates() {
    let (_p1, _p2, a, b) = two_peers();
    let var_a = Shared::new(&a, "x", 0i32, Mode::Global).unwrap();
    let var_b = Shared::new(&b, "x", 0i32, Mode::Global).unwrap();
    thread::sleep(SETTLE);

    var_a.set(1);
    var_b.set(2);
    thread::scope(|scope| {
        scope.spawn(|| var_a.send_update().unwrap());
        scope.spawn(|| var_b.send_update().unwrap());
    });
    thread::sleep(SETTLE);

    // Nobody has synced, so the lowest id holds it on both servers.
    let expected = if a.id() < b.id() { 1 } else { 2 };
    var_a.set_update_mode(UpdateMode::Never);
    var_b.set_update_mode(UpdateMode::Never);
    let (low, high) = if a.id() < b.id() {
        (&var_a, &var_b)
    } else {
        (&var_b, &var_a)
    };
    high.get_update().unwrap();
    low.get_update().unwrap();
    assert_eq!(var_a.get(), expected);
    assert_eq!(var_b.get(), expected);
}

/// Based on `autopeerbarriertest.cpp`, except that here it really synchronizes
/// across servers: in the C++ code, a global barrier only blocks on one server.
#[test]
fn global_barrier_holds_clients_of_all_peers() {
    let p1 = first_peer();
    let p2 = join(&p1);
    let p3 = join(&p1);
    wait_for_mesh(&[&p1, &p2, &p3]);
    let clients = [client(&p1, "1"), client(&p2, "2"), client(&p3, "3")];
    let barriers: Vec<Barrier> = clients
        .iter()
        .map(|c| Barrier::new(c, 1, 0, true, Mode::Global).unwrap())
        .collect();
    thread::sleep(SETTLE);

    for _ in 0..5 {
        thread::scope(|scope| {
            let first = scope.spawn(|| barriers[0].sync().unwrap());
            let second = scope.spawn(|| barriers[1].sync().unwrap());
            thread::sleep(SHORT);
            assert!(!first.is_finished() && !second.is_finished());
            barriers[2].sync().unwrap();
            first.join().unwrap();
            second.join().unwrap();
        });
    }
}

#[test]
fn global_barrier_in_fast_loop_does_not_deadlock() {
    // Peers finish rounds at different times, so one may get ahead of another.
    let p1 = first_peer();
    let p2 = join(&p1);
    let p3 = join(&p1);
    wait_for_mesh(&[&p1, &p2, &p3]);
    let clients = [client(&p1, "1"), client(&p2, "2"), client(&p3, "3")];
    let barriers: Vec<Barrier> = clients
        .iter()
        .map(|c| Barrier::new(c, 1, 0, true, Mode::Global).unwrap())
        .collect();
    thread::sleep(SETTLE);

    thread::scope(|scope| {
        for barrier in &barriers {
            scope.spawn(move || {
                for _ in 0..200 {
                    barrier.sync().unwrap();
                }
            });
        }
    });
}

#[test]
fn local_barrier_does_not_cross_peers() {
    let (_p1, _p2, a, b) = two_peers();
    let barrier_a = Barrier::new(&a, 1, 0, true, Mode::Local).unwrap();
    let _barrier_b = Barrier::new(&b, 1, 0, true, Mode::Local).unwrap();
    thread::sleep(SETTLE);
    barrier_a.sync().unwrap();
}

#[test]
fn inactive_global_barrier_does_not_hold_other_peers_back() {
    let (_p1, _p2, a, b) = two_peers();
    let barrier_a = Barrier::new(&a, 1, 0, true, Mode::Global).unwrap();
    let barrier_b = Barrier::new(&b, 1, 0, false, Mode::Global).unwrap();
    thread::sleep(SETTLE);

    barrier_a.sync().unwrap();

    // Once b goes through a sync, it starts to count.
    thread::scope(|scope| {
        let waiting_b = scope.spawn(|| barrier_b.sync().unwrap());
        thread::sleep(SHORT);
        barrier_a.sync().unwrap();
        waiting_b.join().unwrap();
    });
    thread::scope(|scope| {
        let waiting_a = scope.spawn(|| barrier_a.sync().unwrap());
        thread::sleep(SHORT);
        assert!(!waiting_a.is_finished(), "a must now wait for b");
        barrier_b.sync().unwrap();
        waiting_a.join().unwrap();
    });
}

#[test]
fn global_barrier_is_released_when_a_peer_dies() {
    let (p1, p2, a, b) = two_peers();
    let barrier_a = Barrier::new(&a, 1, 0, true, Mode::Global).unwrap();
    let _barrier_b = Barrier::new(&b, 1, 0, true, Mode::Global).unwrap();
    thread::sleep(SETTLE);

    thread::scope(|scope| {
        let waiting = scope.spawn(|| barrier_a.sync().unwrap());
        thread::sleep(SHORT);
        assert!(!waiting.is_finished());
        drop(p2);
        waiting.join().unwrap();
    });
    wait_until("p1 notices", || p1.peers().is_empty());
}

#[test]
fn clients_keep_working_after_a_peer_dies() {
    let p1 = first_peer();
    let p2 = join(&p1);
    let p3 = join(&p1);
    wait_for_mesh(&[&p1, &p2, &p3]);
    let a = client(&p1, "a");
    let b = client(&p2, "b");
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Global).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Global).unwrap();
    thread::sleep(SETTLE);

    drop(p3);
    wait_for_mesh(&[&p1, &p2]);

    ev_a.enqueue(&1, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 1);
}

#[test]
fn kill_all_reaches_every_peer() {
    let (p1, p2, a, b) = two_peers();
    a.kill_all().unwrap();
    p1.wait();
    p2.wait();
    wait_until("a is disconnected", || !a.is_connected());
    wait_until("b is disconnected", || !b.is_connected());
}

/// A group made of a gateway and a peer, with a client on the peer.
struct Group {
    _peer: GlassServer,
    gateway: GlassServer,
    client: GlassClient,
}

fn group(other_gateway: Option<&GlassServer>, nickname: &str) -> Group {
    let seed_addr = other_gateway.map(seed);
    let gateway = GlassServer::bind_remote_peer("127.0.0.1:0", seed_addr.as_deref()).unwrap();
    let peer = join(&gateway);
    wait_until("the peer joins", || !gateway.peers().is_empty());
    let client = client(&peer, nickname);
    Group {
        _peer: peer,
        gateway,
        client,
    }
}

#[test]
fn gateways_connect_groups() {
    let g1 = group(None, "c1");
    let g2 = group(Some(&g1.gateway), "c2");
    let g3 = group(Some(&g1.gateway), "c3");
    for g in [&g1, &g2, &g3] {
        wait_until("the gateway sees the rest", || g.gateway.peers().len() == 3);
    }
    let remote_links = g1
        .gateway
        .peers()
        .values()
        .filter(|info| info.kind == LinkKind::Remote)
        .count();
    assert_eq!(remote_links, 2);

    let events: Vec<Event<i32>> = [&g1, &g2, &g3]
        .iter()
        .map(|g| Event::new(&g.client, "ev", Mode::Global).unwrap())
        .collect();
    let barriers: Vec<Barrier> = [&g1, &g2, &g3]
        .iter()
        .map(|g| Barrier::new(&g.client, 0, 0, true, Mode::Global).unwrap())
        .collect();
    let vars: Vec<Shared<i32>> = [&g1, &g2, &g3]
        .iter()
        .map(|g| Shared::new(&g.client, "x", 0, Mode::Global).unwrap())
        .collect();
    thread::sleep(SETTLE);

    // An event crosses from a peer of one group to a peer of another.
    events[0].enqueue(&5, EVERYBODY_ID, false).unwrap();
    assert_eq!(events[1].get_timeout(TIMEOUT).unwrap(), 5);
    assert_eq!(events[2].get_timeout(TIMEOUT).unwrap(), 5);
    thread::sleep(SETTLE);
    assert!(events.iter().all(|e| e.is_empty()), "exactly once");

    // So does a shared variable.
    vars[1].set(8);
    vars[1].send_update().unwrap();
    vars[1].get_update().unwrap();
    wait_until("the variable arrives", || {
        vars[0].get_update().unwrap();
        vars[2].get_update().unwrap();
        vars[0].get() == 8 && vars[2].get() == 8
    });

    // And the barrier holds all the groups.
    for _ in 0..3 {
        thread::scope(|scope| {
            let first = scope.spawn(|| barriers[0].sync().unwrap());
            let second = scope.spawn(|| barriers[1].sync().unwrap());
            thread::sleep(SHORT);
            assert!(!first.is_finished() && !second.is_finished());
            barriers[2].sync().unwrap();
            first.join().unwrap();
            second.join().unwrap();
        });
    }
}

/// A client of one group holds a shared variable, and the link to its group breaks.
#[test]
fn losing_a_gateway_releases_variables_held_through_it() {
    let g1 = group(None, "c1");
    let g2 = group(Some(&g1.gateway), "c2");
    wait_until("the gateways connect", || {
        g1.gateway.peers().len() == 2 && g2.gateway.peers().len() == 2
    });
    // Only a lower id holds the variable against another client.
    let (holder, other) = if g1.client.id() < g2.client.id() {
        (g1, g2)
    } else {
        (g2, g1)
    };
    let held = Shared::new(&holder.client, "x", 0, Mode::Global).unwrap();
    let wanted = Shared::new(&other.client, "x", 0, Mode::Global).unwrap();
    thread::sleep(SETTLE);

    // It is held while its client does not call get_update.
    held.set(1);
    held.send_update().unwrap();
    wait_until("the other group sees it", || {
        wanted.get_update().unwrap();
        wanted.get() == 1
    });
    wanted.set(2);
    wanted.send_update().unwrap();
    wanted.get_update().unwrap();
    assert_eq!(wanted.get(), 1, "the variable should be held");

    let Group { gateway, .. } = holder;
    drop(gateway);
    wait_until("the link is gone", || other.gateway.peers().len() == 1);
    wait_until("the variable is released", || {
        wanted.set(2);
        wanted.send_update().unwrap();
        wanted.get_update().unwrap();
        wanted.get() == 2
    });
}

#[test]
fn local_objects_stay_in_the_server_even_with_gateways() {
    let g1 = group(None, "c1");
    let g2 = group(Some(&g1.gateway), "c2");
    wait_until("the gateways connect", || g2.gateway.peers().len() == 2);

    let ev1 = Event::<i32>::new(&g1.client, "ev", Mode::Local).unwrap();
    let ev2 = Event::<i32>::new(&g2.client, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);
    ev1.enqueue(&1, EVERYBODY_ID, false).unwrap();
    assert!(matches!(ev2.get_timeout(SHORT), Err(Error::Timeout)));
}

fn read_root(stream: &mut TcpStream) -> (NodeId, RootMsg) {
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    let packet = Packet::read_from(stream).unwrap().unwrap();
    assert_eq!(packet.plugin, ROOT_PLUGIN_ID);
    (packet.from, RootMsg::decode(&packet.payload).unwrap())
}

fn send_root(stream: &mut TcpStream, from: NodeId, message: RootMsg) {
    let packet = Packet {
        from,
        plugin: ROOT_PLUGIN_ID,
        targets: BTreeSet::new(),
        payload: message.encode(),
    };
    stream.write_all(&packet.encode().unwrap()).unwrap();
}

fn fake_addr() -> PeerAddr {
    PeerAddr {
        host: "127.0.0.1".into(),
        port: 1,
    }
}

/// The numbers that fake servers drew when they started.
const NONCE_X: u64 = 0x1111;
const NONCE_Y: u64 = 0x2222;

/// Reads the answer to `PeerStart`, up to the list of servers.
fn read_list(stream: &mut TcpStream) -> BTreeMap<NodeId, PeerAddr> {
    loop {
        match read_root(stream).1 {
            RootMsg::PeerList(_, list) => return list,
            RootMsg::PeerAccepted(..) | RootMsg::PeerChangeId(..) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
}

fn assert_closed(stream: &mut TcpStream, what: &str) {
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    loop {
        match Packet::read_from(stream) {
            Ok(None) | Err(_) => return,
            // Anything the server sent before closing it.
            Ok(Some(_)) => {}
        }
        assert!(stream.peer_addr().is_ok(), "{what}");
    }
}

fn connect(server: &GlassServer) -> TcpStream {
    let addr: SocketAddr = server.local_addr();
    TcpStream::connect(addr).unwrap()
}

#[test]
fn new_peer_with_an_id_in_use_is_told_to_change_it() {
    let peer = first_peer();

    let mut raw = connect(&peer);
    send_root(
        &mut raw,
        peer.id(),
        RootMsg::PeerStart(LinkKind::Peer, fake_addr(), NONCE_X),
    );
    let (_, message) = read_root(&mut raw);
    let RootMsg::PeerChangeId(LinkKind::Peer, new_id) = message else {
        panic!("expected a new id, got {message:?}");
    };
    assert_ne!(new_id, peer.id());

    let (_, message) = read_root(&mut raw);
    assert!(
        matches!(message, RootMsg::PeerAccepted(LinkKind::Peer, nonce) if nonce != 0),
        "expected the acceptance, with the number of the server, got {message:?}"
    );
    let (from, message) = read_root(&mut raw);
    assert_eq!(from, peer.id());
    let RootMsg::PeerList(LinkKind::Peer, list) = message else {
        panic!("expected the list, got {message:?}");
    };
    assert!(list.contains_key(&peer.id()));
    wait_until("the peer is registered", || {
        peer.peers().contains_key(&new_id)
    });
}

#[test]
fn second_new_peer_with_the_same_id_is_also_told() {
    let peer = first_peer();
    let taken = peer.id() ^ 0x0001_0000;

    let mut first = connect(&peer);
    send_root(
        &mut first,
        taken,
        RootMsg::PeerStart(LinkKind::Peer, fake_addr(), NONCE_X),
    );
    read_list(&mut first);
    wait_until("the first is registered", || peer.peers().len() == 1);

    // Another server, as it drew another number.
    let mut second = connect(&peer);
    send_root(
        &mut second,
        taken,
        RootMsg::PeerStart(LinkKind::Peer, fake_addr(), NONCE_Y),
    );
    let (_, message) = read_root(&mut second);
    assert!(matches!(message, RootMsg::PeerChangeId(..)));
    wait_until("both are registered", || peer.peers().len() == 2);
}

#[test]
fn peer_list_tells_the_new_peer_about_the_others() {
    let p1 = first_peer();
    let p2 = join(&p1);
    wait_for_mesh(&[&p1, &p2]);

    let mut raw = connect(&p1);
    send_root(
        &mut raw,
        0x7FFF_0000,
        RootMsg::PeerStart(LinkKind::Peer, fake_addr(), NONCE_X),
    );
    let list = read_list(&mut raw);
    assert_eq!(
        list.keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([p1.id(), p2.id()])
    );
    assert_eq!(list[&p2.id()].port, p2.local_addr().port());
    assert_eq!(list[&p2.id()].host, "127.0.0.1");
}

#[test]
fn plain_server_rejects_peers() {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let mut raw = connect(&server);
    send_root(
        &mut raw,
        0x7FFF_0000,
        RootMsg::PeerStart(LinkKind::Peer, fake_addr(), NONCE_X),
    );
    thread::sleep(SETTLE);
    assert!(server.peers().is_empty());
}

#[test]
fn peer_rejects_remote_peers_but_gateway_accepts_them() {
    let peer = first_peer();
    let mut raw = connect(&peer);
    send_root(
        &mut raw,
        0x7FFF_0000,
        RootMsg::PeerStart(LinkKind::Remote, fake_addr(), NONCE_X),
    );
    thread::sleep(SETTLE);
    assert!(peer.peers().is_empty());

    let gateway = GlassServer::bind_remote_peer("127.0.0.1:0", None).unwrap();
    let mut raw = connect(&gateway);
    send_root(
        &mut raw,
        0x7FFF_0000,
        RootMsg::PeerStart(LinkKind::Remote, fake_addr(), NONCE_X),
    );
    read_list(&mut raw);
    wait_until("the gateway registers it", || {
        gateway
            .peers()
            .get(&0x7FFF_0000)
            .is_some_and(|info| info.kind == LinkKind::Remote)
    });
}

#[test]
fn announce_with_an_invalid_id_is_rejected() {
    let peer = first_peer();
    for bad_id in [NOBODY_ID, EVERYBODY_ID, peer.id(), 0x7FFF_0001] {
        let mut raw = connect(&peer);
        send_root(
            &mut raw,
            bad_id,
            RootMsg::PeerListUpdate(LinkKind::Peer, fake_addr(), NONCE_X),
        );
    }
    thread::sleep(SETTLE);
    assert!(peer.peers().is_empty());
}

fn announce(server: &GlassServer, id: NodeId, nonce: u64) -> (TcpStream, RootMsg) {
    let mut stream = connect(server);
    let update = RootMsg::PeerListUpdate(LinkKind::Peer, fake_addr(), nonce);
    send_root(&mut stream, id, update);
    let (_, answer) = read_root(&mut stream);
    (stream, answer)
}

#[test]
fn a_new_connection_from_the_same_server_replaces_the_old_one() {
    let peer = first_peer();
    let id = 0x7FFF_0000;

    let (mut first, answer) = announce(&peer, id, NONCE_X);
    assert!(matches!(answer, RootMsg::PeerAccepted(LinkKind::Peer, n) if n != 0));
    wait_until("registered", || peer.peers().contains_key(&id));

    // The same server, with the same number: a new connection, which replaces the old.
    let (second, answer) = announce(&peer, id, NONCE_X);
    assert!(matches!(answer, RootMsg::PeerAccepted(..)));
    assert_closed(&mut first, "the first connection must be closed");
    thread::sleep(SHORT);
    assert!(
        peer.peers().contains_key(&id),
        "replacing a connection must not remove the peer"
    );

    // Another server with that id is refused, and the first one stays.
    let (mut other, answer) = announce(&peer, id, NONCE_Y);
    assert_eq!(answer, RootMsg::PeerIdTaken(LinkKind::Peer));
    assert_closed(&mut other, "the refused connection must be closed");
    thread::sleep(SHORT);
    assert!(peer.peers().contains_key(&id));

    // And the node only leaves when the connection in use closes.
    drop(second);
    wait_until("the peer is removed", || !peer.peers().contains_key(&id));
}

/// Two new servers drew the same id, and each one started through a different server of
/// the group, which did not know about the other yet.
#[test]
fn servers_joining_at_once_with_the_same_id_are_told_to_change() {
    let p1 = first_peer();
    let p2 = join(&p1);
    wait_for_mesh(&[&p1, &p2]);
    let same = 0x7FFF_0000;

    let mut x_start = connect(&p1);
    send_root(
        &mut x_start,
        same,
        RootMsg::PeerStart(LinkKind::Peer, fake_addr(), NONCE_X),
    );
    read_list(&mut x_start);
    let mut y_start = connect(&p2);
    send_root(
        &mut y_start,
        same,
        RootMsg::PeerStart(LinkKind::Peer, fake_addr(), NONCE_Y),
    );
    read_list(&mut y_start);

    // Each one then announces itself to the server where the other one started.
    let (mut x_announce, answer) = announce(&p2, same, NONCE_X);
    assert_eq!(answer, RootMsg::PeerIdTaken(LinkKind::Peer));
    assert_closed(&mut x_announce, "the refused connection must be closed");
    let (mut y_announce, answer) = announce(&p1, same, NONCE_Y);
    assert_eq!(answer, RootMsg::PeerIdTaken(LinkKind::Peer));
    assert_closed(&mut y_announce, "the refused connection must be closed");

    // Until they leave, each server keeps the one that started through it.
    assert!(p1.peers().contains_key(&same) && p2.peers().contains_key(&same));
    drop((x_start, y_start));
    wait_until("they leave", || {
        !p1.peers().contains_key(&same) && !p2.peers().contains_key(&same)
    });
}

/// A real server joins a fake group, where a server other than the seed already knows
/// another server with the id it drew.
#[test]
fn joining_server_starts_over_with_another_id_if_its_id_is_taken() {
    let seed = TcpListener::bind("127.0.0.1:0").unwrap();
    let other = TcpListener::bind("127.0.0.1:0").unwrap();
    let (seed_id, other_id) = (0x0011_0000, 0x0022_0000);
    let address = |listener: &TcpListener| PeerAddr {
        host: "127.0.0.1".into(),
        port: listener.local_addr().unwrap().port(),
    };
    let list = BTreeMap::from([(seed_id, address(&seed)), (other_id, address(&other))]);
    let seed_addr = seed.local_addr().unwrap().to_string();

    let fake_group = thread::spawn(move || {
        let mut ids = Vec::new();
        for attempt in 0..2 {
            let (mut start, _) = seed.accept().unwrap();
            let (id, message) = read_root(&mut start);
            assert!(matches!(message, RootMsg::PeerStart(LinkKind::Peer, _, n) if n != 0));
            ids.push(id);
            send_root(
                &mut start,
                seed_id,
                RootMsg::PeerAccepted(LinkKind::Peer, 1),
            );
            send_root(
                &mut start,
                seed_id,
                RootMsg::PeerList(LinkKind::Peer, list.clone()),
            );

            let (mut update, _) = other.accept().unwrap();
            let (from, message) = read_root(&mut update);
            assert_eq!(from, id);
            assert!(matches!(
                message,
                RootMsg::PeerListUpdate(LinkKind::Peer, _, _)
            ));
            if attempt == 0 {
                send_root(&mut update, other_id, RootMsg::PeerIdTaken(LinkKind::Peer));
                assert_closed(&mut start, "it must leave the seed before starting over");
            } else {
                send_root(
                    &mut update,
                    other_id,
                    RootMsg::PeerAccepted(LinkKind::Peer, 2),
                );
                return (ids, start, update);
            }
        }
        unreachable!()
    });

    let server = GlassServer::bind_peer("127.0.0.1:0", Some(&seed_addr)).unwrap();
    let (ids, _start, _update) = fake_group.join().unwrap();
    assert_ne!(ids[0], ids[1], "it must try another id");
    assert_eq!(server.id(), ids[1]);
    wait_until("both links are made", || {
        let peers = server.peers();
        peers.contains_key(&seed_id) && peers.contains_key(&other_id)
    });
}

#[test]
fn peers_listening_on_all_interfaces_advertise_a_usable_address() {
    let p1 = GlassServer::bind_peer("0.0.0.0:0", None).unwrap();
    let seed = format!("127.0.0.1:{}", p1.local_addr().port());
    let p2 = GlassServer::bind_peer("0.0.0.0:0", Some(&seed)).unwrap();
    // p3 can only reach p2 through the address p2 gave to p1.
    let p3 = GlassServer::bind_peer("0.0.0.0:0", Some(&seed)).unwrap();
    wait_for_mesh(&[&p1, &p2, &p3]);
    assert_eq!(p3.peers()[&p2.id()].addr.host, "127.0.0.1");
}

#[test]
fn updates_are_not_lost_when_the_seed_dies() {
    let p1 = first_peer();
    let p2 = join(&p1);
    let p3 = join(&p1);
    wait_for_mesh(&[&p1, &p2, &p3]);
    let b = client(&p2, "b");
    let c = client(&p3, "c");
    let var_b = Shared::new(&b, "x", 0i32, Mode::Global).unwrap();
    let var_c = Shared::new(&c, "x", 0i32, Mode::Global).unwrap();
    thread::sleep(SETTLE);

    drop(p1);
    wait_for_mesh(&[&p2, &p3]);

    var_b.set(3);
    var_b.send_update().unwrap();
    var_b.get_update().unwrap();
    wait_until("c sees it", || {
        var_c.get_update().unwrap();
        var_c.get() == 3
    });
}

#[test]
fn a_client_leaving_releases_a_shared_variable_on_other_peers() {
    let (_p1, _p2, a, b) = two_peers();
    let var_a = Shared::new(&a, "x", 0i32, Mode::Global).unwrap();
    let var_b = Shared::new(&b, "x", 0i32, Mode::Global).unwrap();
    thread::sleep(SETTLE);

    // Whichever holds it has the lower id; make sure the holder leaves.
    let (holder, holder_var, other_var, other_client) = if a.id() < b.id() {
        (a, var_a, var_b, b)
    } else {
        (b, var_b, var_a, a)
    };
    holder_var.set(1);
    holder_var.send_update().unwrap();
    wait_until("the value arrives", || {
        other_var.get_update().unwrap();
        other_var.get() == 1
    });

    // The other client cannot change it while the holder has it.
    other_var.set(2);
    other_var.send_update().unwrap();
    thread::sleep(SETTLE);
    other_var.get_update().unwrap();
    assert_eq!(other_var.get(), 1);

    drop(holder_var);
    drop(holder);
    thread::sleep(SETTLE);

    other_var.set(2);
    other_var.send_update().unwrap();
    other_var.get_update().unwrap();
    assert_eq!(other_var.get(), 2);
    assert!(other_client.is_connected());
}

#[test]
fn malformed_peer_messages_do_not_affect_a_server() {
    let peer = first_peer();
    let a = client(&peer, "a");

    let mut raw = connect(&peer);
    for payload in [
        vec![101],
        vec![101, 0, 0, 0, 3, 0, 1, 0, 0],
        vec![102, 0, 0, 0, 3, 0xFF, 0xFF, 0xFF, 0xFF],
        vec![104],
        vec![109],
        vec![116],
    ] {
        let packet = Packet {
            from: 0x7FFF_0000,
            plugin: ROOT_PLUGIN_ID,
            targets: BTreeSet::new(),
            payload,
        };
        raw.write_all(&packet.encode().unwrap()).unwrap();
    }
    thread::sleep(SETTLE);
    assert!(peer.peers().is_empty());
    assert!(a.is_connected());
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
fn peers_reconnect_after_their_link_breaks() {
    let p1 = first_peer();
    let proxy = Proxy::new(p1.local_addr());
    // The second one joins through the proxy, which then carries the link between them.
    let p2 = GlassServer::bind_peer("127.0.0.1:0", Some(&proxy.addr.to_string())).unwrap();
    wait_for_mesh(&[&p1, &p2]);
    let a = client(&p1, "a");
    let b = client(&p2, "b");
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Global).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Global).unwrap();
    thread::sleep(SETTLE);
    ev_a.enqueue(&1, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 1);

    proxy.cut();
    // The one with the lower id calls the other back, without the proxy, and the
    // objects are known on both sides again.
    wait_until("events cross again", || {
        ev_a.enqueue(&2, EVERYBODY_ID, false).unwrap();
        matches!(ev_b.get_timeout(SHORT), Ok(2))
    });
    ev_b.enqueue(&3, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_a.get_timeout(TIMEOUT).unwrap(), 3);
    assert_eq!(p1.peers().len(), 1);
    assert_eq!(p2.peers().len(), 1);
}

#[test]
fn a_peer_that_leaves_on_purpose_is_not_called_back() {
    let p1 = first_peer();
    let p2 = join(&p1);
    wait_for_mesh(&[&p1, &p2]);
    // Only the one with the lower id would call the other back.
    let (stays, leaves) = if p1.id() < p2.id() {
        (p1, p2)
    } else {
        (p2, p1)
    };
    let addr = leaves.local_addr();
    drop(leaves);
    wait_until("it leaves", || stays.peers().is_empty());

    let listener = TcpListener::bind(addr).unwrap();
    listener.set_nonblocking(true).unwrap();
    thread::sleep(Duration::from_secs(1));
    assert!(
        listener.accept().is_err(),
        "it called back the server that left"
    );
}
