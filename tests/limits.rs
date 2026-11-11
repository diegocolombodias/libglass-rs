//! What a slow client, a stopped client, or many clients must not do to the others.

use libglass::packet::Packet;
use libglass::protocol::{EventMsg, RootMsg};
use libglass::*;
use std::collections::BTreeSet;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);
const SETTLE: Duration = Duration::from_millis(300);
const SHORT: Duration = Duration::from_millis(200);
const MIB: usize = 1024 * 1024;

fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn send_raw(stream: &mut TcpStream, plugin: PluginId, payload: Vec<u8>, targets: &[NodeId]) {
    let packet = Packet {
        from: NOBODY_ID,
        plugin,
        targets: targets.iter().copied().collect(),
        payload,
    };
    stream.write_all(&packet.encode().unwrap()).unwrap();
}

/// A client made by hand, which only reads when a test does.
fn raw_client(addr: SocketAddr, nickname: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).unwrap();
    send_raw(
        &mut stream,
        ROOT_PLUGIN_ID,
        RootMsg::Hello(nickname.into()).encode(),
        &[],
    );
    stream
}

fn id_of(server: &GlassServer, nickname: &str) -> NodeId {
    wait_until("the client is known", || {
        server.client_list().values().any(|nick| nick == nickname)
    });
    let list = server.client_list();
    *list.iter().find(|(_, nick)| *nick == nickname).unwrap().0
}

/// A server with a client that never reads but has the event "ev", and two clients
/// that use it.
fn with_a_stopped_client() -> (GlassServer, TcpStream, NodeId, GlassClient, GlassClient) {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let mut stopped = raw_client(addr, "stopped");
    let create = EventMsg::CreateLocal("ev".into()).encode();
    send_raw(&mut stopped, EVENT_PLUGIN_ID, create, &[SERVERS_ID]);
    let stopped_id = id_of(&server, "stopped");
    let sender = GlassClient::connect(addr, "sender").unwrap();
    let receiver = GlassClient::connect(addr, "receiver").unwrap();
    (server, stopped, stopped_id, sender, receiver)
}

#[test]
fn a_client_that_stops_reading_does_not_delay_the_others() {
    let (server, _stopped, stopped_id, sender, receiver) = with_a_stopped_client();
    let ev_sender = Event::<Bytes>::new(&sender, "ev", Mode::Local).unwrap();
    let ev_receiver = Event::<Bytes>::new(&receiver, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    // Far more than the buffers of the system hold, and less than a server queues.
    let chunk = Bytes(vec![7; MIB]);
    let start = Instant::now();
    for _ in 0..40 {
        ev_sender.enqueue(&chunk, EVERYBODY_ID, false).unwrap();
    }
    for _ in 0..40 {
        ev_receiver.get_timeout(TIMEOUT).unwrap();
    }
    // Writing to the stopped client used to hold the others for 5 s.
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_secs(4), "it took {elapsed:?}");
    assert!(
        server.client_list().contains_key(&stopped_id),
        "it is behind, but not too far yet"
    );
}

#[test]
fn a_client_too_far_behind_is_disconnected_and_the_others_go_on() {
    let (server, _stopped, stopped_id, sender, receiver) = with_a_stopped_client();
    let ev_sender = Event::<Bytes>::new(&sender, "ev", Mode::Local).unwrap();
    let ev_receiver = Event::<Bytes>::new(&receiver, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    // More than the 64 MiB a server keeps for one connection.
    let chunk = Bytes(vec![7; MIB]);
    for _ in 0..80 {
        ev_sender.enqueue(&chunk, EVERYBODY_ID, false).unwrap();
    }
    for _ in 0..80 {
        ev_receiver.get_timeout(TIMEOUT).unwrap();
    }
    wait_until("the stopped client is dropped", || {
        !server.client_list().contains_key(&stopped_id)
    });
    assert!(sender.is_connected() && receiver.is_connected());
}

#[test]
fn a_join_costs_the_same_with_one_client_or_with_many() {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let mut watcher = raw_client(addr, "watcher");
    watcher.set_read_timeout(Some(TIMEOUT)).unwrap();
    let mut next_root = || loop {
        let packet = Packet::read_from(&mut watcher).unwrap().unwrap();
        if packet.plugin == ROOT_PLUGIN_ID {
            return packet.payload;
        }
    };
    assert!(matches!(
        RootMsg::decode(&next_root()).unwrap(),
        RootMsg::Id(_)
    ));
    assert!(matches!(
        RootMsg::decode(&next_root()).unwrap(),
        RootMsg::Members { .. }
    ));

    let mut clients = Vec::new();
    let mut sizes = BTreeSet::new();
    for i in 0..40 {
        clients.push(GlassClient::connect(addr, &format!("c{i:02}")).unwrap());
        let payload = next_root();
        let RootMsg::MemberJoined { nickname, .. } = RootMsg::decode(&payload).unwrap() else {
            panic!("expected a join");
        };
        assert_eq!(nickname, format!("c{i:02}"));
        sizes.insert(payload.len());
    }
    // The whole list, as before, would have grown by 260 bytes with each client.
    assert_eq!(sizes, BTreeSet::from([12]), "{sizes:?}");

    for _ in 0..20 {
        clients.pop();
        let payload = next_root();
        assert!(matches!(
            RootMsg::decode(&payload).unwrap(),
            RootMsg::MemberLeft { .. }
        ));
        assert_eq!(payload.len(), 9);
    }
}

#[test]
fn every_client_has_the_same_list_as_the_server() {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let mut clients: Vec<GlassClient> = (0..30)
        .map(|i| GlassClient::connect(addr, &format!("c{i}")).unwrap())
        .collect();
    // Some leave, including the master.
    let mut index = 0;
    clients.retain(|_| {
        index += 1;
        index % 3 != 1
    });

    let expected = |server: &GlassServer| server.client_list();
    wait_until("the server sees who left", || expected(&server).len() == 20);
    for client in &clients {
        wait_until("every client has the list", || {
            client.client_list() == expected(&server)
        });
    }
    let master = server.master().unwrap();
    assert_eq!(clients.iter().filter(|c| c.is_master()).count(), 1);
    assert!(clients.iter().any(|c| c.id() == master && c.is_master()));

    // And one that joins now gets the whole list.
    let late = GlassClient::connect(addr, "late").unwrap();
    wait_until("the late one has the list", || {
        late.client_list() == expected(&server)
    });
}

#[test]
fn barrier_sync_timeout_gives_up_and_still_counts() {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let a = GlassClient::connect(server.local_addr(), "a").unwrap();
    let b = GlassClient::connect(server.local_addr(), "b").unwrap();
    let barrier_a = Barrier::new(&a, 0, 0, true, Mode::Local).unwrap();
    let barrier_b = Barrier::new(&b, 0, 0, true, Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let start = Instant::now();
    assert!(matches!(barrier_a.sync_timeout(SHORT), Err(Error::Timeout)));
    assert!(start.elapsed() < TIMEOUT);

    // a still counts as arrived: b gets through alone, and a's next call returns.
    barrier_b.sync_timeout(TIMEOUT).unwrap();
    barrier_a.sync_timeout(TIMEOUT).unwrap();

    // The next round is a normal one.
    thread::scope(|scope| {
        let waiting = scope.spawn(|| barrier_a.sync().unwrap());
        thread::sleep(SHORT);
        assert!(!waiting.is_finished(), "a must wait for b");
        barrier_b.sync().unwrap();
    });
}
