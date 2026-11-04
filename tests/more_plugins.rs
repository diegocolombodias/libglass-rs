//! Messenger, Stream, serializable events, plugins from outside the library, UDP,
//! Unix sockets, shared memory and everything else added after the first plugins.

use libglass::packet::Packet;
use libglass::*;
use std::any::Any;
use std::collections::BTreeSet;
use std::io::{self, Read};
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc::channel;
use std::sync::Arc;
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

fn start_server() -> (GlassServer, SocketAddr) {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    (server, addr)
}

fn two_peers() -> (GlassServer, GlassServer, GlassClient, GlassClient) {
    let p1 = GlassServer::bind_peer("127.0.0.1:0", None).unwrap();
    let seed = p1.local_addr().to_string();
    let p2 = GlassServer::bind_peer("127.0.0.1:0", Some(&seed)).unwrap();
    wait_until("the peers connect", || p1.peers().len() == 1);
    let a = GlassClient::connect(p1.local_addr(), "a").unwrap();
    let b = GlassClient::connect(p2.local_addr(), "b").unwrap();
    (p1, p2, a, b)
}

#[test]
fn messenger_delivers_to_one_node_with_its_sender() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let c = GlassClient::connect(addr, "c").unwrap();
    let m_a = Messenger::<String>::new(&a, "m", Mode::Local).unwrap();
    let m_b = Messenger::<String>::new(&b, "m", Mode::Local).unwrap();
    let m_c = Messenger::<String>::new(&c, "m", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    m_a.send(b.id(), &"oi".to_owned()).unwrap();
    assert_eq!(m_b.get_timeout(TIMEOUT).unwrap(), (a.id(), "oi".to_owned()));
    thread::sleep(SHORT);
    assert!(m_c.is_empty());
    assert!(m_a.is_empty());
}

#[test]
fn messenger_can_wait_for_a_given_sender() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let c = GlassClient::connect(addr, "c").unwrap();
    let m_a = Messenger::<i32>::new(&a, "m", Mode::Local).unwrap();
    let m_b = Messenger::<i32>::new(&b, "m", Mode::Local).unwrap();
    let m_c = Messenger::<i32>::new(&c, "m", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    m_b.send(a.id(), &1).unwrap();
    wait_until("it arrives", || m_a.queue_size() == 1);
    m_c.send(a.id(), &2).unwrap();
    wait_until("it arrives", || m_a.queue_size() == 2);

    assert_eq!(m_a.try_get_from(c.id()).unwrap(), Some(2));
    assert_eq!(m_a.try_get_from(c.id()).unwrap(), None);
    assert!(matches!(
        m_a.get_from_timeout(c.id(), SHORT),
        Err(Error::Timeout)
    ));
    assert_eq!(m_a.get_from_timeout(b.id(), TIMEOUT).unwrap(), 1);
    assert!(m_a.try_get().unwrap().is_none());
}

#[test]
fn messenger_rejects_groups_and_invalid_names() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let m = Messenger::<i32>::new(&a, "m", Mode::Local).unwrap();
    assert!(matches!(
        m.send(EVERYBODY_ID, &1),
        Err(Error::InvalidTarget(_))
    ));
    assert!(matches!(
        Messenger::<i32>::new(&a, "m", Mode::Local),
        Err(Error::AlreadyExists(_))
    ));
    assert!(matches!(
        Messenger::<i32>::new(&a, "", Mode::Local),
        Err(Error::InvalidName)
    ));
}

#[test]
fn messenger_handler_gets_queued_and_new_messages() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let m_a = Messenger::<i32>::new(&a, "m", Mode::Local).unwrap();
    let m_b = Messenger::<i32>::new(&b, "m", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    m_a.send(b.id(), &1).unwrap();
    wait_until("it is queued", || m_b.queue_size() == 1);

    let (tx, rx) = channel();
    m_b.set_handler(move |from, value| tx.send((from, value)).unwrap());
    assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), (a.id(), 1));
    m_a.send(b.id(), &2).unwrap();
    assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), (a.id(), 2));
}

#[test]
fn messenger_works_across_peers_only_when_global() {
    let (_p1, _p2, a, b) = two_peers();
    let local_a = Messenger::<i32>::new(&a, "local", Mode::Local).unwrap();
    let local_b = Messenger::<i32>::new(&b, "local", Mode::Local).unwrap();
    let global_a = Messenger::<i32>::new(&a, "global", Mode::Global).unwrap();
    let global_b = Messenger::<i32>::new(&b, "global", Mode::Global).unwrap();
    thread::sleep(SETTLE);

    global_a.send(b.id(), &1).unwrap();
    assert_eq!(global_b.get_timeout(TIMEOUT).unwrap(), (a.id(), 1));
    global_b.send(a.id(), &2).unwrap();
    assert_eq!(global_a.get_timeout(TIMEOUT).unwrap(), (b.id(), 2));

    local_a.send(b.id(), &3).unwrap();
    assert!(matches!(local_b.get_timeout(SHORT), Err(Error::Timeout)));
}

#[test]
fn messenger_waiting_is_woken_when_the_server_stops() {
    let (server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let m = Messenger::<i32>::new(&a, "m", Mode::Local).unwrap();
    thread::scope(|scope| {
        let waiting = scope.spawn(|| m.get_timeout(TIMEOUT));
        thread::sleep(SHORT);
        drop(server);
        assert!(matches!(waiting.join().unwrap(), Err(Error::Disconnected)));
    });
}

/// More than one chunk, with content that shows if anything is out of order.
fn large_data(seed: u8) -> Vec<u8> {
    (0..700_000u32).map(|i| (i % 251) as u8 ^ seed).collect()
}

#[test]
fn stream_transfers_large_data_intact() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let s_a = Stream::new(&a, "s", Mode::Local).unwrap();
    let s_b = Stream::new(&b, "s", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let data = large_data(0);
    assert_eq!(
        s_a.send_from(b.id(), &mut data.as_slice()).unwrap(),
        data.len() as u64
    );
    let mut received = Vec::new();
    let (from, size) = s_b.receive(&mut received, TIMEOUT).unwrap();
    assert_eq!(from, a.id());
    assert_eq!(size, data.len() as u64);
    assert_eq!(received, data);

    // Empty transfers are transfers too.
    s_a.send_from(b.id(), &mut io::empty()).unwrap();
    let mut received = Vec::new();
    assert_eq!(s_b.receive(&mut received, TIMEOUT).unwrap(), (a.id(), 0));
    assert!(received.is_empty());
}

#[test]
fn stream_keeps_simultaneous_transfers_apart() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let c = GlassClient::connect(addr, "c").unwrap();
    let s_a = Stream::new(&a, "s", Mode::Local).unwrap();
    let s_b = Stream::new(&b, "s", Mode::Local).unwrap();
    let s_c = Stream::new(&c, "s", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let data_b = large_data(1);
    let data_c = large_data(2);
    thread::scope(|scope| {
        scope.spawn(|| s_b.send_from(a.id(), &mut data_b.as_slice()).unwrap());
        scope.spawn(|| s_c.send_from(a.id(), &mut data_c.as_slice()).unwrap());
    });

    let mut received = BTreeSet::new();
    for _ in 0..2 {
        let mut data = Vec::new();
        let (from, _) = s_a.receive(&mut data, TIMEOUT).unwrap();
        let expected = if from == b.id() { &data_b } else { &data_c };
        assert!(data == *expected, "the data from {from:#x} is wrong");
        received.insert(from);
    }
    assert_eq!(received, BTreeSet::from([b.id(), c.id()]));
}

#[test]
fn stream_transfers_files_across_peers() {
    let (_p1, _p2, a, b) = two_peers();
    let s_a = Stream::new(&a, "s", Mode::Global).unwrap();
    let s_b = Stream::new(&b, "s", Mode::Global).unwrap();
    thread::sleep(SETTLE);

    let dir = std::env::temp_dir();
    let id = std::process::id();
    let source = dir.join(format!("libglass-test-{id}-in"));
    let target = dir.join(format!("libglass-test-{id}-out"));
    let data = large_data(3);
    std::fs::write(&source, &data).unwrap();

    s_a.send_file(b.id(), &source).unwrap();
    let (from, size) = s_b.receive_file(&target, TIMEOUT).unwrap();
    assert_eq!((from, size), (a.id(), data.len() as u64));
    assert!(std::fs::read(&target).unwrap() == data);

    std::fs::remove_file(source).unwrap();
    std::fs::remove_file(target).unwrap();
}

struct FailsMidway {
    remaining: usize,
}

impl Read for FailsMidway {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::other("disk on fire"));
        }
        let n = buf.len().min(self.remaining);
        self.remaining -= n;
        Ok(n)
    }
}

#[test]
fn stream_reports_a_failed_transfer_on_both_sides() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let s_a = Stream::new(&a, "s", Mode::Local).unwrap();
    let s_b = Stream::new(&b, "s", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let mut failing = FailsMidway { remaining: 1000 };
    assert!(matches!(
        s_a.send_from(b.id(), &mut failing),
        Err(Error::Io(_))
    ));
    let mut received = Vec::new();
    assert!(matches!(
        s_b.receive(&mut received, TIMEOUT),
        Err(Error::TransferAborted)
    ));

    // And the next one works.
    s_a.send_from(b.id(), &mut [1u8, 2].as_slice()).unwrap();
    let mut received = Vec::new();
    s_b.receive(&mut received, TIMEOUT).unwrap();
    assert_eq!(received, [1, 2]);
}

#[test]
fn stream_with_nothing_to_receive_times_out() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let s = Stream::new(&a, "s", Mode::Local).unwrap();
    let mut received = Vec::new();
    assert!(matches!(
        s.receive(&mut received, SHORT),
        Err(Error::Timeout)
    ));
}

#[derive(Debug, PartialEq)]
struct Circle(u32);
#[derive(Debug, PartialEq)]
struct Label(String);

impl Serializable for Circle {
    fn class_name(&self) -> &str {
        "Circle"
    }
    fn pack(&self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Serializable for Label {
    fn class_name(&self) -> &str {
        "Label"
    }
    fn pack(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn shapes() -> Arc<ObjectRegistry> {
    let mut registry = ObjectRegistry::new();
    registry.register("Circle", |bytes| {
        Some(Box::new(Circle(u32::from_be_bytes(bytes.try_into().ok()?))))
    });
    registry.register("Label", |bytes| {
        Some(Box::new(Label(String::from_utf8(bytes.to_vec()).ok()?)))
    });
    Arc::new(registry)
}

#[test]
fn serializable_events_carry_objects_of_different_classes() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let e_a = EventSerializable::new(&a, "shapes", shapes(), Mode::Local).unwrap();
    let e_b = EventSerializable::new(&b, "shapes", shapes(), Mode::Local).unwrap();
    thread::sleep(SETTLE);

    e_a.enqueue(&Circle(5), EVERYBODY_ID).unwrap();
    e_a.enqueue(&Label("oi".into()), EVERYBODY_ID).unwrap();

    let first = e_b.get_timeout(TIMEOUT).unwrap();
    assert_eq!(first.as_any().downcast_ref(), Some(&Circle(5)));
    let second = e_b.get_timeout(TIMEOUT).unwrap();
    assert_eq!(second.class_name(), "Label");
    assert_eq!(second.as_any().downcast_ref(), Some(&Label("oi".into())));
    assert!(e_b.is_empty());
    assert_eq!(e_b.queue_size(), 0);
}

#[test]
fn serializable_event_of_unknown_class_is_an_error_and_is_removed() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let e_a = EventSerializable::new(&a, "shapes", shapes(), Mode::Local).unwrap();
    let e_b =
        EventSerializable::new(&b, "shapes", Arc::new(ObjectRegistry::new()), Mode::Local).unwrap();
    thread::sleep(SETTLE);

    e_a.enqueue(&Circle(5), EVERYBODY_ID).unwrap();
    assert!(matches!(e_b.get_timeout(TIMEOUT), Err(Error::Decode)));
    assert!(e_b.is_empty());
}

#[test]
fn serializable_event_handler_gets_objects() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let e_a = EventSerializable::new(&a, "shapes", shapes(), Mode::Local).unwrap();
    let e_b = EventSerializable::new(&b, "shapes", shapes(), Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let (tx, rx) = channel();
    e_b.set_handler(move |object| {
        let circle = object.as_any().downcast_ref::<Circle>().unwrap();
        tx.send(circle.0).unwrap();
    });
    e_a.enqueue(&Circle(9), EVERYBODY_ID).unwrap();
    assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), 9);
}

const COUNTER_PLUGIN: PluginId = 500;

/// Counts what it receives and tells everybody, and says when someone leaves.
#[derive(Default)]
struct CounterPlugin {
    total: u32,
}

impl ServerPlugin for CounterPlugin {
    fn on_packet(
        &mut self,
        source: PacketSource,
        _targets: &BTreeSet<NodeId>,
        payload: &[u8],
    ) -> Vec<PluginMessage> {
        self.total += payload.len() as u32;
        let mut message = PluginMessage::to(EVERYBODY_ID, self.total.to_be_bytes().to_vec());
        // Only what comes from a client is worth telling other servers.
        message.to_other_servers = source.kind == NodeKind::Client;
        vec![message]
    }

    fn on_node_left(&mut self, id: NodeId) -> Vec<PluginMessage> {
        vec![PluginMessage::to(CLIENTS_ID, id.to_be_bytes().to_vec())]
    }
}

fn to_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().unwrap())
}

#[test]
fn plugin_from_outside_the_library_handles_packets() {
    let (server, addr) = start_server();
    server
        .add_plugin(COUNTER_PLUGIN, CounterPlugin::default())
        .unwrap();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();

    let (tx, rx) = channel();
    b.set_plugin_handler(COUNTER_PLUGIN, move |from, payload| {
        tx.send((from, to_u32(payload))).unwrap();
    });
    let a_id = a.id();
    a.send_to_plugin(COUNTER_PLUGIN, vec![0; 3], BTreeSet::new())
        .unwrap();
    assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), (a_id, 3));
    a.send_to_plugin(COUNTER_PLUGIN, vec![0; 2], BTreeSet::new())
        .unwrap();
    assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), (a_id, 5));

    drop(a);
    let (_, left) = rx.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(left, a_id);
}

#[test]
fn plugin_ids_are_checked() {
    let (server, _addr) = start_server();
    assert!(matches!(
        server.add_plugin(MAX_RESERVED_PLUGIN_ID, CounterPlugin::default()),
        Err(Error::InvalidPluginId(_))
    ));
    server
        .add_plugin(COUNTER_PLUGIN, CounterPlugin::default())
        .unwrap();
    assert!(matches!(
        server.add_plugin(COUNTER_PLUGIN, CounterPlugin::default()),
        Err(Error::AlreadyExists(_))
    ));
}

#[test]
fn plugin_messages_can_reach_other_servers() {
    let (p1, p2, a, b) = two_peers();
    p1.add_plugin(COUNTER_PLUGIN, CounterPlugin::default())
        .unwrap();
    p2.add_plugin(COUNTER_PLUGIN, CounterPlugin::default())
        .unwrap();

    let (tx, rx) = channel();
    b.set_plugin_handler(COUNTER_PLUGIN, move |_, payload| {
        tx.send(to_u32(payload)).unwrap();
    });
    thread::sleep(SHORT);
    a.send_to_plugin(COUNTER_PLUGIN, vec![0; 4], BTreeSet::new())
        .unwrap();
    assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), 4);
    // And only once.
    assert!(rx.recv_timeout(SHORT).is_err());
}

#[test]
fn packets_for_a_plugin_nobody_has_are_ignored() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    a.send_to_plugin(999, vec![1], BTreeSet::new()).unwrap();
    thread::sleep(SHORT);
    assert!(a.is_connected());
}

#[test]
fn events_can_travel_through_udp() {
    let server = GlassServer::bind_with_udp("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let a = GlassClient::connect_with_udp(addr, "a").unwrap();
    let b = GlassClient::connect_with_udp(addr, "b").unwrap();
    assert!(a.uses_udp());
    assert!(b.uses_udp());
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let before = server.datagrams_received();
    ev_a.enqueue_unreliable(&7, EVERYBODY_ID).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 7);
    assert_eq!(
        server.datagrams_received(),
        before + 1,
        "it went through UDP"
    );

    // Reliable ones do not.
    ev_a.enqueue(&8, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 8);
    assert_eq!(server.datagrams_received(), before + 1);
}

#[test]
fn servers_with_udp_on_chosen_ports_do_not_collide() {
    // Many sockets at once, so that TCP gets ports that UDP is already using.
    let servers: Vec<GlassServer> = (0..20)
        .map(|_| GlassServer::bind_with_udp("127.0.0.1:0").unwrap())
        .collect();
    let clients: Vec<GlassClient> = servers
        .iter()
        .map(|server| GlassClient::connect_with_udp(server.local_addr(), "a").unwrap())
        .collect();
    assert!(clients.iter().all(GlassClient::uses_udp));

    // A port that was explicitly asked for is not replaced by another one.
    let taken = servers[0].local_addr();
    assert!(GlassServer::bind_with_udp(taken).is_err());

    let free_port = {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let listener = std::net::TcpListener::bind(socket.local_addr().unwrap());
        listener.map(|l| l.local_addr().unwrap())
    };
    if let Ok(addr) = free_port {
        // The same number on both, when it is available on both.
        thread::sleep(SHORT);
        let server = GlassServer::bind_with_udp(addr).unwrap();
        assert_eq!(server.udp_addr().unwrap().port(), addr.port());
    }
}

#[test]
fn udp_packet_too_large_goes_through_tcp() {
    let server = GlassServer::bind_with_udp("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let a = GlassClient::connect_with_udp(addr, "a").unwrap();
    let b = GlassClient::connect_with_udp(addr, "b").unwrap();
    let ev_a = Event::<Bytes>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<Bytes>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let before = server.datagrams_received();
    let data = Bytes(vec![7; 100_000]);
    ev_a.enqueue_unreliable(&data, EVERYBODY_ID).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), data);
    assert_eq!(server.datagrams_received(), before);
}

#[test]
fn clients_with_and_without_udp_get_the_same_events() {
    let server = GlassServer::bind_with_udp("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let with_udp = GlassClient::connect_with_udp(addr, "a").unwrap();
    let without = GlassClient::connect(addr, "b").unwrap();
    assert!(!without.uses_udp());
    let ev_a = Event::<i32>::new(&with_udp, "ev", Mode::Local).unwrap();
    let ev_b = Event::<i32>::new(&without, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    ev_a.enqueue_unreliable(&1, EVERYBODY_ID).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 1);
    ev_b.enqueue_unreliable(&2, EVERYBODY_ID).unwrap();
    assert_eq!(ev_a.get_timeout(TIMEOUT).unwrap(), 2);
}

#[test]
fn client_works_without_udp_if_the_server_does_not_have_it() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect_with_udp(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    assert!(!a.uses_udp());
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);
    ev_a.enqueue_unreliable(&1, EVERYBODY_ID).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 1);
}

#[test]
fn udp_events_reach_clients_of_other_peers() {
    let p1 = GlassServer::bind_peer_with_udp("127.0.0.1:0", None).unwrap();
    let seed = p1.local_addr().to_string();
    let p2 = GlassServer::bind_peer_with_udp("127.0.0.1:0", Some(&seed)).unwrap();
    wait_until("the peers connect", || p1.peers().len() == 1);
    let a = GlassClient::connect_with_udp(p1.local_addr(), "a").unwrap();
    let b = GlassClient::connect_with_udp(p2.local_addr(), "b").unwrap();
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Global).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Global).unwrap();
    thread::sleep(SETTLE);

    ev_a.enqueue_unreliable(&5, EVERYBODY_ID).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 5);
}

#[test]
fn datagrams_from_strangers_are_ignored() {
    let server = GlassServer::bind_with_udp("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let stranger = UdpSocket::bind("127.0.0.1:0").unwrap();
    // Pretending to be a client, garbage, and a token nobody has announced.
    let forged = Packet {
        from: a.id(),
        plugin: EVENT_PLUGIN_ID,
        targets: BTreeSet::from([EVERYBODY_ID]),
        payload: protocol::EventMsg::Event("ev".into(), b"9".to_vec()).encode(),
    };
    let udp = server.udp_addr().unwrap();
    stranger.send_to(&forged.encode().unwrap(), udp).unwrap();
    stranger.send_to(&[1, 2, 3], udp).unwrap();
    let token = Packet {
        plugin: ROOT_PLUGIN_ID,
        payload: protocol::RootMsg::UdpRegister(12345).encode(),
        ..Packet::default()
    };
    stranger.send_to(&token.encode().unwrap(), udp).unwrap();

    wait_until("they arrive", || server.datagrams_received() == 3);
    thread::sleep(SHORT);
    assert!(ev_b.is_empty());
    assert!(ev_a.is_empty());
}

#[test]
fn connect_retry_waits_for_the_server() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let addr = format!("127.0.0.1:{port}");
    assert!(GlassClient::connect_retry(&addr, "a", SHORT).is_err());

    let server_addr = addr.clone();
    let starter = thread::spawn(move || {
        thread::sleep(SHORT);
        GlassServer::bind(&server_addr).unwrap()
    });
    let client = GlassClient::connect_retry(&addr, "a", TIMEOUT).unwrap();
    assert!(client.is_connected());
    drop(client);
    drop(starter.join().unwrap());
}

#[test]
fn barrier_can_be_disabled_and_enabled_again() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let barrier_a = Barrier::new(&a, 0, 0, true, Mode::Local).unwrap();
    let barrier_b = Barrier::new(&b, 0, 0, true, Mode::Local).unwrap();
    thread::sleep(SETTLE);

    barrier_b.disable().unwrap();
    thread::sleep(SETTLE);
    barrier_a.sync().unwrap();
    barrier_b.sync().unwrap();

    barrier_b.enable().unwrap();
    thread::sleep(SETTLE);
    thread::scope(|scope| {
        let waiting = scope.spawn(|| barrier_a.sync().unwrap());
        thread::sleep(SHORT);
        assert!(!waiting.is_finished(), "a must wait for b again");
        barrier_b.sync().unwrap();
        waiting.join().unwrap();
    });
}

/// Run by the test below, in another process.
#[test]
#[ignore = "only runs as a child of shared_region_works_between_processes"]
fn shared_region_child() {
    let name = std::env::var("LIBGLASS_TEST_REGION").unwrap();
    let mut region = SharedRegion::open(&name, 8).unwrap();
    for _ in 0..1000 {
        region
            .update(|bytes| {
                let value = u64::from_ne_bytes((&*bytes).try_into().unwrap());
                bytes.copy_from_slice(&(value + 1).to_ne_bytes());
            })
            .unwrap();
    }
}

#[test]
fn shared_region_works_between_processes() {
    let name = format!("test-processes-{}", std::process::id());
    let exe = std::env::current_exe().unwrap();
    let children: Vec<_> = (0..3)
        .map(|_| {
            std::process::Command::new(&exe)
                .args(["--ignored", "--exact", "shared_region_child"])
                .env("LIBGLASS_TEST_REGION", &name)
                .output_spawned()
        })
        .collect();
    for child in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    let region = SharedRegion::open(&name, 8).unwrap();
    let bytes = region.read(0, 8).unwrap();
    assert_eq!(u64::from_ne_bytes(bytes.try_into().unwrap()), 3000);
    drop(region);
    SharedRegion::remove(&name).unwrap();
}

trait SpawnQuietly {
    fn output_spawned(&mut self) -> std::process::Child;
}

impl SpawnQuietly for std::process::Command {
    fn output_spawned(&mut self) -> std::process::Child {
        self.stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap()
    }
}

#[cfg(feature = "serde")]
#[test]
fn any_serde_type_can_be_sent() {
    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
    struct Pose {
        name: String,
        position: [f64; 3],
    }

    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let ev_a = Event::<Serde<Pose>>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<Serde<Pose>>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let pose = Serde(Pose {
        name: "head".into(),
        position: [1.0, 2.5, -3.0],
    });
    ev_a.enqueue(&pose, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), pose);
}

#[cfg(unix)]
mod unix {
    use super::*;

    fn socket_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("libglass-{}-{name}", std::process::id()))
    }

    #[test]
    fn everything_works_over_a_unix_socket() {
        let path = socket_path("basic");
        let server = GlassServer::bind_unix(&path).unwrap();
        let a = GlassClient::connect_unix(&path, "a").unwrap();
        let b = GlassClient::connect_unix(&path, "b").unwrap();
        assert!(a.is_master());
        let ev_a = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
        let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
        let barrier_a = Barrier::new(&a, 0, 0, true, Mode::Local).unwrap();
        let barrier_b = Barrier::new(&b, 0, 0, true, Mode::Local).unwrap();
        thread::sleep(SETTLE);

        ev_a.enqueue(&1, EVERYBODY_ID, false).unwrap();
        assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), 1);
        thread::scope(|scope| {
            scope.spawn(|| barrier_a.sync().unwrap());
            barrier_b.sync().unwrap();
        });

        drop((ev_a, ev_b, barrier_a, barrier_b, a, b));
        drop(server);
        assert!(!path.exists(), "the socket must be removed");
    }

    #[test]
    fn stale_socket_is_replaced_but_a_regular_file_is_not() {
        let path = socket_path("stale");
        {
            let _left_behind = std::os::unix::net::UnixListener::bind(&path).unwrap();
        }
        assert!(path.exists());
        let server = GlassServer::bind_unix(&path).unwrap();
        drop(server);

        let file = socket_path("regular");
        std::fs::write(&file, b"precious").unwrap();
        assert!(GlassServer::bind_unix(&file).is_err());
        assert_eq!(std::fs::read(&file).unwrap(), b"precious");
        std::fs::remove_file(file).unwrap();
    }
}
