//! Full client/server scenarios over real sockets, based on the C++ test suite
//! in `test/automatic`.

use libglass::packet::Packet;
use libglass::protocol::RootMsg;
use libglass::*;
use std::collections::BTreeSet;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);
/// Time for the server to process what other connections have sent.
const SETTLE: Duration = Duration::from_millis(300);

fn start_server() -> (GlassServer, SocketAddr) {
    let server = GlassServer::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr();
    (server, addr)
}

fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn clients_learn_about_each_other_and_the_master() {
    let (server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();

    assert_eq!(a.id(), BASE_SERVER_ID + 1);
    assert_eq!(b.id(), BASE_SERVER_ID + 2);
    assert_eq!(a.server_id(), BASE_SERVER_ID);
    assert!(a.is_master());
    assert!(!b.is_master());

    wait_until("a sees b", || a.total_nodes() == 3);
    assert_eq!(server.total_nodes(), 3);
    assert_eq!(a.nickname_to_id("b").unwrap(), b.id());
    assert!(matches!(
        a.nickname_to_id("nobody"),
        Err(Error::UnknownNode(_))
    ));

    drop(a);
    wait_until("b becomes the master", || b.is_master());
    assert_eq!(b.total_nodes(), 2);
    assert_eq!(server.master(), Some(b.id()));
}

#[test]
fn abrupt_disconnection_removes_the_node() {
    let (server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();

    let raw = TcpStream::connect(addr).unwrap();
    let hello = Packet {
        plugin: ROOT_PLUGIN_ID,
        payload: RootMsg::Hello("raw".into()).encode(),
        ..Packet::default()
    };
    (&raw).write_all(&hello.encode().unwrap()).unwrap();
    wait_until("server sees raw client", || server.total_nodes() == 3);

    drop(raw);
    wait_until("server removes raw client", || server.total_nodes() == 2);
    wait_until("a is told", || a.total_nodes() == 2);
}

/// Based on `autotestevent.cpp`: the master sends and everybody gets each event.
#[test]
fn events_reach_every_client_in_order() {
    const CLIENTS: usize = 5;
    const LOOPS: i32 = 5;

    let (_server, addr) = start_server();
    let start = Arc::new(std::sync::Barrier::new(CLIENTS));

    let mut threads = Vec::new();
    for i in 0..CLIENTS {
        let start = start.clone();
        threads.push(thread::spawn(move || {
            let client = GlassClient::connect(addr, &format!("node{i}")).unwrap();
            let barrier = Barrier::new(&client, 0, 0, true, Mode::Local).unwrap();
            let event = Event::<i32>::new(&client, "eventint", Mode::Local).unwrap();
            start.wait();
            thread::sleep(SETTLE);

            assert_eq!(client.total_nodes(), CLIENTS + 1);
            assert!(event.is_empty());
            start.wait();

            for value in 0..LOOPS {
                if client.is_master() {
                    event.enqueue(&value, EVERYBODY_ID, true).unwrap();
                }
                barrier.sync().unwrap();
                assert_eq!(event.get_timeout(TIMEOUT).unwrap(), value);
            }
            barrier.sync().unwrap();
            assert!(event.is_empty());
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn events_respect_targets_and_ownership() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let c = GlassClient::connect(addr, "c").unwrap();

    let ev_a = Event::<String>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<String>::new(&b, "ev", Mode::Local).unwrap();
    let ev_c = Event::<String>::new(&c, "ev", Mode::Local).unwrap();
    assert!(matches!(
        Event::<String>::new(&a, "ev", Mode::Local),
        Err(Error::AlreadyExists(_))
    ));
    thread::sleep(SETTLE);

    ev_a.enqueue(&"only b".to_owned(), b.id(), false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), "only b");

    let to_all = "all".to_owned();
    ev_a.enqueue(&to_all, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), "all");
    assert_eq!(ev_c.get_timeout(TIMEOUT).unwrap(), "all");
    assert!(ev_a.is_empty(), "the sender must not get its own event");

    let targets = BTreeSet::from([b.id(), c.id()]);
    ev_a.enqueue_many(&"many".to_owned(), &targets, false)
        .unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), "many");
    assert_eq!(ev_c.get_timeout(TIMEOUT).unwrap(), "many");

    drop(ev_c);
    thread::sleep(SETTLE);
    ev_a.enqueue(&"not c".to_owned(), EVERYBODY_ID, false)
        .unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), "not c");
    let ev_c = Event::<String>::new(&c, "ev", Mode::Local).unwrap();
    assert!(matches!(
        ev_c.get_timeout(Duration::from_millis(200)),
        Err(Error::Timeout)
    ));
}

#[test]
fn event_handler_gets_queued_and_new_events() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let ev_a = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    ev_a.enqueue(&1, EVERYBODY_ID, false).unwrap();
    wait_until("the event is queued", || ev_b.queue_size() == 1);

    let (tx, rx) = channel();
    ev_b.set_handler(move |value| tx.send(value).unwrap());
    assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), 1);

    ev_a.enqueue(&2, EVERYBODY_ID, false).unwrap();
    assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), 2);
    assert!(ev_b.is_empty());
}

#[test]
fn event_with_wrong_type_is_an_error_not_a_crash() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let ev_a = Event::<String>::new(&a, "ev", Mode::Local).unwrap();
    let ev_b = Event::<i32>::new(&b, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    ev_a.enqueue(&"not a number".to_owned(), EVERYBODY_ID, false)
        .unwrap();
    assert!(matches!(ev_b.get_timeout(TIMEOUT), Err(Error::Decode)));
}

#[test]
fn binary_events_keep_nul_bytes() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let ev_a = Event::<Bytes>::new(&a, "bin", Mode::Local).unwrap();
    let ev_b = Event::<Bytes>::new(&b, "bin", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    let data = Bytes(vec![0, 1, 0, 255, 0]);
    ev_a.enqueue(&data, EVERYBODY_ID, false).unwrap();
    assert_eq!(ev_b.get_timeout(TIMEOUT).unwrap(), data);
}

/// Based on `autotestshared.cpp`.
#[test]
fn shared_variable_travels_between_clients() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let barrier_a = Arc::new(Barrier::new(&a, 0, 0, true, Mode::Local).unwrap());
    let barrier_b = Barrier::new(&b, 0, 0, true, Mode::Local).unwrap();
    let var_a = Shared::new(&a, "x", 0i32, Mode::Local).unwrap();
    let var_b = Shared::new(&b, "x", 0i32, Mode::Local).unwrap();
    thread::sleep(SETTLE);

    // Nothing published: the local value stays.
    var_b.set(5);
    assert!(var_b.has_changed());
    var_b.get_update().unwrap();
    assert_eq!(var_b.get(), 5);
    assert!(!var_b.has_changed());

    for value in 1..=3 {
        var_a.set(value);
        var_a.send_update().unwrap();
        // Getting the answer proves that the server has processed the update.
        var_a.get_update().unwrap();

        var_b.get_update().unwrap();
        assert_eq!(var_b.get(), value);

        let barrier = barrier_a.clone();
        let other = thread::spawn(move || barrier.sync().unwrap());
        barrier_b.sync().unwrap();
        other.join().unwrap();
    }
}

#[test]
fn shared_update_modes() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let var = Shared::new(&a, "x", 1i32, Mode::Local).unwrap();

    var.set_update_mode(UpdateMode::Never);
    var.set(2);
    var.send_update().unwrap();
    var.set(3);
    var.get_update().unwrap();
    assert_eq!(var.get(), 3, "nothing was published");

    var.set_update_mode(UpdateMode::Changed);
    var.send_update().unwrap();
    var.get_update().unwrap();
    assert_eq!(var.get(), 3, "unchanged since get_update, so not published");

    var.set(4);
    var.send_update().unwrap();
    var.set(5);
    var.get_update().unwrap();
    assert_eq!(var.get(), 4);

    var.set_update_mode(UpdateMode::NextOnly);
    var.set(6);
    var.send_update().unwrap();
    var.get_update().unwrap();
    assert_eq!(var.update_mode(), UpdateMode::Never);
    assert_eq!(var.get(), 6);
}

/// Based on `autotestbarrier.cpp`.
#[test]
fn barrier_holds_clients_until_all_arrive() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let barrier_a = Barrier::new(&a, 1, 0, true, Mode::Local).unwrap();
    let barrier_b = Barrier::new(&b, 1, 0, true, Mode::Local).unwrap();
    assert!(matches!(
        Barrier::new(&a, 1, 0, true, Mode::Local),
        Err(Error::AlreadyExists(_))
    ));
    thread::sleep(SETTLE);

    for _ in 0..3 {
        let (tx, rx) = channel();
        thread::scope(|scope| {
            scope.spawn(|| {
                barrier_a.sync().unwrap();
                tx.send(()).unwrap();
            });
            assert!(rx.recv_timeout(SETTLE).is_err(), "a must wait for b");
            barrier_b.sync().unwrap();
            rx.recv_timeout(TIMEOUT).unwrap();
        });
    }
}

#[test]
fn threads_of_a_client_share_one_sync() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let barrier = Barrier::new(&a, 1, 0, true, Mode::Local).unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let barrier_b = Barrier::new(&b, 1, 0, true, Mode::Local).unwrap();
    thread::sleep(SETTLE);

    thread::scope(|scope| {
        scope.spawn(|| barrier.sync().unwrap());
        scope.spawn(|| barrier.sync().unwrap());
        thread::sleep(SETTLE);
        barrier_b.sync().unwrap();
    });
}

#[test]
fn barrier_is_released_when_the_missing_client_leaves() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let barrier_a = Barrier::new(&a, 1, 0, true, Mode::Local).unwrap();
    let barrier_b = Barrier::new(&b, 1, 0, true, Mode::Local).unwrap();
    thread::sleep(SETTLE);

    thread::scope(|scope| {
        let waiting = scope.spawn(|| barrier_a.sync());
        thread::sleep(SETTLE);
        drop(barrier_b);
        drop(b);
        waiting.join().unwrap().unwrap();
    });
}

#[test]
fn waiting_client_is_woken_when_the_server_stops() {
    let (server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let barrier = Barrier::new(&a, 1, 0, true, Mode::Local).unwrap();
    let _barrier_b = Barrier::new(&b, 1, 0, true, Mode::Local).unwrap();
    let event = Event::<i32>::new(&a, "ev", Mode::Local).unwrap();
    thread::sleep(SETTLE);

    thread::scope(|scope| {
        let waiting = scope.spawn(|| barrier.sync());
        thread::sleep(SETTLE);
        drop(server);
        assert!(matches!(waiting.join().unwrap(), Err(Error::Disconnected)));
    });
    assert!(!a.is_connected());
    assert!(matches!(
        event.get_timeout(TIMEOUT),
        Err(Error::Disconnected)
    ));
}

#[test]
fn kill_all_stops_server_and_clients_without_ending_the_process() {
    let (server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();

    a.kill_all().unwrap();
    server.wait();
    wait_until("a is disconnected", || !a.is_connected());
    wait_until("b is disconnected", || !b.is_connected());
    assert_eq!(server.total_nodes(), 1);
    assert!(GlassClient::connect(addr, "late").is_err());
}

#[test]
fn malformed_data_does_not_affect_other_clients() {
    let (server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();

    // Lengths that underflow in the C++ server.
    let mut bad = TcpStream::connect(addr).unwrap();
    bad.write_all(&[0xFF, 0xFF, 0, 0, 0, 7, 1, 2, 3]).unwrap();
    drop(bad);

    // A valid packet, but not a hello.
    let mut early = TcpStream::connect(addr).unwrap();
    let packet = Packet {
        plugin: EVENT_PLUGIN_ID,
        payload: vec![3, b'x', 0],
        ..Packet::default()
    };
    early.write_all(&packet.encode().unwrap()).unwrap();

    // Registered, then sends garbage to every plugin.
    let mut raw = TcpStream::connect(addr).unwrap();
    let hello = Packet {
        plugin: ROOT_PLUGIN_ID,
        payload: RootMsg::Hello("raw".into()).encode(),
        ..Packet::default()
    };
    raw.write_all(&hello.encode().unwrap()).unwrap();
    wait_until("raw is registered", || server.total_nodes() == 3);
    for plugin in [
        ROOT_PLUGIN_ID,
        EVENT_PLUGIN_ID,
        SHARED_PLUGIN_ID,
        BARRIER_PLUGIN_ID,
        999,
    ] {
        for payload in [
            vec![],
            vec![255],
            vec![3],
            vec![1, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0],
        ] {
            let packet = Packet {
                plugin,
                payload,
                ..Packet::default()
            };
            raw.write_all(&packet.encode().unwrap()).unwrap();
        }
    }
    drop(raw);

    wait_until("raw is removed", || server.total_nodes() == 2);
    assert!(a.is_connected());
    let event = Event::<i32>::new(&a, "still-works", Mode::Local).unwrap();
    event.enqueue(&1, a.id(), true).unwrap();
    assert_eq!(event.get_timeout(TIMEOUT).unwrap(), 1);
}

#[test]
fn a_client_cannot_impersonate_another() {
    let (server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();

    let mut raw = TcpStream::connect(addr).unwrap();
    let hello = Packet {
        plugin: ROOT_PLUGIN_ID,
        payload: RootMsg::Hello("raw".into()).encode(),
        ..Packet::default()
    };
    raw.write_all(&hello.encode().unwrap()).unwrap();
    wait_until("raw is registered", || server.total_nodes() == 3);

    // Says goodbye pretending to be `a`: the connection that says it is removed.
    let bye = Packet {
        from: a.id(),
        plugin: ROOT_PLUGIN_ID,
        payload: RootMsg::Bye.encode(),
        ..Packet::default()
    };
    raw.write_all(&bye.encode().unwrap()).unwrap();
    wait_until("raw is removed", || server.total_nodes() == 2);
    assert!(server.client_list().contains_key(&a.id()));
    assert!(a.is_connected());
}

#[test]
fn invalid_names_are_rejected() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    assert!(matches!(
        Event::<i32>::new(&a, "", Mode::Local),
        Err(Error::InvalidName)
    ));
    assert!(matches!(
        Shared::new(&a, "a\0b", 0i32, Mode::Local),
        Err(Error::InvalidName)
    ));
}
