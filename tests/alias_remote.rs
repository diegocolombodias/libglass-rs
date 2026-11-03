//! Aliases and remote variables, based on the C++ suite: `autotestalias.cpp`,
//! `autotestremote.cpp` and `autopeeraliastest.cpp`.

use libglass::packet::Packet;
use libglass::*;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
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

fn seed(server: &GlassServer) -> String {
    server.local_addr().to_string()
}

fn two_peers() -> (GlassServer, GlassServer, GlassClient, GlassClient) {
    let p1 = GlassServer::bind_peer("127.0.0.1:0", None).unwrap();
    let p2 = GlassServer::bind_peer("127.0.0.1:0", Some(&seed(&p1))).unwrap();
    wait_until("the peers connect", || p1.peers().len() == 1);
    let a = GlassClient::connect(p1.local_addr(), "a").unwrap();
    let b = GlassClient::connect(p2.local_addr(), "b").unwrap();
    (p1, p2, a, b)
}

fn alias_for<'a>(client: &GlassClient, name: &str, mode: Mode) -> Alias<&'a str> {
    let alias = Alias::new(client, name, "default", mode).unwrap();
    alias.add_alias("one", "value one");
    alias.add_alias("two", "value two");
    alias
}

fn wait_for_value(alias: &Alias<&str>, expected: &str) {
    wait_until("the association arrives", || alias.get_data() == expected);
}

/// Based on `autotestalias.cpp`: the master gives each client its own name.
#[test]
fn master_gives_each_client_its_association() {
    const CLIENTS: usize = 5;
    let (_server, addr) = start_server();
    let start = Arc::new(std::sync::Barrier::new(CLIENTS));

    let threads: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let start = start.clone();
            thread::spawn(move || {
                let client = GlassClient::connect(addr, &format!("node{i}")).unwrap();
                let barrier = Barrier::new(&client, 0, 0, true, Mode::Local).unwrap();
                let alias = Alias::new(&client, "integer", 0u64, Mode::Local).unwrap();
                for id in 1..=CLIENTS as u64 {
                    alias.add_alias(&format!("name{id}"), id * 100);
                }
                assert!(!alias.check_alias());
                assert_eq!(alias.get_data(), 0);
                start.wait();
                thread::sleep(SETTLE);

                if client.is_master() {
                    for (position, id) in client.client_list().into_keys().enumerate() {
                        alias
                            .associate(id, &format!("name{}", position + 1))
                            .unwrap();
                    }
                }
                barrier.sync().unwrap();

                let position = client
                    .client_list()
                    .into_keys()
                    .position(|id| id == client.id())
                    .unwrap() as u64;
                let expected = (position + 1) * 100;
                wait_until("the association arrives", || alias.get_data() == expected);
                assert!(alias.check_alias());
                barrier.sync().unwrap();
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn alias_value_falls_back_to_the_default() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let alias = alias_for(&a, "x", Mode::Local);

    assert_eq!(alias.get_data(), "default");
    assert_eq!(alias.association(), None);

    // Associated to a name this client has no value for.
    alias.associate(a.id(), "unregistered").unwrap();
    wait_until("it arrives", || alias.check_alias());
    assert_eq!(alias.association().unwrap(), "unregistered");
    assert_eq!(alias.get_data(), "default");

    alias.add_alias("unregistered", "now it has");
    assert_eq!(alias.get_data(), "now it has");
    assert!(alias.delete_alias("unregistered"));
    assert!(!alias.delete_alias("unregistered"));
    assert_eq!(alias.get_data(), "default");
}

#[test]
fn association_is_kept_for_a_client_that_creates_the_alias_later() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let alias_a = alias_for(&a, "x", Mode::Local);
    alias_a.associate(b.id(), "two").unwrap();
    alias_a.associate(a.id(), "one").unwrap();
    wait_for_value(&alias_a, "value one");

    let alias_b = alias_for(&b, "x", Mode::Local);
    wait_for_value(&alias_b, "value two");

    // And when it creates it again, since the server still has it.
    drop(alias_b);
    let alias_b = alias_for(&b, "x", Mode::Local);
    wait_for_value(&alias_b, "value two");
}

#[test]
fn association_for_everybody_reaches_current_and_future_owners() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let c = GlassClient::connect(addr, "c").unwrap();
    let alias_a = alias_for(&a, "x", Mode::Local);
    let alias_b = alias_for(&b, "x", Mode::Local);
    thread::sleep(SETTLE);

    alias_a.associate(EVERYBODY_ID, "two").unwrap();
    wait_for_value(&alias_a, "value two");
    wait_for_value(&alias_b, "value two");

    let alias_c = alias_for(&c, "x", Mode::Local);
    wait_for_value(&alias_c, "value two");
}

#[test]
fn associate_many_and_invalid_names() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let alias_a = alias_for(&a, "x", Mode::Local);
    let alias_b = alias_for(&b, "x", Mode::Local);
    thread::sleep(SETTLE);

    let map = BTreeMap::from([(a.id(), "one".to_owned()), (b.id(), "two".to_owned())]);
    alias_a.associate_many(&map).unwrap();
    wait_for_value(&alias_a, "value one");
    wait_for_value(&alias_b, "value two");

    assert!(matches!(
        Alias::new(&a, "x", "d", Mode::Local),
        Err(Error::AlreadyExists(_))
    ));
    assert!(matches!(
        Alias::new(&a, "", "d", Mode::Local),
        Err(Error::InvalidName)
    ));
    assert!(matches!(
        Remote::<i32>::new(&a, "a\0b", &0),
        Err(Error::InvalidName)
    ));
}

/// Based on `autopeeraliastest.cpp`.
#[test]
fn global_alias_crosses_peers_and_local_one_does_not() {
    let (_p1, _p2, a, b) = two_peers();
    let local_a = alias_for(&a, "local", Mode::Local);
    let local_b = alias_for(&b, "local", Mode::Local);
    let global_a = alias_for(&a, "global", Mode::Global);
    let global_b = alias_for(&b, "global", Mode::Global);
    thread::sleep(SETTLE);

    global_a.associate(b.id(), "two").unwrap();
    wait_for_value(&global_b, "value two");
    global_b.associate(a.id(), "one").unwrap();
    wait_for_value(&global_a, "value one");

    local_a.associate(b.id(), "two").unwrap();
    thread::sleep(SETTLE);
    assert_eq!(local_b.get_data(), "default");

    global_a.associate(EVERYBODY_ID, "two").unwrap();
    wait_for_value(&global_a, "value two");
    wait_for_value(&global_b, "value two");
}

#[test]
fn client_of_another_peer_gets_its_association_when_it_arrives() {
    let (_p1, p2, a, b) = two_peers();
    let global_a = alias_for(&a, "x", Mode::Global);
    thread::sleep(SETTLE);

    // b has not created the alias yet, and c does not even exist.
    global_a.associate(b.id(), "two").unwrap();
    global_a.associate(EVERYBODY_ID, "one").unwrap();
    thread::sleep(SETTLE);

    // "Everybody" replaces what came before, on all the servers.
    let global_b = alias_for(&b, "x", Mode::Global);
    wait_for_value(&global_b, "value one");

    let c = GlassClient::connect(p2.local_addr(), "c").unwrap();
    let global_c = alias_for(&c, "x", Mode::Global);
    wait_for_value(&global_c, "value one");
}

#[test]
fn peer_that_joins_later_gets_existing_associations() {
    let p1 = GlassServer::bind_peer("127.0.0.1:0", None).unwrap();
    let a = GlassClient::connect(p1.local_addr(), "a").unwrap();
    let alias_a = alias_for(&a, "x", Mode::Global);
    let remote_a = Remote::new(&a, "r", &5i32).unwrap();
    alias_a.associate(EVERYBODY_ID, "two").unwrap();
    wait_for_value(&alias_a, "value two");

    let p2 = GlassServer::bind_peer("127.0.0.1:0", Some(&seed(&p1))).unwrap();
    wait_until("the peers connect", || p1.peers().len() == 1);
    thread::sleep(SETTLE);

    let b = GlassClient::connect(p2.local_addr(), "b").unwrap();
    let alias_b = alias_for(&b, "x", Mode::Global);
    wait_for_value(&alias_b, "value two");

    let remote_b = Remote::new(&b, "r", &0i32).unwrap();
    thread::sleep(SETTLE);
    assert_eq!(remote_b.get_value(a.id()).unwrap(), 5);
    remote_b.set_value(a.id(), &6).unwrap();
    wait_until("the value arrives", || remote_a.get().unwrap() == 6);
}

/// Based on `autotestremote.cpp`: the master sets a different value on each.
#[test]
fn master_sets_the_value_of_each_client() {
    const CLIENTS: usize = 5;
    let (_server, addr) = start_server();
    let start = Arc::new(std::sync::Barrier::new(CLIENTS));

    let threads: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let start = start.clone();
            thread::spawn(move || {
                let client = GlassClient::connect(addr, &format!("node{i}")).unwrap();
                let remote = Remote::new(&client, "remoteint", &0u32).unwrap();
                assert_eq!(remote.get().unwrap(), 0);
                start.wait();
                thread::sleep(SETTLE);

                for round in 1..=2 {
                    if client.is_master() {
                        for id in client.client_list().into_keys() {
                            remote.set_value(id, &(id + round)).unwrap();
                        }
                    }
                    let expected = client.id() + round;
                    wait_until("the value arrives", || remote.get().unwrap() == expected);
                    start.wait();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn remote_value_can_be_read_from_another_client() {
    // `GLASS_GET` is a TODO in the C++ code.
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let remote_a = Remote::new(&a, "x", &1i32).unwrap();
    let remote_b = Remote::new(&b, "x", &2i32).unwrap();
    thread::sleep(SETTLE);

    assert_eq!(remote_a.get_value(b.id()).unwrap(), 2);
    assert_eq!(remote_b.get_value(a.id()).unwrap(), 1);

    remote_b.set(&3);
    assert_eq!(remote_a.get_value(b.id()).unwrap(), 3);
    assert_eq!(
        remote_a.get().unwrap(),
        1,
        "the local value does not change"
    );

    // Local shortcuts.
    assert_eq!(remote_a.get_value(a.id()).unwrap(), 1);
    assert_eq!(remote_a.get_value(NOBODY_ID).unwrap(), 1);
    remote_a.set_value(NOBODY_ID, &4).unwrap();
    assert_eq!(remote_a.get().unwrap(), 4);
    remote_a.set_value(a.id(), &5).unwrap();
    assert_eq!(remote_a.get().unwrap(), 5);
}

#[test]
fn remote_get_handles_concurrent_requests() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let c = GlassClient::connect(addr, "c").unwrap();
    let remote_a = Remote::new(&a, "x", &1i32).unwrap();
    let _remote_b = Remote::new(&b, "x", &2i32).unwrap();
    let _remote_c = Remote::new(&c, "x", &3i32).unwrap();
    thread::sleep(SETTLE);

    // The answers of b and c must not be mixed up.
    thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..50 {
                    assert_eq!(remote_a.get_value(b.id()).unwrap(), 2);
                    assert_eq!(remote_a.get_value(c.id()).unwrap(), 3);
                }
            });
        }
    });
}

#[test]
fn remote_set_to_everybody_skips_the_sender() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let c = GlassClient::connect(addr, "c").unwrap();
    let remote_a = Remote::new(&a, "x", &0i32).unwrap();
    let remote_b = Remote::new(&b, "x", &0i32).unwrap();
    let remote_c = Remote::new(&c, "x", &0i32).unwrap();
    thread::sleep(SETTLE);

    remote_a.set_value(EVERYBODY_ID, &9).unwrap();
    wait_until("b gets it", || remote_b.get().unwrap() == 9);
    wait_until("c gets it", || remote_c.get().unwrap() == 9);
    assert_eq!(remote_a.get().unwrap(), 0);
}

#[test]
fn remote_set_only_reaches_its_target() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let c = GlassClient::connect(addr, "c").unwrap();
    let remote_a = Remote::new(&a, "x", &0i32).unwrap();
    let remote_b = Remote::new(&b, "x", &0i32).unwrap();
    let remote_c = Remote::new(&c, "x", &0i32).unwrap();
    thread::sleep(SETTLE);

    remote_a.set_value(b.id(), &1).unwrap();
    wait_until("b gets it", || remote_b.get().unwrap() == 1);
    thread::sleep(SHORT);
    assert_eq!(remote_c.get().unwrap(), 0);
}

#[test]
fn remote_copies_move_the_local_value() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let remote_a = Remote::new(&a, "x", &1i32).unwrap();
    let remote_b = Remote::new(&b, "x", &2i32).unwrap();
    thread::sleep(SETTLE);

    remote_a.copy_data_from_remote(b.id()).unwrap();
    assert_eq!(remote_a.get().unwrap(), 2);

    remote_a.set(&7);
    remote_a.copy_data_to_remote(b.id()).unwrap();
    wait_until("b gets it", || remote_b.get().unwrap() == 7);

    remote_a.copy_data_to_remote(a.id()).unwrap();
    remote_a.copy_data_from_remote(a.id()).unwrap();
    assert_eq!(remote_a.get().unwrap(), 7);
}

#[test]
fn remote_get_fails_cleanly() {
    let (server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let c = GlassClient::connect(addr, "c").unwrap();
    let remote_a = Remote::new(&a, "x", &1i32).unwrap();
    let text_b = Remote::new(&b, "x", &"not a number".to_owned()).unwrap();
    assert!(matches!(
        Remote::<i32>::new(&a, "x", &1),
        Err(Error::AlreadyExists(_))
    ));
    thread::sleep(SETTLE);

    // Wrong type: it reports an error instead of showing garbage.
    assert!(matches!(remote_a.get_value(b.id()), Err(Error::Decode)));
    drop(text_b);

    // A node without this variable, and a node that does not exist.
    assert!(matches!(
        remote_a.get_value_timeout(c.id(), SHORT),
        Err(Error::Timeout)
    ));
    assert!(matches!(
        remote_a.get_value_timeout(0x7FFF_0001, SHORT),
        Err(Error::Timeout)
    ));

    // A group would give several answers.
    assert!(matches!(
        remote_a.get_value(EVERYBODY_ID),
        Err(Error::InvalidTarget(EVERYBODY_ID))
    ));

    // Whoever is waiting for an answer is told when the server goes away.
    thread::scope(|scope| {
        let waiting = scope.spawn(|| remote_a.get_value(c.id()));
        thread::sleep(SHORT);
        drop(server);
        assert!(matches!(waiting.join().unwrap(), Err(Error::Disconnected)));
    });
}

#[test]
fn raw_format_works_with_remote_variables() {
    let (_server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let b = GlassClient::connect(addr, "b").unwrap();
    let remote_a = Remote::new(&a, "x", &Raw(1i32)).unwrap();
    let remote_b = Remote::new(&b, "x", &Raw(2i32)).unwrap();
    thread::sleep(SETTLE);

    remote_a.set_value(b.id(), &Raw(-5)).unwrap();
    wait_until("b gets it", || remote_b.get().unwrap() == Raw(-5));
    assert_eq!(remote_b.get_value(a.id()).unwrap(), Raw(1));
}

#[test]
fn remote_variables_work_across_peers() {
    let (_p1, p2, a, b) = two_peers();
    let b2 = GlassClient::connect(p2.local_addr(), "b2").unwrap();
    let remote_a = Remote::new(&a, "x", &1i32).unwrap();
    let remote_b = Remote::new(&b, "x", &2i32).unwrap();
    let remote_b2 = Remote::new(&b2, "x", &3i32).unwrap();
    thread::sleep(SETTLE);

    assert_eq!(remote_a.get_value(b.id()).unwrap(), 2);
    assert_eq!(remote_a.get_value(b2.id()).unwrap(), 3);
    assert_eq!(remote_b2.get_value(a.id()).unwrap(), 1);

    remote_a.set_value(b2.id(), &30).unwrap();
    wait_until("b2 gets it", || remote_b2.get().unwrap() == 30);
    assert_eq!(remote_b.get().unwrap(), 2, "b was not the target");

    remote_b.set_value(EVERYBODY_ID, &20).unwrap();
    wait_until("a gets it", || remote_a.get().unwrap() == 20);
    wait_until("b2 gets it", || remote_b2.get().unwrap() == 20);
}

#[test]
fn aliases_and_remote_variables_cross_groups() {
    let g1 = GlassServer::bind_remote_peer("127.0.0.1:0", None).unwrap();
    let g2 = GlassServer::bind_remote_peer("127.0.0.1:0", Some(&seed(&g1))).unwrap();
    let p1 = GlassServer::bind_peer("127.0.0.1:0", Some(&seed(&g1))).unwrap();
    let p2 = GlassServer::bind_peer("127.0.0.1:0", Some(&seed(&g2))).unwrap();
    wait_until("the groups form", || {
        g1.peers().len() == 2 && g2.peers().len() == 2
    });
    let a = GlassClient::connect(p1.local_addr(), "a").unwrap();
    let b = GlassClient::connect(p2.local_addr(), "b").unwrap();

    let alias_a = alias_for(&a, "alias", Mode::Global);
    let alias_b = alias_for(&b, "alias", Mode::Global);
    let remote_a = Remote::new(&a, "remote", &1i32).unwrap();
    let remote_b = Remote::new(&b, "remote", &2i32).unwrap();
    thread::sleep(SETTLE);

    alias_a.associate(b.id(), "two").unwrap();
    wait_for_value(&alias_b, "value two");

    assert_eq!(remote_a.get_value(b.id()).unwrap(), 2);
    assert_eq!(remote_b.get_value(a.id()).unwrap(), 1);
    remote_a.set_value(b.id(), &9).unwrap();
    wait_until("b gets it", || remote_b.get().unwrap() == 9);
}

#[test]
fn malformed_packets_for_the_new_plugins_do_not_affect_a_server() {
    let (server, addr) = start_server();
    let a = GlassClient::connect(addr, "a").unwrap();
    let alias = alias_for(&a, "x", Mode::Local);

    let mut raw = TcpStream::connect(addr).unwrap();
    let hello = Packet {
        plugin: ROOT_PLUGIN_ID,
        payload: protocol::RootMsg::Hello("raw".into()).encode(),
        ..Packet::default()
    };
    raw.write_all(&hello.encode().unwrap()).unwrap();
    wait_until("raw is registered", || server.total_nodes() == 3);

    for plugin in [ALIAS_PLUGIN_ID, REMOTE_PLUGIN_ID] {
        for payload in [
            vec![],
            vec![255],
            vec![3],
            vec![3, b'x', 0],
            vec![3, b'x', 0, 1],
            vec![3, b'x', 0, 0, 0, 0, 1, 0xFF],
            vec![2, b'x'],
        ] {
            let packet = Packet {
                plugin,
                targets: BTreeSet::from([EVERYBODY_ID]),
                payload,
                ..Packet::default()
            };
            raw.write_all(&packet.encode().unwrap()).unwrap();
        }
    }
    drop(raw);

    wait_until("raw is removed", || server.total_nodes() == 2);
    assert!(a.is_connected());
    alias.associate(a.id(), "one").unwrap();
    wait_for_value(&alias, "value one");
}
