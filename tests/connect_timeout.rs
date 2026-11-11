//! How long connecting may take. It is set for the whole process, so these tests have a
//! process of their own.

#![cfg(unix)]

use libglass::*;
use socket2::{Domain, Socket, Type};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

/// A server whose queue of connections is full: the system drops the handshakes that
/// arrive, as a lossy network would.
fn deaf_server() -> (Socket, SocketAddr, Vec<TcpStream>) {
    let listener = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
    listener.bind(&any.into()).unwrap();
    listener.listen(0).unwrap();
    let addr = listener.local_addr().unwrap().as_socket().unwrap();
    let fillers = (0..4)
        .filter_map(|_| TcpStream::connect_timeout(&addr, Duration::from_millis(300)).ok())
        .collect();
    (listener, addr, fillers)
}

#[test]
fn connecting_gives_up_after_the_timeout() {
    assert_eq!(connect_timeout(), Duration::from_secs(10), "the default");

    let (_listener, addr, _fillers) = deaf_server();
    set_connect_timeout(Duration::from_millis(500));
    let start = Instant::now();
    assert!(GlassClient::connect(addr, "a").is_err());
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(400) && elapsed < Duration::from_secs(3),
        "it took {elapsed:?}"
    );

    // Also when a client keeps trying, and when a server joins a group.
    let start = Instant::now();
    assert!(GlassClient::connect_retry(addr, "a", Duration::from_secs(1)).is_err());
    assert!(start.elapsed() < Duration::from_secs(4));
    let start = Instant::now();
    let peer = GlassServer::bind_peer("127.0.0.1:0", Some(&addr.to_string())).unwrap();
    assert!(peer.peers().is_empty(), "it starts a group of its own");
    assert!(start.elapsed() < Duration::from_secs(3));
}
