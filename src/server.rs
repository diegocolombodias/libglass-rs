use crate::alias::AliasServer;
use crate::barrier::BarrierServer;
use crate::conn::Conn;
use crate::error::{Error, Result};
use crate::event::EventServer;
use crate::messenger::MessengerServer;
use crate::outbound::Outbound;
use crate::owners::{Links, Relation, Source};
use crate::packet::Packet;
use crate::plugin::{self, ServerPlugin, MAX_RESERVED_PLUGIN_ID};
use crate::protocol::{PeerAddr, RootMsg, NO_MASTER};
#[cfg(feature = "quic")]
use crate::quic::{QuicFingerprint, QuicIdentity, QuicTrust};
use crate::remote::RemoteServer;
use crate::shared::SharedServer;
use crate::transport::{Listener, Stream, Transport};
use crate::types::*;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{BuildHasher, Hasher};
use std::io::ErrorKind;
use std::net::Ipv4Addr;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, TcpListener, ToSocketAddrs, UdpSocket};
#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);
/// How many ids a joining server tries, when another server already has the one it drew.
const JOIN_ATTEMPTS: usize = 8;
/// How long a server keeps calling back another one whose link broke.
const RECONNECT_WINDOW: Duration = Duration::from_secs(30);
const FIRST_RECONNECT_PAUSE: Duration = Duration::from_millis(100);
const MAX_RECONNECT_PAUSE: Duration = Duration::from_secs(2);
const MAX_CLIENTS: NodeId = 0xFFFF;
const UDP_ERROR_PAUSE: Duration = Duration::from_millis(10);
/// The largest UDP payload. Whatever is larger goes through the stream.
pub(crate) const MAX_DATAGRAM_SIZE: usize = 65507;

type ConnId = u64;

enum Incoming {
    Accepted(ConnId, Arc<Conn>),
    /// A connection this server opened to another server, which accepted it.
    Dialed {
        conn_id: ConnId,
        conn: Arc<Conn>,
        id: NodeId,
        kind: LinkKind,
        addr: PeerAddr,
        nonce: u64,
    },
    Packet(ConnId, Packet),
    Datagram(SocketAddr, Packet),
    Closed(ConnId),
    AddPlugin(PluginId, Box<dyn ServerPlugin>),
    Stop,
}

struct Client {
    conn: Arc<Conn>,
    nickname: String,
    /// Where to send unreliable packets, if the client asked for them.
    udp: Option<SocketAddr>,
}

/// What this server knows about another server it is connected to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerInfo {
    pub kind: LinkKind,
    pub addr: PeerAddr,
}

struct Link {
    conn: Arc<Conn>,
    info: PeerInfo,
    /// The number the other server drew when it started. Two servers with the same id
    /// have different ones, unless one of them is from the C++ library, which has none.
    nonce: u64,
}

#[derive(Default)]
struct Directory {
    clients: BTreeMap<NodeId, Client>,
    links: BTreeMap<NodeId, Link>,
    master: NodeId,
    plugin_ids: BTreeSet<PluginId>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Standalone,
    Peer,
    RemotePeer,
}

impl Role {
    fn accepts(self, kind: LinkKind) -> bool {
        match self {
            Role::Standalone => false,
            Role::Peer => kind == LinkKind::Peer,
            Role::RemotePeer => true,
        }
    }
}

/// The coordinator of a group of clients.
///
/// Packets are processed one at a time by a single thread, which owns the state
/// of the plugins. The server stops when dropped or when a client calls
/// [`GlassClient::kill_all`](crate::GlassClient::kill_all).
pub struct GlassServer {
    id: NodeId,
    addr: SocketAddr,
    directory: Arc<Mutex<Directory>>,
    stop: Arc<AtomicBool>,
    stopped: Arc<(Mutex<bool>, Condvar)>,
    tx: Sender<Incoming>,
    accept_thread: Option<JoinHandle<()>>,
    dispatch_thread: Option<JoinHandle<()>>,
    udp_thread: Option<JoinHandle<()>>,
    udp_addr: Option<SocketAddr>,
    datagrams: Arc<AtomicU64>,
    #[cfg(unix)]
    unix_path: Option<PathBuf>,
    #[cfg(feature = "quic")]
    quic_fingerprint: Option<QuicFingerprint>,
}

struct Options<'a> {
    role: Role,
    seed: Option<&'a str>,
    udp: Option<UdpSocket>,
    transport: Transport,
}

impl GlassServer {
    /// Starts a server. Use port 0 to let the system choose a free port.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<GlassServer> {
        GlassServer::start_tcp(addr, Role::Standalone, None, false)
    }

    /// Starts a server that also accepts unreliable packets through UDP from
    /// clients that use
    /// [`GlassClient::connect_with_udp`](crate::GlassClient::connect_with_udp).
    pub fn bind_with_udp<A: ToSocketAddrs>(addr: A) -> Result<GlassServer> {
        GlassServer::start_tcp(addr, Role::Standalone, None, true)
    }

    /// Starts a server that works together with others as a group: clients of
    /// any of them share the objects created with [`Mode::Global`].
    ///
    /// `seed` is the address (`"host:port"`) of any server of the group. It
    /// tells this one about the others, and they all connect to each other. If
    /// the seed cannot be reached, this server starts as the first of a group.
    pub fn bind_peer<A: ToSocketAddrs>(addr: A, seed: Option<&str>) -> Result<GlassServer> {
        GlassServer::start_tcp(addr, Role::Peer, seed, false)
    }

    /// Like [`bind_peer`](Self::bind_peer), and it accepts UDP from its clients
    /// like [`bind_with_udp`](Self::bind_with_udp). Between servers, packets
    /// always go through TCP.
    pub fn bind_peer_with_udp<A: ToSocketAddrs>(
        addr: A,
        seed: Option<&str>,
    ) -> Result<GlassServer> {
        GlassServer::start_tcp(addr, Role::Peer, seed, true)
    }

    /// Starts a peer that is also a gateway between groups.
    ///
    /// Peers join its group with [`bind_peer`](Self::bind_peer). `seed` is the
    /// address of any gateway of another group.
    pub fn bind_remote_peer<A: ToSocketAddrs>(addr: A, seed: Option<&str>) -> Result<GlassServer> {
        GlassServer::start_tcp(addr, Role::RemotePeer, seed, false)
    }

    /// Starts a server on SCTP, for clients that use
    /// [`GlassClient::connect_sctp`](crate::GlassClient::connect_sctp). Returns an
    /// error if the kernel does not have SCTP.
    #[cfg(unix)]
    pub fn bind_sctp<A: ToSocketAddrs>(addr: A) -> Result<GlassServer> {
        GlassServer::start_sctp(addr, Role::Standalone, None)
    }

    /// Like [`bind_peer`](Self::bind_peer), with SCTP for the other servers and for
    /// its clients. All the servers of a group must use it.
    #[cfg(unix)]
    pub fn bind_peer_sctp<A: ToSocketAddrs>(addr: A, seed: Option<&str>) -> Result<GlassServer> {
        GlassServer::start_sctp(addr, Role::Peer, seed)
    }

    /// Like [`bind_remote_peer`](Self::bind_remote_peer), with SCTP for the other
    /// servers and for its clients. All the servers involved must use it.
    #[cfg(unix)]
    pub fn bind_remote_peer_sctp<A: ToSocketAddrs>(
        addr: A,
        seed: Option<&str>,
    ) -> Result<GlassServer> {
        GlassServer::start_sctp(addr, Role::RemotePeer, seed)
    }

    /// Starts a server on QUIC, for clients that use
    /// [`GlassClient::connect_quic`](crate::GlassClient::connect_quic). It generates a
    /// new self-signed certificate, whose fingerprint
    /// [`quic_fingerprint`](Self::quic_fingerprint) tells. Clients can only be sure
    /// they reached this server if they get the fingerprint by a channel they trust:
    /// see [`QuicTrust`].
    #[cfg(feature = "quic")]
    pub fn bind_quic<A: ToSocketAddrs>(addr: A) -> Result<GlassServer> {
        GlassServer::start_quic(addr, Role::Standalone, None, &QuicIdentity::generate()?)
    }

    /// Like [`bind_peer`](Self::bind_peer), with QUIC for the other servers and for
    /// its clients. All the servers of a group must use it, with the same `identity`:
    /// each one accepts only that certificate from the others.
    #[cfg(feature = "quic")]
    pub fn bind_peer_quic<A: ToSocketAddrs>(
        addr: A,
        seed: Option<&str>,
        identity: &QuicIdentity,
    ) -> Result<GlassServer> {
        GlassServer::start_quic(addr, Role::Peer, seed, identity)
    }

    /// Like [`bind_remote_peer`](Self::bind_remote_peer), with QUIC for the other
    /// servers and for its clients. All the servers involved, in every group, must
    /// use it with the same `identity`.
    #[cfg(feature = "quic")]
    pub fn bind_remote_peer_quic<A: ToSocketAddrs>(
        addr: A,
        seed: Option<&str>,
        identity: &QuicIdentity,
    ) -> Result<GlassServer> {
        GlassServer::start_quic(addr, Role::RemotePeer, seed, identity)
    }

    /// The fingerprint of the certificate of a QUIC server, for
    /// [`QuicTrust::Fingerprint`].
    #[cfg(feature = "quic")]
    pub fn quic_fingerprint(&self) -> Option<QuicFingerprint> {
        self.quic_fingerprint
    }

    /// Starts a server on a Unix socket, for clients on the same machine that
    /// use [`GlassClient::connect_unix`](crate::GlassClient::connect_unix).
    ///
    /// A socket left behind by a server that has died is replaced, but any other
    /// kind of file at that path is an error. The socket is removed when the
    /// server is dropped.
    #[cfg(unix)]
    pub fn bind_unix<P: AsRef<Path>>(path: P) -> Result<GlassServer> {
        use std::os::unix::fs::FileTypeExt;
        let path = path.as_ref();
        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            if metadata.file_type().is_socket() {
                std::fs::remove_file(path)?;
            }
        }
        let listener = std::os::unix::net::UnixListener::bind(path)?;
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let options = Options {
            role: Role::Standalone,
            seed: None,
            udp: None,
            transport: Transport::Tcp,
        };
        let mut server = GlassServer::start(Listener::Unix(listener), addr, options)?;
        server.unix_path = Some(path.to_owned());
        Ok(server)
    }

    fn start_tcp<A: ToSocketAddrs>(
        addr: A,
        role: Role,
        seed: Option<&str>,
        udp: bool,
    ) -> Result<GlassServer> {
        let addrs: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();
        let listener = TcpListener::bind(&addrs[..])?;
        let addr = listener.local_addr()?;
        let port_chosen_by_system = addrs.iter().all(|addr| addr.port() == 0);
        let udp = udp
            .then(|| bind_udp(addr, port_chosen_by_system))
            .transpose()?;
        let options = Options {
            role,
            seed,
            udp,
            transport: Transport::Tcp,
        };
        GlassServer::start(Listener::Tcp(listener), addr, options)
    }

    #[cfg(unix)]
    fn start_sctp<A: ToSocketAddrs>(
        addr: A,
        role: Role,
        seed: Option<&str>,
    ) -> Result<GlassServer> {
        let addrs: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();
        let (listener, addr) = Listener::bind_sctp(&addrs)?;
        let options = Options {
            role,
            seed,
            udp: None,
            transport: Transport::Sctp,
        };
        GlassServer::start(listener, addr, options)
    }

    #[cfg(feature = "quic")]
    fn start_quic<A: ToSocketAddrs>(
        addr: A,
        role: Role,
        seed: Option<&str>,
        identity: &QuicIdentity,
    ) -> Result<GlassServer> {
        let addrs: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();
        let (listener, addr) = Listener::bind_quic(&addrs, identity)?;
        let fingerprint = identity.fingerprint();
        let options = Options {
            role,
            seed,
            udp: None,
            transport: Transport::Quic(QuicTrust::Fingerprint(fingerprint)),
        };
        let mut server = GlassServer::start(listener, addr, options)?;
        server.quic_fingerprint = Some(fingerprint);
        Ok(server)
    }

    fn start(listener: Listener, addr: SocketAddr, options: Options) -> Result<GlassServer> {
        let Options {
            role,
            seed,
            udp,
            transport,
        } = options;
        listener.set_nonblocking()?;

        let directory = Arc::new(Mutex::new(Directory::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new((Mutex::new(false), Condvar::new()));
        let next_conn = Arc::new(AtomicU64::new(0));
        let (tx, rx) = channel();

        let mut id = match role {
            Role::Standalone => BASE_SERVER_ID,
            Role::Peer | Role::RemotePeer => generate_id(),
        };
        let kind = match role {
            Role::RemotePeer => LinkKind::Remote,
            _ => LinkKind::Peer,
        };
        let nonce = generate_nonce();
        if let Some(seed) = seed {
            let joining = Joining {
                transport,
                kind,
                nonce,
                listen: addr,
            };
            joining.join(seed, &mut id, &tx, &next_conn)?;
        }

        let udp_socket = udp.map(Arc::new);
        let udp_addr = match &udp_socket {
            Some(socket) => Some(socket.local_addr()?),
            None => None,
        };
        let datagrams = Arc::new(AtomicU64::new(0));
        let udp_thread = udp_socket.clone().map(|socket| {
            let tx = tx.clone();
            let stop = stop.clone();
            let datagrams = datagrams.clone();
            thread::spawn(move || udp_loop(socket, tx, stop, datagrams))
        });

        let accept_thread = {
            let tx = tx.clone();
            let stop = stop.clone();
            let next_conn = next_conn.clone();
            thread::spawn(move || accept_loop(listener, tx, stop, next_conn))
        };
        let dispatch_thread = {
            let dispatcher = Dispatcher {
                id,
                nonce,
                role,
                transport,
                listen: addr,
                tx: tx.clone(),
                next_conn: next_conn.clone(),
                stop: stop.clone(),
                departed: BTreeSet::new(),
                directory: directory.clone(),
                conns: BTreeMap::new(),
                nodes_by_conn: BTreeMap::new(),
                event: EventServer::default(),
                shared: SharedServer::default(),
                barrier: BarrierServer::default(),
                alias: AliasServer::default(),
                remote: RemoteServer::default(),
                messenger: MessengerServer::default(),
                plugins: BTreeMap::new(),
                udp: udp_socket,
                udp_tokens: BTreeMap::new(),
                udp_addrs: BTreeMap::new(),
                quit: false,
            };
            let stop = stop.clone();
            let stopped = stopped.clone();
            thread::spawn(move || dispatcher.run(rx, stop, stopped))
        };

        Ok(GlassServer {
            id,
            addr,
            directory,
            stop,
            stopped,
            tx,
            accept_thread: Some(accept_thread),
            dispatch_thread: Some(dispatch_thread),
            udp_thread,
            udp_addr,
            datagrams,
            #[cfg(unix)]
            unix_path: None,
            #[cfg(feature = "quic")]
            quic_fingerprint: None,
        })
    }

    /// Adds a plugin, which gets the packets clients send with
    /// [`GlassClient::send_to_plugin`](crate::GlassClient::send_to_plugin).
    /// Ids up to [`MAX_RESERVED_PLUGIN_ID`] are reserved for the library.
    pub fn add_plugin<P: ServerPlugin + 'static>(&self, id: PluginId, plugin: P) -> Result<()> {
        if id <= MAX_RESERVED_PLUGIN_ID {
            return Err(Error::InvalidPluginId(id));
        }
        if !self.directory.lock().unwrap().plugin_ids.insert(id) {
            return Err(Error::AlreadyExists(format!("plugin {id}")));
        }
        self.tx
            .send(Incoming::AddPlugin(id, Box::new(plugin)))
            .map_err(|_| Error::Disconnected)
    }

    /// The address of the UDP socket, if this server has one. It has the same port
    /// number as the TCP one, unless the system was left to choose them.
    pub fn udp_addr(&self) -> Option<SocketAddr> {
        self.udp_addr
    }

    /// How many UDP packets this server has received.
    pub fn datagrams_received(&self) -> u64 {
        self.datagrams.load(Ordering::SeqCst)
    }

    /// The address this server is listening on. On a Unix socket, it has no
    /// meaning.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// The id of this server.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Number of nodes, including this server. Other servers do not count.
    pub fn total_nodes(&self) -> usize {
        self.directory.lock().unwrap().clients.len() + 1
    }

    /// The id and nickname of each client of this server.
    pub fn client_list(&self) -> BTreeMap<NodeId, String> {
        let directory = self.directory.lock().unwrap();
        directory
            .clients
            .iter()
            .map(|(&id, client)| (id, client.nickname.clone()))
            .collect()
    }

    /// The other servers this one is connected to.
    pub fn peers(&self) -> BTreeMap<NodeId, PeerInfo> {
        let directory = self.directory.lock().unwrap();
        directory
            .links
            .iter()
            .map(|(&id, link)| (id, link.info.clone()))
            .collect()
    }

    /// The id of the master client of this server, if it has any client.
    pub fn master(&self) -> Option<NodeId> {
        let master = self.directory.lock().unwrap().master;
        (master != NO_MASTER).then_some(master)
    }

    /// Blocks until the server has stopped.
    pub fn wait(&self) {
        let (lock, cond) = &*self.stopped;
        let mut stopped = lock.lock().unwrap();
        while !*stopped {
            stopped = cond.wait(stopped).unwrap();
        }
    }
}

impl Drop for GlassServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.accept_thread.take() {
            let _ = thread.join();
        }
        let _ = self.tx.send(Incoming::Stop);
        if let Some(thread) = self.dispatch_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.udp_thread.take() {
            let _ = thread.join();
        }
        #[cfg(unix)]
        if let Some(path) = &self.unix_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Opens the UDP socket. If the system chose the TCP port, it chooses this one too:
/// the same number may not be available for UDP, for example on Windows, which
/// reserves ranges of ports separately for each protocol.
fn bind_udp(tcp: SocketAddr, port_chosen_by_system: bool) -> Result<UdpSocket> {
    let port = if port_chosen_by_system { 0 } else { tcp.port() };
    Ok(UdpSocket::bind((tcp.ip(), port))?)
}

/// Sends a datagram to a socket, to wake up a thread blocked reading from it.
pub(crate) fn wake_udp(socket: &UdpSocket) {
    let Ok(mut target) = socket.local_addr() else {
        return;
    };
    if target.ip().is_unspecified() {
        let loopback = match target {
            SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        };
        target.set_ip(loopback);
    }
    let local = match target {
        SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    };
    if let Ok(sender) = UdpSocket::bind((local, 0)) {
        let _ = sender.send_to(&[0], target);
    }
}

/// A random number, other than 0, which stands for no number.
fn generate_nonce() -> u64 {
    loop {
        let nonce = RandomState::new().build_hasher().finish();
        if nonce != 0 {
            return nonce;
        }
    }
}

/// A random id for a server. Clashes are detected when joining a group.
fn generate_id() -> NodeId {
    let random = RandomState::new().build_hasher().finish() as NodeId;
    match random & SERVER_ID_MASK {
        0 => BASE_SERVER_ID,
        id => id,
    }
}

/// The address other servers should use to reach this one. If it listens on all
/// interfaces, the one used by this connection is the one known to work.
fn advertised_addr(listen: SocketAddr, conn: &Conn) -> PeerAddr {
    let ip: IpAddr = if listen.ip().is_unspecified() {
        conn.local_ip()
    } else {
        listen.ip()
    };
    PeerAddr {
        host: ip.to_string(),
        port: listen.port(),
    }
}

/// What a server that joins a group needs to know about itself.
struct Joining {
    transport: Transport,
    kind: LinkKind,
    nonce: u64,
    listen: SocketAddr,
}

/// A connection to a server of the group, which has accepted this one.
struct Joined {
    conn: Arc<Conn>,
    reader: Stream,
    id: NodeId,
    addr: PeerAddr,
    nonce: u64,
}

impl Joining {
    /// Joins a group through a server of it, then connects to the others found, and
    /// only returns when they have all accepted this server. If one of them already
    /// knows another server with this id, this one starts over with another id, which
    /// it can do because it has no clients yet. On return, `id` is the final id.
    ///
    /// If the seed cannot be reached, the server starts a group of its own.
    fn join(
        &self,
        seed: &str,
        id: &mut NodeId,
        tx: &Sender<Incoming>,
        next_conn: &Arc<AtomicU64>,
    ) -> Result<()> {
        for attempt in 0..JOIN_ATTEMPTS {
            let stream = match self.transport.connect(seed) {
                Ok(stream) => stream,
                Err(e) if attempt == 0 => {
                    log::warn!("could not reach seed {seed}, starting a new group: {e}");
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };
            match self.try_join(stream, id)? {
                Some(links) => {
                    for link in links {
                        let conn_id = next_conn.fetch_add(1, Ordering::SeqCst);
                        let dialed = Incoming::Dialed {
                            conn_id,
                            conn: link.conn,
                            id: link.id,
                            kind: self.kind,
                            addr: link.addr,
                            nonce: link.nonce,
                        };
                        tx.send(dialed).map_err(|_| Error::Disconnected)?;
                        let tx = tx.clone();
                        thread::spawn(move || read_loop(link.reader, conn_id, tx));
                    }
                    return Ok(());
                }
                None => {
                    let taken = *id;
                    *id = generate_id();
                    log::info!("another server has id {taken:#x}, trying {:#x}", *id);
                }
            }
        }
        Err(Error::AlreadyExists("server id".into()))
    }

    /// One attempt to join. Gives `None` if the id is taken.
    fn try_join(&self, stream: Stream, id: &mut NodeId) -> Result<Option<Vec<Joined>>> {
        let (conn, mut reader) = Conn::new(stream)?;
        let addr = advertised_addr(self.listen, &conn);
        send_root(&conn, *id, RootMsg::PeerStart(self.kind, addr, self.nonce))?;

        reader.set_read_timeout(Some(JOIN_TIMEOUT))?;
        let mut seed_nonce = 0;
        let (seed_id, mut peers) = loop {
            let packet = Packet::read_from(&mut reader)?.ok_or(Error::Disconnected)?;
            if packet.plugin != ROOT_PLUGIN_ID {
                continue;
            }
            match RootMsg::decode(&packet.payload)? {
                RootMsg::PeerChangeId(_, new_id) => {
                    log::info!("id {:#x} is taken, using {new_id:#x}", *id);
                    *id = new_id;
                }
                RootMsg::PeerAccepted(_, nonce) => seed_nonce = nonce,
                RootMsg::PeerList(_, peers) => break (packet.from, peers),
                other => log::warn!("unexpected message while joining: {other:?}"),
            }
        };
        reader.set_read_timeout(None)?;

        let seed_addr = peers.remove(&seed_id).unwrap_or(PeerAddr {
            host: String::new(),
            port: 0,
        });
        peers.remove(id);
        let mut links = vec![Joined {
            conn,
            reader,
            id: seed_id,
            addr: seed_addr,
            nonce: seed_nonce,
        }];

        for (peer_id, addr) in peers {
            match self.announce(*id, peer_id, &addr) {
                Ok(Some((conn, reader, nonce))) => links.push(Joined {
                    conn,
                    reader,
                    id: peer_id,
                    addr,
                    nonce,
                }),
                Ok(None) => {
                    for link in &links {
                        link.conn.close();
                    }
                    return Ok(None);
                }
                Err(e) => log::warn!("could not join peer {peer_id:#x} at {addr:?}: {e}"),
            }
        }
        Ok(Some(links))
    }

    /// Tells another server of the group about this one, and waits for its answer.
    /// Gives `None` if it says the id is taken.
    fn announce(
        &self,
        id: NodeId,
        peer_id: NodeId,
        addr: &PeerAddr,
    ) -> Result<Option<(Arc<Conn>, Stream, u64)>> {
        let stream = self.transport.connect((addr.host.as_str(), addr.port))?;
        let (conn, mut reader) = Conn::new(stream)?;
        let own = advertised_addr(self.listen, &conn);
        send_root(
            &conn,
            id,
            RootMsg::PeerListUpdate(self.kind, own, self.nonce),
        )?;

        reader.set_read_timeout(Some(JOIN_TIMEOUT))?;
        let packet = Packet::read_from(&mut reader)?.ok_or(Error::Disconnected)?;
        reader.set_read_timeout(None)?;
        let answer = match packet.plugin {
            ROOT_PLUGIN_ID => RootMsg::decode(&packet.payload)?,
            _ => return Err(Error::InvalidPacket("no answer to the announce")),
        };
        match answer {
            RootMsg::PeerAccepted(_, nonce) => Ok(Some((conn, reader, nonce))),
            RootMsg::PeerIdTaken(_) => {
                log::info!("server {peer_id:#x} already knows a server with id {id:#x}");
                conn.close();
                Ok(None)
            }
            _ => Err(Error::InvalidPacket("unexpected answer to the announce")),
        }
    }
}

/// Writes to a connection that does not belong to a known node yet.
fn send_root(conn: &Conn, from: NodeId, message: RootMsg) -> Result<()> {
    let packet = Packet {
        from,
        plugin: ROOT_PLUGIN_ID,
        targets: BTreeSet::new(),
        payload: message.encode(),
    };
    conn.send(&packet.encode()?)
}

fn accept_loop(
    listener: Listener,
    tx: Sender<Incoming>,
    stop: Arc<AtomicBool>,
    next_conn: Arc<AtomicU64>,
) {
    while !stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok(stream) => {
                let id = next_conn.fetch_add(1, Ordering::SeqCst);
                if let Err(e) = start_connection(stream, id, &tx) {
                    log::warn!("could not set up a connection: {e}");
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => thread::sleep(ACCEPT_POLL_INTERVAL),
            Err(e) => {
                log::warn!("accept failed: {e}");
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
        }
    }
}

fn start_connection(stream: Stream, id: ConnId, tx: &Sender<Incoming>) -> Result<()> {
    let (conn, reader) = Conn::new(stream)?;
    if tx.send(Incoming::Accepted(id, conn)).is_err() {
        return Err(Error::Disconnected);
    }
    let tx = tx.clone();
    thread::spawn(move || read_loop(reader, id, tx));
    Ok(())
}

fn udp_loop(
    socket: Arc<UdpSocket>,
    tx: Sender<Incoming>,
    stop: Arc<AtomicBool>,
    datagrams: Arc<AtomicU64>,
) {
    // It blocks, with no timeout: on Windows, a datagram that arrives just as a read
    // times out can be lost. To stop, the dispatcher sends this socket a datagram.
    let mut buffer = vec![0u8; MAX_DATAGRAM_SIZE + 1];
    while !stop.load(Ordering::SeqCst) {
        let (size, from) = match socket.recv_from(&mut buffer) {
            Ok(received) => received,
            Err(e) => {
                // Windows reports here that a previous send got no one to receive it.
                log::debug!("UDP read failed: {e}");
                thread::sleep(UDP_ERROR_PAUSE);
                continue;
            }
        };
        if stop.load(Ordering::SeqCst) {
            return;
        }
        datagrams.fetch_add(1, Ordering::SeqCst);
        match Packet::decode(&buffer[..size]) {
            Ok(packet) => {
                if tx.send(Incoming::Datagram(from, packet)).is_err() {
                    return;
                }
            }
            Err(e) => log::debug!("ignoring invalid datagram from {from}: {e}"),
        }
    }
}

fn read_loop(mut reader: Stream, id: ConnId, tx: Sender<Incoming>) {
    loop {
        match Packet::read_from(&mut reader) {
            Ok(Some(packet)) => {
                if tx.send(Incoming::Packet(id, packet)).is_err() {
                    return;
                }
            }
            Ok(None) => break,
            Err(e) => {
                log::debug!("closing connection {id}: {e}");
                break;
            }
        }
    }
    let _ = tx.send(Incoming::Closed(id));
}

struct Dispatcher {
    id: NodeId,
    /// The number this server drew when it started.
    nonce: u64,
    role: Role,
    transport: Transport,
    listen: SocketAddr,
    /// What a thread that calls back another server needs.
    tx: Sender<Incoming>,
    next_conn: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    /// Servers that said they were leaving, which are not called back.
    departed: BTreeSet<NodeId>,
    directory: Arc<Mutex<Directory>>,
    conns: BTreeMap<ConnId, Arc<Conn>>,
    nodes_by_conn: BTreeMap<ConnId, (NodeId, Relation)>,
    event: EventServer,
    shared: SharedServer,
    barrier: BarrierServer,
    alias: AliasServer,
    remote: RemoteServer,
    messenger: MessengerServer,
    plugins: BTreeMap<PluginId, Box<dyn ServerPlugin>>,
    udp: Option<Arc<UdpSocket>>,
    /// Tokens clients have announced, waiting for their first datagram.
    udp_tokens: BTreeMap<u64, ConnId>,
    udp_addrs: BTreeMap<SocketAddr, ConnId>,
    quit: bool,
}

impl Dispatcher {
    fn run(
        mut self,
        rx: Receiver<Incoming>,
        stop: Arc<AtomicBool>,
        stopped: Arc<(Mutex<bool>, Condvar)>,
    ) {
        while let Ok(message) = rx.recv() {
            match message {
                Incoming::Accepted(id, conn) => {
                    self.conns.insert(id, conn);
                }
                Incoming::Dialed {
                    conn_id,
                    conn,
                    id,
                    kind,
                    addr,
                    nonce,
                } => self.on_dialed(conn_id, conn, id, kind, addr, nonce),
                Incoming::Packet(id, packet) => self.on_packet(id, packet),
                Incoming::Datagram(from, packet) => self.on_datagram(from, packet),
                Incoming::Closed(id) => self.on_closed(id),
                Incoming::AddPlugin(id, plugin) => {
                    self.plugins.insert(id, plugin);
                }
                Incoming::Stop => break,
            }
            if self.quit {
                break;
            }
        }

        stop.store(true, Ordering::SeqCst);
        if let Some(socket) = &self.udp {
            wake_udp(socket);
        }
        // So that the other servers do not try to call this one back.
        for link in self.directory.lock().unwrap().links.values() {
            let _ = send_root(&link.conn, self.id, RootMsg::Bye);
        }
        for conn in self.conns.values() {
            conn.close();
        }
        while let Ok(message) = rx.try_recv() {
            match message {
                Incoming::Accepted(_, conn) | Incoming::Dialed { conn, .. } => conn.close(),
                _ => {}
            }
        }
        {
            let mut directory = self.directory.lock().unwrap();
            directory.clients.clear();
            directory.links.clear();
        }

        let (lock, cond) = &*stopped;
        *lock.lock().unwrap() = true;
        cond.notify_all();
    }

    fn on_packet(&mut self, conn_id: ConnId, packet: Packet) {
        // The sender comes from the connection, not from the packet's "from"
        // field, so that a client cannot impersonate another one.
        let Some(&(id, relation)) = self.nodes_by_conn.get(&conn_id) else {
            self.on_first_packet(conn_id, &packet);
            return;
        };
        let source = Source {
            id,
            relation,
            origin: match relation {
                Relation::Client => id,
                Relation::Peer | Relation::Remote => packet.from,
            },
        };

        let result = match packet.plugin {
            ROOT_PLUGIN_ID => {
                self.on_root(source, conn_id, &packet.payload);
                return;
            }
            EVENT_PLUGIN_ID => self
                .event
                .on_packet(source, &packet.targets, &packet.payload),
            SHARED_PLUGIN_ID => self.shared.on_packet(source, &packet.payload),
            BARRIER_PLUGIN_ID => self.barrier.on_packet(source, &packet.payload),
            ALIAS_PLUGIN_ID => self.alias.on_packet(source, &packet.payload),
            REMOTE_PLUGIN_ID => self
                .remote
                .on_packet(source, &packet.targets, &packet.payload),
            MESSENGER_PLUGIN_ID => self.messenger.on_packet(source, &packet.payload),
            other => {
                self.on_custom_plugin(other, source, &packet);
                return;
            }
        };
        match result {
            Ok(outbound) => self.send_all(outbound),
            Err(e) => log::warn!("ignoring packet from {id:#x}: {e}"),
        }
    }

    fn on_custom_plugin(&mut self, id: PluginId, source: Source, packet: &Packet) {
        let Some(plugin) = self.plugins.get_mut(&id) else {
            log::warn!("ignoring packet for unknown plugin {id}");
            return;
        };
        let messages = plugin.on_packet(
            plugin::public_source(source),
            &packet.targets,
            &packet.payload,
        );
        for message in messages {
            let mut out = plugin::outbound(id, message, Some(source.relation), Some(source.id));
            out.from = Some(source.origin);
            self.send(out);
        }
    }

    /// A packet that came through UDP. Only events can use it, as they are the
    /// only ones that can afford to be lost, and they go on through UDP as well.
    fn on_datagram(&mut self, from: SocketAddr, packet: Packet) {
        if packet.plugin == ROOT_PLUGIN_ID {
            if let Ok(RootMsg::UdpRegister(token)) = RootMsg::decode(&packet.payload) {
                self.on_udp_token(from, token);
            }
            return;
        }
        let node = self
            .udp_addrs
            .get(&from)
            .and_then(|conn_id| self.nodes_by_conn.get(conn_id))
            .copied();
        let Some((id, relation)) = node else {
            log::debug!("ignoring datagram from unknown address {from}");
            return;
        };
        if packet.plugin != EVENT_PLUGIN_ID {
            log::warn!("ignoring datagram for plugin {}", packet.plugin);
            return;
        }
        let source = Source {
            id,
            relation,
            origin: id,
        };
        match self
            .event
            .on_packet(source, &packet.targets, &packet.payload)
        {
            Ok(outbound) => {
                for mut out in outbound {
                    out.unreliable = true;
                    self.send(out);
                }
            }
            Err(e) => log::warn!("ignoring datagram from {id:#x}: {e}"),
        }
    }

    /// A client proved it owns this address by sending the token over UDP, which
    /// it had already sent over its connection.
    fn on_udp_token(&mut self, from: SocketAddr, token: u64) {
        let Some(conn_id) = self.udp_tokens.remove(&token) else {
            return;
        };
        let Some(&(id, _)) = self.nodes_by_conn.get(&conn_id) else {
            return;
        };
        if let Some(client) = self.directory.lock().unwrap().clients.get_mut(&id) {
            client.udp = Some(from);
        }
        self.udp_addrs.insert(from, conn_id);
        self.send(Outbound::to_node(
            ROOT_PLUGIN_ID,
            RootMsg::UdpReady.encode(),
            id,
        ));
    }

    fn forget_udp(&mut self, conn_id: ConnId) {
        self.udp_tokens.retain(|_, c| *c != conn_id);
        self.udp_addrs.retain(|_, c| *c != conn_id);
    }

    /// The first packet of a connection says what is at the other end.
    fn on_first_packet(&mut self, conn_id: ConnId, packet: &Packet) {
        let message = match (packet.plugin, RootMsg::decode(&packet.payload)) {
            (ROOT_PLUGIN_ID, Ok(message)) => message,
            _ => {
                log::warn!("ignoring packet from connection {conn_id} before hello");
                return;
            }
        };
        match message {
            RootMsg::Hello(nickname) => self.on_hello(conn_id, nickname, None),
            RootMsg::HelloAgain(nickname, id) => self.on_hello(conn_id, nickname, Some(id)),
            RootMsg::PeerStart(kind, addr, nonce) if self.role.accepts(kind) => {
                self.on_peer_start(conn_id, packet.from, kind, addr, nonce)
            }
            RootMsg::PeerListUpdate(kind, addr, nonce) if self.role.accepts(kind) => {
                self.on_peer_announce(conn_id, packet.from, kind, addr, nonce)
            }
            other => log::warn!("ignoring first message of connection {conn_id}: {other:?}"),
        }
    }

    fn on_root(&mut self, source: Source, conn_id: ConnId, payload: &[u8]) {
        match RootMsg::decode(payload) {
            Ok(RootMsg::Bye) if source.relation != Relation::Client => {
                self.departed.insert(source.id);
            }
            Ok(RootMsg::Bye) => {
                self.remove_node(source.id, source.relation);
                if let Some(conn) = self.conns.remove(&conn_id) {
                    conn.close();
                }
                self.nodes_by_conn.remove(&conn_id);
                self.forget_udp(conn_id);
            }
            Ok(RootMsg::UdpRegister(token)) if source.relation == Relation::Client => {
                let port = match &self.udp {
                    Some(socket) => {
                        self.udp_tokens.insert(token, conn_id);
                        socket.local_addr().map_or(0, |addr| addr.port())
                    }
                    None => 0,
                };
                self.send(Outbound::to_node(
                    ROOT_PLUGIN_ID,
                    RootMsg::UdpPort(port).encode(),
                    source.id,
                ));
            }
            Ok(RootMsg::QuitAll) => {
                // Whoever sent it to this server has also sent it to the rest of
                // the group, but not necessarily to the other groups.
                let mut out =
                    Outbound::to_node(ROOT_PLUGIN_ID, RootMsg::QuitAll.encode(), EVERYBODY_ID);
                out.links = Links::forward_from(source.relation);
                if source.relation != Relation::Client {
                    out.black.insert(source.id);
                }
                self.send(out);
                self.quit = true;
            }
            Ok(other) => log::warn!("unexpected root message from {:#x}: {other:?}", source.id),
            Err(e) => log::warn!("invalid root message from {:#x}: {e}", source.id),
        }
    }

    /// A client connects. One that lost its connection asks for the id it had, which it
    /// gets if it belongs to this server and nobody has it.
    fn on_hello(&mut self, conn_id: ConnId, nickname: String, wanted: Option<NodeId>) {
        let Some(conn) = self.conns.get(&conn_id).cloned() else {
            return;
        };
        let new_id = {
            let mut directory = self.directory.lock().unwrap();
            let free = |id: NodeId| {
                id & SERVER_ID_MASK == self.id
                    && id != self.id
                    && !directory.clients.contains_key(&id)
            };
            let last = directory.clients.keys().next_back().copied();
            let new_id = match wanted {
                Some(id) if free(id) => id,
                _ => last.unwrap_or(self.id) + 1,
            };
            if new_id > self.id + MAX_CLIENTS {
                log::error!("too many clients, refusing connection");
                conn.close();
                return;
            }
            let client = Client {
                conn,
                nickname,
                udp: None,
            };
            directory.clients.insert(new_id, client);
            if directory.master == NO_MASTER {
                directory.master = new_id;
            }
            new_id
        };
        self.nodes_by_conn
            .insert(conn_id, (new_id, Relation::Client));
        log::info!("new client {new_id:#x}");

        self.send(Outbound::to_node(
            ROOT_PLUGIN_ID,
            RootMsg::Id(new_id).encode(),
            new_id,
        ));
        self.update_clients();
    }

    /// A new server wants to join the group through this one.
    fn on_peer_start(
        &mut self,
        conn_id: ConnId,
        from: NodeId,
        kind: LinkKind,
        addr: PeerAddr,
        nonce: u64,
    ) {
        let Some(conn) = self.conns.get(&conn_id).cloned() else {
            return;
        };
        let (id, peers) = {
            let directory = self.directory.lock().unwrap();
            let taken = |id: NodeId| id == self.id || taken_by_another(&directory, id, nonce);
            let mut id = from;
            while taken(id) || !is_server_id(id) {
                id = generate_id();
            }

            let mut peers: BTreeMap<NodeId, PeerAddr> = directory
                .links
                .iter()
                .filter(|(_, link)| link.info.kind == kind)
                .map(|(&id, link)| (id, link.info.addr.clone()))
                .collect();
            peers.insert(self.id, advertised_addr(self.listen, &conn));
            (id, peers)
        };

        let mut messages = Vec::new();
        if id != from {
            log::info!("telling new peer that id {from:#x} is taken, use {id:#x}");
            messages.push(RootMsg::PeerChangeId(kind, id));
        }
        messages.push(RootMsg::PeerAccepted(kind, self.nonce));
        messages.push(RootMsg::PeerList(kind, peers));
        for message in messages {
            if let Err(e) = send_root(&conn, self.id, message) {
                log::warn!("could not answer new peer: {e}");
                return;
            }
        }
        self.add_link(conn_id, conn, id, kind, addr, nonce);
    }

    /// A server that has just joined the group connected to this one.
    fn on_peer_announce(
        &mut self,
        conn_id: ConnId,
        id: NodeId,
        kind: LinkKind,
        addr: PeerAddr,
        nonce: u64,
    ) {
        let Some(conn) = self.conns.get(&conn_id).cloned() else {
            return;
        };
        if !is_server_id(id) {
            log::error!("connection {conn_id} has an invalid server id {id:#x}");
            conn.close();
            return;
        }
        let taken = id == self.id || taken_by_another(&self.directory.lock().unwrap(), id, nonce);
        let answer = if taken {
            log::warn!("a new server has id {id:#x}, which another one has: it must change");
            RootMsg::PeerIdTaken(kind)
        } else {
            RootMsg::PeerAccepted(kind, self.nonce)
        };
        if let Err(e) = send_root(&conn, self.id, answer) {
            log::warn!("could not answer server {id:#x}: {e}");
            conn.close();
            return;
        }
        if taken {
            conn.close();
            return;
        }
        self.add_link(conn_id, conn, id, kind, addr, nonce);
    }

    fn on_dialed(
        &mut self,
        conn_id: ConnId,
        conn: Arc<Conn>,
        id: NodeId,
        kind: LinkKind,
        addr: PeerAddr,
        nonce: u64,
    ) {
        self.conns.insert(conn_id, conn.clone());
        self.add_link(conn_id, conn, id, kind, addr, nonce);
    }

    /// Links another server. Whoever calls it has checked that no other server has this
    /// id, so a link with the same id is an older connection of the same server, which
    /// the new one replaces.
    fn add_link(
        &mut self,
        conn_id: ConnId,
        conn: Arc<Conn>,
        id: NodeId,
        kind: LinkKind,
        addr: PeerAddr,
        nonce: u64,
    ) {
        {
            let mut directory = self.directory.lock().unwrap();
            if let Some(old) = directory.links.remove(&id) {
                log::debug!("a new connection from {id:#x} replaces the old one");
                old.conn.close();
                self.nodes_by_conn.retain(|_, node| node.0 != id);
            }
            let info = PeerInfo { kind, addr };
            directory.links.insert(id, Link { conn, info, nonce });
        }
        self.nodes_by_conn.insert(conn_id, (id, kind.into()));
        log::info!("new {kind:?} link to {id:#x}");

        let mut out = self.event.sync_link(kind, id);
        out.extend(self.shared.sync_link(kind, id));
        out.extend(self.barrier.sync_link(kind, id));
        out.extend(self.alias.sync_link(kind, id));
        out.extend(self.remote.sync_link(kind, id));
        out.extend(self.messenger.sync_link(kind, id));
        self.send_all(out);
    }

    fn on_closed(&mut self, conn_id: ConnId) {
        if let Some(conn) = self.conns.remove(&conn_id) {
            conn.close();
        }
        if let Some((id, relation)) = self.nodes_by_conn.remove(&conn_id) {
            let link = match relation {
                Relation::Client => None,
                _ => self
                    .directory
                    .lock()
                    .unwrap()
                    .links
                    .get(&id)
                    .map(|l| l.info.clone()),
            };
            self.remove_node(id, relation);
            if let Some(info) = link {
                self.call_back(id, info);
            }
        }
        self.forget_udp(conn_id);
    }

    /// The link to another server broke, but that server may still be up. Only the
    /// one with the lower id calls back, so that the two calls do not replace each
    /// other. What was sent while the link was down is lost.
    fn call_back(&mut self, id: NodeId, info: PeerInfo) {
        if self.departed.remove(&id) || self.quit || self.id > id {
            return;
        }
        log::info!("lost the link to {id:#x}, calling it back");
        let joining = Joining {
            transport: self.transport,
            kind: info.kind,
            nonce: self.nonce,
            listen: self.listen,
        };
        let own_id = self.id;
        let tx = self.tx.clone();
        let next_conn = self.next_conn.clone();
        let stop = self.stop.clone();
        thread::spawn(move || {
            let deadline = Instant::now() + RECONNECT_WINDOW;
            let mut pause = FIRST_RECONNECT_PAUSE;
            while !stop.load(Ordering::SeqCst) && Instant::now() < deadline {
                thread::sleep(pause);
                pause = (pause * 2).min(MAX_RECONNECT_PAUSE);
                match joining.announce(own_id, id, &info.addr) {
                    Ok(Some((conn, reader, nonce))) => {
                        let conn_id = next_conn.fetch_add(1, Ordering::SeqCst);
                        let kind = info.kind;
                        let dialed = Incoming::Dialed {
                            conn_id,
                            conn,
                            id,
                            kind,
                            addr: info.addr,
                            nonce,
                        };
                        if tx.send(dialed).is_ok() {
                            read_loop(reader, conn_id, tx);
                        }
                        return;
                    }
                    Ok(None) => {
                        log::warn!("{id:#x} refused this server when it called back");
                        return;
                    }
                    Err(e) => log::debug!("could not call {id:#x} back yet: {e}"),
                }
            }
            log::warn!("gave up calling {id:#x} back");
        });
    }

    fn remove_node(&mut self, id: NodeId, relation: Relation) {
        {
            let mut directory = self.directory.lock().unwrap();
            directory.clients.remove(&id);
            directory.links.remove(&id);
            if directory.master == id {
                directory.master = directory
                    .clients
                    .keys()
                    .next()
                    .copied()
                    .unwrap_or(NO_MASTER);
            }
        }
        log::info!("node {id:#x} left");

        let mut out = self.event.unregister_node(id);
        out.extend(self.shared.unregister_node(id, relation));
        out.extend(self.barrier.unregister_node(id));
        out.extend(self.alias.unregister_node(id));
        out.extend(self.remote.unregister_node(id));
        out.extend(self.messenger.unregister_node(id));
        for (&plugin_id, plugin) in &mut self.plugins {
            for message in plugin.on_node_left(id) {
                out.push(plugin::outbound(plugin_id, message, None, None));
            }
        }
        self.send_all(out);
        if relation == Relation::Client {
            self.update_clients();
        }
    }

    fn update_clients(&mut self) {
        let list = {
            let directory = self.directory.lock().unwrap();
            RootMsg::NodeList {
                master: directory.master,
                nodes: directory
                    .clients
                    .iter()
                    .map(|(&id, client)| (id, client.nickname.clone()))
                    .collect(),
            }
        };
        self.send(Outbound::to_node(ROOT_PLUGIN_ID, list.encode(), CLIENTS_ID));
    }

    fn send_all(&mut self, outbound: Vec<Outbound>) {
        for out in outbound {
            self.send(out);
        }
    }

    fn send(&mut self, out: Outbound) {
        let recipients = {
            let directory = self.directory.lock().unwrap();
            resolve(&directory, &out)
        };
        if recipients.is_empty() {
            return;
        }
        let packet = Packet {
            from: out.from.unwrap_or(self.id),
            plugin: out.plugin,
            targets: out.targets,
            payload: out.payload,
        };
        let frame = match packet.encode() {
            Ok(frame) => frame,
            Err(e) => {
                log::error!("could not encode packet: {e}");
                return;
            }
        };
        for (id, conn, udp) in recipients {
            if let (true, Some(addr), Some(socket)) = (out.unreliable, udp, &self.udp) {
                if frame.len() <= MAX_DATAGRAM_SIZE && socket.send_to(&frame, addr).is_ok() {
                    continue;
                }
            }
            // A failed write closes the connection, and its reading thread then
            // reports it as closed, which removes the node.
            if let Err(e) = conn.send(&frame) {
                log::warn!("could not send to {id:#x}: {e}");
            }
        }
    }
}

/// Whether a server other than the one with this number has the id.
fn taken_by_another(directory: &Directory, id: NodeId, nonce: u64) -> bool {
    directory
        .links
        .get(&id)
        .is_some_and(|link| link.nonce != nonce || nonce == 0)
}

fn is_server_id(id: NodeId) -> bool {
    id >= BASE_SERVER_ID && id & !SERVER_ID_MASK == 0
}

/// Which connections a packet goes to.
fn resolve(directory: &Directory, out: &Outbound) -> Vec<(NodeId, Arc<Conn>, Option<SocketAddr>)> {
    let allowed =
        |id: &NodeId| !out.black.contains(id) && (out.white.is_empty() || out.white.contains(id));
    let everyone = out.targets.contains(&EVERYBODY_ID) || out.targets.contains(&CLIENTS_ID);
    let beyond_this_server = out.targets.is_empty()
        || out
            .targets
            .iter()
            .any(|t| !directory.clients.contains_key(t));

    let clients = directory
        .clients
        .iter()
        .filter(|(id, _)| everyone || out.targets.contains(id))
        .filter(|(id, _)| allowed(id))
        .map(|(&id, client)| (id, client.conn.clone(), client.udp));
    let links = directory
        .links
        .iter()
        .filter(|(id, link)| {
            out.targets.contains(id) || (beyond_this_server && out.links.includes(link.info.kind))
        })
        .filter(|(id, _)| allowed(id))
        .map(|(&id, link)| (id, link.conn.clone(), None));
    clients.chain(links).collect()
}
