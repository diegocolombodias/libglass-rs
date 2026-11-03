use crate::alias::AliasServer;
use crate::barrier::BarrierServer;
use crate::conn::Conn;
use crate::error::{Error, Result};
use crate::event::EventServer;
use crate::outbound::Outbound;
use crate::owners::{Links, Relation, Source};
use crate::packet::Packet;
use crate::protocol::{PeerAddr, RootMsg, NO_MASTER};
use crate::remote::RemoteServer;
use crate::shared::SharedServer;
use crate::types::*;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{BuildHasher, Hasher};
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CLIENTS: NodeId = 0xFFFF;

type ConnId = u64;

enum Incoming {
    Accepted(ConnId, Arc<Conn>),
    /// A connection this server opened to another server.
    Dialed {
        conn_id: ConnId,
        conn: Arc<Conn>,
        id: NodeId,
        kind: LinkKind,
        addr: PeerAddr,
        /// Does the other server still need to be told who this one is?
        announce: bool,
    },
    Packet(ConnId, Packet),
    Closed(ConnId),
    Stop,
}

struct Client {
    conn: Arc<Conn>,
    nickname: String,
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
}

#[derive(Default)]
struct Directory {
    clients: BTreeMap<NodeId, Client>,
    links: BTreeMap<NodeId, Link>,
    master: NodeId,
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
}

impl GlassServer {
    /// Starts a server. Use port 0 to let the system choose a free port.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<GlassServer> {
        GlassServer::start(addr, Role::Standalone, None)
    }

    /// Starts a server that works together with others as a group: clients of
    /// any of them share the objects created with [`Mode::Global`].
    ///
    /// `seed` is the address (`"host:port"`) of any server of the group. It
    /// tells this one about the others, and they all connect to each other. If
    /// the seed cannot be reached, this server starts as the first of a group.
    pub fn bind_peer<A: ToSocketAddrs>(addr: A, seed: Option<&str>) -> Result<GlassServer> {
        GlassServer::start(addr, Role::Peer, seed)
    }

    /// Starts a peer that is also a gateway between groups.
    ///
    /// Peers join its group with [`bind_peer`](Self::bind_peer). `seed` is the
    /// address of any gateway of another group.
    pub fn bind_remote_peer<A: ToSocketAddrs>(addr: A, seed: Option<&str>) -> Result<GlassServer> {
        GlassServer::start(addr, Role::RemotePeer, seed)
    }

    fn start<A: ToSocketAddrs>(addr: A, role: Role, seed: Option<&str>) -> Result<GlassServer> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;

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
        if let Some(seed) = seed {
            match connect_any(seed) {
                Ok(stream) => join(stream, kind, &mut id, addr, &tx, &next_conn)?,
                Err(e) => log::warn!("could not reach seed {seed}, starting a new group: {e}"),
            }
        }

        let accept_thread = {
            let tx = tx.clone();
            let stop = stop.clone();
            let next_conn = next_conn.clone();
            thread::spawn(move || accept_loop(listener, tx, stop, next_conn))
        };
        let dispatch_thread = {
            let dispatcher = Dispatcher {
                id,
                role,
                listen: addr,
                directory: directory.clone(),
                conns: BTreeMap::new(),
                nodes_by_conn: BTreeMap::new(),
                event: EventServer::default(),
                shared: SharedServer::default(),
                barrier: BarrierServer::default(),
                alias: AliasServer::default(),
                remote: RemoteServer::default(),
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
        })
    }

    /// The address this server is listening on.
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

fn connect_any<A: ToSocketAddrs>(addr: A) -> std::io::Result<TcpStream> {
    let mut last_error = None;
    for addr in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(stream) => return Ok(stream),
            Err(e) => last_error = Some(e),
        }
    }
    Err(last_error.unwrap_or_else(|| ErrorKind::NotFound.into()))
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

/// Joins a group through a server of it, then connects to the others found. On
/// return, `id` is this server's final id.
fn join(
    stream: TcpStream,
    kind: LinkKind,
    id: &mut NodeId,
    listen: SocketAddr,
    tx: &Sender<Incoming>,
    next_conn: &Arc<AtomicU64>,
) -> Result<()> {
    let (conn, mut reader) = Conn::new(stream)?;
    let start = Packet {
        from: *id,
        plugin: ROOT_PLUGIN_ID,
        targets: BTreeSet::new(),
        payload: RootMsg::PeerStart(kind, advertised_addr(listen, &conn)).encode(),
    };
    conn.send(&start.encode()?)?;

    reader.set_read_timeout(Some(JOIN_TIMEOUT))?;
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

    let conn_id = next_conn.fetch_add(1, Ordering::SeqCst);
    tx.send(Incoming::Dialed {
        conn_id,
        conn,
        id: seed_id,
        kind,
        addr: seed_addr,
        announce: false,
    })
    .map_err(|_| Error::Disconnected)?;
    {
        let tx = tx.clone();
        thread::spawn(move || read_loop(reader, conn_id, tx));
    }

    for (peer_id, addr) in peers {
        let tx = tx.clone();
        let next_conn = next_conn.clone();
        thread::spawn(move || dial(peer_id, kind, addr, tx, next_conn));
    }
    Ok(())
}

/// Opens a connection to another server of the group.
fn dial(
    id: NodeId,
    kind: LinkKind,
    addr: PeerAddr,
    tx: Sender<Incoming>,
    next_conn: Arc<AtomicU64>,
) {
    let stream = match connect_any((addr.host.as_str(), addr.port)) {
        Ok(stream) => stream,
        Err(e) => {
            log::warn!("could not connect to peer {id:#x} at {addr:?}: {e}");
            return;
        }
    };
    let (conn, reader) = match Conn::new(stream) {
        Ok(pair) => pair,
        Err(e) => {
            log::warn!("could not set up connection to peer {id:#x}: {e}");
            return;
        }
    };
    let conn_id = next_conn.fetch_add(1, Ordering::SeqCst);
    let dialed = Incoming::Dialed {
        conn_id,
        conn,
        id,
        kind,
        addr,
        announce: true,
    };
    if tx.send(dialed).is_ok() {
        read_loop(reader, conn_id, tx);
    }
}

fn accept_loop(
    listener: TcpListener,
    tx: Sender<Incoming>,
    stop: Arc<AtomicBool>,
    next_conn: Arc<AtomicU64>,
) {
    while !stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, peer)) => {
                log::debug!("connection from {peer}");
                let id = next_conn.fetch_add(1, Ordering::SeqCst);
                if let Err(e) = start_connection(stream, id, &tx) {
                    log::warn!("could not set up connection from {peer}: {e}");
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

fn start_connection(stream: TcpStream, id: ConnId, tx: &Sender<Incoming>) -> Result<()> {
    // Accepted sockets inherit non-blocking mode from the listener on Windows.
    stream.set_nonblocking(false)?;
    let (conn, reader) = Conn::new(stream)?;
    if tx.send(Incoming::Accepted(id, conn)).is_err() {
        return Err(Error::Disconnected);
    }
    let tx = tx.clone();
    thread::spawn(move || read_loop(reader, id, tx));
    Ok(())
}

fn read_loop(mut reader: TcpStream, id: ConnId, tx: Sender<Incoming>) {
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
    role: Role,
    listen: SocketAddr,
    directory: Arc<Mutex<Directory>>,
    conns: BTreeMap<ConnId, Arc<Conn>>,
    nodes_by_conn: BTreeMap<ConnId, (NodeId, Relation)>,
    event: EventServer,
    shared: SharedServer,
    barrier: BarrierServer,
    alias: AliasServer,
    remote: RemoteServer,
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
                    announce,
                } => self.on_dialed(conn_id, conn, id, kind, addr, announce),
                Incoming::Packet(id, packet) => self.on_packet(id, packet),
                Incoming::Closed(id) => self.on_closed(id),
                Incoming::Stop => break,
            }
            if self.quit {
                break;
            }
        }

        stop.store(true, Ordering::SeqCst);
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
            other => {
                log::warn!("ignoring packet for unknown plugin {other}");
                return;
            }
        };
        match result {
            Ok(outbound) => self.send_all(outbound),
            Err(e) => log::warn!("ignoring packet from {id:#x}: {e}"),
        }
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
            RootMsg::Hello(nickname) => self.on_hello(conn_id, nickname),
            RootMsg::PeerStart(kind, addr) if self.role.accepts(kind) => {
                self.on_peer_start(conn_id, packet.from, kind, addr)
            }
            RootMsg::PeerListUpdate(kind, addr) if self.role.accepts(kind) => {
                self.on_peer_announce(conn_id, packet.from, kind, addr)
            }
            other => log::warn!("ignoring first message of connection {conn_id}: {other:?}"),
        }
    }

    fn on_root(&mut self, source: Source, conn_id: ConnId, payload: &[u8]) {
        match RootMsg::decode(payload) {
            Ok(RootMsg::Bye) if source.relation == Relation::Client => {
                self.remove_node(source.id, source.relation);
                if let Some(conn) = self.conns.remove(&conn_id) {
                    conn.close();
                }
                self.nodes_by_conn.remove(&conn_id);
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

    fn on_hello(&mut self, conn_id: ConnId, nickname: String) {
        let Some(conn) = self.conns.get(&conn_id).cloned() else {
            return;
        };
        let new_id = {
            let mut directory = self.directory.lock().unwrap();
            let last = directory.clients.keys().next_back().copied();
            let new_id = last.unwrap_or(self.id) + 1;
            if new_id > self.id + MAX_CLIENTS {
                log::error!("too many clients, refusing connection");
                conn.close();
                return;
            }
            directory.clients.insert(new_id, Client { conn, nickname });
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
    fn on_peer_start(&mut self, conn_id: ConnId, from: NodeId, kind: LinkKind, addr: PeerAddr) {
        let Some(conn) = self.conns.get(&conn_id).cloned() else {
            return;
        };
        let (id, peers) = {
            let directory = self.directory.lock().unwrap();
            let taken = |id: NodeId| id == self.id || directory.links.contains_key(&id);
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
        messages.push(RootMsg::PeerList(kind, peers));
        for message in messages {
            if let Err(e) = self.send_direct(&conn, &message) {
                log::warn!("could not answer new peer: {e}");
                return;
            }
        }
        self.add_link(conn_id, conn, id, kind, addr);
    }

    /// A server that has just joined the group connected to this one.
    fn on_peer_announce(&mut self, conn_id: ConnId, id: NodeId, kind: LinkKind, addr: PeerAddr) {
        let Some(conn) = self.conns.get(&conn_id).cloned() else {
            return;
        };
        if id == self.id || !is_server_id(id) {
            log::error!("connection {conn_id} has an invalid server id {id:#x}");
            conn.close();
            return;
        }
        self.add_link(conn_id, conn, id, kind, addr);
    }

    fn on_dialed(
        &mut self,
        conn_id: ConnId,
        conn: Arc<Conn>,
        id: NodeId,
        kind: LinkKind,
        addr: PeerAddr,
        announce: bool,
    ) {
        self.conns.insert(conn_id, conn.clone());
        if announce {
            let message = RootMsg::PeerListUpdate(kind, advertised_addr(self.listen, &conn));
            if let Err(e) = self.send_direct(&conn, &message) {
                log::warn!("could not announce this server to {id:#x}: {e}");
                return;
            }
        }
        self.add_link(conn_id, conn, id, kind, addr);
    }

    /// Writes to a connection that may not belong to a known node yet.
    fn send_direct(&self, conn: &Conn, message: &RootMsg) -> Result<()> {
        let packet = Packet {
            from: self.id,
            plugin: ROOT_PLUGIN_ID,
            targets: BTreeSet::new(),
            payload: message.encode(),
        };
        conn.send(&packet.encode()?)
    }

    fn add_link(
        &mut self,
        conn_id: ConnId,
        conn: Arc<Conn>,
        id: NodeId,
        kind: LinkKind,
        addr: PeerAddr,
    ) {
        {
            let mut directory = self.directory.lock().unwrap();
            if directory.links.contains_key(&id) {
                // Two servers may connect to each other at the same time. Both
                // keep the connection opened by the lower id. It is also how a
                // restarted server with the same id gets back in.
                if id > self.id {
                    log::debug!("dropping second connection from {id:#x}");
                    conn.close();
                    return;
                }
                if let Some(old) = directory.links.remove(&id) {
                    old.conn.close();
                }
                self.nodes_by_conn.retain(|_, node| node.0 != id);
            }
            let info = PeerInfo { kind, addr };
            directory.links.insert(id, Link { conn, info });
        }
        self.nodes_by_conn.insert(conn_id, (id, kind.into()));
        log::info!("new {kind:?} link to {id:#x}");

        let mut out = self.event.sync_link(kind, id);
        out.extend(self.shared.sync_link(kind, id));
        out.extend(self.barrier.sync_link(kind, id));
        out.extend(self.alias.sync_link(kind, id));
        out.extend(self.remote.sync_link(kind, id));
        self.send_all(out);
    }

    fn on_closed(&mut self, conn_id: ConnId) {
        if let Some(conn) = self.conns.remove(&conn_id) {
            conn.close();
        }
        if let Some((id, relation)) = self.nodes_by_conn.remove(&conn_id) {
            self.remove_node(id, relation);
        }
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
        for (id, conn) in recipients {
            // A failed write closes the connection, and its reading thread then
            // reports it as closed, which removes the node.
            if let Err(e) = conn.send(&frame) {
                log::warn!("could not send to {id:#x}: {e}");
            }
        }
    }
}

fn is_server_id(id: NodeId) -> bool {
    id >= BASE_SERVER_ID && id & !SERVER_ID_MASK == 0
}

/// Which connections a packet goes to.
fn resolve(directory: &Directory, out: &Outbound) -> Vec<(NodeId, Arc<Conn>)> {
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
        .map(|(&id, client)| (id, client.conn.clone()));
    let links = directory
        .links
        .iter()
        .filter(|(id, link)| {
            out.targets.contains(id) || (beyond_this_server && out.links.includes(link.info.kind))
        })
        .filter(|(id, _)| allowed(id))
        .map(|(&id, link)| (id, link.conn.clone()));
    clients.chain(links).collect()
}
