use crate::alias::AliasSlot;
use crate::barrier::BarrierSlot;
use crate::conn::Conn;
use crate::error::{Error, Result};
use crate::event::{self, EventSlot};
use crate::messenger::{self, MessengerSlot};
use crate::packet::Packet;
use crate::protocol::{
    AliasMsg, BarrierKind, BarrierMsg, EventMsg, MessengerMsg, RemoteMsg, RootMsg, SharedMsg,
};
#[cfg(feature = "quic")]
use crate::quic::QuicTrust;
use crate::remote::RemoteSlot;
use crate::server::{wake_udp, MAX_DATAGRAM_SIZE};
use crate::shared::SharedSlot;
use crate::transport::Stream;
#[cfg(any(unix, feature = "quic"))]
use crate::transport::Transport;
use crate::types::*;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{BuildHasher, Hasher};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs, UdpSocket};
#[cfg(unix)]
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const UDP_SETUP_TIMEOUT: Duration = Duration::from_secs(2);
const UDP_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const MAX_RECONNECT_PAUSE: Duration = Duration::from_secs(2);

/// Opens a new connection to the same server, to replace a lost one.
type Dial = Box<dyn Fn() -> std::io::Result<Stream> + Send + Sync>;

type PluginHandler = Arc<Mutex<dyn FnMut(NodeId, &[u8]) + Send>>;

/// The state of a client. A single lock and condition variable keep every wait
/// simple: waiters check their own condition each time anything changes.
#[derive(Default)]
pub(crate) struct ClientState {
    pub(crate) nodes: BTreeMap<NodeId, String>,
    pub(crate) server_id: NodeId,
    pub(crate) master: NodeId,
    pub(crate) closed: bool,
    pub(crate) events: BTreeMap<String, EventSlot>,
    pub(crate) shared: BTreeMap<String, SharedSlot>,
    pub(crate) barriers: BTreeMap<(u32, u32), BarrierSlot>,
    pub(crate) aliases: BTreeMap<String, AliasSlot>,
    pub(crate) remotes: BTreeMap<String, RemoteSlot>,
    pub(crate) messengers: BTreeMap<String, MessengerSlot>,
    plugin_handlers: BTreeMap<PluginId, PluginHandler>,
    udp_port: Option<u16>,
    udp_ready: bool,
}

/// The part of a client shared with its reading thread and plugin objects.
pub(crate) struct ClientCore {
    id: AtomicU32,
    nickname: String,
    /// Replaced when the client connects again.
    conn: Mutex<Arc<Conn>>,
    dial: Dial,
    reconnect_window: Mutex<Option<Duration>>,
    /// Set when the client is dropped, so that it does not connect again.
    leaving: AtomicBool,
    /// What each object told the server when it was created, by plugin and name, to
    /// tell it again after a reconnection.
    registrations: Mutex<BTreeMap<(PluginId, String), Vec<u8>>>,
    udp: Mutex<Option<(Arc<UdpSocket>, SocketAddr)>>,
    pub(crate) state: Mutex<ClientState>,
    pub(crate) changed: Condvar,
}

impl ClientCore {
    /// Sends a packet to the server. Group ids in `targets` are resolved there.
    pub(crate) fn send(
        &self,
        plugin: PluginId,
        payload: Vec<u8>,
        targets: BTreeSet<NodeId>,
    ) -> Result<()> {
        let packet = Packet {
            from: self.id(),
            plugin,
            targets,
            payload,
        };
        self.conn().send(&packet.encode()?)
    }

    /// Sends through UDP if possible. Otherwise, or if it does not fit in a
    /// datagram, it goes through the connection, which is just more reliable.
    pub(crate) fn send_unreliable(
        &self,
        plugin: PluginId,
        payload: Vec<u8>,
        targets: BTreeSet<NodeId>,
    ) -> Result<()> {
        let packet = Packet {
            from: self.id(),
            plugin,
            targets,
            payload,
        };
        let frame = packet.encode()?;
        let udp = self.udp.lock().unwrap().clone();
        if let Some((socket, server)) = udp {
            if frame.len() <= MAX_DATAGRAM_SIZE && socket.send_to(&frame, server).is_ok() {
                return Ok(());
            }
        }
        self.conn().send(&frame)
    }

    pub(crate) fn send_to_server(&self, plugin: PluginId, payload: Vec<u8>) -> Result<()> {
        self.send(plugin, payload, BTreeSet::from([SERVERS_ID]))
    }

    fn conn(&self) -> Arc<Conn> {
        self.conn.lock().unwrap().clone()
    }

    /// Creates an object in the server, and keeps the message to create it again
    /// after a reconnection.
    pub(crate) fn register(&self, plugin: PluginId, name: &str, message: Vec<u8>) -> Result<()> {
        let key = (plugin, name.to_owned());
        let mut registrations = self.registrations.lock().unwrap();
        registrations.insert(key.clone(), message.clone());
        drop(registrations);
        let result = self.send_to_server(plugin, message);
        if result.is_err() {
            self.registrations.lock().unwrap().remove(&key);
        }
        result
    }

    /// Changes how an object is created again after a reconnection.
    pub(crate) fn update_registration(&self, plugin: PluginId, name: &str, message: Vec<u8>) {
        let mut registrations = self.registrations.lock().unwrap();
        if let Some(kept) = registrations.get_mut(&(plugin, name.to_owned())) {
            *kept = message;
        }
    }

    pub(crate) fn unregister(&self, plugin: PluginId, name: &str) {
        let key = (plugin, name.to_owned());
        self.registrations.lock().unwrap().remove(&key);
    }

    pub(crate) fn id(&self) -> NodeId {
        self.id.load(Ordering::SeqCst)
    }

    fn mark_closed(&self) {
        self.state.lock().unwrap().closed = true;
        self.changed.notify_all();
    }
}

/// A node connected to a [`GlassServer`](crate::GlassServer).
///
/// Dropping it tells the server that the client is leaving.
pub struct GlassClient {
    core: Arc<ClientCore>,
    nickname: String,
    reader: Option<JoinHandle<()>>,
    udp_reader: Option<JoinHandle<()>>,
}

impl GlassClient {
    /// Connects to a server. Returns only after the handshake has finished.
    pub fn connect<A: ToSocketAddrs>(server: A, nickname: &str) -> Result<GlassClient> {
        let addrs: Vec<SocketAddr> = server.to_socket_addrs()?.collect();
        let stream = Stream::Tcp(TcpStream::connect(&addrs[..])?);
        GlassClient::from_stream(stream, nickname, tcp_dial(addrs))
    }

    /// Keeps trying to connect until it works or the time runs out, for when the
    /// server may not be up yet. Returns the error of the last attempt.
    pub fn connect_retry<A: ToSocketAddrs>(
        server: A,
        nickname: &str,
        timeout: Duration,
    ) -> Result<GlassClient> {
        retry(timeout, || GlassClient::connect(&server, nickname))
    }

    /// Like [`connect_retry`](Self::connect_retry), through SCTP.
    #[cfg(unix)]
    pub fn connect_retry_sctp<A: ToSocketAddrs>(
        server: A,
        nickname: &str,
        timeout: Duration,
    ) -> Result<GlassClient> {
        retry(timeout, || GlassClient::connect_sctp(&server, nickname))
    }

    /// Like [`connect_retry`](Self::connect_retry), through QUIC.
    #[cfg(feature = "quic")]
    pub fn connect_retry_quic<A: ToSocketAddrs>(
        server: A,
        nickname: &str,
        trust: QuicTrust,
        timeout: Duration,
    ) -> Result<GlassClient> {
        retry(timeout, || {
            GlassClient::connect_quic(&server, nickname, trust)
        })
    }

    /// Connects to a server, and asks to use UDP for
    /// [`Event::enqueue_unreliable`](crate::Event::enqueue_unreliable). If that
    /// does not work, for example because of a firewall, the client still works
    /// and everything goes through TCP: see [`uses_udp`](Self::uses_udp).
    pub fn connect_with_udp<A: ToSocketAddrs>(server: A, nickname: &str) -> Result<GlassClient> {
        let addrs: Vec<SocketAddr> = server.to_socket_addrs()?.collect();
        let stream = TcpStream::connect(&addrs[..])?;
        let server_ip = stream.peer_addr()?.ip();
        let local_ip = stream.local_addr()?.ip();
        let mut client = GlassClient::from_stream(Stream::Tcp(stream), nickname, tcp_dial(addrs))?;
        if let Err(e) = client.enable_udp(local_ip, server_ip) {
            log::warn!("could not set up UDP: {e}");
        }
        Ok(client)
    }

    /// Connects through SCTP to a server started with
    /// [`GlassServer::bind_sctp`](crate::GlassServer::bind_sctp) or its variants for
    /// peers. Returns an error if the kernel does not have SCTP.
    #[cfg(unix)]
    pub fn connect_sctp<A: ToSocketAddrs>(server: A, nickname: &str) -> Result<GlassClient> {
        let addrs: Vec<SocketAddr> = server.to_socket_addrs()?.collect();
        let stream = Transport::Sctp.connect(&addrs[..])?;
        let dial = Box::new(move || Transport::Sctp.connect(&addrs[..]));
        GlassClient::from_stream(stream, nickname, dial)
    }

    /// Connects through QUIC to a server started with
    /// [`GlassServer::bind_quic`](crate::GlassServer::bind_quic) or its variants for
    /// peers. `trust` tells which certificate the server must present; with
    /// [`QuicTrust::AnyCertificate`], nothing protects against a man in the middle.
    ///
    /// The library runs QUIC on a tokio runtime of its own, so this client, and a
    /// QUIC server, must not be used from inside a tokio runtime: that panics.
    #[cfg(feature = "quic")]
    pub fn connect_quic<A: ToSocketAddrs>(
        server: A,
        nickname: &str,
        trust: QuicTrust,
    ) -> Result<GlassClient> {
        let addrs: Vec<SocketAddr> = server.to_socket_addrs()?.collect();
        let stream = Transport::Quic(trust).connect(&addrs[..])?;
        let dial = Box::new(move || Transport::Quic(trust).connect(&addrs[..]));
        GlassClient::from_stream(stream, nickname, dial)
    }

    /// Connects to a server on a Unix socket.
    #[cfg(unix)]
    pub fn connect_unix<P: AsRef<Path>>(path: P, nickname: &str) -> Result<GlassClient> {
        use std::os::unix::net::UnixStream;
        let path = path.as_ref().to_owned();
        let stream = Stream::Unix(UnixStream::connect(&path)?);
        let dial = Box::new(move || UnixStream::connect(&path).map(Stream::Unix));
        GlassClient::from_stream(stream, nickname, dial)
    }

    fn from_stream(stream: Stream, nickname: &str, dial: Dial) -> Result<GlassClient> {
        let (conn, mut reader) = Conn::new(stream)?;
        let core = Arc::new(ClientCore {
            id: AtomicU32::new(NOBODY_ID),
            nickname: nickname.to_owned(),
            conn: Mutex::new(conn),
            dial,
            reconnect_window: Mutex::new(None),
            leaving: AtomicBool::new(false),
            registrations: Mutex::new(BTreeMap::new()),
            udp: Mutex::new(None),
            state: Mutex::new(ClientState::default()),
            changed: Condvar::new(),
        });

        core.send(
            ROOT_PLUGIN_ID,
            RootMsg::Hello(nickname.to_owned()).encode(),
            BTreeSet::new(),
        )?;

        reader.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
        handshake(&core, &mut reader).inspect_err(|_| core.conn().close())?;
        reader.set_read_timeout(None)?;

        let reader = {
            let core = core.clone();
            thread::spawn(move || read_loop(core, reader))
        };
        Ok(GlassClient {
            core,
            nickname: nickname.to_owned(),
            reader: Some(reader),
            udp_reader: None,
        })
    }

    /// The socket uses the address of the connection, and not all of them, as it
    /// could otherwise take datagrams meant for a server on the same machine.
    fn enable_udp(&mut self, local_ip: IpAddr, server_ip: IpAddr) -> Result<()> {
        let socket = UdpSocket::bind((local_ip, 0))?;
        let deadline = Instant::now() + UDP_SETUP_TIMEOUT;

        let token = RandomState::new().build_hasher().finish();
        let message = RootMsg::UdpRegister(token).encode();
        self.core.send_to_server(ROOT_PLUGIN_ID, message.clone())?;

        // The server says which port to use, or that it has no UDP.
        let mut state = self.core.state.lock().unwrap();
        let port = loop {
            if let Some(port) = state.udp_port {
                break port;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || state.closed {
                return Err(Error::Timeout);
            }
            state = self.core.changed.wait_timeout(state, remaining).unwrap().0;
        };
        if port == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::Unsupported).into());
        }
        let server = SocketAddr::new(server_ip, port);

        let datagram = Packet {
            from: self.id(),
            plugin: ROOT_PLUGIN_ID,
            targets: BTreeSet::new(),
            payload: message,
        }
        .encode()?;

        // Datagrams can be lost, so it repeats until the server has seen one.
        while !state.udp_ready {
            if Instant::now() >= deadline || state.closed {
                return Err(Error::Timeout);
            }
            // A refused datagram is only an error on some systems.
            let _ = socket.send_to(&datagram, server);
            state = self
                .core
                .changed
                .wait_timeout(state, UDP_RETRY_INTERVAL)
                .unwrap()
                .0;
        }
        drop(state);

        let socket = Arc::new(socket);
        *self.core.udp.lock().unwrap() = Some((socket.clone(), server));
        let core = self.core.clone();
        self.udp_reader = Some(thread::spawn(move || udp_loop(core, socket, server)));
        Ok(())
    }

    /// Makes the client connect again when it loses the server, trying for up to
    /// `window` each time; `None`, the default, turns it off. While it is away,
    /// [`is_connected`](Self::is_connected) is false and waits fail with
    /// [`Error::Disconnected`]. Then it gets back its id, if the server still has it
    /// free, and its objects are created there again. Packets sent while it was away
    /// are lost, and so are the values held only by the server, such as aliases.
    /// UDP is not set up again.
    pub fn set_reconnect(&self, window: Option<Duration>) {
        *self.core.reconnect_window.lock().unwrap() = window;
    }

    /// Is this client using UDP for unreliable packets?
    pub fn uses_udp(&self) -> bool {
        self.core.udp.lock().unwrap().is_some()
    }

    /// Sends a packet to a [`ServerPlugin`](crate::ServerPlugin).
    pub fn send_to_plugin(
        &self,
        plugin: PluginId,
        payload: Vec<u8>,
        targets: BTreeSet<NodeId>,
    ) -> Result<()> {
        self.core.send(plugin, payload, targets)
    }

    /// Calls `handler` with the sender and content of each packet sent by a plugin
    /// to this client. It runs on the client's reading thread, so it must not wait
    /// for another packet. Packets arriving before there is a handler are lost.
    pub fn set_plugin_handler<F>(&self, plugin: PluginId, handler: F)
    where
        F: FnMut(NodeId, &[u8]) + Send + 'static,
    {
        let mut state = self.core.state.lock().unwrap();
        state
            .plugin_handlers
            .insert(plugin, Arc::new(Mutex::new(handler)));
    }

    pub(crate) fn core(&self) -> &Arc<ClientCore> {
        &self.core
    }

    /// The id given by the server.
    pub fn id(&self) -> NodeId {
        self.core.id()
    }

    pub fn nickname(&self) -> &str {
        &self.nickname
    }

    /// Is this the master client? The server chooses exactly one.
    pub fn is_master(&self) -> bool {
        self.core.state.lock().unwrap().master == self.id()
    }

    /// Number of nodes, including the server and this client.
    pub fn total_nodes(&self) -> usize {
        self.core.state.lock().unwrap().nodes.len() + 1
    }

    /// The id and nickname of every client, including this one.
    pub fn client_list(&self) -> BTreeMap<NodeId, String> {
        self.core.state.lock().unwrap().nodes.clone()
    }

    /// Finds the id of a client by nickname.
    pub fn nickname_to_id(&self, nickname: &str) -> Result<NodeId> {
        let state = self.core.state.lock().unwrap();
        state
            .nodes
            .iter()
            .find(|(_, nick)| nick.as_str() == nickname)
            .map(|(&id, _)| id)
            .ok_or_else(|| Error::UnknownNode(nickname.to_owned()))
    }

    /// False once the server has gone away.
    pub fn is_connected(&self) -> bool {
        !self.core.state.lock().unwrap().closed
    }

    /// The id of the server.
    pub fn server_id(&self) -> NodeId {
        self.core.state.lock().unwrap().server_id
    }

    /// Asks the server to stop itself and disconnect every client.
    ///
    /// Unlike the C++ library, this does not terminate any process: clients see
    /// [`is_connected`](Self::is_connected) become false, and
    /// [`GlassServer::wait`](crate::GlassServer::wait) returns.
    pub fn kill_all(&self) -> Result<()> {
        self.core
            .send_to_server(ROOT_PLUGIN_ID, RootMsg::QuitAll.encode())
    }
}

impl Drop for GlassClient {
    fn drop(&mut self) {
        self.core.leaving.store(true, Ordering::SeqCst);
        let _ = self
            .core
            .send_to_server(ROOT_PLUGIN_ID, RootMsg::Bye.encode());
        self.core.conn().close();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.udp_reader.take() {
            if let Some((socket, _)) = &*self.core.udp.lock().unwrap() {
                wake_udp(socket);
            }
            let _ = reader.join();
        }
    }
}

/// Waits for the server's id and node list.
fn handshake(core: &ClientCore, reader: &mut Stream) -> Result<()> {
    let mut has_id = false;
    let mut has_list = false;
    while !(has_id && has_list) {
        let packet = Packet::read_from(reader)?.ok_or(Error::Disconnected)?;
        if packet.plugin != ROOT_PLUGIN_ID {
            continue;
        }
        match RootMsg::decode(&packet.payload)? {
            RootMsg::Id(id) => {
                core.id.store(id, Ordering::SeqCst);
                core.state.lock().unwrap().server_id = packet.from;
                has_id = true;
            }
            RootMsg::NodeList { master, nodes } => {
                let mut state = core.state.lock().unwrap();
                state.master = master;
                state.nodes = nodes;
                has_list = true;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Marks the client as closed even if an event handler panics.
struct CloseOnExit(Arc<ClientCore>);

impl Drop for CloseOnExit {
    fn drop(&mut self) {
        self.0.mark_closed();
    }
}

/// Reads datagrams until the client closes. Only events travel through UDP, and
/// only what comes from the server counts. It blocks, with no timeout: on Windows, a
/// datagram that arrives just as a read times out can be lost.
fn udp_loop(core: Arc<ClientCore>, socket: Arc<UdpSocket>, server: SocketAddr) {
    let mut buffer = vec![0u8; MAX_DATAGRAM_SIZE + 1];
    while !core.state.lock().unwrap().closed {
        let (size, from) = match socket.recv_from(&mut buffer) {
            Ok(received) => received,
            Err(e) => {
                // Windows reports here that a previous send got no one to receive it.
                log::debug!("UDP read failed: {e}");
                thread::sleep(UDP_RETRY_INTERVAL);
                continue;
            }
        };
        if from != server {
            continue;
        }
        match Packet::decode(&buffer[..size]) {
            Ok(packet) if packet.plugin == EVENT_PLUGIN_ID => {
                on_packet(&core, &packet);
            }
            Ok(packet) => log::warn!("ignoring datagram for plugin {}", packet.plugin),
            Err(e) => log::debug!("ignoring invalid datagram: {e}"),
        }
    }
}

fn read_loop(core: Arc<ClientCore>, mut reader: Stream) {
    let _close = CloseOnExit(core.clone());
    loop {
        if !read_until_closed(&core, &mut reader) || core.leaving.load(Ordering::SeqCst) {
            return;
        }
        match reconnect(&core) {
            Some(new_reader) => reader = new_reader,
            None => return,
        }
    }
}

/// Handles packets until the connection closes. Returns false if the server told the
/// client to stop.
fn read_until_closed(core: &ClientCore, reader: &mut Stream) -> bool {
    loop {
        match Packet::read_from(reader) {
            Ok(Some(packet)) => {
                if !on_packet(core, &packet) {
                    return false;
                }
            }
            Ok(None) => return true,
            Err(e) => {
                log::debug!("connection closed: {e}");
                return true;
            }
        }
    }
}

/// Connects again, if the client was asked to. Gives the reading half of the new
/// connection, or `None` if it gave up.
fn reconnect(core: &ClientCore) -> Option<Stream> {
    let window = (*core.reconnect_window.lock().unwrap())?;
    // Meanwhile, waits fail instead of waiting for what will not come.
    core.mark_closed();
    // UDP is not set up again: its reading thread ends, and events go by the connection.
    if let Some((socket, _)) = core.udp.lock().unwrap().take() {
        wake_udp(&socket);
    }

    let old_id = core.id();
    let deadline = Instant::now() + window;
    let mut pause = CONNECT_RETRY_INTERVAL;
    while Instant::now() < deadline && !core.leaving.load(Ordering::SeqCst) {
        match connect_again(core, old_id) {
            Ok(reader) => {
                log::info!("connected again, as {:#x}", core.id());
                return Some(reader);
            }
            Err(e) => log::debug!("could not connect again yet: {e}"),
        }
        thread::sleep(pause);
        pause = (pause * 2).min(MAX_RECONNECT_PAUSE);
    }
    log::warn!("gave up connecting again");
    None
}

fn connect_again(core: &ClientCore, old_id: NodeId) -> Result<Stream> {
    let (conn, mut reader) = Conn::new((core.dial)()?)?;
    *core.conn.lock().unwrap() = conn.clone();
    // Dropping the client closes the connection it sees, which may be the old one.
    if core.leaving.load(Ordering::SeqCst) {
        conn.close();
        return Err(Error::Disconnected);
    }
    let hello = RootMsg::HelloAgain(core.nickname.clone(), old_id);
    core.send(ROOT_PLUGIN_ID, hello.encode(), BTreeSet::new())?;
    reader.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
    handshake(core, &mut reader).inspect_err(|_| conn.close())?;
    reader.set_read_timeout(None)?;

    let registrations: Vec<(PluginId, Vec<u8>)> = core
        .registrations
        .lock()
        .unwrap()
        .iter()
        .map(|((plugin, _), message)| (*plugin, message.clone()))
        .collect();
    for (plugin, message) in registrations {
        core.send_to_server(plugin, message)?;
    }
    core.state.lock().unwrap().closed = false;
    core.changed.notify_all();
    Ok(reader)
}

/// Handles a packet from the server. Returns false if the client should stop.
fn on_packet(core: &ClientCore, packet: &Packet) -> bool {
    match packet.plugin {
        ROOT_PLUGIN_ID => return on_root(core, packet),
        EVENT_PLUGIN_ID => match EventMsg::decode(&packet.payload) {
            Ok(EventMsg::Event(name, data)) => event::deliver(core, &name, packet.from, data),
            Ok(other) => log::warn!("unexpected event message: {other:?}"),
            Err(e) => log::warn!("invalid event message: {e}"),
        },
        SHARED_PLUGIN_ID => match SharedMsg::decode(&packet.payload) {
            Ok(SharedMsg::Changed(name, data)) => {
                let mut state = core.state.lock().unwrap();
                if let Some(slot) = state.shared.get_mut(&name) {
                    slot.data = data;
                    slot.generation += 1;
                }
                drop(state);
                core.changed.notify_all();
            }
            Ok(other) => log::warn!("unexpected shared message: {other:?}"),
            Err(e) => log::warn!("invalid shared message: {e}"),
        },
        BARRIER_PLUGIN_ID => match BarrierMsg::decode(&packet.payload) {
            Ok(msg) if msg.kind == BarrierKind::GetOut => {
                let mut state = core.state.lock().unwrap();
                if let Some(slot) = state.barriers.get_mut(&(msg.group, msg.number)) {
                    slot.syncing = false;
                }
                drop(state);
                core.changed.notify_all();
            }
            Ok(other) => log::warn!("unexpected barrier message: {other:?}"),
            Err(e) => log::warn!("invalid barrier message: {e}"),
        },
        ALIAS_PLUGIN_ID => match AliasMsg::decode(&packet.payload) {
            Ok(AliasMsg::Associate {
                name, association, ..
            }) => {
                let mut state = core.state.lock().unwrap();
                if let Some(slot) = state.aliases.get_mut(&name) {
                    slot.association = Some(association);
                }
            }
            Ok(other) => log::warn!("unexpected alias message: {other:?}"),
            Err(e) => log::warn!("invalid alias message: {e}"),
        },
        REMOTE_PLUGIN_ID => match RemoteMsg::decode(&packet.payload) {
            Ok(msg) => on_remote(core, packet.from, msg),
            Err(e) => log::warn!("invalid remote message: {e}"),
        },
        MESSENGER_PLUGIN_ID => match MessengerMsg::decode(&packet.payload) {
            Ok(MessengerMsg::Message { name, data, .. }) => {
                messenger::deliver(core, &name, packet.from, data)
            }
            Ok(other) => log::warn!("unexpected messenger message: {other:?}"),
            Err(e) => log::warn!("invalid messenger message: {e}"),
        },
        other => {
            let handler = core
                .state
                .lock()
                .unwrap()
                .plugin_handlers
                .get(&other)
                .cloned();
            match handler {
                Some(handler) => (handler.lock().unwrap())(packet.from, &packet.payload),
                None => log::warn!("ignoring packet for unknown plugin {other}"),
            }
        }
    }
    true
}

fn on_remote(core: &ClientCore, from: NodeId, msg: RemoteMsg) {
    let mut state = core.state.lock().unwrap();
    match msg {
        RemoteMsg::Set(name, data) => {
            if let Some(slot) = state.remotes.get_mut(&name) {
                slot.data = data;
            }
        }
        RemoteMsg::Value(name, data) => {
            if let Some(slot) = state.remotes.get_mut(&name) {
                slot.store_reply(from, data);
            }
            drop(state);
            core.changed.notify_all();
        }
        RemoteMsg::Get(name) => {
            let Some(data) = state.remotes.get(&name).map(|slot| slot.data.clone()) else {
                return;
            };
            drop(state);
            let answer = RemoteMsg::Value(name, data);
            let target = BTreeSet::from([from]);
            if let Err(e) = core.send(REMOTE_PLUGIN_ID, answer.encode(), target) {
                log::warn!("could not answer {from:#x}: {e}");
            }
        }
        other => log::warn!("unexpected remote message: {other:?}"),
    }
}

fn on_root(core: &ClientCore, packet: &Packet) -> bool {
    match RootMsg::decode(&packet.payload) {
        Ok(RootMsg::NodeList { master, nodes }) => {
            let mut state = core.state.lock().unwrap();
            state.master = master;
            state.nodes = nodes;
            true
        }
        Ok(RootMsg::QuitAll) | Ok(RootMsg::Bye) => false,
        Ok(RootMsg::UdpPort(port)) => {
            core.state.lock().unwrap().udp_port = Some(port);
            core.changed.notify_all();
            true
        }
        Ok(RootMsg::UdpReady) => {
            core.state.lock().unwrap().udp_ready = true;
            core.changed.notify_all();
            true
        }
        Ok(other) => {
            log::warn!("unexpected root message: {other:?}");
            true
        }
        Err(e) => {
            log::warn!("invalid root message: {e}");
            true
        }
    }
}

fn tcp_dial(addrs: Vec<SocketAddr>) -> Dial {
    Box::new(move || TcpStream::connect(&addrs[..]).map(Stream::Tcp))
}

fn retry(
    timeout: Duration,
    mut connect: impl FnMut() -> Result<GlassClient>,
) -> Result<GlassClient> {
    let deadline = Instant::now() + timeout;
    loop {
        match connect() {
            Ok(client) => return Ok(client),
            Err(e) if Instant::now() >= deadline => return Err(e),
            Err(e) => log::debug!("could not connect yet: {e}"),
        }
        thread::sleep(CONNECT_RETRY_INTERVAL);
    }
}
