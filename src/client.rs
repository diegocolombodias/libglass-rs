use crate::alias::AliasSlot;
use crate::barrier::BarrierSlot;
use crate::conn::Conn;
use crate::error::{Error, Result};
use crate::event::{self, EventSlot};
use crate::packet::Packet;
use crate::protocol::{AliasMsg, BarrierKind, BarrierMsg, EventMsg, RemoteMsg, RootMsg, SharedMsg};
use crate::remote::RemoteSlot;
use crate::shared::SharedSlot;
use crate::types::*;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

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
}

/// The part of a client shared with its reading thread and plugin objects.
pub(crate) struct ClientCore {
    id: AtomicU32,
    conn: Arc<Conn>,
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
        self.conn.send(&packet.encode()?)
    }

    pub(crate) fn send_to_server(&self, plugin: PluginId, payload: Vec<u8>) -> Result<()> {
        self.send(plugin, payload, BTreeSet::from([SERVERS_ID]))
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
}

impl GlassClient {
    /// Connects to a server. Returns only after the handshake has finished.
    pub fn connect<A: ToSocketAddrs>(server: A, nickname: &str) -> Result<GlassClient> {
        let stream = TcpStream::connect(server)?;
        let (conn, mut reader) = Conn::new(stream)?;
        let core = Arc::new(ClientCore {
            id: AtomicU32::new(NOBODY_ID),
            conn,
            state: Mutex::new(ClientState::default()),
            changed: Condvar::new(),
        });

        core.send(
            ROOT_PLUGIN_ID,
            RootMsg::Hello(nickname.to_owned()).encode(),
            BTreeSet::new(),
        )?;

        reader.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
        handshake(&core, &mut reader).inspect_err(|_| core.conn.close())?;
        reader.set_read_timeout(None)?;

        let reader = {
            let core = core.clone();
            thread::spawn(move || read_loop(core, reader))
        };
        Ok(GlassClient {
            core,
            nickname: nickname.to_owned(),
            reader: Some(reader),
        })
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
        let _ = self
            .core
            .send_to_server(ROOT_PLUGIN_ID, RootMsg::Bye.encode());
        self.core.conn.close();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

/// Waits for the server's id and node list.
fn handshake(core: &ClientCore, reader: &mut TcpStream) -> Result<()> {
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

fn read_loop(core: Arc<ClientCore>, mut reader: TcpStream) {
    let _close = CloseOnExit(core.clone());
    loop {
        match Packet::read_from(&mut reader) {
            Ok(Some(packet)) => {
                if !on_packet(&core, &packet) {
                    return;
                }
            }
            Ok(None) => return,
            Err(e) => {
                log::debug!("connection closed: {e}");
                return;
            }
        }
    }
}

/// Handles a packet from the server. Returns false if the client should stop.
fn on_packet(core: &ClientCore, packet: &Packet) -> bool {
    match packet.plugin {
        ROOT_PLUGIN_ID => return on_root(core, packet),
        EVENT_PLUGIN_ID => match EventMsg::decode(&packet.payload) {
            Ok(EventMsg::Event(name, data)) => event::deliver(core, &name, data),
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
        other => log::warn!("ignoring packet for unknown plugin {other}"),
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
