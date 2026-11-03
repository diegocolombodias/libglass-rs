/// Unique id of a node; used to address packets.
pub type NodeId = u32;

/// Unique id of a plugin; used to route packets.
pub type PluginId = u16;

/// Id for "nobody". Also terminates the target list on the wire.
pub const NOBODY_ID: NodeId = 0;
/// Id for "everybody" (multicast).
pub const EVERYBODY_ID: NodeId = 1;
/// Id for servers.
pub const SERVERS_ID: NodeId = 2;
/// Id for clients.
pub const CLIENTS_ID: NodeId = 3;
/// Id for peers.
pub const PEERS_ID: NodeId = 4;
/// Id for remote peers.
pub const REMOTE_PEERS_ID: NodeId = 5;
/// Id of a server. Clients get consecutive ids after it.
pub const BASE_SERVER_ID: NodeId = 0x10000;
/// A server id only uses these bits. Its clients take the ids that follow it.
pub(crate) const SERVER_ID_MASK: NodeId = 0xFFFF_0000;

pub const ROOT_PLUGIN_ID: PluginId = 10;
pub const ALIAS_PLUGIN_ID: PluginId = 11;
pub const BARRIER_PLUGIN_ID: PluginId = 12;
pub const SHARED_PLUGIN_ID: PluginId = 13;
pub const EVENT_PLUGIN_ID: PluginId = 14;
pub const REMOTE_PLUGIN_ID: PluginId = 15;
pub const MESSENGER_PLUGIN_ID: PluginId = 16;

/// Default TCP port.
pub const DEFAULT_PORT: u16 = 9191;

/// Propagation mode of a plugin object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Shared only among the clients of one server.
    Local,
    /// Shared with the clients of the peers of that server, and of other groups.
    Global,
}

/// The kind of connection between two servers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkKind {
    /// Between peers of the same group.
    Peer,
    /// Between gateways (remote peers) of different groups.
    Remote,
}
