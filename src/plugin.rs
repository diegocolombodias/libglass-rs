//! Plugins defined outside of this library.

use crate::outbound::Outbound;
use crate::owners::{Links, Relation, Source};
use crate::types::{NodeId, PluginId};
use std::collections::BTreeSet;

/// Plugin ids up to this one are reserved for the library.
pub const MAX_RESERVED_PLUGIN_ID: PluginId = 99;

/// What a node is to the server handling its packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    Client,
    /// A peer of the same group.
    Peer,
    /// A gateway of another group.
    RemotePeer,
}

/// A node a packet came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PacketSource {
    /// The node at the other end of the connection.
    pub id: NodeId,
    pub kind: NodeKind,
    /// The client that originated the packet, if it came through another server.
    pub origin: NodeId,
}

/// A packet that a [`ServerPlugin`] wants to send.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PluginMessage {
    pub payload: Vec<u8>,
    /// Who it is for: clients of this server, or groups like `EVERYBODY_ID`.
    pub targets: BTreeSet<NodeId>,
    /// Who it must not reach, even if it is one of the targets.
    pub excluded: BTreeSet<NodeId>,
    /// Should the same plugin on other servers get it too? It follows the path
    /// that does not make packets go in circles, so it arrives once per server.
    pub to_other_servers: bool,
}

impl PluginMessage {
    /// A message for one node or group.
    pub fn to(target: NodeId, payload: Vec<u8>) -> PluginMessage {
        PluginMessage {
            payload,
            targets: BTreeSet::from([target]),
            ..PluginMessage::default()
        }
    }
}

/// The server side of a plugin. All its methods run on the thread that processes
/// packets, one at a time, so it does not need any lock. It must not block.
pub trait ServerPlugin: Send {
    /// Handles a packet addressed to this plugin.
    fn on_packet(
        &mut self,
        source: PacketSource,
        targets: &BTreeSet<NodeId>,
        payload: &[u8],
    ) -> Vec<PluginMessage>;

    /// A node has left. The default is to do nothing.
    fn on_node_left(&mut self, id: NodeId) -> Vec<PluginMessage> {
        let _ = id;
        vec![]
    }
}

pub(crate) fn public_source(source: Source) -> PacketSource {
    PacketSource {
        id: source.id,
        kind: match source.relation {
            Relation::Client => NodeKind::Client,
            Relation::Peer => NodeKind::Peer,
            Relation::Remote => NodeKind::RemotePeer,
        },
        origin: source.origin,
    }
}

/// Converts what a plugin returned. `relation` is the kind of node that caused
/// it, which decides where it goes next. When nothing did, it goes everywhere.
pub(crate) fn outbound(
    plugin: PluginId,
    message: PluginMessage,
    relation: Option<Relation>,
    black: Option<NodeId>,
) -> Outbound {
    let links = match (message.to_other_servers, relation) {
        (false, _) => Links::NONE,
        (true, Some(relation)) => Links::forward_from(relation),
        (true, None) => Links::ALL,
    };
    let mut out = Outbound::to_links(plugin, message.payload, links);
    out.targets = message.targets;
    out.black = message.excluded;
    if message.to_other_servers {
        out.black.extend(black);
    }
    out
}
