use crate::owners::Links;
use crate::types::{NodeId, PluginId};
use std::collections::BTreeSet;

/// A packet a server-side plugin wants to send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Outbound {
    pub(crate) plugin: PluginId,
    pub(crate) payload: Vec<u8>,
    /// Nodes it is addressed to. Local clients only get it if listed here.
    pub(crate) targets: BTreeSet<NodeId>,
    /// Never send to these nodes. Takes precedence over `white`.
    pub(crate) black: BTreeSet<NodeId>,
    /// If not empty, send only to these nodes.
    pub(crate) white: BTreeSet<NodeId>,
    /// Which servers it also goes to, if some target is not a local client.
    pub(crate) links: Links,
    /// The sender written in the packet, if not this server.
    pub(crate) from: Option<NodeId>,
}

impl Outbound {
    /// A packet for one node, or for a group such as `CLIENTS_ID`.
    pub(crate) fn to_node(plugin: PluginId, payload: Vec<u8>, target: NodeId) -> Outbound {
        Outbound {
            plugin,
            payload,
            targets: BTreeSet::from([target]),
            black: BTreeSet::new(),
            white: BTreeSet::new(),
            links: Links::NONE,
            from: None,
        }
    }

    /// A packet for other servers only.
    pub(crate) fn to_links(plugin: PluginId, payload: Vec<u8>, links: Links) -> Outbound {
        Outbound {
            plugin,
            payload,
            targets: BTreeSet::new(),
            black: BTreeSet::new(),
            white: BTreeSet::new(),
            links,
            from: None,
        }
    }
}
