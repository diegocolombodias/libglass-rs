//! How global objects propagate among servers.
//!
//! Servers form a two-level hierarchy, which is what keeps packets from looping:
//! peers of a group are all connected to each other, and some of them are also
//! connected to the gateways (remote peers) of other groups. A message from a
//! client goes to both levels, a message from a peer only goes on to other
//! groups, and a message from another group only goes on to the local peers.

use crate::outbound::Outbound;
use crate::types::{LinkKind, NodeId, PluginId};
use std::collections::{BTreeMap, BTreeSet};

/// What a node is to this server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Relation {
    Client,
    Peer,
    Remote,
}

impl From<LinkKind> for Relation {
    fn from(kind: LinkKind) -> Relation {
        match kind {
            LinkKind::Peer => Relation::Peer,
            LinkKind::Remote => Relation::Remote,
        }
    }
}

impl Relation {
    /// Does a node with this relation matter to the servers across this kind of
    /// link? Peers do not relay for peers, nor remote peers for remote peers.
    pub(crate) fn counts_toward(self, kind: LinkKind) -> bool {
        matches!(
            (self, kind),
            (Relation::Client, _)
                | (Relation::Remote, LinkKind::Peer)
                | (Relation::Peer, LinkKind::Remote)
        )
    }
}

/// Where a packet came from.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Source {
    /// The node at the other end of the connection.
    pub(crate) id: NodeId,
    pub(crate) relation: Relation,
    /// The client that originated it, as far as it is known.
    pub(crate) origin: NodeId,
}

/// Which kinds of link a packet also goes through.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Links {
    pub(crate) peers: bool,
    pub(crate) remotes: bool,
}

impl Links {
    pub(crate) const NONE: Links = Links {
        peers: false,
        remotes: false,
    };
    pub(crate) const ALL: Links = Links {
        peers: true,
        remotes: true,
    };

    pub(crate) fn only(kind: LinkKind) -> Links {
        Links {
            peers: kind == LinkKind::Peer,
            remotes: kind == LinkKind::Remote,
        }
    }

    pub(crate) fn includes(self, kind: LinkKind) -> bool {
        match kind {
            LinkKind::Peer => self.peers,
            LinkKind::Remote => self.remotes,
        }
    }

    /// Where a message that arrived from this kind of node goes next.
    pub(crate) fn forward_from(relation: Relation) -> Links {
        match relation {
            Relation::Client => Links::ALL,
            Relation::Peer => Links::only(LinkKind::Remote),
            Relation::Remote => Links::only(LinkKind::Peer),
        }
    }
}

/// The nodes that own an event or shared variable, and what other servers have
/// been told about it.
#[derive(Default)]
pub(crate) struct Owners {
    nodes: BTreeMap<NodeId, Relation>,
    global: bool,
    told_peers: bool,
    told_remotes: bool,
}

impl Owners {
    pub(crate) fn insert(&mut self, source: Source, global: bool) {
        self.nodes.insert(source.id, source.relation);
        self.global |= global;
    }

    pub(crate) fn remove(&mut self, id: NodeId) -> bool {
        self.nodes.remove(&id).is_some()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub(crate) fn ids(&self) -> BTreeSet<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// The owners that are clients of this server.
    pub(crate) fn clients(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes
            .iter()
            .filter(|(_, relation)| **relation == Relation::Client)
            .map(|(&id, _)| id)
    }

    /// Where a message from `relation` about this object goes next.
    pub(crate) fn forward_links(&self, relation: Relation) -> Links {
        if self.global {
            Links::forward_from(relation)
        } else {
            Links::NONE
        }
    }

    fn relevant(&self, kind: LinkKind) -> bool {
        self.nodes.values().any(|r| r.counts_toward(kind))
    }

    /// Tells other servers about changes: they have to know about this object
    /// for as long as there is an owner on this side of the link.
    pub(crate) fn reconcile(
        &mut self,
        plugin: PluginId,
        create: &[u8],
        delete: &[u8],
    ) -> Vec<Outbound> {
        if !self.global {
            return vec![];
        }
        let mut out = Vec::new();
        for kind in [LinkKind::Peer, LinkKind::Remote] {
            let relevant = self.relevant(kind);
            let told = match kind {
                LinkKind::Peer => &mut self.told_peers,
                LinkKind::Remote => &mut self.told_remotes,
            };
            if relevant != *told {
                *told = relevant;
                let payload = if relevant { create } else { delete };
                out.push(Outbound::to_links(
                    plugin,
                    payload.to_vec(),
                    Links::only(kind),
                ));
            }
        }
        out
    }

    /// Whether a new link has to be told about this object.
    pub(crate) fn concerns_new_link(&self, kind: LinkKind) -> bool {
        self.global && self.relevant(kind)
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) fn client(id: NodeId) -> Source {
        Source {
            id,
            relation: Relation::Client,
            origin: id,
        }
    }

    pub(crate) fn peer(id: NodeId, origin: NodeId) -> Source {
        Source {
            id,
            relation: Relation::Peer,
            origin,
        }
    }

    pub(crate) fn remote(id: NodeId, origin: NodeId) -> Source {
        Source {
            id,
            relation: Relation::Remote,
            origin,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    const CREATE: &[u8] = b"create";
    const DELETE: &[u8] = b"delete";

    fn reconcile(owners: &mut Owners) -> Vec<(Links, Vec<u8>)> {
        owners
            .reconcile(1, CREATE, DELETE)
            .into_iter()
            .map(|out| (out.links, out.payload))
            .collect()
    }

    #[test]
    fn forwarding_never_goes_back_to_the_same_level() {
        assert_eq!(Links::forward_from(Relation::Client), Links::ALL);
        assert!(!Links::forward_from(Relation::Peer).peers);
        assert!(!Links::forward_from(Relation::Remote).remotes);
    }

    #[test]
    fn local_objects_tell_nobody() {
        let mut owners = Owners::default();
        owners.insert(client(1), false);
        assert!(reconcile(&mut owners).is_empty());
        assert_eq!(owners.forward_links(Relation::Client), Links::NONE);
    }

    #[test]
    fn a_client_is_announced_to_both_levels_once() {
        let mut owners = Owners::default();
        owners.insert(client(1), true);
        assert_eq!(
            reconcile(&mut owners),
            [
                (Links::only(LinkKind::Peer), CREATE.to_vec()),
                (Links::only(LinkKind::Remote), CREATE.to_vec()),
            ]
        );
        owners.insert(client(2), true);
        assert!(reconcile(&mut owners).is_empty());
    }

    #[test]
    fn a_peer_is_only_announced_to_other_groups() {
        let mut owners = Owners::default();
        owners.insert(peer(1, 1), true);
        assert_eq!(
            reconcile(&mut owners),
            [(Links::only(LinkKind::Remote), CREATE.to_vec())]
        );
    }

    #[test]
    fn delete_is_sent_when_no_owner_is_left_on_that_side() {
        let mut owners = Owners::default();
        owners.insert(client(1), true);
        owners.insert(peer(2, 2), true);
        reconcile(&mut owners);

        // The peer still needs to know about the remote side, and vice versa.
        assert!(owners.remove(1));
        assert_eq!(
            reconcile(&mut owners),
            [(Links::only(LinkKind::Peer), DELETE.to_vec())]
        );
        assert!(owners.remove(2));
        assert_eq!(
            reconcile(&mut owners),
            [(Links::only(LinkKind::Remote), DELETE.to_vec())]
        );
        assert!(owners.is_empty());
    }

    #[test]
    fn a_local_object_becomes_global_if_any_owner_asks() {
        let mut owners = Owners::default();
        owners.insert(client(1), false);
        owners.insert(client(2), true);
        assert_eq!(owners.forward_links(Relation::Client), Links::ALL);
    }
}
