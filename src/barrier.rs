use crate::client::{ClientCore, GlassClient};
use crate::error::{Error, Result};
use crate::outbound::Outbound;
use crate::owners::{Links, Relation, Source};
use crate::protocol::{BarrierKind, BarrierMsg};
use crate::types::*;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct BarrierSlot {
    /// A sync is in progress. Other threads calling sync wait for the same one.
    pub(crate) syncing: bool,
}

/// A synchronization point among the clients that created it.
///
/// [`sync`](Self::sync) blocks until every client with this barrier has called
/// it. A barrier created as inactive does not hold the others back until its
/// client gets through a sync on any barrier of the group, which lets clients
/// join a group that is already running.
pub struct Barrier {
    core: Arc<ClientCore>,
    number: u32,
    group: u32,
    /// How it was created, to create it again after a reconnection.
    create: BarrierKind,
}

impl Barrier {
    /// Creates a barrier. Each client can only have one with a given number and
    /// group.
    pub fn new(
        client: &GlassClient,
        number: u32,
        group: u32,
        active: bool,
        mode: Mode,
    ) -> Result<Barrier> {
        let core = client.core().clone();
        let key = (group, number);
        {
            let mut state = core.state.lock().unwrap();
            if state.barriers.contains_key(&key) {
                return Err(Error::AlreadyExists(format!("barrier {number}:{group}")));
            }
            state.barriers.insert(key, BarrierSlot::default());
        }

        let kind = match mode {
            Mode::Local => BarrierKind::CreateLocal,
            Mode::Global => BarrierKind::CreateGlobal,
        };
        // If sending fails, dropping the barrier removes it from the client.
        let barrier = Barrier {
            core,
            number,
            group,
            create: kind,
        };
        barrier.register(active)?;
        Ok(barrier)
    }

    fn key(&self) -> String {
        format!("{}:{}", self.group, self.number)
    }

    /// Creates the barrier in the server, and keeps how, to create it again after a
    /// reconnection.
    fn register(&self, active: bool) -> Result<()> {
        let msg = BarrierMsg {
            kind: self.create,
            number: self.number,
            group: self.group,
            active,
        };
        self.core
            .register(BARRIER_PLUGIN_ID, &self.key(), msg.encode())
    }

    fn send(&self, kind: BarrierKind, active: bool) -> Result<()> {
        let msg = BarrierMsg {
            kind,
            number: self.number,
            group: self.group,
            active,
        };
        self.core.send_to_server(BARRIER_PLUGIN_ID, msg.encode())
    }

    /// Stops this client from taking part in the barrier: the others no longer
    /// wait for it, and its own calls to sync return right away.
    pub fn disable(&self) -> Result<()> {
        self.send(BarrierKind::Disable, false)?;
        self.core
            .update_registration(BARRIER_PLUGIN_ID, &self.key(), self.create_message(false));
        Ok(())
    }

    /// Makes this client take part in the barrier again.
    pub fn enable(&self) -> Result<()> {
        self.send(BarrierKind::Enable, false)?;
        self.core
            .update_registration(BARRIER_PLUGIN_ID, &self.key(), self.create_message(true));
        Ok(())
    }

    fn create_message(&self, active: bool) -> Vec<u8> {
        BarrierMsg {
            kind: self.create,
            number: self.number,
            group: self.group,
            active,
        }
        .encode()
    }

    /// Blocks until all the clients with this barrier have called sync.
    pub fn sync(&self) -> Result<()> {
        let key = (self.group, self.number);
        let mut state = self.core.state.lock().unwrap();
        let slot = state.barriers.get_mut(&key).ok_or(Error::Disconnected)?;
        if !slot.syncing {
            slot.syncing = true;
            drop(state);
            if let Err(e) = self.send(BarrierKind::Sync, false) {
                let mut state = self.core.state.lock().unwrap();
                if let Some(slot) = state.barriers.get_mut(&key) {
                    slot.syncing = false;
                }
                drop(state);
                self.core.changed.notify_all();
                return Err(e);
            }
            state = self.core.state.lock().unwrap();
        }
        loop {
            let slot = state.barriers.get(&key).ok_or(Error::Disconnected)?;
            if !slot.syncing {
                return Ok(());
            }
            if state.closed {
                return Err(Error::Disconnected);
            }
            state = self.core.changed.wait(state).unwrap();
        }
    }
}

impl Drop for Barrier {
    fn drop(&mut self) {
        let key = (self.group, self.number);
        self.core.state.lock().unwrap().barriers.remove(&key);
        self.core.changed.notify_all();
        self.core.unregister(BARRIER_PLUGIN_ID, &self.key());
        let _ = self.send(BarrierKind::Delete, false);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Active,
    Inactive,
    Locked,
    Disabled,
}

#[derive(Clone, Copy)]
struct NodeBarrier {
    relation: Relation,
    state: State,
    /// For other servers: how many times they have said that everything on
    /// their side is at the barrier. It is a count because a server can finish a
    /// round and start the next before this one has finished the first.
    pending: u32,
}

impl NodeBarrier {
    fn is_locked(&self) -> bool {
        self.state == State::Locked || self.pending > 0
    }

    fn is_active(&self) -> bool {
        self.state == State::Active && self.pending == 0
    }
}

#[derive(Default)]
struct Side {
    /// What the servers on this side know: nothing, or whether it is active.
    told: Option<bool>,
    /// Have they been told that everything on this side is at the barrier?
    announced: bool,
}

#[derive(Default)]
struct BarrierEntry {
    nodes: BTreeMap<NodeId, NodeBarrier>,
    global: bool,
    peers: Side,
    remotes: Side,
}

fn message(kind: BarrierKind, group: u32, number: u32, active: bool) -> Vec<u8> {
    BarrierMsg {
        kind,
        number,
        group,
        active,
    }
    .encode()
}

impl BarrierEntry {
    fn relevant(&self, kind: LinkKind) -> impl Iterator<Item = &NodeBarrier> {
        self.nodes
            .values()
            .filter(move |node| node.state != State::Disabled && node.relation.counts_toward(kind))
    }

    /// Tells other servers what has changed on this side of them.
    fn reconcile(&mut self, group: u32, number: u32, out: &mut Vec<Outbound>) {
        if !self.global {
            return;
        }
        for kind in [LinkKind::Peer, LinkKind::Remote] {
            let exists = self.relevant(kind).next().is_some();
            let active = self.relevant(kind).any(|n| n.state != State::Inactive);
            let ready = !self.relevant(kind).any(|n| n.is_active())
                && self.relevant(kind).any(|n| n.is_locked());
            let side = match kind {
                LinkKind::Peer => &mut self.peers,
                LinkKind::Remote => &mut self.remotes,
            };
            let mut send = |kind_of_message, active| {
                out.push(Outbound::to_links(
                    BARRIER_PLUGIN_ID,
                    message(kind_of_message, group, number, active),
                    Links::only(kind),
                ));
            };

            if !exists {
                if side.told.take().is_some() {
                    send(BarrierKind::Delete, false);
                }
                side.announced = false;
                continue;
            }
            if side.told.is_none() || (side.told == Some(false) && active) {
                send(BarrierKind::CreateGlobal, active);
                side.told = Some(active);
            }
            if ready && !side.announced {
                send(BarrierKind::Sync, false);
                side.announced = true;
            }
        }
    }
}

/// Server side of the barrier plugin.
///
/// In a global barrier, another server counts as one node: it is locked when
/// everything on its side is. Each server releases its own clients when nothing
/// that it sees is active anymore.
#[derive(Default)]
pub(crate) struct BarrierServer {
    /// group -> barrier number -> its state.
    groups: BTreeMap<u32, BTreeMap<u32, BarrierEntry>>,
    /// Has the client already passed a barrier? New barriers it creates are then
    /// active, which avoids a deadlock between it and nodes waiting for it.
    activated: BTreeMap<NodeId, bool>,
}

impl BarrierServer {
    pub(crate) fn on_packet(&mut self, source: Source, payload: &[u8]) -> Result<Vec<Outbound>> {
        let msg = BarrierMsg::decode(payload)?;
        let (group, number) = (msg.group, msg.number);
        match msg.kind {
            BarrierKind::CreateLocal => self.create(source, &msg, false),
            BarrierKind::CreateGlobal => self.create(source, &msg, true),
            BarrierKind::Sync => {
                let node = self
                    .groups
                    .get_mut(&group)
                    .and_then(|barriers| barriers.get_mut(&number))
                    .and_then(|entry| entry.nodes.get_mut(&source.id));
                let Some(node) = node else {
                    log::warn!("invalid sync {number}:{group} from {:#x}", source.id);
                    return Ok(vec![]);
                };
                match source.relation {
                    Relation::Client if node.state == State::Disabled => {
                        let get_out = message(BarrierKind::GetOut, group, number, false);
                        return Ok(vec![Outbound::to_node(
                            BARRIER_PLUGIN_ID,
                            get_out,
                            source.id,
                        )]);
                    }
                    Relation::Client => node.state = State::Locked,
                    Relation::Peer | Relation::Remote => node.pending += 1,
                }
            }
            BarrierKind::Delete => {
                let removed = self
                    .groups
                    .get_mut(&group)
                    .and_then(|barriers| barriers.get_mut(&number))
                    .and_then(|entry| entry.nodes.remove(&source.id));
                if removed.is_none() {
                    log::warn!(
                        "node {:#x} does not own barrier {number}:{group}",
                        source.id
                    );
                    return Ok(vec![]);
                }
            }
            BarrierKind::Disable | BarrierKind::Enable => {
                let node = self
                    .groups
                    .get_mut(&group)
                    .and_then(|barriers| barriers.get_mut(&number))
                    .and_then(|entry| entry.nodes.get_mut(&source.id));
                let Some(node) = node else {
                    log::warn!(
                        "node {:#x} does not own barrier {number}:{group}",
                        source.id
                    );
                    return Ok(vec![]);
                };
                let was_locked = node.state == State::Locked;
                node.state = if msg.kind == BarrierKind::Disable {
                    State::Disabled
                } else {
                    State::Active
                };
                let mut out = Vec::new();
                if was_locked {
                    // Its sync is in progress, and no one else will end it now.
                    let get_out = message(BarrierKind::GetOut, group, number, false);
                    out.push(Outbound::to_node(BARRIER_PLUGIN_ID, get_out, source.id));
                }
                out.extend(self.check_update(group, number));
                return Ok(out);
            }
            BarrierKind::GetOut => {
                log::warn!(
                    "unexpected barrier message from {:#x}: {:?}",
                    source.id,
                    msg.kind
                );
                return Ok(vec![]);
            }
        }
        Ok(self.check_update(group, number))
    }

    fn create(&mut self, source: Source, msg: &BarrierMsg, global: bool) {
        let entry = self
            .groups
            .entry(msg.group)
            .or_default()
            .entry(msg.number)
            .or_default();
        entry.global |= global;

        if let Some(node) = entry.nodes.get_mut(&source.id) {
            if source.relation == Relation::Client {
                log::warn!("node {:#x} created barrier {} twice", source.id, msg.number);
            } else if msg.active && node.state == State::Inactive {
                // A server repeats this when its side becomes active.
                node.state = State::Active;
            }
            return;
        }

        let activated = source.relation == Relation::Client
            && *self.activated.entry(source.id).or_insert(false);
        let state = if msg.active || activated {
            State::Active
        } else {
            State::Inactive
        };
        entry.nodes.insert(
            source.id,
            NodeBarrier {
                relation: source.relation,
                state,
                pending: 0,
            },
        );
    }

    fn check_update(&mut self, group: u32, number: u32) -> Vec<Outbound> {
        let mut out = Vec::new();
        while self.step(group, number, &mut out) {}
        out
    }

    /// Updates other servers, then releases the barrier if it is time to.
    /// Returns true if it did, since that may allow another round.
    fn step(&mut self, group: u32, number: u32, out: &mut Vec<Outbound>) -> bool {
        let Some(barriers) = self.groups.get_mut(&group) else {
            return false;
        };
        let Some(entry) = barriers.get_mut(&number) else {
            return false;
        };

        entry.reconcile(group, number, out);
        if entry.nodes.is_empty() {
            barriers.remove(&number);
            if barriers.is_empty() {
                self.groups.remove(&group);
            }
            return false;
        }
        if entry.nodes.values().any(NodeBarrier::is_active) {
            return false;
        }

        let locked: Vec<NodeId> = entry
            .nodes
            .iter()
            .filter(|(_, node)| node.is_locked())
            .map(|(&id, _)| id)
            .collect();
        if locked.is_empty() {
            return false;
        }

        let get_out = message(BarrierKind::GetOut, group, number, false);
        for id in locked {
            let Some(node) = barriers.get_mut(&number).and_then(|e| e.nodes.get_mut(&id)) else {
                continue;
            };
            if node.relation != Relation::Client {
                node.pending -= 1;
                node.state = State::Active;
                continue;
            }
            node.state = State::Active;
            out.push(Outbound::to_node(BARRIER_PLUGIN_ID, get_out.clone(), id));

            let activated = self.activated.entry(id).or_insert(false);
            if !*activated {
                *activated = true;
                // The node takes part in its group from now on.
                for entry in barriers.values_mut() {
                    if let Some(node) = entry.nodes.get_mut(&id) {
                        if node.state == State::Inactive {
                            node.state = State::Active;
                        }
                    }
                }
            }
        }

        if let Some(entry) = barriers.get_mut(&number) {
            entry.peers.announced = false;
            entry.remotes.announced = false;
        }
        for (&other, entry) in barriers.iter_mut() {
            if other != number {
                entry.reconcile(group, other, out);
            }
        }
        true
    }

    /// Removes a node, releasing whoever was waiting only for it.
    pub(crate) fn unregister_node(&mut self, id: NodeId) -> Vec<Outbound> {
        let mut touched = Vec::new();
        for (&group, barriers) in &mut self.groups {
            for (&number, entry) in barriers {
                if entry.nodes.remove(&id).is_some() {
                    touched.push((group, number));
                }
            }
        }
        self.activated.remove(&id);
        touched
            .into_iter()
            .flat_map(|(group, number)| self.check_update(group, number))
            .collect()
    }

    /// What a server that has just connected needs to know.
    pub(crate) fn sync_link(&self, kind: LinkKind, id: NodeId) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (&group, barriers) in &self.groups {
            for (&number, entry) in barriers {
                if !entry.global || entry.relevant(kind).next().is_none() {
                    continue;
                }
                let active = entry.relevant(kind).any(|n| n.state != State::Inactive);
                let create = message(BarrierKind::CreateGlobal, group, number, active);
                out.push(Outbound::to_node(BARRIER_PLUGIN_ID, create, id));
                let side = match kind {
                    LinkKind::Peer => &entry.peers,
                    LinkKind::Remote => &entry.remotes,
                };
                if side.announced {
                    let sync = message(BarrierKind::Sync, group, number, false);
                    out.push(Outbound::to_node(BARRIER_PLUGIN_ID, sync, id));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owners::test_support::*;
    use std::collections::BTreeSet;

    const A: NodeId = 0x10001;
    const B: NodeId = 0x10002;
    const PEER: NodeId = 0x20000;
    const OTHER_PEER: NodeId = 0x30000;

    fn packet(kind: BarrierKind, active: bool) -> Vec<u8> {
        message(kind, 0, 1, active)
    }

    fn create(server: &mut BarrierServer, node: NodeId, active: bool) {
        let msg = packet(BarrierKind::CreateLocal, active);
        assert!(server.on_packet(client(node), &msg).unwrap().is_empty());
    }

    fn create_global(server: &mut BarrierServer, source: Source, active: bool) -> Vec<Outbound> {
        let msg = packet(BarrierKind::CreateGlobal, active);
        server.on_packet(source, &msg).unwrap()
    }

    fn sync(server: &mut BarrierServer, node: NodeId) -> BTreeSet<NodeId> {
        released(&sync_out(server, client(node)))
    }

    fn sync_out(server: &mut BarrierServer, source: Source) -> Vec<Outbound> {
        let msg = packet(BarrierKind::Sync, false);
        server.on_packet(source, &msg).unwrap()
    }

    fn released(out: &[Outbound]) -> BTreeSet<NodeId> {
        out.iter()
            .filter(|o| BarrierMsg::decode(&o.payload).unwrap().kind == BarrierKind::GetOut)
            .flat_map(|o| o.targets.iter().copied())
            .collect()
    }

    fn kinds_sent(out: &[Outbound]) -> Vec<BarrierKind> {
        out.iter()
            .map(|o| BarrierMsg::decode(&o.payload).unwrap().kind)
            .collect()
    }

    #[test]
    fn releases_when_all_active_nodes_have_synced() {
        let mut server = BarrierServer::default();
        create(&mut server, A, true);
        create(&mut server, B, true);
        assert!(sync(&mut server, A).is_empty());
        assert_eq!(sync(&mut server, B), BTreeSet::from([A, B]));
        // And it can be used again.
        assert!(sync(&mut server, B).is_empty());
        assert_eq!(sync(&mut server, A), BTreeSet::from([A, B]));
    }

    #[test]
    fn inactive_node_does_not_hold_others_back() {
        let mut server = BarrierServer::default();
        create(&mut server, A, true);
        create(&mut server, B, false);
        assert_eq!(sync(&mut server, A), BTreeSet::from([A]));
    }

    #[test]
    fn inactive_node_holds_others_back_after_passing_a_sync() {
        let mut server = BarrierServer::default();
        create(&mut server, A, true);
        create(&mut server, B, false);
        // Its first sync waits for the active node, like any other.
        assert!(sync(&mut server, B).is_empty());
        assert_eq!(sync(&mut server, A), BTreeSet::from([A, B]));
        // Now it counts: A has to wait for it.
        assert_eq!(server.activated.get(&B), Some(&true));
        assert!(sync(&mut server, A).is_empty());
        assert_eq!(sync(&mut server, B), BTreeSet::from([A, B]));
    }

    #[test]
    fn leaving_node_releases_the_others() {
        let mut server = BarrierServer::default();
        create(&mut server, A, true);
        create(&mut server, B, true);
        assert!(sync(&mut server, A).is_empty());
        let out = server.unregister_node(B);
        assert_eq!(released(&out), BTreeSet::from([A]));
    }

    #[test]
    fn deleting_a_barrier_releases_the_others() {
        let mut server = BarrierServer::default();
        create(&mut server, A, true);
        create(&mut server, B, true);
        assert!(sync(&mut server, A).is_empty());
        let delete = packet(BarrierKind::Delete, false);
        let out = server.on_packet(client(B), &delete).unwrap();
        assert_eq!(released(&out), BTreeSet::from([A]));
    }

    #[test]
    fn last_node_leaving_removes_the_group() {
        let mut server = BarrierServer::default();
        create(&mut server, A, true);
        assert!(server.unregister_node(A).is_empty());
        assert!(server.groups.is_empty());
    }

    #[test]
    fn sync_of_unknown_barrier_is_ignored() {
        let mut server = BarrierServer::default();
        assert!(sync(&mut server, A).is_empty());
    }

    #[test]
    fn disabled_node_does_not_hold_others_back() {
        let mut server = BarrierServer::default();
        create(&mut server, A, true);
        create(&mut server, B, true);
        let disable = packet(BarrierKind::Disable, false);
        server.on_packet(client(B), &disable).unwrap();
        assert_eq!(sync(&mut server, A), BTreeSet::from([A]));
        // Its own sync returns right away, without changing anything.
        assert_eq!(sync(&mut server, B), BTreeSet::from([B]));

        let enable = packet(BarrierKind::Enable, false);
        server.on_packet(client(B), &enable).unwrap();
        assert!(sync(&mut server, A).is_empty());
        assert_eq!(sync(&mut server, B), BTreeSet::from([A, B]));
    }

    #[test]
    fn disabling_releases_others_and_a_sync_in_progress() {
        let mut server = BarrierServer::default();
        create(&mut server, A, true);
        create(&mut server, B, true);
        assert!(sync(&mut server, A).is_empty());
        let disable = packet(BarrierKind::Disable, false);
        let out = server.on_packet(client(B), &disable).unwrap();
        assert_eq!(released(&out), BTreeSet::from([A]));

        // Disabling while it is the one waiting.
        let mut server = BarrierServer::default();
        create(&mut server, A, true);
        create(&mut server, B, true);
        assert!(sync(&mut server, A).is_empty());
        let out = server.on_packet(client(A), &disable).unwrap();
        assert_eq!(released(&out), BTreeSet::from([A]));
    }

    #[test]
    fn disabled_side_is_removed_for_other_servers() {
        let mut server = BarrierServer::default();
        create_global(&mut server, client(A), true);
        let disable = packet(BarrierKind::Disable, false);
        let out = server.on_packet(client(A), &disable).unwrap();
        assert_eq!(kinds_sent(&out), [BarrierKind::Delete, BarrierKind::Delete]);

        let enable = packet(BarrierKind::Enable, false);
        let out = server.on_packet(client(A), &enable).unwrap();
        assert_eq!(
            kinds_sent(&out),
            [BarrierKind::CreateGlobal, BarrierKind::CreateGlobal]
        );
    }

    #[test]
    fn local_barrier_tells_no_server() {
        let mut server = BarrierServer::default();
        create(&mut server, A, true);
        assert!(sync_out(&mut server, client(A))
            .iter()
            .all(|o| o.links == Links::NONE));
    }

    #[test]
    fn global_barrier_waits_for_the_peer() {
        let mut server = BarrierServer::default();
        let out = create_global(&mut server, client(A), true);
        assert_eq!(out.len(), 2, "tells peers and remote peers");
        create_global(&mut server, peer(PEER, PEER), true);

        // A is ready: the peer is told, but A has to wait for it. Other groups
        // are not told yet, because that peer is on their side of this server.
        let out = sync_out(&mut server, client(A));
        assert_eq!(kinds_sent(&out), [BarrierKind::Sync]);
        assert_eq!(out[0].links, Links::only(LinkKind::Peer));
        assert!(released(&out).is_empty());

        let out = sync_out(&mut server, peer(PEER, PEER));
        assert_eq!(released(&out), BTreeSet::from([A]));
    }

    #[test]
    fn peer_ahead_by_one_round_is_not_forgotten() {
        let mut server = BarrierServer::default();
        create_global(&mut server, client(A), true);
        create_global(&mut server, peer(PEER, PEER), true);
        create_global(&mut server, peer(OTHER_PEER, OTHER_PEER), true);

        // PEER finishes round 1 and reaches round 2 before OTHER_PEER is heard.
        sync_out(&mut server, peer(PEER, PEER));
        sync_out(&mut server, peer(PEER, PEER));
        assert!(released(&sync_out(&mut server, client(A))).is_empty());

        let out = sync_out(&mut server, peer(OTHER_PEER, OTHER_PEER));
        assert_eq!(released(&out), BTreeSet::from([A]));

        // Round 2: PEER is already there, so it only depends on the other two.
        assert!(released(&sync_out(&mut server, client(A))).is_empty());
        let out = sync_out(&mut server, peer(OTHER_PEER, OTHER_PEER));
        assert_eq!(released(&out), BTreeSet::from([A]));
    }

    #[test]
    fn peers_do_not_relay_for_other_peers() {
        let mut server = BarrierServer::default();
        create_global(&mut server, peer(PEER, PEER), true);
        let out = sync_out(&mut server, peer(PEER, PEER));
        assert!(out.iter().all(|o| !o.links.peers));
    }

    #[test]
    fn inactive_side_does_not_hold_other_servers_back() {
        let mut server = BarrierServer::default();
        create_global(&mut server, client(A), true);
        create_global(&mut server, peer(PEER, PEER), false);
        assert_eq!(sync(&mut server, A), BTreeSet::from([A]));

        // The peer's side becomes active and says so.
        create_global(&mut server, peer(PEER, PEER), true);
        assert!(sync(&mut server, A).is_empty());
    }

    #[test]
    fn side_becoming_active_is_announced_once() {
        let mut server = BarrierServer::default();
        let out = create_global(&mut server, client(A), false);
        assert!(out
            .iter()
            .all(|o| !BarrierMsg::decode(&o.payload).unwrap().active));

        // It passes the barrier, which makes it active.
        let out = sync_out(&mut server, client(A));
        assert_eq!(released(&out), BTreeSet::from([A]));
        let becoming_active = out
            .iter()
            .filter(|o| {
                let msg = BarrierMsg::decode(&o.payload).unwrap();
                msg.kind == BarrierKind::CreateGlobal && msg.active
            })
            .count();
        assert_eq!(becoming_active, 2);
    }

    #[test]
    fn peer_leaving_releases_the_barrier() {
        let mut server = BarrierServer::default();
        create_global(&mut server, client(A), true);
        create_global(&mut server, peer(PEER, PEER), true);
        assert!(released(&sync_out(&mut server, client(A))).is_empty());
        let out = server.unregister_node(PEER);
        assert_eq!(released(&out), BTreeSet::from([A]));
    }

    #[test]
    fn last_owner_on_a_side_is_announced_as_deleted() {
        let mut server = BarrierServer::default();
        create_global(&mut server, client(A), true);
        let delete = packet(BarrierKind::Delete, false);
        let out = server.on_packet(client(A), &delete).unwrap();
        assert_eq!(kinds_sent(&out), [BarrierKind::Delete, BarrierKind::Delete]);
        assert!(server.groups.is_empty());
    }

    #[test]
    fn new_link_learns_about_a_barrier_already_at_sync() {
        let mut server = BarrierServer::default();
        create_global(&mut server, client(A), true);
        create_global(&mut server, peer(PEER, PEER), true);
        sync_out(&mut server, client(A));

        let out = server.sync_link(LinkKind::Peer, OTHER_PEER);
        assert_eq!(
            kinds_sent(&out),
            [BarrierKind::CreateGlobal, BarrierKind::Sync]
        );
    }

    #[test]
    fn gateway_connects_the_two_levels() {
        const GATEWAY: NodeId = 0x40000;
        let mut server = BarrierServer::default();
        // Seen from a gateway: one peer of its group, one gateway of another.
        create_global(&mut server, peer(PEER, PEER), true);
        create_global(&mut server, remote(GATEWAY, GATEWAY), true);

        // The peer is ready: only the other group needs to know.
        let out = sync_out(&mut server, peer(PEER, PEER));
        assert_eq!(kinds_sent(&out), [BarrierKind::Sync]);
        assert_eq!(out[0].links, Links::only(LinkKind::Remote));

        // The other group is ready: only the peer still needs to know.
        let out = sync_out(&mut server, remote(GATEWAY, GATEWAY));
        assert!(out.iter().any(|o| o.links == Links::only(LinkKind::Peer)
            && BarrierMsg::decode(&o.payload).unwrap().kind == BarrierKind::Sync));
    }
}
