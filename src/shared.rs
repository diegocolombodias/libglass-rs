use crate::client::{ClientCore, GlassClient};
use crate::error::{Error, Result};
use crate::outbound::Outbound;
use crate::owners::{Links, Owners, Relation, Source};
use crate::protocol::SharedMsg;
use crate::types::*;
use crate::wire::Wire;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long to wait for the server's answer to [`Shared::get_update`].
const SYNC_TIMEOUT: Duration = Duration::from_secs(10);

/// When [`Shared::send_update`] actually sends the value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateMode {
    /// Never.
    Never,
    /// Until the next [`Shared::get_update`].
    NextOnly,
    /// If the value has changed.
    Changed,
    /// Always. This is the default.
    Forced,
}

/// The last value received from the server.
#[derive(Default)]
pub(crate) struct SharedSlot {
    pub(crate) data: Vec<u8>,
    pub(crate) generation: u64,
}

struct Local<T> {
    value: T,
    changed: bool,
    mode: UpdateMode,
}

/// A variable shared among clients through the server.
///
/// Changes are explicit: [`set`](Self::set) changes the local value,
/// [`send_update`](Self::send_update) publishes it, and
/// [`get_update`](Self::get_update) fetches the newest published value.
///
/// As in the C++ library, once a client publishes a value, clients with higher
/// ids cannot overwrite it until that client calls `get_update`. Lower ids win.
pub struct Shared<T> {
    core: Arc<ClientCore>,
    name: String,
    local: Mutex<Local<T>>,
}

impl<T: Wire + Clone> Shared<T> {
    /// Creates a variable. Each client can only have one with a given name.
    pub fn new(client: &GlassClient, name: &str, initial: T, mode: Mode) -> Result<Shared<T>> {
        if name.is_empty() || name.contains('\0') {
            return Err(Error::InvalidName);
        }
        let core = client.core().clone();
        {
            let mut state = core.state.lock().unwrap();
            if state.shared.contains_key(name) {
                return Err(Error::AlreadyExists(name.to_owned()));
            }
            state.shared.insert(name.to_owned(), SharedSlot::default());
        }

        let msg = match mode {
            Mode::Local => SharedMsg::CreateLocal(name.to_owned()),
            Mode::Global => SharedMsg::CreateGlobal(name.to_owned()),
        };
        if let Err(e) = core.register(SHARED_PLUGIN_ID, name, msg.encode()) {
            core.state.lock().unwrap().shared.remove(name);
            return Err(e);
        }
        Ok(Shared {
            core,
            name: name.to_owned(),
            local: Mutex::new(Local {
                value: initial,
                changed: false,
                mode: UpdateMode::Forced,
            }),
        })
    }

    /// The local value.
    pub fn get(&self) -> T {
        self.local.lock().unwrap().value.clone()
    }

    /// Changes the local value. Other clients only see it after
    /// [`send_update`](Self::send_update).
    pub fn set(&self, value: T) {
        let mut local = self.local.lock().unwrap();
        local.value = value;
        local.changed = true;
    }

    /// Has the local value changed since the last [`get_update`](Self::get_update)?
    pub fn has_changed(&self) -> bool {
        self.local.lock().unwrap().changed
    }

    pub fn update_mode(&self) -> UpdateMode {
        self.local.lock().unwrap().mode
    }

    pub fn set_update_mode(&self, mode: UpdateMode) {
        self.local.lock().unwrap().mode = mode;
    }

    /// Publishes the local value, according to the update mode.
    pub fn send_update(&self) -> Result<()> {
        let bytes = {
            let local = self.local.lock().unwrap();
            if local.mode == UpdateMode::Never {
                return Ok(());
            }
            if !local.changed && local.mode != UpdateMode::Forced {
                return Ok(());
            }
            local.value.pack()
        };
        let msg = SharedMsg::Update(self.name.clone(), bytes);
        self.core.send_to_server(SHARED_PLUGIN_ID, msg.encode())
    }

    /// Fetches the newest published value, waiting for the server's answer. If
    /// nothing has been published, the local value is kept.
    pub fn get_update(&self) -> Result<()> {
        let mut state = self.core.state.lock().unwrap();
        let generation = state
            .shared
            .get(&self.name)
            .ok_or(Error::Disconnected)?
            .generation;
        drop(state);

        let msg = SharedMsg::Sync(self.name.clone());
        self.core.send_to_server(SHARED_PLUGIN_ID, msg.encode())?;

        let deadline = Instant::now() + SYNC_TIMEOUT;
        state = self.core.state.lock().unwrap();
        let data = loop {
            let slot = state.shared.get(&self.name).ok_or(Error::Disconnected)?;
            if slot.generation > generation {
                break slot.data.clone();
            }
            if state.closed {
                return Err(Error::Disconnected);
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(Error::Timeout)?;
            state = self.core.changed.wait_timeout(state, remaining).unwrap().0;
        };
        drop(state);

        let mut local = self.local.lock().unwrap();
        if local.mode == UpdateMode::NextOnly {
            local.mode = UpdateMode::Never;
        }
        if !data.is_empty() {
            local.value = T::unpack(&data).ok_or(Error::Decode)?;
        }
        local.changed = false;
        Ok(())
    }
}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        self.core.state.lock().unwrap().shared.remove(&self.name);
        let msg = SharedMsg::Delete(self.name.clone());
        self.core.unregister(SHARED_PLUGIN_ID, &self.name);
        let _ = self.core.send_to_server(SHARED_PLUGIN_ID, msg.encode());
    }
}

#[derive(Default)]
struct Var {
    owners: Owners,
    last_changed: NodeId,
    /// The client or server that the update of `last_changed` came through.
    holder_link: NodeId,
    data: Vec<u8>,
}

/// Server side of the shared plugin: the published value of each variable.
///
/// Updates carry the id of the client that made them, so every server applies
/// the same rule and they all end up with the same value.
#[derive(Default)]
pub(crate) struct SharedServer {
    vars: BTreeMap<String, Var>,
}

fn reconcile(name: &str, var: &mut Var) -> Vec<Outbound> {
    let create = SharedMsg::CreateGlobal(name.to_owned()).encode();
    let delete = SharedMsg::Delete(name.to_owned()).encode();
    var.owners.reconcile(SHARED_PLUGIN_ID, &create, &delete)
}

/// Tells other servers that `origin` no longer holds the variable.
fn forward_sync(name: &str, origin: NodeId, links: Links, source: NodeId) -> Outbound {
    let msg = SharedMsg::Sync(name.to_owned()).encode();
    let mut out = Outbound::to_links(SHARED_PLUGIN_ID, msg, links);
    out.from = Some(origin);
    out.black.insert(source);
    out
}

/// A node has left: if the variable was held by it, or by anyone that this server
/// reached through it, let it go. A client of another group comes through a gateway,
/// with an id that does not tell which one.
fn release(name: &str, var: &mut Var, id: NodeId, relation: Relation) -> Option<Outbound> {
    let holder = var.last_changed;
    if holder == NOBODY_ID || var.holder_link != id {
        return None;
    }
    var.last_changed = NOBODY_ID;
    let links = var.owners.forward_links(relation);
    Some(forward_sync(name, holder, links, id))
}

impl SharedServer {
    pub(crate) fn on_packet(&mut self, source: Source, payload: &[u8]) -> Result<Vec<Outbound>> {
        match SharedMsg::decode(payload)? {
            SharedMsg::CreateLocal(name) => Ok(self.create(source, name, false)),
            SharedMsg::CreateGlobal(name) => Ok(self.create(source, name, true)),
            SharedMsg::Update(name, data) => {
                let Some(var) = self.vars.get_mut(&name) else {
                    log::warn!("update of inexistent variable {name}");
                    return Ok(vec![]);
                };
                let rejected = if source.origin == NOBODY_ID {
                    // The state of a server that has just connected. It only
                    // fills a gap, and must not replace anything newer.
                    var.last_changed != NOBODY_ID || !var.data.is_empty()
                } else {
                    var.last_changed != NOBODY_ID && var.last_changed < source.origin
                };
                if rejected {
                    return Ok(vec![]);
                }
                var.data = data;
                var.last_changed = source.origin;
                var.holder_link = source.id;
                let mut out = Outbound::to_links(
                    SHARED_PLUGIN_ID,
                    payload.to_vec(),
                    var.owners.forward_links(source.relation),
                );
                out.black.insert(source.id);
                out.white = var.owners.ids();
                out.from = Some(source.origin);
                Ok(vec![out])
            }
            SharedMsg::Delete(name) => {
                let Some(var) = self.vars.get_mut(&name) else {
                    log::warn!("inexistent variable {name} deleted");
                    return Ok(vec![]);
                };
                if !var.owners.remove(source.id) {
                    return Ok(vec![]);
                }
                let mut out: Vec<Outbound> = release(&name, var, source.id, source.relation)
                    .into_iter()
                    .collect();
                out.extend(reconcile(&name, var));
                if var.owners.is_empty() {
                    self.vars.remove(&name);
                }
                Ok(out)
            }
            SharedMsg::Sync(name) => {
                let Some(var) = self.vars.get_mut(&name) else {
                    log::warn!("sync of inexistent variable {name}");
                    return Ok(vec![]);
                };
                let mut out = Vec::new();
                if var.last_changed == source.origin {
                    var.last_changed = NOBODY_ID;
                    let links = var.owners.forward_links(source.relation);
                    out.push(forward_sync(&name, source.origin, links, source.id));
                }
                if source.relation == Relation::Client {
                    let answer = SharedMsg::Changed(name, var.data.clone());
                    out.push(Outbound::to_node(
                        SHARED_PLUGIN_ID,
                        answer.encode(),
                        source.id,
                    ));
                }
                Ok(out)
            }
            SharedMsg::Changed(..) => Err(Error::InvalidPacket("clients cannot send changed")),
        }
    }

    fn create(&mut self, source: Source, name: String, global: bool) -> Vec<Outbound> {
        let var = self.vars.entry(name.clone()).or_default();
        var.owners.insert(source, global);
        reconcile(&name, var)
    }

    pub(crate) fn unregister_node(&mut self, id: NodeId, relation: Relation) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (name, var) in &mut self.vars {
            out.extend(release(name, var, id, relation));
            if var.owners.remove(id) {
                out.extend(reconcile(name, var));
            }
        }
        self.vars.retain(|_, var| !var.owners.is_empty());
        out
    }

    /// What a server that has just connected needs to know, including the
    /// current values. They carry no sender, so that they hold nothing there.
    pub(crate) fn sync_link(&self, kind: LinkKind, id: NodeId) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (name, var) in &self.vars {
            if !var.owners.concerns_new_link(kind) {
                continue;
            }
            let create = SharedMsg::CreateGlobal(name.clone()).encode();
            out.push(Outbound::to_node(SHARED_PLUGIN_ID, create, id));
            if !var.data.is_empty() {
                let update = SharedMsg::Update(name.clone(), var.data.clone()).encode();
                let mut update = Outbound::to_node(SHARED_PLUGIN_ID, update, id);
                update.from = Some(NOBODY_ID);
                out.push(update);
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

    const LOW: NodeId = 0x10001;
    const HIGH: NodeId = 0x10002;
    const PEER: NodeId = 0x20000;
    const PEER_CLIENT: NodeId = 0x20001;

    fn server_with_owners() -> SharedServer {
        let mut server = SharedServer::default();
        for node in [LOW, HIGH] {
            let create = SharedMsg::CreateLocal("x".into()).encode();
            server.on_packet(client(node), &create).unwrap();
        }
        server
    }

    fn global_server() -> SharedServer {
        let mut server = SharedServer::default();
        let create = SharedMsg::CreateGlobal("x".into()).encode();
        server.on_packet(client(LOW), &create).unwrap();
        server.on_packet(peer(PEER, PEER), &create).unwrap();
        server
    }

    fn update(server: &mut SharedServer, node: NodeId, value: u8) {
        let msg = SharedMsg::Update("x".into(), vec![value]).encode();
        server.on_packet(client(node), &msg).unwrap();
    }

    fn sync(server: &mut SharedServer, node: NodeId) -> Vec<u8> {
        let msg = SharedMsg::Sync("x".into()).encode();
        let out = server.on_packet(client(node), &msg).unwrap();
        let answer = out.last().unwrap();
        assert_eq!(answer.targets, BTreeSet::from([node]));
        match SharedMsg::decode(&answer.payload).unwrap() {
            SharedMsg::Changed(_, data) => data,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn lower_id_keeps_its_update_until_it_syncs() {
        let mut server = server_with_owners();
        update(&mut server, LOW, 1);
        update(&mut server, HIGH, 2);
        assert_eq!(sync(&mut server, HIGH), [1]);

        assert_eq!(sync(&mut server, LOW), [1]);
        update(&mut server, HIGH, 2);
        assert_eq!(sync(&mut server, LOW), [2]);
    }

    #[test]
    fn lower_id_overwrites_higher_id() {
        let mut server = server_with_owners();
        update(&mut server, HIGH, 2);
        update(&mut server, LOW, 1);
        assert_eq!(sync(&mut server, HIGH), [1]);
    }

    #[test]
    fn leaving_releases_the_lock_on_updates() {
        let mut server = server_with_owners();
        update(&mut server, LOW, 1);
        server.unregister_node(LOW, Relation::Client);
        update(&mut server, HIGH, 2);
        assert_eq!(sync(&mut server, HIGH), [2]);
    }

    #[test]
    fn variable_disappears_with_its_last_owner() {
        let mut server = server_with_owners();
        let delete = SharedMsg::Delete("x".into()).encode();
        server.on_packet(client(LOW), &delete).unwrap();
        server.on_packet(client(HIGH), &delete).unwrap();
        assert!(server.vars.is_empty());
    }

    #[test]
    fn sync_of_unknown_variable_gets_no_answer() {
        let mut server = SharedServer::default();
        let msg = SharedMsg::Sync("nope".into()).encode();
        assert!(server.on_packet(client(LOW), &msg).unwrap().is_empty());
    }

    #[test]
    fn local_updates_are_not_forwarded() {
        let mut server = server_with_owners();
        let msg = SharedMsg::Update("x".into(), vec![1]).encode();
        let out = server.on_packet(client(LOW), &msg).unwrap();
        assert_eq!(out[0].links, Links::NONE);
    }

    #[test]
    fn updates_are_forwarded_with_the_id_of_the_client() {
        let mut server = global_server();
        let msg = SharedMsg::Update("x".into(), vec![1]).encode();
        let out = server.on_packet(client(LOW), &msg).unwrap();
        assert_eq!(out[0].links, Links::ALL);
        assert_eq!(out[0].from, Some(LOW));
    }

    #[test]
    fn servers_agree_on_concurrent_updates() {
        // Each server gets the updates in a different order.
        let low = SharedMsg::Update("x".into(), vec![1]).encode();
        let high = SharedMsg::Update("x".into(), vec![2]).encode();

        let mut here = global_server();
        here.on_packet(client(LOW), &low).unwrap();
        here.on_packet(peer(PEER, PEER_CLIENT), &high).unwrap();

        let mut there = global_server();
        there.on_packet(peer(PEER, PEER_CLIENT), &high).unwrap();
        there.on_packet(client(LOW), &low).unwrap();

        assert_eq!(here.vars["x"].data, [1]);
        assert_eq!(there.vars["x"].data, [1]);
    }

    #[test]
    fn sync_releases_the_lock_on_other_servers_too() {
        let mut server = global_server();
        let msg = SharedMsg::Update("x".into(), vec![1]).encode();
        server.on_packet(client(LOW), &msg).unwrap();

        let sync = SharedMsg::Sync("x".into()).encode();
        let out = server.on_packet(client(LOW), &sync).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].from, Some(LOW));
        assert_eq!(out[0].links, Links::ALL);

        // On the other side, it is not answered: it only clears the lock.
        let mut there = global_server();
        let update = SharedMsg::Update("x".into(), vec![1]).encode();
        there.on_packet(peer(PEER, PEER_CLIENT), &update).unwrap();
        assert_eq!(there.vars["x"].last_changed, PEER_CLIENT);
        let out = there.on_packet(peer(PEER, PEER_CLIENT), &sync).unwrap();
        assert_eq!(there.vars["x"].last_changed, NOBODY_ID);
        assert!(out.iter().all(|o| o.targets.is_empty()));
    }

    #[test]
    fn a_peer_leaving_releases_the_locks_of_its_clients() {
        let mut server = global_server();
        let update = SharedMsg::Update("x".into(), vec![1]).encode();
        server.on_packet(peer(PEER, PEER_CLIENT), &update).unwrap();
        server.unregister_node(PEER, Relation::Peer);
        assert_eq!(server.vars["x"].last_changed, NOBODY_ID);
    }

    const GATEWAY: NodeId = 0x30000;
    /// A client of a peer of another group, reached through `GATEWAY`.
    const OTHER_GROUP_CLIENT: NodeId = 0x40001;

    fn server_with_a_gateway() -> SharedServer {
        let mut server = global_server();
        let create = SharedMsg::CreateGlobal("x".into()).encode();
        server.on_packet(remote(GATEWAY, GATEWAY), &create).unwrap();
        server
    }

    #[test]
    fn losing_a_gateway_releases_clients_of_other_groups() {
        let mut server = server_with_a_gateway();
        let msg = SharedMsg::Update("x".into(), vec![1]).encode();
        server
            .on_packet(remote(GATEWAY, OTHER_GROUP_CLIENT), &msg)
            .unwrap();
        assert_eq!(server.vars["x"].last_changed, OTHER_GROUP_CLIENT);

        let out = server.unregister_node(GATEWAY, Relation::Remote);
        assert_eq!(server.vars["x"].last_changed, NOBODY_ID);
        // The peers of this group are told too.
        let released = out
            .iter()
            .find(|o| o.from == Some(OTHER_GROUP_CLIENT))
            .expect("a sync for the other servers");
        assert_eq!(released.links, Links::only(LinkKind::Peer));
        update(&mut server, HIGH, 2);
        assert_eq!(sync(&mut server, HIGH), [2]);
    }

    #[test]
    fn losing_one_link_keeps_holders_reached_through_another() {
        let mut server = server_with_a_gateway();
        let msg = SharedMsg::Update("x".into(), vec![1]).encode();
        server.on_packet(peer(PEER, PEER_CLIENT), &msg).unwrap();
        server.unregister_node(GATEWAY, Relation::Remote);
        assert_eq!(server.vars["x"].last_changed, PEER_CLIENT);
    }

    #[test]
    fn state_of_a_new_link_does_not_replace_anything() {
        let state = SharedMsg::Update("x".into(), vec![9]).encode();
        let new_link = peer(PEER, NOBODY_ID);

        // Some client holds the variable.
        let mut server = global_server();
        update(&mut server, LOW, 1);
        server.on_packet(new_link, &state).unwrap();
        assert_eq!(server.vars["x"].data, [1]);
        assert_eq!(server.vars["x"].last_changed, LOW);

        // Nobody holds it anymore, but its value is newer.
        sync(&mut server, LOW);
        server.on_packet(new_link, &state).unwrap();
        assert_eq!(server.vars["x"].data, [1]);
    }

    #[test]
    fn new_link_gets_the_value_without_a_lock() {
        let mut server = global_server();
        let msg = SharedMsg::Update("x".into(), vec![9]).encode();
        server.on_packet(client(LOW), &msg).unwrap();

        let out = server.sync_link(LinkKind::Peer, PEER);
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].from, Some(NOBODY_ID));

        let mut there = SharedServer::default();
        there.on_packet(peer(PEER, PEER), &out[0].payload).unwrap();
        there
            .on_packet(peer(PEER, NOBODY_ID), &out[1].payload)
            .unwrap();
        assert_eq!(there.vars["x"].data, [9]);
        assert_eq!(there.vars["x"].last_changed, NOBODY_ID);
    }
}
