use crate::client::{ClientCore, GlassClient};
use crate::error::{Error, Result};
use crate::outbound::Outbound;
use crate::owners::{Owners, Relation, Source};
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
/// The last update to reach a server wins, whichever client made it. With several
/// servers, the highest version wins, and updates made at the same time on different
/// servers are decided by the lower id of the client, the same way in all of them.
/// Unlike the C++ library, a client does not hold the variable until `get_update`.
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
        let msg = SharedMsg::Update(self.name.clone(), 0, bytes);
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
    data: Vec<u8>,
    /// Numbered by the servers: each one gives an update from its clients a version
    /// above any it has seen. 0 means that nothing was published.
    version: u64,
    /// The client that made the current value, which breaks ties between versions.
    writer: NodeId,
}

impl Var {
    /// Whether an update from another server replaces the current value. Every server
    /// compares in the same way, so they all keep the same value.
    fn is_older_than(&self, version: u64, writer: NodeId) -> bool {
        self.version == 0
            || version > self.version
            || (version == self.version && writer < self.writer)
    }
}

/// Server side of the shared plugin: the published value of each variable.
///
/// The last update to reach a server wins there. Between servers, updates carry their
/// version, and the highest one wins; for updates made at the same time on different
/// servers, which get the same version, the lower id of the client wins.
#[derive(Default)]
pub(crate) struct SharedServer {
    vars: BTreeMap<String, Var>,
}

fn reconcile(name: &str, var: &mut Var) -> Vec<Outbound> {
    let create = SharedMsg::CreateGlobal(name.to_owned()).encode();
    let delete = SharedMsg::Delete(name.to_owned()).encode();
    var.owners.reconcile(SHARED_PLUGIN_ID, &create, &delete)
}

impl SharedServer {
    pub(crate) fn on_packet(&mut self, source: Source, payload: &[u8]) -> Result<Vec<Outbound>> {
        match SharedMsg::decode(payload)? {
            SharedMsg::CreateLocal(name) => Ok(self.create(source, name, false)),
            SharedMsg::CreateGlobal(name) => Ok(self.create(source, name, true)),
            SharedMsg::Update(name, version, data) => {
                let Some(var) = self.vars.get_mut(&name) else {
                    log::warn!("update of inexistent variable {name}");
                    return Ok(vec![]);
                };
                let version = match source.relation {
                    Relation::Client => var.version + 1,
                    _ if var.is_older_than(version, source.origin) => version,
                    _ => return Ok(vec![]),
                };
                var.data = data;
                var.version = version;
                var.writer = source.origin;
                let update = SharedMsg::Update(name, version, var.data.clone());
                let mut out = Outbound::to_links(
                    SHARED_PLUGIN_ID,
                    update.encode(),
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
                let out = reconcile(&name, var);
                if var.owners.is_empty() {
                    self.vars.remove(&name);
                }
                Ok(out)
            }
            SharedMsg::Sync(name) => {
                let Some(var) = self.vars.get(&name) else {
                    log::warn!("sync of inexistent variable {name}");
                    return Ok(vec![]);
                };
                if source.relation != Relation::Client {
                    return Ok(vec![]);
                }
                let answer = SharedMsg::Changed(name, var.data.clone());
                Ok(vec![Outbound::to_node(
                    SHARED_PLUGIN_ID,
                    answer.encode(),
                    source.id,
                )])
            }
            SharedMsg::Changed(..) => Err(Error::InvalidPacket("clients cannot send changed")),
        }
    }

    fn create(&mut self, source: Source, name: String, global: bool) -> Vec<Outbound> {
        let var = self.vars.entry(name.clone()).or_default();
        var.owners.insert(source, global);
        reconcile(&name, var)
    }

    pub(crate) fn unregister_node(&mut self, id: NodeId) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (name, var) in &mut self.vars {
            if var.owners.remove(id) {
                out.extend(reconcile(name, var));
            }
        }
        self.vars.retain(|_, var| !var.owners.is_empty());
        out
    }

    /// What a server that has just connected needs to know, including the current
    /// values, with their versions and writers, so that it keeps the newest one.
    pub(crate) fn sync_link(&self, kind: LinkKind, id: NodeId) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (name, var) in &self.vars {
            if !var.owners.concerns_new_link(kind) {
                continue;
            }
            let create = SharedMsg::CreateGlobal(name.clone()).encode();
            out.push(Outbound::to_node(SHARED_PLUGIN_ID, create, id));
            if var.version != 0 {
                let update = SharedMsg::Update(name.clone(), var.version, var.data.clone());
                let mut update = Outbound::to_node(SHARED_PLUGIN_ID, update.encode(), id);
                update.from = Some(var.writer);
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
    use crate::owners::Links;
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

    /// An update from a client, which sends no version.
    fn update(server: &mut SharedServer, node: NodeId, value: u8) -> Vec<Outbound> {
        let msg = SharedMsg::Update("x".into(), 0, vec![value]).encode();
        server.on_packet(client(node), &msg).unwrap()
    }

    /// An update forwarded by another server, made by `writer` with `version`.
    fn from_peer(server: &mut SharedServer, writer: NodeId, version: u64, value: u8) {
        let msg = SharedMsg::Update("x".into(), version, vec![value]).encode();
        server.on_packet(peer(PEER, writer), &msg).unwrap();
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
    fn the_last_update_wins_whatever_the_ids() {
        let mut server = server_with_owners();
        update(&mut server, LOW, 1);
        update(&mut server, HIGH, 2);
        assert_eq!(sync(&mut server, LOW), [2]);
        update(&mut server, LOW, 3);
        assert_eq!(sync(&mut server, HIGH), [3]);
    }

    #[test]
    fn nobody_holds_the_variable() {
        // The one that wrote first never syncs, and still does not stop the others.
        let mut server = server_with_owners();
        update(&mut server, LOW, 1);
        update(&mut server, HIGH, 2);
        update(&mut server, HIGH, 3);
        assert_eq!(sync(&mut server, HIGH), [3]);
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
        let out = update(&mut server, LOW, 1);
        assert_eq!(out[0].links, Links::NONE);
    }

    #[test]
    fn updates_are_forwarded_with_their_version_and_writer() {
        let mut server = global_server();
        from_peer(&mut server, PEER_CLIENT, 4, 1);
        let out = update(&mut server, LOW, 2);
        assert_eq!(out[0].links, Links::ALL);
        assert_eq!(out[0].from, Some(LOW));
        let forwarded = SharedMsg::decode(&out[0].payload).unwrap();
        assert_eq!(forwarded, SharedMsg::Update("x".into(), 5, vec![2]));
    }

    #[test]
    fn a_newer_version_from_another_server_wins_and_an_older_one_does_not() {
        let mut server = global_server();
        update(&mut server, LOW, 1);
        update(&mut server, LOW, 2);
        from_peer(&mut server, PEER_CLIENT, 1, 9);
        assert_eq!(sync(&mut server, LOW), [2], "version 1 is older than 2");
        from_peer(&mut server, PEER_CLIENT, 3, 7);
        assert_eq!(sync(&mut server, LOW), [7]);
        // And what a client of this server writes next is newer still.
        update(&mut server, LOW, 8);
        assert_eq!(server.vars["x"].version, 4);
    }

    #[test]
    fn servers_agree_on_updates_made_at_the_same_time() {
        // Each server numbers its own client's update 1, and then gets the other.
        let mut here = global_server();
        update(&mut here, LOW, 1);
        from_peer(&mut here, PEER_CLIENT, 1, 2);

        let mut there = global_server();
        let msg = SharedMsg::Update("x".into(), 0, vec![2]).encode();
        there.on_packet(client(PEER_CLIENT), &msg).unwrap();
        from_peer(&mut there, LOW, 1, 1);

        assert_eq!(here.vars["x"].data, [1], "the lower id breaks the tie");
        assert_eq!(there.vars["x"].data, [1]);
    }

    #[test]
    fn an_update_that_comes_back_is_not_forwarded_again() {
        let mut server = global_server();
        update(&mut server, LOW, 1);
        let msg = SharedMsg::Update("x".into(), 1, vec![1]).encode();
        let out = server.on_packet(peer(PEER, LOW), &msg).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn a_new_link_gets_the_value_with_its_version() {
        let mut server = global_server();
        update(&mut server, LOW, 9);

        let out = server.sync_link(LinkKind::Peer, PEER);
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].from, Some(LOW));

        let mut there = SharedServer::default();
        there.on_packet(peer(PEER, PEER), &out[0].payload).unwrap();
        there.on_packet(peer(PEER, LOW), &out[1].payload).unwrap();
        assert_eq!(there.vars["x"].data, [9]);
        assert_eq!(there.vars["x"].version, 1);

        // A server that has something newer keeps it.
        let mut newer = global_server();
        from_peer(&mut newer, PEER_CLIENT, 2, 5);
        newer.on_packet(peer(PEER, LOW), &out[1].payload).unwrap();
        assert_eq!(newer.vars["x"].data, [5]);
    }
}
