use crate::client::{ClientCore, GlassClient};
use crate::error::{Error, Result};
use crate::outbound::Outbound;
use crate::owners::{Owners, Source};
use crate::protocol::RemoteMsg;
use crate::types::*;
use crate::wire::Wire;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long [`Remote::get_value`] waits for the other node.
const GET_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Reply {
    data: Vec<u8>,
    generation: u64,
}

/// The local value, kept packed so that the reading thread can answer requests
/// from other nodes without knowing its type.
#[derive(Default)]
pub(crate) struct RemoteSlot {
    pub(crate) data: Vec<u8>,
    replies: BTreeMap<NodeId, Reply>,
}

impl RemoteSlot {
    pub(crate) fn store_reply(&mut self, from: NodeId, data: Vec<u8>) {
        let reply = self.replies.entry(from).or_default();
        reply.data = data;
        reply.generation += 1;
    }
}

/// A variable each client has its own copy of, which any other client can read
/// or change directly by id, across all servers.
///
/// Unlike [`Shared`](crate::Shared), there is no common value: each operation
/// involves one specific node. Remote variables are always global.
pub struct Remote<T> {
    core: Arc<ClientCore>,
    name: String,
    _type: PhantomData<fn() -> T>,
}

impl<T: Wire> Remote<T> {
    /// Creates a variable. Each client can only have one with a given name.
    pub fn new(client: &GlassClient, name: &str, initial: &T) -> Result<Remote<T>> {
        if name.is_empty() || name.contains('\0') {
            return Err(Error::InvalidName);
        }
        let core = client.core().clone();
        {
            let mut state = core.state.lock().unwrap();
            if state.remotes.contains_key(name) {
                return Err(Error::AlreadyExists(name.to_owned()));
            }
            let slot = RemoteSlot {
                data: initial.pack(),
                replies: BTreeMap::new(),
            };
            state.remotes.insert(name.to_owned(), slot);
        }

        let msg = RemoteMsg::Create(name.to_owned());
        if let Err(e) = core.send_to_server(REMOTE_PLUGIN_ID, msg.encode()) {
            core.state.lock().unwrap().remotes.remove(name);
            return Err(e);
        }
        Ok(Remote {
            core,
            name: name.to_owned(),
            _type: PhantomData,
        })
    }

    fn is_local(&self, id: NodeId) -> bool {
        id == NOBODY_ID || id == self.core.id()
    }

    fn local_bytes(&self) -> Result<Vec<u8>> {
        let state = self.core.state.lock().unwrap();
        let slot = state.remotes.get(&self.name).ok_or(Error::Disconnected)?;
        Ok(slot.data.clone())
    }

    fn set_local_bytes(&self, data: Vec<u8>) {
        let mut state = self.core.state.lock().unwrap();
        if let Some(slot) = state.remotes.get_mut(&self.name) {
            slot.data = data;
        }
    }

    fn send_set(&self, id: NodeId, data: Vec<u8>) -> Result<()> {
        let msg = RemoteMsg::Set(self.name.clone(), data);
        self.core
            .send(REMOTE_PLUGIN_ID, msg.encode(), BTreeSet::from([id]))
    }

    /// The local value.
    pub fn get(&self) -> Result<T> {
        T::unpack(&self.local_bytes()?).ok_or(Error::Decode)
    }

    /// Changes the local value.
    pub fn set(&self, value: &T) {
        self.set_local_bytes(value.pack());
    }

    /// Changes the value in a node, or in all others with [`EVERYBODY_ID`]. It
    /// is the local value if `id` is this client or [`NOBODY_ID`].
    ///
    /// Returning `Ok` means that the message was sent, not that it has arrived.
    pub fn set_value(&self, id: NodeId, value: &T) -> Result<()> {
        if self.is_local(id) {
            self.set(value);
            return Ok(());
        }
        self.send_set(id, value.pack())
    }

    /// Asks another node for its value, and waits for the answer.
    pub fn get_value(&self, id: NodeId) -> Result<T> {
        self.get_value_timeout(id, GET_TIMEOUT)
    }

    /// Like [`get_value`](Self::get_value), but with a time limit. It runs out if
    /// the node does not exist or does not have this variable.
    pub fn get_value_timeout(&self, id: NodeId, timeout: Duration) -> Result<T> {
        if self.is_local(id) {
            return self.get();
        }
        let data = self.fetch(id, timeout)?;
        T::unpack(&data).ok_or(Error::Decode)
    }

    /// Sends the local value to another node.
    pub fn copy_data_to_remote(&self, id: NodeId) -> Result<()> {
        if self.is_local(id) {
            return Ok(());
        }
        self.send_set(id, self.local_bytes()?)
    }

    /// Replaces the local value by the value of another node.
    pub fn copy_data_from_remote(&self, id: NodeId) -> Result<()> {
        if self.is_local(id) {
            return Ok(());
        }
        let data = self.fetch(id, GET_TIMEOUT)?;
        self.set_local_bytes(data);
        Ok(())
    }

    fn fetch(&self, id: NodeId, timeout: Duration) -> Result<Vec<u8>> {
        if id < BASE_SERVER_ID {
            // A group: there would be no way to tell which answer to use.
            return Err(Error::InvalidTarget(id));
        }
        let generation = {
            let state = self.core.state.lock().unwrap();
            let slot = state.remotes.get(&self.name).ok_or(Error::Disconnected)?;
            slot.replies.get(&id).map_or(0, |reply| reply.generation)
        };

        let msg = RemoteMsg::Get(self.name.clone());
        self.core
            .send(REMOTE_PLUGIN_ID, msg.encode(), BTreeSet::from([id]))?;

        let deadline = Instant::now() + timeout;
        let mut state = self.core.state.lock().unwrap();
        loop {
            let slot = state.remotes.get(&self.name).ok_or(Error::Disconnected)?;
            if let Some(reply) = slot.replies.get(&id) {
                if reply.generation > generation {
                    return Ok(reply.data.clone());
                }
            }
            if state.closed {
                return Err(Error::Disconnected);
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(Error::Timeout)?;
            state = self.core.changed.wait_timeout(state, remaining).unwrap().0;
        }
    }
}

impl<T> Drop for Remote<T> {
    fn drop(&mut self) {
        self.core.state.lock().unwrap().remotes.remove(&self.name);
        self.core.changed.notify_all();
        let msg = RemoteMsg::Delete(self.name.clone());
        let _ = self.core.send_to_server(REMOTE_PLUGIN_ID, msg.encode());
    }
}

/// Server side of the remote plugin: it only needs to know who owns each one.
#[derive(Default)]
pub(crate) struct RemoteServer {
    vars: BTreeMap<String, Owners>,
}

fn reconcile(name: &str, owners: &mut Owners) -> Vec<Outbound> {
    let create = RemoteMsg::Create(name.to_owned()).encode();
    let delete = RemoteMsg::Delete(name.to_owned()).encode();
    owners.reconcile(REMOTE_PLUGIN_ID, &create, &delete)
}

impl RemoteServer {
    pub(crate) fn on_packet(
        &mut self,
        source: Source,
        targets: &BTreeSet<NodeId>,
        payload: &[u8],
    ) -> Result<Vec<Outbound>> {
        let msg = RemoteMsg::decode(payload)?;
        let name = match msg {
            RemoteMsg::Create(name) => {
                let owners = self.vars.entry(name.clone()).or_default();
                owners.insert(source, true);
                return Ok(reconcile(&name, owners));
            }
            RemoteMsg::Delete(name) => {
                let Some(owners) = self.vars.get_mut(&name) else {
                    log::warn!("inexistent remote variable {name} deleted");
                    return Ok(vec![]);
                };
                if !owners.remove(source.id) {
                    return Ok(vec![]);
                }
                let out = reconcile(&name, owners);
                if owners.is_empty() {
                    self.vars.remove(&name);
                }
                return Ok(out);
            }
            RemoteMsg::Set(name, _) | RemoteMsg::Value(name, _) | RemoteMsg::Get(name) => name,
        };

        let Some(owners) = self.vars.get(&name) else {
            log::warn!("message about inexistent remote variable {name}");
            return Ok(vec![]);
        };
        // It keeps the sender, so that a request can be answered.
        Ok(vec![Outbound {
            plugin: REMOTE_PLUGIN_ID,
            payload: payload.to_vec(),
            targets: targets.clone(),
            black: BTreeSet::from([source.id]),
            white: owners.ids(),
            links: owners.forward_links(source.relation),
            from: Some(source.origin),
        }])
    }

    pub(crate) fn unregister_node(&mut self, id: NodeId) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (name, owners) in &mut self.vars {
            if owners.remove(id) {
                out.extend(reconcile(name, owners));
            }
        }
        self.vars.retain(|_, owners| !owners.is_empty());
        out
    }

    /// What a server that has just connected needs to know.
    pub(crate) fn sync_link(&self, kind: LinkKind, id: NodeId) -> Vec<Outbound> {
        self.vars
            .iter()
            .filter(|(_, owners)| owners.concerns_new_link(kind))
            .map(|(name, _)| {
                let create = RemoteMsg::Create(name.clone()).encode();
                Outbound::to_node(REMOTE_PLUGIN_ID, create, id)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owners::test_support::*;
    use crate::owners::Links;

    const A: NodeId = 0x10001;
    const B: NodeId = 0x10002;
    const PEER: NodeId = 0x20000;

    fn create(server: &mut RemoteServer, source: Source) -> Vec<Outbound> {
        let msg = RemoteMsg::Create("x".into()).encode();
        server.on_packet(source, &BTreeSet::new(), &msg).unwrap()
    }

    fn set(server: &mut RemoteServer, source: Source, target: NodeId) -> Vec<Outbound> {
        let msg = RemoteMsg::Set("x".into(), vec![1]).encode();
        server
            .on_packet(source, &BTreeSet::from([target]), &msg)
            .unwrap()
    }

    #[test]
    fn creating_tells_other_servers_because_it_is_always_global() {
        let mut server = RemoteServer::default();
        assert_eq!(create(&mut server, client(A)).len(), 2);
        assert!(create(&mut server, client(B)).is_empty());
    }

    #[test]
    fn set_keeps_target_and_original_sender() {
        let mut server = RemoteServer::default();
        create(&mut server, client(A));
        create(&mut server, client(B));

        let out = set(&mut server, client(A), B);
        assert_eq!(out[0].targets, BTreeSet::from([B]));
        assert_eq!(out[0].from, Some(A));
        assert_eq!(out[0].black, BTreeSet::from([A]));
        assert_eq!(out[0].white, BTreeSet::from([A, B]));
        assert_eq!(out[0].links, Links::ALL);
    }

    #[test]
    fn message_from_a_peer_keeps_the_original_sender() {
        let mut server = RemoteServer::default();
        create(&mut server, client(A));
        let out = set(&mut server, peer(PEER, 0x20001), A);
        assert_eq!(out[0].from, Some(0x20001));
        assert!(!out[0].links.peers);
        assert!(out[0].black.contains(&PEER));
    }

    #[test]
    fn get_and_value_are_routed_like_set() {
        let mut server = RemoteServer::default();
        create(&mut server, client(A));
        create(&mut server, client(B));
        for msg in [
            RemoteMsg::Get("x".into()),
            RemoteMsg::Value("x".into(), vec![1]),
        ] {
            let out = server
                .on_packet(client(A), &BTreeSet::from([B]), &msg.encode())
                .unwrap();
            assert_eq!(out[0].targets, BTreeSet::from([B]));
            assert_eq!(out[0].from, Some(A));
        }
    }

    #[test]
    fn message_about_an_inexistent_variable_is_dropped() {
        let mut server = RemoteServer::default();
        assert!(set(&mut server, client(A), B).is_empty());
    }

    #[test]
    fn variable_disappears_with_its_last_owner() {
        let mut server = RemoteServer::default();
        create(&mut server, client(A));
        create(&mut server, client(B));
        let delete = RemoteMsg::Delete("x".into()).encode();
        server
            .on_packet(client(A), &BTreeSet::new(), &delete)
            .unwrap();
        assert_eq!(server.vars.len(), 1);
        assert_eq!(server.unregister_node(B).len(), 2);
        assert!(server.vars.is_empty());
    }

    #[test]
    fn new_link_is_told_about_variables() {
        let mut server = RemoteServer::default();
        create(&mut server, client(A));
        let out = server.sync_link(LinkKind::Peer, PEER);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload, RemoteMsg::Create("x".into()).encode());
    }
}
