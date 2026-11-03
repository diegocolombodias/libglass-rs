use crate::client::{ClientCore, GlassClient};
use crate::error::{Error, Result};
use crate::outbound::Outbound;
use crate::owners::{Owners, Source};
use crate::protocol::MessengerMsg;
use crate::types::*;
use crate::wire::Wire;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type RawHandler = Arc<Mutex<dyn FnMut(NodeId, &[u8]) + Send>>;

#[derive(Default)]
pub(crate) struct MessengerSlot {
    queue: VecDeque<(NodeId, Vec<u8>)>,
    handler: Option<RawHandler>,
}

/// Delivers a message from the network. The handler runs without the client
/// lock so it can use the client's objects.
pub(crate) fn deliver(core: &ClientCore, name: &str, from: NodeId, data: Vec<u8>) {
    let mut state = core.state.lock().unwrap();
    let Some(slot) = state.messengers.get_mut(name) else {
        log::warn!("messenger {name} not found");
        return;
    };
    match slot.handler.clone() {
        Some(handler) => {
            drop(state);
            (handler.lock().unwrap())(from, &data);
        }
        None => {
            slot.queue.push_back((from, data));
            drop(state);
            core.changed.notify_all();
        }
    }
}

/// Point-to-point messages between clients: unlike an [`Event`](crate::Event),
/// each one goes to a single node, and the receiver knows who sent it.
pub struct Messenger<T> {
    core: Arc<ClientCore>,
    name: String,
    _type: PhantomData<fn() -> T>,
}

impl<T: Wire + 'static> Messenger<T> {
    /// Creates a messenger. Each client can only have one with a given name.
    pub fn new(client: &GlassClient, name: &str, mode: Mode) -> Result<Messenger<T>> {
        if name.is_empty() || name.contains('\0') {
            return Err(Error::InvalidName);
        }
        let core = client.core().clone();
        {
            let mut state = core.state.lock().unwrap();
            if state.messengers.contains_key(name) {
                return Err(Error::AlreadyExists(name.to_owned()));
            }
            state
                .messengers
                .insert(name.to_owned(), MessengerSlot::default());
        }

        let msg = match mode {
            Mode::Local => MessengerMsg::CreateLocal(name.to_owned()),
            Mode::Global => MessengerMsg::CreateGlobal(name.to_owned()),
        };
        if let Err(e) = core.send_to_server(MESSENGER_PLUGIN_ID, msg.encode()) {
            core.state.lock().unwrap().messengers.remove(name);
            return Err(e);
        }
        Ok(Messenger {
            core,
            name: name.to_owned(),
            _type: PhantomData,
        })
    }

    /// Sends a message to one node, which must have a messenger with this name.
    /// Returning `Ok` means that it was sent, not that it has arrived.
    pub fn send(&self, id: NodeId, message: &T) -> Result<()> {
        if id < BASE_SERVER_ID {
            return Err(Error::InvalidTarget(id));
        }
        let msg = MessengerMsg::Message {
            name: self.name.clone(),
            target: id,
            data: message.pack(),
        };
        self.core.send_to_server(MESSENGER_PLUGIN_ID, msg.encode())
    }

    /// Removes the first message that matches, if any.
    fn pop(
        &self,
        state: &mut crate::client::ClientState,
        sender: Option<NodeId>,
    ) -> Result<Option<(NodeId, Vec<u8>)>> {
        let slot = state
            .messengers
            .get_mut(&self.name)
            .ok_or(Error::Disconnected)?;
        let position = slot
            .queue
            .iter()
            .position(|(from, _)| sender.is_none_or(|id| *from == id));
        Ok(position.and_then(|p| slot.queue.remove(p)))
    }

    fn get(
        &self,
        sender: Option<NodeId>,
        timeout: Option<Duration>,
    ) -> Result<Option<(NodeId, T)>> {
        let deadline = timeout.map(|t| Instant::now() + t);
        let mut state = self.core.state.lock().unwrap();
        loop {
            if let Some((from, bytes)) = self.pop(&mut state, sender)? {
                let value = T::unpack(&bytes).ok_or(Error::Decode)?;
                return Ok(Some((from, value)));
            }
            let Some(deadline) = deadline else {
                return Ok(None);
            };
            if state.closed {
                return Err(Error::Disconnected);
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(Error::Timeout)?;
            state = self.core.changed.wait_timeout(state, remaining).unwrap().0;
        }
    }

    /// Removes the first message in the queue, with its sender, if there is any.
    pub fn try_get(&self) -> Result<Option<(NodeId, T)>> {
        self.get(None, None)
    }

    /// Removes the first message sent by a given node, if there is any.
    pub fn try_get_from(&self, sender: NodeId) -> Result<Option<T>> {
        Ok(self.get(Some(sender), None)?.map(|(_, value)| value))
    }

    /// Waits for a message.
    pub fn get_timeout(&self, timeout: Duration) -> Result<(NodeId, T)> {
        self.get(None, Some(timeout))?.ok_or(Error::Timeout)
    }

    /// Waits for a message from a given node.
    pub fn get_from_timeout(&self, sender: NodeId, timeout: Duration) -> Result<T> {
        let found = self.get(Some(sender), Some(timeout))?;
        found.map(|(_, value)| value).ok_or(Error::Timeout)
    }

    pub fn is_empty(&self) -> bool {
        self.queue_size() == 0
    }

    pub fn queue_size(&self) -> usize {
        let state = self.core.state.lock().unwrap();
        let slot = state.messengers.get(&self.name);
        slot.map_or(0, |s| s.queue.len())
    }

    /// Calls `handler` for each queued message and for each new one, instead of
    /// queueing them. It runs on the client's reading thread, so it must not
    /// wait for another packet. Messages that cannot be decoded are discarded.
    pub fn set_handler<F>(&self, mut handler: F)
    where
        F: FnMut(NodeId, T) + Send + 'static,
    {
        let name = self.name.clone();
        let raw: RawHandler = Arc::new(Mutex::new(
            move |from: NodeId, bytes: &[u8]| match T::unpack(bytes) {
                Some(value) => handler(from, value),
                None => log::warn!("could not decode message of {name}"),
            },
        ));

        let pending = {
            let mut state = self.core.state.lock().unwrap();
            let Some(slot) = state.messengers.get_mut(&self.name) else {
                return;
            };
            slot.handler = Some(raw.clone());
            std::mem::take(&mut slot.queue)
        };
        for (from, bytes) in pending {
            (raw.lock().unwrap())(from, &bytes);
        }
    }
}

impl<T> Drop for Messenger<T> {
    fn drop(&mut self) {
        self.core
            .state
            .lock()
            .unwrap()
            .messengers
            .remove(&self.name);
        self.core.changed.notify_all();
        let msg = MessengerMsg::Delete(self.name.clone());
        let _ = self.core.send_to_server(MESSENGER_PLUGIN_ID, msg.encode());
    }
}

/// Server side of the messenger plugin: who owns each messenger.
#[derive(Default)]
pub(crate) struct MessengerServer {
    messengers: BTreeMap<String, Owners>,
}

fn reconcile(name: &str, owners: &mut Owners) -> Vec<Outbound> {
    let create = MessengerMsg::CreateGlobal(name.to_owned()).encode();
    let delete = MessengerMsg::Delete(name.to_owned()).encode();
    owners.reconcile(MESSENGER_PLUGIN_ID, &create, &delete)
}

impl MessengerServer {
    pub(crate) fn on_packet(&mut self, source: Source, payload: &[u8]) -> Result<Vec<Outbound>> {
        match MessengerMsg::decode(payload)? {
            MessengerMsg::CreateLocal(name) => Ok(self.create(source, name, false)),
            MessengerMsg::CreateGlobal(name) => Ok(self.create(source, name, true)),
            MessengerMsg::Delete(name) => {
                let Some(owners) = self.messengers.get_mut(&name) else {
                    log::warn!("inexistent messenger {name} deleted");
                    return Ok(vec![]);
                };
                if !owners.remove(source.id) {
                    return Ok(vec![]);
                }
                let out = reconcile(&name, owners);
                if owners.is_empty() {
                    self.messengers.remove(&name);
                }
                Ok(out)
            }
            MessengerMsg::Message { name, target, .. } => {
                let Some(owners) = self.messengers.get(&name) else {
                    log::warn!("message for inexistent messenger {name}");
                    return Ok(vec![]);
                };
                // It keeps the sender, so that the receiver knows who sent it.
                Ok(vec![Outbound {
                    plugin: MESSENGER_PLUGIN_ID,
                    payload: payload.to_vec(),
                    targets: BTreeSet::from([target]),
                    black: BTreeSet::from([source.id]),
                    white: owners.ids(),
                    links: owners.forward_links(source.relation),
                    from: Some(source.origin),
                    unreliable: false,
                }])
            }
        }
    }

    fn create(&mut self, source: Source, name: String, global: bool) -> Vec<Outbound> {
        let owners = self.messengers.entry(name.clone()).or_default();
        owners.insert(source, global);
        reconcile(&name, owners)
    }

    pub(crate) fn unregister_node(&mut self, id: NodeId) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (name, owners) in &mut self.messengers {
            if owners.remove(id) {
                out.extend(reconcile(name, owners));
            }
        }
        self.messengers.retain(|_, owners| !owners.is_empty());
        out
    }

    /// What a server that has just connected needs to know.
    pub(crate) fn sync_link(&self, kind: LinkKind, id: NodeId) -> Vec<Outbound> {
        self.messengers
            .iter()
            .filter(|(_, owners)| owners.concerns_new_link(kind))
            .map(|(name, _)| {
                let create = MessengerMsg::CreateGlobal(name.clone()).encode();
                Outbound::to_node(MESSENGER_PLUGIN_ID, create, id)
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

    fn create(server: &mut MessengerServer, source: Source, global: bool) {
        let msg = if global {
            MessengerMsg::CreateGlobal("m".into())
        } else {
            MessengerMsg::CreateLocal("m".into())
        };
        server.on_packet(source, &msg.encode()).unwrap();
    }

    fn message(target: NodeId) -> Vec<u8> {
        MessengerMsg::Message {
            name: "m".into(),
            target,
            data: vec![1],
        }
        .encode()
    }

    #[test]
    fn message_goes_to_the_target_with_the_sender() {
        let mut server = MessengerServer::default();
        create(&mut server, client(A), false);
        create(&mut server, client(B), false);
        let out = server.on_packet(client(A), &message(B)).unwrap();
        assert_eq!(out[0].targets, BTreeSet::from([B]));
        assert_eq!(out[0].from, Some(A));
        assert_eq!(out[0].white, BTreeSet::from([A, B]));
        assert_eq!(out[0].links, Links::NONE);
    }

    #[test]
    fn global_message_is_forwarded_with_the_original_sender() {
        let mut server = MessengerServer::default();
        create(&mut server, client(A), true);
        let out = server.on_packet(client(A), &message(0x20001)).unwrap();
        assert_eq!(out[0].links, Links::ALL);

        let out = server.on_packet(peer(PEER, 0x20001), &message(A)).unwrap();
        assert_eq!(out[0].from, Some(0x20001));
        assert!(!out[0].links.peers);
    }

    #[test]
    fn message_for_an_inexistent_messenger_is_dropped() {
        let mut server = MessengerServer::default();
        assert!(server.on_packet(client(A), &message(B)).unwrap().is_empty());
    }

    #[test]
    fn messenger_disappears_with_its_last_owner() {
        let mut server = MessengerServer::default();
        create(&mut server, client(A), false);
        create(&mut server, client(B), false);
        let delete = MessengerMsg::Delete("m".into()).encode();
        server.on_packet(client(A), &delete).unwrap();
        assert_eq!(server.messengers.len(), 1);
        server.unregister_node(B);
        assert!(server.messengers.is_empty());
    }

    #[test]
    fn new_link_is_told_about_global_messengers_only() {
        let mut server = MessengerServer::default();
        create(&mut server, client(A), false);
        assert!(server.sync_link(LinkKind::Peer, PEER).is_empty());
        create(&mut server, client(B), true);
        assert_eq!(server.sync_link(LinkKind::Peer, PEER).len(), 1);
    }
}
