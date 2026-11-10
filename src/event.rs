use crate::client::{ClientCore, GlassClient};
use crate::error::{Error, Result};
use crate::outbound::Outbound;
use crate::owners::{Owners, Source};
use crate::protocol::EventMsg;
use crate::types::*;
use crate::wire::Wire;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type RawHandler = Arc<Mutex<dyn FnMut(&[u8]) + Send>>;

#[derive(Default)]
pub(crate) struct EventSlot {
    /// Each event, with the client that sent it.
    queue: VecDeque<(NodeId, Vec<u8>)>,
    handler: Option<RawHandler>,
}

/// Delivers an event from the server. The handler runs without the client lock
/// so it can use the client's objects.
pub(crate) fn deliver(core: &ClientCore, name: &str, from: NodeId, data: Vec<u8>) {
    let mut state = core.state.lock().unwrap();
    let Some(slot) = state.events.get_mut(name) else {
        log::warn!("event {name} not found");
        return;
    };
    match slot.handler.clone() {
        Some(handler) => {
            drop(state);
            (handler.lock().unwrap())(&data);
        }
        None => {
            slot.queue.push_back((from, data));
            drop(state);
            core.changed.notify_all();
        }
    }
}

/// A named queue of messages between clients.
///
/// A client only receives events sent after it created its own `Event` with the
/// same name.
pub struct Event<T> {
    core: Arc<ClientCore>,
    name: String,
    _type: PhantomData<fn() -> T>,
}

impl<T: Wire + 'static> Event<T> {
    /// Creates an event. Each client can only have one event with a given name.
    pub fn new(client: &GlassClient, name: &str, mode: Mode) -> Result<Event<T>> {
        if name.is_empty() || name.contains('\0') {
            return Err(Error::InvalidName);
        }
        let core = client.core().clone();
        {
            let mut state = core.state.lock().unwrap();
            if state.events.contains_key(name) {
                return Err(Error::AlreadyExists(name.to_owned()));
            }
            state.events.insert(name.to_owned(), EventSlot::default());
        }

        let msg = match mode {
            Mode::Local => EventMsg::CreateLocal(name.to_owned()),
            Mode::Global => EventMsg::CreateGlobal(name.to_owned()),
        };
        if let Err(e) = core.register(EVENT_PLUGIN_ID, name, msg.encode()) {
            core.state.lock().unwrap().events.remove(name);
            return Err(e);
        }
        Ok(Event {
            core,
            name: name.to_owned(),
            _type: PhantomData,
        })
    }

    /// Sends an event to a node or group, such as [`EVERYBODY_ID`].
    ///
    /// The sender only receives its own event if `send_to_self` is true.
    pub fn enqueue(&self, data: &T, target: NodeId, send_to_self: bool) -> Result<()> {
        self.enqueue_many(data, &BTreeSet::from([target]), send_to_self)
    }

    /// Sends an event to several nodes or groups.
    pub fn enqueue_many(
        &self,
        data: &T,
        targets: &BTreeSet<NodeId>,
        send_to_self: bool,
    ) -> Result<()> {
        let bytes = data.pack();
        let msg = EventMsg::Event(self.name.clone(), bytes.clone());
        self.core
            .send(EVENT_PLUGIN_ID, msg.encode(), targets.clone())?;
        let own = self.core.id();
        if send_to_self && (targets.contains(&EVERYBODY_ID) || targets.contains(&own)) {
            deliver(&self.core, &self.name, own, bytes);
        }
        Ok(())
    }

    /// Sends an event that may be lost or arrive out of order, through UDP, which
    /// is faster for data that is soon replaced by newer data. Without UDP, it
    /// goes like any other event: see
    /// [`GlassClient::connect_with_udp`](crate::GlassClient::connect_with_udp).
    pub fn enqueue_unreliable(&self, data: &T, target: NodeId) -> Result<()> {
        let msg = EventMsg::Event(self.name.clone(), data.pack());
        self.core
            .send_unreliable(EVENT_PLUGIN_ID, msg.encode(), BTreeSet::from([target]))
    }

    /// Removes the first event in the queue, if any.
    pub fn try_get(&self) -> Result<Option<T>> {
        Ok(self.try_get_with_sender()?.map(|(_, value)| value))
    }

    /// Removes the first event in the queue, if any, with the client that sent it.
    pub fn try_get_with_sender(&self) -> Result<Option<(NodeId, T)>> {
        let mut state = self.core.state.lock().unwrap();
        let slot = state
            .events
            .get_mut(&self.name)
            .ok_or(Error::Disconnected)?;
        match slot.queue.pop_front() {
            Some((from, bytes)) => Ok(Some((from, T::unpack(&bytes).ok_or(Error::Decode)?))),
            None => Ok(None),
        }
    }

    /// Waits for an event and removes it from the queue.
    pub fn get_timeout(&self, timeout: Duration) -> Result<T> {
        Ok(self.get_with_sender_timeout(timeout)?.1)
    }

    /// Waits for an event and removes it from the queue, and tells which client sent
    /// it. The server writes the sender, so a client cannot pass for another.
    pub fn get_with_sender_timeout(&self, timeout: Duration) -> Result<(NodeId, T)> {
        let deadline = Instant::now() + timeout;
        let mut state = self.core.state.lock().unwrap();
        loop {
            let slot = state
                .events
                .get_mut(&self.name)
                .ok_or(Error::Disconnected)?;
            if let Some((from, bytes)) = slot.queue.pop_front() {
                return Ok((from, T::unpack(&bytes).ok_or(Error::Decode)?));
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

    pub fn is_empty(&self) -> bool {
        self.queue_size() == 0
    }

    pub fn queue_size(&self) -> usize {
        let state = self.core.state.lock().unwrap();
        state.events.get(&self.name).map_or(0, |s| s.queue.len())
    }

    /// Discards the events in the queue.
    pub fn clear(&self) {
        let mut state = self.core.state.lock().unwrap();
        if let Some(slot) = state.events.get_mut(&self.name) {
            slot.queue.clear();
        }
    }

    /// Calls `handler` for each queued event and for each new one, instead of
    /// queueing them. It runs on the client's reading thread, so it must not
    /// wait for another packet. Events that cannot be decoded are discarded.
    pub fn set_handler<F>(&self, mut handler: F)
    where
        F: FnMut(T) + Send + 'static,
    {
        let name = self.name.clone();
        let raw: RawHandler = Arc::new(Mutex::new(move |bytes: &[u8]| match T::unpack(bytes) {
            Some(value) => handler(value),
            None => log::warn!("could not decode event {name}"),
        }));

        let pending = {
            let mut state = self.core.state.lock().unwrap();
            let Some(slot) = state.events.get_mut(&self.name) else {
                return;
            };
            slot.handler = Some(raw.clone());
            std::mem::take(&mut slot.queue)
        };
        for (_, bytes) in pending {
            (raw.lock().unwrap())(&bytes);
        }
    }

    /// Goes back to queueing events.
    pub fn clear_handler(&self) {
        let mut state = self.core.state.lock().unwrap();
        if let Some(slot) = state.events.get_mut(&self.name) {
            slot.handler = None;
        }
    }
}

impl<T> Drop for Event<T> {
    fn drop(&mut self) {
        self.core.state.lock().unwrap().events.remove(&self.name);
        let msg = EventMsg::Delete(self.name.clone());
        self.core.unregister(EVENT_PLUGIN_ID, &self.name);
        let _ = self.core.send_to_server(EVENT_PLUGIN_ID, msg.encode());
    }
}

/// Server side of the event plugin: who owns each event.
#[derive(Default)]
pub(crate) struct EventServer {
    events: BTreeMap<String, Owners>,
}

fn reconcile(name: &str, owners: &mut Owners) -> Vec<Outbound> {
    let create = EventMsg::CreateGlobal(name.to_owned()).encode();
    let delete = EventMsg::Delete(name.to_owned()).encode();
    owners.reconcile(EVENT_PLUGIN_ID, &create, &delete)
}

impl EventServer {
    pub(crate) fn on_packet(
        &mut self,
        source: Source,
        targets: &BTreeSet<NodeId>,
        payload: &[u8],
    ) -> Result<Vec<Outbound>> {
        match EventMsg::decode(payload)? {
            EventMsg::CreateLocal(name) => Ok(self.create(source, name, false)),
            EventMsg::CreateGlobal(name) => Ok(self.create(source, name, true)),
            EventMsg::Delete(name) => {
                let Some(owners) = self.events.get_mut(&name) else {
                    log::warn!("inexistent event {name} deleted");
                    return Ok(vec![]);
                };
                owners.remove(source.id);
                let out = reconcile(&name, owners);
                if owners.is_empty() {
                    self.events.remove(&name);
                }
                Ok(out)
            }
            EventMsg::Event(name, _) => {
                let Some(owners) = self.events.get(&name) else {
                    log::warn!("inexistent event {name} enqueued");
                    return Ok(vec![]);
                };
                Ok(vec![Outbound {
                    plugin: EVENT_PLUGIN_ID,
                    payload: payload.to_vec(),
                    targets: targets.clone(),
                    black: BTreeSet::from([source.id]),
                    white: owners.ids(),
                    links: owners.forward_links(source.relation),
                    // The client that sent it, which the server knows by its connection,
                    // or which another server told.
                    from: Some(source.origin),
                    unreliable: false,
                }])
            }
        }
    }

    fn create(&mut self, source: Source, name: String, global: bool) -> Vec<Outbound> {
        let owners = self.events.entry(name.clone()).or_default();
        owners.insert(source, global);
        reconcile(&name, owners)
    }

    pub(crate) fn unregister_node(&mut self, id: NodeId) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (name, owners) in &mut self.events {
            if owners.remove(id) {
                out.extend(reconcile(name, owners));
            }
        }
        self.events.retain(|_, owners| !owners.is_empty());
        out
    }

    /// What a server that has just connected needs to know.
    pub(crate) fn sync_link(&self, kind: LinkKind, id: NodeId) -> Vec<Outbound> {
        self.events
            .iter()
            .filter(|(_, owners)| owners.concerns_new_link(kind))
            .map(|(name, _)| {
                let create = EventMsg::CreateGlobal(name.clone()).encode();
                Outbound::to_node(EVENT_PLUGIN_ID, create, id)
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

    fn targets() -> BTreeSet<NodeId> {
        BTreeSet::from([EVERYBODY_ID])
    }

    fn create(server: &mut EventServer, node: NodeId) {
        let msg = EventMsg::CreateLocal("e".into()).encode();
        assert!(server
            .on_packet(client(node), &targets(), &msg)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn events_go_to_owners_except_the_sender() {
        let mut server = EventServer::default();
        create(&mut server, A);
        create(&mut server, B);

        let msg = EventMsg::Event("e".into(), vec![1]).encode();
        let out = server.on_packet(client(A), &targets(), &msg).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload, msg);
        assert_eq!(out[0].from, Some(A), "it goes with the id of its sender");
        assert_eq!(out[0].black, BTreeSet::from([A]));
        assert_eq!(out[0].white, BTreeSet::from([A, B]));
        assert_eq!(out[0].links, Links::NONE);
    }

    #[test]
    fn unknown_event_is_dropped() {
        let mut server = EventServer::default();
        let msg = EventMsg::Event("e".into(), vec![1]).encode();
        assert!(server
            .on_packet(client(A), &targets(), &msg)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn event_disappears_with_its_last_owner() {
        let mut server = EventServer::default();
        create(&mut server, A);
        create(&mut server, B);
        let delete = EventMsg::Delete("e".into()).encode();
        server.on_packet(client(A), &targets(), &delete).unwrap();
        assert_eq!(server.events.len(), 1);
        server.unregister_node(B);
        assert!(server.events.is_empty());
    }

    #[test]
    fn deleting_an_unknown_event_is_harmless() {
        let mut server = EventServer::default();
        let delete = EventMsg::Delete("nope".into()).encode();
        assert!(server
            .on_packet(client(A), &targets(), &delete)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn global_event_from_a_client_is_forwarded_to_owner_peers() {
        let mut server = EventServer::default();
        let create = EventMsg::CreateGlobal("e".into()).encode();
        let out = server.on_packet(client(A), &targets(), &create).unwrap();
        assert_eq!(out.len(), 2, "tells peers and remote peers");
        server
            .on_packet(peer(PEER, PEER), &targets(), &create)
            .unwrap();

        let msg = EventMsg::Event("e".into(), vec![1]).encode();
        let out = server.on_packet(client(A), &targets(), &msg).unwrap();
        assert_eq!(out[0].links, Links::ALL);
        assert_eq!(out[0].white, BTreeSet::from([A, PEER]));
    }

    #[test]
    fn event_from_a_peer_is_not_sent_back_to_the_peers() {
        let mut server = EventServer::default();
        let create = EventMsg::CreateGlobal("e".into()).encode();
        server.on_packet(client(A), &targets(), &create).unwrap();

        let msg = EventMsg::Event("e".into(), vec![1]).encode();
        let out = server
            .on_packet(peer(PEER, PEER), &targets(), &msg)
            .unwrap();
        assert!(!out[0].links.peers);
        assert!(out[0].links.remotes);
        assert!(out[0].black.contains(&PEER));
    }

    #[test]
    fn new_link_is_told_about_global_events_only() {
        let mut server = EventServer::default();
        create(&mut server, A);
        assert!(server.sync_link(LinkKind::Peer, PEER).is_empty());

        let create = EventMsg::CreateGlobal("g".into()).encode();
        server.on_packet(client(A), &targets(), &create).unwrap();
        let out = server.sync_link(LinkKind::Peer, PEER);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload, create);
        assert_eq!(out[0].targets, BTreeSet::from([PEER]));
    }
}
