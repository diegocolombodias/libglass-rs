use crate::client::{ClientCore, GlassClient};
use crate::error::{Error, Result};
use crate::outbound::Outbound;
use crate::owners::{Owners, Source};
use crate::protocol::AliasMsg;
use crate::types::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// What the server said about this client.
#[derive(Default)]
pub(crate) struct AliasSlot {
    pub(crate) association: Option<String>,
}

/// A value that depends on which name the group gave to this node.
///
/// Every client registers the same names, each with its own local value, which
/// can be anything since it never leaves the client. Any client can then tell
/// the others which name to use, so the same code can behave differently on
/// each node: this is how a client is told to draw the left part of a display
/// wall, for example.
pub struct Alias<T> {
    core: Arc<ClientCore>,
    name: String,
    values: Mutex<BTreeMap<String, T>>,
    default: T,
}

impl<T: Clone> Alias<T> {
    /// Creates an alias. `default` is its value while this client has no
    /// association, or has one for which it has not registered a value.
    pub fn new(client: &GlassClient, name: &str, default: T, mode: Mode) -> Result<Alias<T>> {
        if name.is_empty() || name.contains('\0') {
            return Err(Error::InvalidName);
        }
        let core = client.core().clone();
        {
            let mut state = core.state.lock().unwrap();
            if state.aliases.contains_key(name) {
                return Err(Error::AlreadyExists(name.to_owned()));
            }
            state.aliases.insert(name.to_owned(), AliasSlot::default());
        }

        let msg = match mode {
            Mode::Local => AliasMsg::CreateLocal(name.to_owned()),
            Mode::Global => AliasMsg::CreateGlobal(name.to_owned()),
        };
        if let Err(e) = core.register(ALIAS_PLUGIN_ID, name, msg.encode()) {
            core.state.lock().unwrap().aliases.remove(name);
            return Err(e);
        }
        Ok(Alias {
            core,
            name: name.to_owned(),
            values: Mutex::new(BTreeMap::new()),
            default,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Registers the local value of an association.
    pub fn add_alias(&self, association: &str, value: T) {
        let mut values = self.values.lock().unwrap();
        values.insert(association.to_owned(), value);
    }

    /// Removes the local value of an association. Returns false if it had none.
    pub fn delete_alias(&self, association: &str) -> bool {
        self.values.lock().unwrap().remove(association).is_some()
    }

    /// The association of this client, if it has one.
    pub fn association(&self) -> Option<String> {
        let state = self.core.state.lock().unwrap();
        let slot = state.aliases.get(&self.name)?;
        slot.association.clone()
    }

    /// Has this client been given an association?
    pub fn check_alias(&self) -> bool {
        self.association().is_some()
    }

    /// The value of the current association, or the default value.
    pub fn get_data(&self) -> T {
        let values = self.values.lock().unwrap();
        self.association()
            .and_then(|association| values.get(&association).cloned())
            .unwrap_or_else(|| self.default.clone())
    }

    /// Gives an association to a client, or to all of them with [`EVERYBODY_ID`].
    ///
    /// The client does not have to exist or to own this alias yet: it gets the
    /// association when it creates the alias. Returning `Ok` means that the
    /// message was sent, not that the client has received it.
    pub fn associate(&self, id: NodeId, association: &str) -> Result<()> {
        let msg = AliasMsg::Associate {
            name: self.name.clone(),
            target: id,
            association: association.to_owned(),
        };
        self.core.send_to_server(ALIAS_PLUGIN_ID, msg.encode())
    }

    /// Gives associations to several clients.
    pub fn associate_many(&self, associations: &BTreeMap<NodeId, String>) -> Result<()> {
        for (&id, association) in associations {
            self.associate(id, association)?;
        }
        Ok(())
    }
}

impl<T> Drop for Alias<T> {
    fn drop(&mut self) {
        self.core.state.lock().unwrap().aliases.remove(&self.name);
        let msg = AliasMsg::Delete(self.name.clone());
        self.core.unregister(ALIAS_PLUGIN_ID, &self.name);
        let _ = self.core.send_to_server(ALIAS_PLUGIN_ID, msg.encode());
    }
}

#[derive(Default)]
struct AliasEntry {
    owners: Owners,
    /// The association of each node. `EVERYBODY_ID` applies to the others.
    associations: BTreeMap<NodeId, String>,
}

impl AliasEntry {
    fn association_of(&self, id: NodeId) -> Option<&String> {
        self.associations
            .get(&id)
            .or_else(|| self.associations.get(&EVERYBODY_ID))
    }
}

/// Server side of the alias plugin: who owns each alias and all associations.
#[derive(Default)]
pub(crate) struct AliasServer {
    aliases: BTreeMap<String, AliasEntry>,
}

fn reconcile(name: &str, entry: &mut AliasEntry) -> Vec<Outbound> {
    let create = AliasMsg::CreateGlobal(name.to_owned()).encode();
    let delete = AliasMsg::Delete(name.to_owned()).encode();
    entry.owners.reconcile(ALIAS_PLUGIN_ID, &create, &delete)
}

fn association_for(name: &str, id: NodeId, association: &str) -> Outbound {
    let msg = AliasMsg::Associate {
        name: name.to_owned(),
        target: id,
        association: association.to_owned(),
    };
    Outbound::to_node(ALIAS_PLUGIN_ID, msg.encode(), id)
}

impl AliasServer {
    pub(crate) fn on_packet(&mut self, source: Source, payload: &[u8]) -> Result<Vec<Outbound>> {
        match AliasMsg::decode(payload)? {
            AliasMsg::CreateLocal(name) => Ok(self.create(source, name, false)),
            AliasMsg::CreateGlobal(name) => Ok(self.create(source, name, true)),
            AliasMsg::Delete(name) => {
                let Some(entry) = self.aliases.get_mut(&name) else {
                    log::warn!("inexistent alias {name} deleted");
                    return Ok(vec![]);
                };
                if !entry.owners.remove(source.id) {
                    return Ok(vec![]);
                }
                let out = reconcile(&name, entry);
                if entry.owners.is_empty() {
                    self.aliases.remove(&name);
                }
                Ok(out)
            }
            AliasMsg::Associate {
                name,
                target,
                association,
            } => {
                let Some(entry) = self.aliases.get_mut(&name) else {
                    log::warn!("inexistent alias {name} associated");
                    return Ok(vec![]);
                };
                let mut out = Vec::new();
                if target == EVERYBODY_ID {
                    entry.associations.clear();
                    for id in entry.owners.clients() {
                        out.push(association_for(&name, id, &association));
                    }
                } else if entry.owners.clients().any(|id| id == target) {
                    out.push(association_for(&name, target, &association));
                }
                entry.associations.insert(target, association);

                // Other servers keep it too, for clients that are yet to come.
                let links = entry.owners.forward_links(source.relation);
                let mut forward = Outbound::to_links(ALIAS_PLUGIN_ID, payload.to_vec(), links);
                forward.black.insert(source.id);
                out.push(forward);
                Ok(out)
            }
        }
    }

    fn create(&mut self, source: Source, name: String, global: bool) -> Vec<Outbound> {
        let entry = self.aliases.entry(name.clone()).or_default();
        entry.owners.insert(source, global);
        let mut out = reconcile(&name, entry);
        if entry.owners.clients().any(|id| id == source.id) {
            if let Some(association) = entry.association_of(source.id) {
                out.push(association_for(&name, source.id, association));
            }
        }
        out
    }

    /// An id can be given to a new client later, which must not inherit this.
    pub(crate) fn unregister_node(&mut self, id: NodeId) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (name, entry) in &mut self.aliases {
            entry.associations.remove(&id);
            if entry.owners.remove(id) {
                out.extend(reconcile(name, entry));
            }
        }
        self.aliases.retain(|_, entry| !entry.owners.is_empty());
        out
    }

    /// What a server that has just connected needs to know.
    pub(crate) fn sync_link(&self, kind: LinkKind, id: NodeId) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (name, entry) in &self.aliases {
            if !entry.owners.concerns_new_link(kind) {
                continue;
            }
            let create = AliasMsg::CreateGlobal(name.clone()).encode();
            out.push(Outbound::to_node(ALIAS_PLUGIN_ID, create, id));
            for (&target, association) in &entry.associations {
                let msg = AliasMsg::Associate {
                    name: name.clone(),
                    target,
                    association: association.clone(),
                };
                out.push(Outbound::to_node(ALIAS_PLUGIN_ID, msg.encode(), id));
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

    const A: NodeId = 0x10001;
    const B: NodeId = 0x10002;
    const PEER: NodeId = 0x20000;

    fn create(server: &mut AliasServer, source: Source, global: bool) -> Vec<Outbound> {
        let msg = if global {
            AliasMsg::CreateGlobal("x".into())
        } else {
            AliasMsg::CreateLocal("x".into())
        };
        server.on_packet(source, &msg.encode()).unwrap()
    }

    fn associate(server: &mut AliasServer, source: Source, target: NodeId) -> Vec<Outbound> {
        let msg = AliasMsg::Associate {
            name: "x".into(),
            target,
            association: "left".into(),
        };
        server.on_packet(source, &msg.encode()).unwrap()
    }

    /// The clients that were told their association.
    fn told(out: &[Outbound]) -> BTreeSet<NodeId> {
        out.iter()
            .filter(|o| o.links == Links::NONE)
            .flat_map(|o| o.targets.iter().copied())
            .collect()
    }

    #[test]
    fn association_only_reaches_its_target() {
        let mut server = AliasServer::default();
        create(&mut server, client(A), false);
        create(&mut server, client(B), false);
        let out = associate(&mut server, client(A), B);
        assert_eq!(told(&out), BTreeSet::from([B]));
    }

    #[test]
    fn association_of_everybody_reaches_all_owners() {
        let mut server = AliasServer::default();
        create(&mut server, client(A), false);
        create(&mut server, client(B), false);
        let out = associate(&mut server, client(A), EVERYBODY_ID);
        assert_eq!(told(&out), BTreeSet::from([A, B]));
    }

    #[test]
    fn association_waits_for_a_client_that_has_not_created_the_alias() {
        let mut server = AliasServer::default();
        create(&mut server, client(A), false);
        assert!(told(&associate(&mut server, client(A), B)).is_empty());
        assert_eq!(told(&create(&mut server, client(B), false)), [B].into());
    }

    #[test]
    fn association_of_everybody_applies_to_new_owners() {
        // In the C++ code, it is stored but never looked up.
        let mut server = AliasServer::default();
        create(&mut server, client(A), false);
        associate(&mut server, client(A), EVERYBODY_ID);
        assert_eq!(told(&create(&mut server, client(B), false)), [B].into());
    }

    #[test]
    fn association_of_everybody_replaces_individual_ones() {
        let mut server = AliasServer::default();
        create(&mut server, client(A), false);
        associate(&mut server, client(A), B);
        let msg = AliasMsg::Associate {
            name: "x".into(),
            target: EVERYBODY_ID,
            association: "all".into(),
        };
        server.on_packet(client(A), &msg.encode()).unwrap();
        let entry = &server.aliases["x"];
        assert_eq!(entry.association_of(B).unwrap(), "all");
    }

    #[test]
    fn individual_association_wins_over_everybody() {
        let mut server = AliasServer::default();
        create(&mut server, client(A), false);
        associate(&mut server, client(A), EVERYBODY_ID);
        let msg = AliasMsg::Associate {
            name: "x".into(),
            target: B,
            association: "mine".into(),
        };
        server.on_packet(client(A), &msg.encode()).unwrap();
        let entry = &server.aliases["x"];
        assert_eq!(entry.association_of(B).unwrap(), "mine");
        assert_eq!(entry.association_of(A).unwrap(), "left");
    }

    #[test]
    fn association_of_an_inexistent_alias_is_ignored() {
        let mut server = AliasServer::default();
        assert!(associate(&mut server, client(A), B).is_empty());
    }

    #[test]
    fn a_new_client_does_not_inherit_the_association_of_a_former_one() {
        let mut server = AliasServer::default();
        create(&mut server, client(A), false);
        create(&mut server, client(B), false);
        associate(&mut server, client(A), B);
        server.unregister_node(B);
        assert!(told(&create(&mut server, client(B), false)).is_empty());
    }

    #[test]
    fn local_associations_are_not_forwarded() {
        let mut server = AliasServer::default();
        create(&mut server, client(A), false);
        let out = associate(&mut server, client(A), B);
        assert!(out.iter().all(|o| o.links == Links::NONE));
    }

    #[test]
    fn global_associations_are_forwarded_once() {
        let mut server = AliasServer::default();
        create(&mut server, client(A), true);
        let out = associate(&mut server, client(A), B);
        assert_eq!(out.last().unwrap().links, Links::ALL);

        // What comes from a peer is kept, but does not go back to the peers.
        let out = associate(&mut server, peer(PEER, A), B);
        let forward = out.last().unwrap();
        assert!(!forward.links.peers);
        assert!(forward.black.contains(&PEER));
    }

    #[test]
    fn new_link_gets_the_associations() {
        let mut server = AliasServer::default();
        create(&mut server, client(A), true);
        associate(&mut server, client(A), B);
        let out = server.sync_link(LinkKind::Peer, PEER);
        assert_eq!(out.len(), 2);
        assert!(matches!(
            AliasMsg::decode(&out[1].payload).unwrap(),
            AliasMsg::Associate { target: B, .. }
        ));
    }

    #[test]
    fn alias_disappears_with_its_last_owner() {
        let mut server = AliasServer::default();
        create(&mut server, client(A), false);
        let delete = AliasMsg::Delete("x".into()).encode();
        server.on_packet(client(A), &delete).unwrap();
        assert!(server.aliases.is_empty());
    }
}
