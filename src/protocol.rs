//! Payload formats of the root, event, shared and barrier plugins.

use crate::error::{Error, Result};
use crate::types::{LinkKind, NodeId, NOBODY_ID};
use std::collections::BTreeMap;

const NICKNAME_FIELD_LEN: usize = 256;
const NODE_ENTRY_LEN: usize = 4 + NICKNAME_FIELD_LEN;
const NODE_LIST_HEADER_LEN: usize = 9;

const PEER_BASE: u8 = 100;
const REMOTE_PEER_BASE: u8 = 110;
const LINK_HELLO: u8 = 0;
const LINK_START: u8 = 1;
const LINK_LIST: u8 = 2;
const LINK_LIST_UPDATE: u8 = 3;
const LINK_CHANGE_ID: u8 = 4;
const LINK_ACCEPTED: u8 = 5;
const LINK_ID_TAKEN: u8 = 6;
/// The highest message code of links.
const LINK_LAST: u8 = REMOTE_PEER_BASE + LINK_ID_TAKEN;

const RELATION_PEER: u32 = 3;
const RELATION_REMOTE_PEER: u32 = 4;

const HOST_FIELD_LEN: usize = 256;
const PEER_ENTRY_LEN: usize = 8 + HOST_FIELD_LEN;
const PEER_LIST_HEADER_LEN: usize = 9;

/// Where a server can be reached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerAddr {
    pub host: String,
    pub port: u16,
}

fn read_port(bytes: &[u8], offset: usize) -> Result<u16> {
    u16::try_from(read_u32(bytes, offset)?).map_err(|_| Error::InvalidPacket("invalid port"))
}

fn link_base(kind: LinkKind) -> u8 {
    match kind {
        LinkKind::Peer => PEER_BASE,
        LinkKind::Remote => REMOTE_PEER_BASE,
    }
}

fn link_relation(kind: LinkKind) -> u32 {
    match kind {
        LinkKind::Peer => RELATION_PEER,
        LinkKind::Remote => RELATION_REMOTE_PEER,
    }
}

/// The address of a server, and the number it drew when it started, after a NUL. Without
/// a number, which is 0, it is the format of the C++ library.
fn encode_link_addr(kind: LinkKind, offset: u8, addr: &PeerAddr, nonce: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(18 + addr.host.len());
    out.push(link_base(kind) + offset);
    out.extend_from_slice(&link_relation(kind).to_be_bytes());
    out.extend_from_slice(&u32::from(addr.port).to_be_bytes());
    out.extend_from_slice(truncated(&addr.host).as_bytes());
    if nonce != 0 {
        out.push(0);
        out.extend_from_slice(&nonce.to_be_bytes());
    }
    out
}

fn decode_link_addr(bytes: &[u8]) -> Result<(PeerAddr, u64)> {
    let port = read_port(bytes, 5)?;
    let host = bytes
        .get(9..)
        .ok_or(Error::InvalidPacket("truncated peer address"))?;
    let end = host.iter().position(|&b| b == 0).unwrap_or(host.len());
    let nonce = match host.get(end + 1..) {
        Some(rest) if rest.len() == 8 => u64::from_be_bytes(rest.try_into().unwrap()),
        _ => 0,
    };
    let addr = PeerAddr {
        host: String::from_utf8_lossy(&host[..end]).into_owned(),
        port,
    };
    Ok((addr, nonce))
}

fn encode_link_nonce(kind: LinkKind, offset: u8, nonce: u64) -> Vec<u8> {
    let mut out = vec![link_base(kind) + offset];
    out.extend_from_slice(&nonce.to_be_bytes());
    out
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    bytes
        .get(offset..offset + 8)
        .map(|b| u64::from_be_bytes(b.try_into().unwrap()))
        .ok_or(Error::InvalidPacket("truncated integer"))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    bytes
        .get(offset..offset + 4)
        .map(|b| u32::from_be_bytes(b.try_into().unwrap()))
        .ok_or(Error::InvalidPacket("truncated integer"))
}

/// Messages between clients and the server, sent to `ROOT_PLUGIN_ID`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootMsg {
    /// Server to client: this is your id.
    Id(NodeId),
    /// Server to client: the current clients and which one is the master, with a
    /// fixed field for each nickname, as in the C++ library.
    NodeList {
        master: NodeId,
        nodes: BTreeMap<NodeId, String>,
    },
    SetMaster,
    /// Server to a client that has just connected: the current clients, with the master.
    Members {
        master: NodeId,
        nodes: BTreeMap<NodeId, String>,
    },
    /// Server to the other clients: a client has connected.
    MemberJoined {
        id: NodeId,
        nickname: String,
        master: NodeId,
    },
    /// Server to the clients: a client has left, and who the master is now.
    MemberLeft {
        id: NodeId,
        master: NodeId,
    },
    /// Client to server: first packet of a connection.
    Hello(String),
    /// Client to server: first packet of a connection that replaces a lost one. The
    /// client gets the id it had, if it is still free in this server.
    HelloAgain(String, NodeId),
    QuitAll,
    Bye,
    /// New server to a server of the group: I am joining, who is there? The number is
    /// drawn by each server when it starts, to tell apart two servers with the same id.
    PeerStart(LinkKind, PeerAddr, u64),
    /// Answer to `PeerStart`: the servers of the group, including the sender.
    PeerList(LinkKind, BTreeMap<NodeId, PeerAddr>),
    /// New server to the other servers it found in the list.
    PeerListUpdate(LinkKind, PeerAddr, u64),
    /// Answer to `PeerStart`: your id is taken, use this one.
    PeerChangeId(LinkKind, NodeId),
    /// Answer to `PeerStart` and `PeerListUpdate`: the link is made. It carries the
    /// number of the server that answers.
    PeerAccepted(LinkKind, u64),
    /// Answer to `PeerListUpdate`: another server already has your id, so start over
    /// with another one. The connection is closed after it.
    PeerIdTaken(LinkKind),
    /// Client to server, through both the connection and UDP, to link the two.
    UdpRegister(u64),
    /// Server to client: the port to send datagrams to, or 0 if it has no UDP.
    UdpPort(u16),
    /// Server to client: a datagram with the token has arrived.
    UdpReady,
}

impl RootMsg {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            RootMsg::Id(id) => {
                let mut out = vec![0];
                out.extend_from_slice(&id.to_be_bytes());
                out
            }
            RootMsg::NodeList { master, nodes } => {
                let mut out =
                    Vec::with_capacity(NODE_LIST_HEADER_LEN + nodes.len() * NODE_ENTRY_LEN);
                out.push(1);
                out.extend_from_slice(&(nodes.len() as u32).to_be_bytes());
                out.extend_from_slice(&master.to_be_bytes());
                for (id, nickname) in nodes {
                    out.extend_from_slice(&id.to_be_bytes());
                    let mut field = [0u8; NICKNAME_FIELD_LEN];
                    let nick = truncated(nickname);
                    field[..nick.len()].copy_from_slice(nick.as_bytes());
                    out.extend_from_slice(&field);
                }
                out
            }
            RootMsg::SetMaster => vec![2],
            RootMsg::Hello(nickname) => {
                let mut out = vec![3];
                out.extend_from_slice(nickname.as_bytes());
                out
            }
            RootMsg::QuitAll => vec![4],
            RootMsg::Bye => vec![5],
            RootMsg::UdpRegister(token) => {
                let mut out = vec![6];
                out.extend_from_slice(&token.to_be_bytes());
                out
            }
            RootMsg::UdpPort(port) => {
                let mut out = vec![7];
                out.extend_from_slice(&port.to_be_bytes());
                out
            }
            RootMsg::UdpReady => vec![8],
            RootMsg::Members { master, nodes } => {
                let mut out = vec![10];
                out.extend_from_slice(&master.to_be_bytes());
                out.extend_from_slice(&(nodes.len() as u32).to_be_bytes());
                for (id, nickname) in nodes {
                    let nick = truncated(nickname);
                    out.extend_from_slice(&id.to_be_bytes());
                    out.push(nick.len() as u8);
                    out.extend_from_slice(nick.as_bytes());
                }
                out
            }
            RootMsg::MemberJoined {
                id,
                nickname,
                master,
            } => {
                let mut out = vec![11];
                out.extend_from_slice(&id.to_be_bytes());
                out.extend_from_slice(&master.to_be_bytes());
                out.extend_from_slice(truncated(nickname).as_bytes());
                out
            }
            RootMsg::MemberLeft { id, master } => {
                let mut out = vec![12];
                out.extend_from_slice(&id.to_be_bytes());
                out.extend_from_slice(&master.to_be_bytes());
                out
            }
            RootMsg::HelloAgain(nickname, id) => {
                let mut out = vec![9];
                out.extend_from_slice(&id.to_be_bytes());
                out.extend_from_slice(nickname.as_bytes());
                out
            }
            RootMsg::PeerStart(kind, addr, nonce) => {
                encode_link_addr(*kind, LINK_START, addr, *nonce)
            }
            RootMsg::PeerListUpdate(kind, addr, nonce) => {
                encode_link_addr(*kind, LINK_LIST_UPDATE, addr, *nonce)
            }
            RootMsg::PeerAccepted(kind, nonce) => encode_link_nonce(*kind, LINK_ACCEPTED, *nonce),
            RootMsg::PeerIdTaken(kind) => vec![link_base(*kind) + LINK_ID_TAKEN],
            RootMsg::PeerList(kind, peers) => {
                let mut out =
                    Vec::with_capacity(PEER_LIST_HEADER_LEN + peers.len() * PEER_ENTRY_LEN);
                out.push(link_base(*kind) + LINK_LIST);
                out.extend_from_slice(&link_relation(*kind).to_be_bytes());
                out.extend_from_slice(&(peers.len() as u32).to_be_bytes());
                for (id, addr) in peers {
                    out.extend_from_slice(&id.to_be_bytes());
                    out.extend_from_slice(&u32::from(addr.port).to_be_bytes());
                    let mut field = [0u8; HOST_FIELD_LEN];
                    let host = truncated(&addr.host);
                    field[..host.len()].copy_from_slice(host.as_bytes());
                    out.extend_from_slice(&field);
                }
                out
            }
            RootMsg::PeerChangeId(kind, id) => {
                let mut out = vec![link_base(*kind) + LINK_CHANGE_ID];
                out.extend_from_slice(&id.to_be_bytes());
                out
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<RootMsg> {
        let kind = *bytes.first().ok_or(Error::InvalidPacket("empty payload"))?;
        match kind {
            0 => Ok(RootMsg::Id(read_u32(bytes, 1)?)),
            1 => {
                let count = read_u32(bytes, 1)? as usize;
                let master = read_u32(bytes, 5)?;
                let entries = &bytes[NODE_LIST_HEADER_LEN..];
                if entries.len() / NODE_ENTRY_LEN < count {
                    return Err(Error::InvalidPacket("truncated node list"));
                }
                let mut nodes = BTreeMap::new();
                for entry in entries.as_chunks::<NODE_ENTRY_LEN>().0.iter().take(count) {
                    let id = read_u32(entry, 0)?;
                    let field = &entry[4..];
                    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
                    nodes.insert(id, String::from_utf8_lossy(&field[..end]).into_owned());
                }
                Ok(RootMsg::NodeList { master, nodes })
            }
            2 => Ok(RootMsg::SetMaster),
            3 => Ok(RootMsg::Hello(
                String::from_utf8_lossy(&bytes[1..]).into_owned(),
            )),
            4 => Ok(RootMsg::QuitAll),
            5 => Ok(RootMsg::Bye),
            6 => {
                let token = bytes
                    .get(1..9)
                    .ok_or(Error::InvalidPacket("truncated token"))?;
                Ok(RootMsg::UdpRegister(u64::from_be_bytes(
                    token.try_into().unwrap(),
                )))
            }
            7 => {
                let port = bytes
                    .get(1..3)
                    .ok_or(Error::InvalidPacket("truncated UDP port"))?;
                Ok(RootMsg::UdpPort(u16::from_be_bytes(
                    port.try_into().unwrap(),
                )))
            }
            8 => Ok(RootMsg::UdpReady),
            10 => {
                let master = read_u32(bytes, 1)?;
                let count = read_u32(bytes, 5)? as usize;
                let mut nodes = BTreeMap::new();
                let mut at = 9;
                for _ in 0..count {
                    let id = read_u32(bytes, at)?;
                    let len = *bytes
                        .get(at + 4)
                        .ok_or(Error::InvalidPacket("truncated member list"))?
                        as usize;
                    let nick = bytes
                        .get(at + 5..at + 5 + len)
                        .ok_or(Error::InvalidPacket("truncated member list"))?;
                    nodes.insert(id, String::from_utf8_lossy(nick).into_owned());
                    at += 5 + len;
                }
                Ok(RootMsg::Members { master, nodes })
            }
            11 => Ok(RootMsg::MemberJoined {
                id: read_u32(bytes, 1)?,
                master: read_u32(bytes, 5)?,
                nickname: String::from_utf8_lossy(&bytes[9..]).into_owned(),
            }),
            12 => Ok(RootMsg::MemberLeft {
                id: read_u32(bytes, 1)?,
                master: read_u32(bytes, 5)?,
            }),
            9 => Ok(RootMsg::HelloAgain(
                String::from_utf8_lossy(bytes.get(5..).unwrap_or_default()).into_owned(),
                read_u32(bytes, 1)?,
            )),
            PEER_BASE..=LINK_LAST => decode_link_message(bytes, kind),
            _ => Err(Error::InvalidPacket("unknown root message")),
        }
    }
}

fn decode_link_message(bytes: &[u8], code: u8) -> Result<RootMsg> {
    let (link, offset) = if code >= REMOTE_PEER_BASE {
        (LinkKind::Remote, code - REMOTE_PEER_BASE)
    } else {
        (LinkKind::Peer, code - PEER_BASE)
    };
    match offset {
        LINK_START => {
            let (addr, nonce) = decode_link_addr(bytes)?;
            Ok(RootMsg::PeerStart(link, addr, nonce))
        }
        LINK_HELLO | LINK_LIST_UPDATE => {
            let (addr, nonce) = decode_link_addr(bytes)?;
            Ok(RootMsg::PeerListUpdate(link, addr, nonce))
        }
        LINK_ACCEPTED => Ok(RootMsg::PeerAccepted(link, read_u64(bytes, 1)?)),
        LINK_ID_TAKEN => Ok(RootMsg::PeerIdTaken(link)),
        LINK_LIST => {
            let count = read_u32(bytes, 5)? as usize;
            let entries = &bytes[PEER_LIST_HEADER_LEN..];
            if entries.len() / PEER_ENTRY_LEN < count {
                return Err(Error::InvalidPacket("truncated peer list"));
            }
            let mut peers = BTreeMap::new();
            for entry in entries.as_chunks::<PEER_ENTRY_LEN>().0.iter().take(count) {
                let field = &entry[8..];
                let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
                peers.insert(
                    read_u32(entry, 0)?,
                    PeerAddr {
                        host: String::from_utf8_lossy(&field[..end]).into_owned(),
                        port: read_port(entry, 4)?,
                    },
                );
            }
            Ok(RootMsg::PeerList(link, peers))
        }
        LINK_CHANGE_ID => Ok(RootMsg::PeerChangeId(link, read_u32(bytes, 1)?)),
        _ => Err(Error::InvalidPacket("unknown peer message")),
    }
}

/// Names travel in fixed NUL-terminated fields of this size.
fn truncated(name: &str) -> &str {
    let mut end = name.len().min(NICKNAME_FIELD_LEN - 1);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    &name[..end]
}

/// Encodes `[kind][name\0][data]`, the format of event and shared packets.
fn encode_named(kind: u8, name: &str, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + name.len() + data.len());
    out.push(kind);
    out.extend_from_slice(name.as_bytes());
    out.push(0);
    out.extend_from_slice(data);
    out
}

fn decode_named(bytes: &[u8]) -> Result<(u8, String, &[u8])> {
    let kind = *bytes.first().ok_or(Error::InvalidPacket("empty payload"))?;
    let rest = &bytes[1..];
    let end = rest
        .iter()
        .position(|&b| b == 0)
        .ok_or(Error::InvalidPacket("unterminated name"))?;
    let name = std::str::from_utf8(&rest[..end])
        .map_err(|_| Error::InvalidPacket("name is not UTF-8"))?
        .to_owned();
    Ok((kind, name, &rest[end + 1..]))
}

/// Event plugin messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventMsg {
    CreateLocal(String),
    CreateGlobal(String),
    Delete(String),
    Event(String, Vec<u8>),
}

impl EventMsg {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            EventMsg::CreateLocal(name) => encode_named(0, name, &[]),
            EventMsg::CreateGlobal(name) => encode_named(1, name, &[]),
            EventMsg::Delete(name) => encode_named(2, name, &[]),
            EventMsg::Event(name, data) => encode_named(3, name, data),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<EventMsg> {
        let (kind, name, data) = decode_named(bytes)?;
        match kind {
            0 => Ok(EventMsg::CreateLocal(name)),
            1 => Ok(EventMsg::CreateGlobal(name)),
            2 => Ok(EventMsg::Delete(name)),
            3 => Ok(EventMsg::Event(name, data.to_vec())),
            _ => Err(Error::InvalidPacket("unknown event message")),
        }
    }
}

/// Shared plugin messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SharedMsg {
    CreateLocal(String),
    CreateGlobal(String),
    /// A new value, and its version. Clients send 0: the server numbers it.
    Update(String, u64, Vec<u8>),
    Delete(String),
    Sync(String),
    Changed(String, Vec<u8>),
}

impl SharedMsg {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            SharedMsg::CreateLocal(name) => encode_named(0, name, &[]),
            SharedMsg::CreateGlobal(name) => encode_named(1, name, &[]),
            SharedMsg::Update(name, version, data) => {
                let mut content = version.to_be_bytes().to_vec();
                content.extend_from_slice(data);
                encode_named(2, name, &content)
            }
            SharedMsg::Delete(name) => encode_named(3, name, &[]),
            SharedMsg::Sync(name) => encode_named(4, name, &[]),
            SharedMsg::Changed(name, data) => encode_named(5, name, data),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<SharedMsg> {
        let (kind, name, data) = decode_named(bytes)?;
        match kind {
            0 => Ok(SharedMsg::CreateLocal(name)),
            1 => Ok(SharedMsg::CreateGlobal(name)),
            2 => {
                let version = read_u64(data, 0)?;
                Ok(SharedMsg::Update(name, version, data[8..].to_vec()))
            }
            3 => Ok(SharedMsg::Delete(name)),
            4 => Ok(SharedMsg::Sync(name)),
            5 => Ok(SharedMsg::Changed(name, data.to_vec())),
            _ => Err(Error::InvalidPacket("unknown shared message")),
        }
    }
}

/// Barrier plugin message kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BarrierKind {
    CreateLocal = 0,
    CreateGlobal = 1,
    Sync = 2,
    Delete = 3,
    GetOut = 4,
    Disable = 5,
    Enable = 6,
}

/// Barrier plugin messages: `[kind][number: u32][group: u32][active: u8]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BarrierMsg {
    pub kind: BarrierKind,
    pub number: u32,
    pub group: u32,
    pub active: bool,
}

impl BarrierMsg {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(10);
        out.push(self.kind as u8);
        out.extend_from_slice(&self.number.to_be_bytes());
        out.extend_from_slice(&self.group.to_be_bytes());
        out.push(self.active as u8);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<BarrierMsg> {
        if bytes.len() < 10 {
            return Err(Error::InvalidPacket("truncated barrier message"));
        }
        let kind = match bytes[0] {
            0 => BarrierKind::CreateLocal,
            1 => BarrierKind::CreateGlobal,
            2 => BarrierKind::Sync,
            3 => BarrierKind::Delete,
            4 => BarrierKind::GetOut,
            5 => BarrierKind::Disable,
            6 => BarrierKind::Enable,
            _ => return Err(Error::InvalidPacket("unknown barrier message")),
        };
        Ok(BarrierMsg {
            kind,
            number: read_u32(bytes, 1)?,
            group: read_u32(bytes, 5)?,
            active: bytes[9] != 0,
        })
    }
}

/// Alias plugin messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AliasMsg {
    CreateLocal(String),
    CreateGlobal(String),
    Delete(String),
    /// `[kind][name\0][target: u32][association]`.
    Associate {
        name: String,
        target: NodeId,
        association: String,
    },
}

impl AliasMsg {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            AliasMsg::CreateLocal(name) => encode_named(0, name, &[]),
            AliasMsg::CreateGlobal(name) => encode_named(1, name, &[]),
            AliasMsg::Delete(name) => encode_named(2, name, &[]),
            AliasMsg::Associate {
                name,
                target,
                association,
            } => {
                let mut data = target.to_be_bytes().to_vec();
                data.extend_from_slice(association.as_bytes());
                encode_named(3, name, &data)
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<AliasMsg> {
        let (kind, name, data) = decode_named(bytes)?;
        match kind {
            0 => Ok(AliasMsg::CreateLocal(name)),
            1 => Ok(AliasMsg::CreateGlobal(name)),
            2 => Ok(AliasMsg::Delete(name)),
            3 => {
                let target = read_u32(data, 0)?;
                let association = std::str::from_utf8(&data[4..])
                    .map_err(|_| Error::InvalidPacket("association is not UTF-8"))?
                    .to_owned();
                Ok(AliasMsg::Associate {
                    name,
                    target,
                    association,
                })
            }
            _ => Err(Error::InvalidPacket("unknown alias message")),
        }
    }
}

/// Remote plugin messages. The node they are for goes in the packet's targets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoteMsg {
    Create(String),
    Delete(String),
    Set(String, Vec<u8>),
    Get(String),
    /// The answer to `Get`.
    Value(String, Vec<u8>),
}

impl RemoteMsg {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            RemoteMsg::Create(name) => encode_named(0, name, &[]),
            RemoteMsg::Delete(name) => encode_named(1, name, &[]),
            RemoteMsg::Set(name, data) => encode_named(2, name, data),
            RemoteMsg::Get(name) => encode_named(3, name, &[]),
            RemoteMsg::Value(name, data) => encode_named(4, name, data),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<RemoteMsg> {
        let (kind, name, data) = decode_named(bytes)?;
        match kind {
            0 => Ok(RemoteMsg::Create(name)),
            1 => Ok(RemoteMsg::Delete(name)),
            2 => Ok(RemoteMsg::Set(name, data.to_vec())),
            3 => Ok(RemoteMsg::Get(name)),
            4 => Ok(RemoteMsg::Value(name, data.to_vec())),
            _ => Err(Error::InvalidPacket("unknown remote message")),
        }
    }
}

/// Messenger plugin messages. The C++ plugin is not part of its build, and cannot
/// be compiled anymore, so there is no format to be compatible with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessengerMsg {
    CreateLocal(String),
    CreateGlobal(String),
    Delete(String),
    /// `[kind][name\0][target: u32][data]`.
    Message {
        name: String,
        target: NodeId,
        data: Vec<u8>,
    },
}

impl MessengerMsg {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            MessengerMsg::CreateLocal(name) => encode_named(b'C', name, &[]),
            MessengerMsg::CreateGlobal(name) => encode_named(b'G', name, &[]),
            MessengerMsg::Delete(name) => encode_named(b'D', name, &[]),
            MessengerMsg::Message { name, target, data } => {
                let mut content = target.to_be_bytes().to_vec();
                content.extend_from_slice(data);
                encode_named(b'M', name, &content)
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<MessengerMsg> {
        let (kind, name, data) = decode_named(bytes)?;
        match kind {
            b'C' => Ok(MessengerMsg::CreateLocal(name)),
            b'G' => Ok(MessengerMsg::CreateGlobal(name)),
            b'D' => Ok(MessengerMsg::Delete(name)),
            b'M' => Ok(MessengerMsg::Message {
                name,
                target: read_u32(data, 0)?,
                data: data[4..].to_vec(),
            }),
            _ => Err(Error::InvalidPacket("unknown messenger message")),
        }
    }
}

/// `NOBODY_ID` means that no client is the master.
pub const NO_MASTER: NodeId = NOBODY_ID;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_messages_match_the_cpp_format() {
        assert_eq!(RootMsg::Id(0x10001).encode(), [0, 0, 1, 0, 1]);
        assert_eq!(RootMsg::Hello("ab".into()).encode(), [3, b'a', b'b']);
        assert_eq!(RootMsg::QuitAll.encode(), [4]);
        assert_eq!(RootMsg::Bye.encode(), [5]);
    }

    #[test]
    fn node_list_uses_fixed_nickname_fields() {
        let msg = RootMsg::NodeList {
            master: 0x10001,
            nodes: BTreeMap::from([(0x10001, "a".to_owned()), (0x10002, "bc".to_owned())]),
        };
        let bytes = msg.encode();
        assert_eq!(bytes.len(), 9 + 2 * 260);
        assert_eq!(bytes[..9], [1, 0, 0, 0, 2, 0, 1, 0, 1]);
        assert_eq!(bytes[9..14], [0, 1, 0, 1, b'a']);
        assert_eq!(bytes[14], 0);
        assert_eq!(RootMsg::decode(&bytes).unwrap(), msg);
    }

    #[test]
    fn member_messages_only_carry_what_they_need() {
        let members = RootMsg::Members {
            master: 0x10001,
            nodes: BTreeMap::from([(0x10001, "a".to_owned()), (0x10002, "bc".to_owned())]),
        };
        let bytes = members.encode();
        assert_eq!(bytes.len(), 9 + (5 + 1) + (5 + 2));
        assert_eq!(RootMsg::decode(&bytes).unwrap(), members);

        let joined = RootMsg::MemberJoined {
            id: 0x10003,
            nickname: "ana".into(),
            master: 0x10001,
        };
        assert_eq!(joined.encode().len(), 9 + 3);
        assert_eq!(RootMsg::decode(&joined.encode()).unwrap(), joined);

        let left = RootMsg::MemberLeft {
            id: 0x10001,
            master: 0x10002,
        };
        assert_eq!(left.encode(), [12, 0, 1, 0, 1, 0, 1, 0, 2]);
        assert_eq!(RootMsg::decode(&left.encode()).unwrap(), left);
    }

    #[test]
    fn member_list_with_lying_count_or_length_is_rejected() {
        let mut bytes = RootMsg::Members {
            master: 0,
            nodes: BTreeMap::from([(0x10001, "a".to_owned())]),
        }
        .encode();
        bytes[5..9].copy_from_slice(&2u32.to_be_bytes());
        assert!(
            RootMsg::decode(&bytes).is_err(),
            "a second member that is not there"
        );
        bytes[5..9].copy_from_slice(&1u32.to_be_bytes());
        bytes[13] = 200;
        assert!(
            RootMsg::decode(&bytes).is_err(),
            "a nickname longer than the packet"
        );
        assert!(RootMsg::decode(&[11, 0, 1]).is_err());
        assert!(RootMsg::decode(&[12, 0, 1, 0, 1]).is_err());
    }

    #[test]
    fn node_list_with_lying_count_is_rejected() {
        let mut bytes = RootMsg::NodeList {
            master: 0,
            nodes: BTreeMap::new(),
        }
        .encode();
        bytes[1..5].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(RootMsg::decode(&bytes).is_err());
    }

    #[test]
    fn peer_messages_match_the_cpp_format() {
        let addr = PeerAddr {
            host: "ab".into(),
            port: 7001,
        };
        let start = RootMsg::PeerStart(LinkKind::Peer, addr.clone(), 0);
        assert_eq!(
            start.encode(),
            [101, 0, 0, 0, 3, 0, 0, 0x1B, 0x59, b'a', b'b']
        );
        assert_eq!(RootMsg::decode(&start.encode()).unwrap(), start);

        let update = RootMsg::PeerListUpdate(LinkKind::Remote, addr, 0);
        assert_eq!(
            update.encode(),
            [113, 0, 0, 0, 4, 0, 0, 0x1B, 0x59, b'a', b'b']
        );
        assert_eq!(RootMsg::decode(&update.encode()).unwrap(), update);

        let change = RootMsg::PeerChangeId(LinkKind::Peer, 0x20000);
        assert_eq!(change.encode(), [104, 0, 2, 0, 0]);
        assert_eq!(RootMsg::decode(&change.encode()).unwrap(), change);
    }

    #[test]
    fn hello_again_carries_the_old_id() {
        let hello = RootMsg::HelloAgain("ana".into(), 0x10003);
        assert_eq!(hello.encode(), [9, 0, 1, 0, 3, b'a', b'n', b'a']);
        assert_eq!(RootMsg::decode(&hello.encode()).unwrap(), hello);
        assert!(RootMsg::decode(&[9, 0, 1]).is_err());
    }

    #[test]
    fn link_messages_carry_the_number_of_the_server() {
        let addr = PeerAddr {
            host: "ab".into(),
            port: 7001,
        };
        let start = RootMsg::PeerStart(LinkKind::Peer, addr.clone(), 0x0102_0304_0506_0708);
        let bytes = start.encode();
        assert_eq!(bytes[9..], [b'a', b'b', 0, 1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(RootMsg::decode(&bytes).unwrap(), start);

        let update = RootMsg::PeerListUpdate(LinkKind::Remote, addr, 9);
        assert_eq!(RootMsg::decode(&update.encode()).unwrap(), update);

        for message in [
            RootMsg::PeerAccepted(LinkKind::Remote, 9),
            RootMsg::PeerIdTaken(LinkKind::Peer),
            RootMsg::PeerIdTaken(LinkKind::Remote),
        ] {
            assert_eq!(RootMsg::decode(&message.encode()).unwrap(), message);
        }
        assert!(RootMsg::decode(&[105, 0, 0]).is_err(), "a truncated number");
    }

    #[test]
    fn peer_list_uses_fixed_host_fields() {
        let msg = RootMsg::PeerList(
            LinkKind::Peer,
            BTreeMap::from([(
                0x20000,
                PeerAddr {
                    host: "ab".into(),
                    port: 7001,
                },
            )]),
        );
        let bytes = msg.encode();
        assert_eq!(bytes.len(), 9 + 264);
        assert_eq!(bytes[..9], [102, 0, 0, 0, 3, 0, 0, 0, 1]);
        assert_eq!(bytes[9..19], [0, 2, 0, 0, 0, 0, 0x1B, 0x59, b'a', b'b']);
        assert_eq!(RootMsg::decode(&bytes).unwrap(), msg);
    }

    #[test]
    fn invalid_peer_messages_are_rejected() {
        // A list that claims more entries than it has.
        assert!(RootMsg::decode(&[102, 0, 0, 0, 3, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
        // A port that does not fit.
        assert!(RootMsg::decode(&[101, 0, 0, 0, 3, 0, 1, 0, 0]).is_err());
        // Truncated.
        assert!(RootMsg::decode(&[101, 0]).is_err());
        assert!(RootMsg::decode(&[104, 0]).is_err());
        // Not used by anything: 105 and 115 are never sent by the C++ library.
        assert!(RootMsg::decode(&[105, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn long_nickname_is_cut_at_a_char_boundary() {
        let long = "é".repeat(200);
        assert_eq!(truncated(&long).len(), 254);
    }

    #[test]
    fn event_messages_match_the_cpp_format() {
        assert_eq!(
            EventMsg::CreateGlobal("ev".into()).encode(),
            [1, b'e', b'v', 0]
        );
        let msg = EventMsg::Event("ev".into(), vec![0, 7]);
        assert_eq!(msg.encode(), [3, b'e', b'v', 0, 0, 7]);
        assert_eq!(EventMsg::decode(&msg.encode()).unwrap(), msg);
    }

    #[test]
    fn named_message_without_terminator_is_rejected() {
        assert!(EventMsg::decode(&[3, b'e', b'v']).is_err());
        assert!(SharedMsg::decode(&[]).is_err());
    }

    #[test]
    fn shared_messages_match_the_cpp_format() {
        assert_eq!(SharedMsg::Sync("x".into()).encode(), [4, b'x', 0]);
        let msg = SharedMsg::Changed("x".into(), vec![9]);
        assert_eq!(msg.encode(), [5, b'x', 0, 9]);
        assert_eq!(SharedMsg::decode(&msg.encode()).unwrap(), msg);

        let update = SharedMsg::Update("x".into(), 0x0102, vec![9]);
        assert_eq!(update.encode(), [2, b'x', 0, 0, 0, 0, 0, 0, 0, 1, 2, 9]);
        assert_eq!(SharedMsg::decode(&update.encode()).unwrap(), update);
        assert!(
            SharedMsg::decode(&[2, b'x', 0, 1, 2]).is_err(),
            "no version"
        );
    }

    #[test]
    fn alias_messages_match_the_cpp_format() {
        assert_eq!(AliasMsg::CreateLocal("a".into()).encode(), [0, b'a', 0]);
        assert_eq!(AliasMsg::CreateGlobal("a".into()).encode(), [1, b'a', 0]);
        assert_eq!(AliasMsg::Delete("a".into()).encode(), [2, b'a', 0]);

        let msg = AliasMsg::Associate {
            name: "a".into(),
            target: 0x10001,
            association: "xy".into(),
        };
        assert_eq!(msg.encode(), [3, b'a', 0, 0, 1, 0, 1, b'x', b'y']);
        assert_eq!(AliasMsg::decode(&msg.encode()).unwrap(), msg);
    }

    #[test]
    fn invalid_alias_messages_are_rejected() {
        assert!(AliasMsg::decode(&[3, b'a', 0, 0, 1]).is_err());
        assert!(AliasMsg::decode(&[3, b'a', 0, 0, 1, 0, 1, 0xFF]).is_err());
        assert!(AliasMsg::decode(&[9, b'a', 0]).is_err());
    }

    #[test]
    fn remote_messages_match_the_cpp_format() {
        assert_eq!(RemoteMsg::Create("r".into()).encode(), [0, b'r', 0]);
        assert_eq!(RemoteMsg::Delete("r".into()).encode(), [1, b'r', 0]);
        assert_eq!(RemoteMsg::Get("r".into()).encode(), [3, b'r', 0]);
        let set = RemoteMsg::Set("r".into(), vec![7, 0]);
        assert_eq!(set.encode(), [2, b'r', 0, 7, 0]);
        assert_eq!(RemoteMsg::decode(&set.encode()).unwrap(), set);
        let value = RemoteMsg::Value("r".into(), vec![7]);
        assert_eq!(value.encode(), [4, b'r', 0, 7]);
        assert_eq!(RemoteMsg::decode(&value.encode()).unwrap(), value);
        assert!(RemoteMsg::decode(&[9, b'r', 0]).is_err());
    }

    #[test]
    fn messenger_and_udp_messages_round_trip() {
        for msg in [
            MessengerMsg::CreateLocal("m".into()),
            MessengerMsg::CreateGlobal("m".into()),
            MessengerMsg::Delete("m".into()),
            MessengerMsg::Message {
                name: "m".into(),
                target: 7,
                data: vec![0, 1],
            },
        ] {
            assert_eq!(MessengerMsg::decode(&msg.encode()).unwrap(), msg);
        }
        assert!(MessengerMsg::decode(&[b'M', b'm', 0, 1]).is_err());
        assert!(MessengerMsg::decode(&[b'X', b'm', 0]).is_err());

        for msg in [
            RootMsg::UdpRegister(0x0102030405060708),
            RootMsg::UdpPort(9191),
            RootMsg::UdpReady,
        ] {
            assert_eq!(RootMsg::decode(&msg.encode()).unwrap(), msg);
        }
        assert!(RootMsg::decode(&[6, 1, 2]).is_err());
        assert!(RootMsg::decode(&[7]).is_err());
    }

    #[test]
    fn barrier_messages_match_the_cpp_format() {
        let msg = BarrierMsg {
            kind: BarrierKind::GetOut,
            number: 2,
            group: 3,
            active: true,
        };
        assert_eq!(msg.encode(), [4, 0, 0, 0, 2, 0, 0, 0, 3, 1]);
        assert_eq!(BarrierMsg::decode(&msg.encode()).unwrap(), msg);
        assert!(BarrierMsg::decode(&[4, 0]).is_err());
    }
}
