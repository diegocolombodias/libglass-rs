//! Glass - a distributed computing library.
//!
//! Rust port of the C++ libGlass. A [`GlassServer`] coordinates any number of
//! [`GlassClient`]s, which communicate through events ([`Event`]), shared
//! variables ([`Shared`]), remote variables ([`Remote`]), barriers ([`Barrier`])
//! and aliases ([`Alias`]).
//!
//! Servers can also work together, so that objects created with [`Mode::Global`]
//! are shared by the clients of all of them. See [`GlassServer::bind_peer`] and
//! [`GlassServer::bind_remote_peer`].
//!
//! The format of the messages is the same as the C++ library's over TCP.

mod alias;
mod barrier;
pub mod capi;
mod client;
mod conn;
mod error;
mod event;
mod messenger;
mod outbound;
mod owners;
pub mod packet;
mod plugin;
pub mod protocol;
mod remote;
mod serial;
mod server;
mod shared;
mod shm;
mod stream;
mod transport;
mod types;
mod wire;

pub use alias::Alias;
pub use barrier::Barrier;
pub use client::GlassClient;
pub use error::{Error, Result};
pub use event::Event;
pub use messenger::Messenger;
pub use plugin::{NodeKind, PacketSource, PluginMessage, ServerPlugin, MAX_RESERVED_PLUGIN_ID};
pub use remote::Remote;
pub use serial::{EventSerializable, ObjectRegistry, Serializable};
pub use server::{GlassServer, PeerInfo};
pub use shared::{Shared, UpdateMode};
pub use shm::SharedRegion;
pub use stream::Stream;
pub use types::*;
#[cfg(feature = "serde")]
pub use wire::Serde;
pub use wire::{Bytes, Raw, Wire};
