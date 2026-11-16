use std::{fmt, io};

/// Errors returned by this library.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    /// A received packet is malformed.
    InvalidPacket(&'static str),
    /// A packet exceeds the maximum size allowed on the wire.
    PacketTooLarge(usize),
    /// The connection to the other node is closed.
    Disconnected,
    /// The operation did not finish in time.
    Timeout,
    /// An object with this name or number already exists in this client.
    AlreadyExists(String),
    /// Names must be non-empty and cannot contain NUL.
    InvalidName,
    /// Received data could not be converted to the requested type.
    Decode,
    /// No node has this nickname.
    UnknownNode(String),
    /// The operation needs one specific node, not a group such as `EVERYBODY_ID`.
    InvalidTarget(u32),
    /// The sender of a [`Stream`](crate::Stream) transfer gave up on it.
    TransferAborted,
    /// A [`Stream`](crate::Stream) transfer arrived with pieces missing, so what was
    /// written is not the whole transfer.
    TransferIncomplete,
    /// A [`SharedRegion`](crate::SharedRegion) cannot have this size or offset.
    InvalidSize(usize),
    /// This plugin id is reserved for the library.
    InvalidPluginId(u16),
}

/// Result type of this library.
pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::InvalidPacket(why) => write!(f, "invalid packet: {why}"),
            Error::PacketTooLarge(len) => write!(f, "packet too large: {len} bytes"),
            Error::Disconnected => write!(f, "disconnected"),
            Error::Timeout => write!(f, "timed out"),
            Error::AlreadyExists(name) => write!(f, "already exists: {name}"),
            Error::InvalidName => write!(f, "invalid name"),
            Error::Decode => write!(f, "could not decode data"),
            Error::UnknownNode(nick) => write!(f, "unknown node: {nick}"),
            Error::InvalidTarget(id) => write!(f, "{id:#x} is not a single node"),
            Error::TransferAborted => write!(f, "the sender aborted the transfer"),
            Error::TransferIncomplete => write!(f, "the transfer arrived with pieces missing"),
            Error::InvalidSize(size) => write!(f, "invalid size: {size}"),
            Error::InvalidPluginId(id) => write!(f, "plugin id {id} is reserved"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
