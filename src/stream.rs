use crate::client::GlassClient;
use crate::error::{Error, Result};
use crate::messenger::Messenger;
use crate::types::*;
use crate::wire::Bytes;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::Duration;

const CHUNK_SIZE: usize = 256 * 1024;

const KIND_DATA: u8 = 0;
const KIND_END: u8 = 1;
const KIND_ABORT: u8 = 2;

fn frame(transfer: u32, kind: u8, data: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(5 + data.len());
    out.extend_from_slice(&transfer.to_be_bytes());
    out.push(kind);
    out.extend_from_slice(data);
    Bytes(out)
}

/// Frames of transfers other than the one being received, in order of arrival.
#[derive(Default)]
struct Pending {
    transfers: BTreeMap<(NodeId, u32), VecDeque<(u8, Vec<u8>)>>,
    arrival: VecDeque<(NodeId, u32)>,
}

impl Pending {
    fn push(&mut self, key: (NodeId, u32), kind: u8, data: Vec<u8>) {
        let queue = self.transfers.entry(key).or_insert_with(|| {
            self.arrival.push_back(key);
            VecDeque::new()
        });
        queue.push_back((kind, data));
    }

    fn pop(&mut self, key: (NodeId, u32)) -> Option<(u8, Vec<u8>)> {
        self.transfers.get_mut(&key)?.pop_front()
    }

    fn forget(&mut self, key: (NodeId, u32)) {
        self.transfers.remove(&key);
        self.arrival.retain(|k| *k != key);
    }
}

/// Sends large amounts of data, such as files, from one node to another.
///
/// The data is split into messages, so a transfer does not hold back the other
/// plugins. Data is kept in memory by the receiver until it calls
/// [`receive`](Self::receive), so the sender can get far ahead of it.
pub struct Stream {
    messenger: Messenger<Bytes>,
    next_transfer: AtomicU32,
    pending: Mutex<Pending>,
}

impl Stream {
    /// Creates a stream. Each client can only have one with a given name.
    pub fn new(client: &GlassClient, name: &str, mode: Mode) -> Result<Stream> {
        if name.is_empty() {
            return Err(Error::InvalidName);
        }
        Ok(Stream {
            messenger: Messenger::new(client, &format!("stream:{name}"), mode)?,
            next_transfer: AtomicU32::new(0),
            pending: Mutex::new(Pending::default()),
        })
    }

    /// Sends everything from a reader to a node, and returns how many bytes it
    /// sent. If reading fails, the receiver is told that the transfer failed.
    pub fn send_from<R: Read>(&self, id: NodeId, reader: &mut R) -> Result<u64> {
        let transfer = self.next_transfer.fetch_add(1, Ordering::SeqCst);
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut total = 0u64;
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    self.messenger
                        .send(id, &frame(transfer, KIND_DATA, &buffer[..n]))?;
                    total += n as u64;
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    self.messenger.send(id, &frame(transfer, KIND_ABORT, &[]))?;
                    return Err(e.into());
                }
            }
        }
        self.messenger.send(id, &frame(transfer, KIND_END, &[]))?;
        Ok(total)
    }

    /// Sends a file to a node.
    pub fn send_file<P: AsRef<Path>>(&self, id: NodeId, path: P) -> Result<u64> {
        self.send_from(id, &mut File::open(path)?)
    }

    /// Writes the next transfer to a writer, and returns who sent it and how many
    /// bytes it had. `timeout` applies to each piece of data, not to the whole
    /// transfer. Transfers from different nodes can arrive at the same time, and
    /// they are received one by one.
    pub fn receive<W: Write>(&self, writer: &mut W, timeout: Duration) -> Result<(NodeId, u64)> {
        let mut pending = self.pending.lock().unwrap();
        let mut current = pending.arrival.front().copied();
        let mut total = 0u64;
        loop {
            let piece = current.and_then(|key| pending.pop(key));
            let (kind, data) = match piece {
                Some(piece) => piece,
                None => {
                    let (from, Bytes(bytes)) = self.messenger.get_timeout(timeout)?;
                    if bytes.len() < 5 {
                        log::warn!("invalid stream frame from {from:#x}");
                        continue;
                    }
                    let transfer = u32::from_be_bytes(bytes[..4].try_into().unwrap());
                    let key = (from, transfer);
                    let kind = bytes[4];
                    let data = bytes[5..].to_vec();
                    if current.is_none() {
                        current = Some(key);
                    }
                    if current != Some(key) {
                        pending.push(key, kind, data);
                        continue;
                    }
                    (kind, data)
                }
            };

            let key = current.expect("a piece belongs to the current transfer");
            match kind {
                KIND_DATA => {
                    writer.write_all(&data)?;
                    total += data.len() as u64;
                }
                KIND_END => {
                    pending.forget(key);
                    return Ok((key.0, total));
                }
                _ => {
                    pending.forget(key);
                    return Err(Error::TransferAborted);
                }
            }
        }
    }

    /// Writes the next transfer to a new file.
    pub fn receive_file<P: AsRef<Path>>(
        &self,
        path: P,
        timeout: Duration,
    ) -> Result<(NodeId, u64)> {
        self.receive(&mut File::create(path)?, timeout)
    }
}
