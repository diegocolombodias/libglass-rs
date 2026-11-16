use crate::client::GlassClient;
use crate::error::{Error, Result};
use crate::messenger::Messenger;
use crate::types::*;
use crate::wire::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

const CHUNK_SIZE: usize = 256 * 1024;
/// Chunks a sender may have sent that the receiver has not written yet. With this,
/// a receiver keeps at most 2 MiB of each transfer in memory.
const WINDOW: u64 = 8;
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(60);
/// How often a wait checks whether the client has been disconnected.
const CHECK_INTERVAL: Duration = Duration::from_millis(100);

const KIND_DATA: u8 = 0;
const KIND_END: u8 = 1;
const KIND_ABORT: u8 = 2;
/// From the receiver: it has written one more chunk of the transfer.
const KIND_ACK: u8 = 3;
/// From the receiver: it gave up on the transfer, so the sender must stop.
const KIND_REJECT: u8 = 4;

fn frame(transfer: u32, kind: u8, data: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(5 + data.len());
    out.extend_from_slice(&transfer.to_be_bytes());
    out.push(kind);
    out.extend_from_slice(data);
    Bytes(out)
}

/// A data frame carries its index in the transfer, and the end frame the number of
/// data frames, so that a receiver can tell that a piece is missing. One can be lost
/// when it reaches the receiver before its stream exists there.
fn numbered(transfer: u32, kind: u8, index: u32, data: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(9 + data.len());
    out.extend_from_slice(&transfer.to_be_bytes());
    out.push(kind);
    out.extend_from_slice(&index.to_be_bytes());
    out.extend_from_slice(data);
    Bytes(out)
}

/// The index or count at the start of a numbered frame, and what follows it.
fn split_index(data: &[u8]) -> Option<(u32, &[u8])> {
    let index = u32::from_be_bytes(data.get(..4)?.try_into().ok()?);
    Some((index, &data[4..]))
}

/// A transfer: the node that sends it, and its number there.
type Key = (NodeId, u32);

/// The kind and content of each frame of a transfer.
type Frames = VecDeque<(u8, Vec<u8>)>;

/// Frames of transfers to this node that have not been written yet, in order of arrival.
#[derive(Default)]
struct Pending {
    transfers: BTreeMap<Key, Frames>,
    arrival: VecDeque<Key>,
}

impl Pending {
    fn push(&mut self, key: Key, kind: u8, data: Vec<u8>) {
        let queue = self.transfers.entry(key).or_insert_with(|| {
            self.arrival.push_back(key);
            VecDeque::new()
        });
        queue.push_back((kind, data));
    }

    fn pop(&mut self, key: Key) -> Option<(u8, Vec<u8>)> {
        self.transfers.get_mut(&key)?.pop_front()
    }

    fn forget(&mut self, key: Key) {
        self.transfers.remove(&key);
        self.arrival.retain(|k| *k != key);
    }
}

/// Where a transfer this node sends stands, as told by the receiver.
#[derive(Default)]
struct Sending {
    acknowledged: u64,
    rejected: bool,
}

/// Everything that arrives, sorted by the reading thread of the client.
#[derive(Default)]
struct Inbox {
    pending: Pending,
    /// Transfers to this node that it gave up on, whose remaining frames are dropped.
    discarded: BTreeSet<Key>,
    /// Transfers from this node, by receiver and number.
    sending: BTreeMap<Key, Sending>,
}

impl Inbox {
    fn sort(&mut self, from: NodeId, bytes: &[u8]) {
        if bytes.len() < 5 {
            log::warn!("invalid stream frame from {from:#x}");
            return;
        }
        let transfer = u32::from_be_bytes(bytes[..4].try_into().unwrap());
        let key = (from, transfer);
        let kind = bytes[4];
        match kind {
            KIND_DATA | KIND_END | KIND_ABORT => {
                if !self.discarded.contains(&key) {
                    self.pending.push(key, kind, bytes[5..].to_vec());
                } else if kind != KIND_DATA {
                    self.discarded.remove(&key);
                }
            }
            // Answers about transfers that are over are ignored.
            KIND_ACK => {
                if let Some(sending) = self.sending.get_mut(&key) {
                    sending.acknowledged += 1;
                }
            }
            KIND_REJECT => {
                if let Some(sending) = self.sending.get_mut(&key) {
                    sending.rejected = true;
                }
            }
            _ => log::warn!("unknown stream frame {kind} from {from:#x}"),
        }
    }
}

type SharedInbox = Arc<(Mutex<Inbox>, Condvar)>;

/// Sends large amounts of data, such as files, from one node to another.
///
/// The data is split into messages, so a transfer does not hold back the other
/// plugins. The receiver acknowledges each piece it writes, and the sender waits when
/// it is 2 MiB ahead, so neither side keeps a whole transfer in memory. A transfer
/// larger than that only finishes if the receiver calls [`receive`](Self::receive)
/// meanwhile, from another thread or another node.
pub struct Stream {
    messenger: Messenger<Bytes>,
    inbox: SharedInbox,
    next_transfer: AtomicU32,
    send_timeout_ms: AtomicU64,
    /// Only one call to `receive` at a time, as each takes the oldest transfer.
    receiving: Mutex<()>,
}

impl Stream {
    /// Creates a stream. Each client can only have one with a given name.
    pub fn new(client: &GlassClient, name: &str, mode: Mode) -> Result<Stream> {
        if name.is_empty() {
            return Err(Error::InvalidName);
        }
        let messenger = Messenger::new(client, &format!("stream:{name}"), mode)?;
        let inbox = SharedInbox::default();
        let sorter = inbox.clone();
        messenger.set_handler(move |from, Bytes(bytes)| {
            let (lock, changed) = &*sorter;
            lock.lock().unwrap().sort(from, &bytes);
            changed.notify_all();
        });
        Ok(Stream {
            messenger,
            inbox,
            next_transfer: AtomicU32::new(0),
            send_timeout_ms: AtomicU64::new(DEFAULT_SEND_TIMEOUT.as_millis() as u64),
            receiving: Mutex::new(()),
        })
    }

    /// How long a sender waits for the receiver to make room before it gives up,
    /// telling the receiver that the transfer failed. One minute by default.
    pub fn set_send_timeout(&self, timeout: Duration) {
        let millis = timeout.as_millis().min(u64::MAX as u128) as u64;
        self.send_timeout_ms.store(millis, Ordering::SeqCst);
    }

    /// Waits on the inbox for a while, or until something arrives.
    fn wait<'a>(
        &self,
        inbox: MutexGuard<'a, Inbox>,
        deadline: Instant,
    ) -> Result<MutexGuard<'a, Inbox>> {
        if self.messenger.is_closed() {
            return Err(Error::Disconnected);
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(Error::Timeout)?;
        let (_, changed) = &*self.inbox;
        Ok(changed
            .wait_timeout(inbox, remaining.min(CHECK_INTERVAL))
            .unwrap()
            .0)
    }

    /// Waits until the receiver has room for one more chunk.
    fn wait_for_room(&self, key: Key, sent: u64) -> Result<()> {
        let timeout = Duration::from_millis(self.send_timeout_ms.load(Ordering::SeqCst));
        let deadline = Instant::now() + timeout;
        let mut inbox = self.inbox.0.lock().unwrap();
        loop {
            let sending = &inbox.sending[&key];
            if sending.rejected {
                return Err(Error::TransferAborted);
            }
            if sent - sending.acknowledged < WINDOW {
                return Ok(());
            }
            inbox = self.wait(inbox, deadline)?;
        }
    }

    /// Sends everything from a reader to a node, and returns how many bytes it
    /// sent. If reading fails, or the receiver takes longer than the send timeout to
    /// make room, the receiver is told that the transfer failed.
    pub fn send_from<R: Read>(&self, id: NodeId, reader: &mut R) -> Result<u64> {
        let transfer = self.next_transfer.fetch_add(1, Ordering::SeqCst);
        let key = (id, transfer);
        self.inbox
            .0
            .lock()
            .unwrap()
            .sending
            .insert(key, Sending::default());
        let result = self.send_chunks(key, reader);
        self.inbox.0.lock().unwrap().sending.remove(&key);
        result
    }

    fn send_chunks<R: Read>(&self, (id, transfer): Key, reader: &mut R) -> Result<u64> {
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut total = 0u64;
        let mut sent = 0u64;
        loop {
            let n = match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    self.messenger.send(id, &frame(transfer, KIND_ABORT, &[]))?;
                    return Err(e.into());
                }
            };
            // Also after a rejection: the abort tells the receiver that nothing more
            // of this transfer is coming.
            if let Err(e) = self.wait_for_room((id, transfer), sent) {
                let _ = self.messenger.send(id, &frame(transfer, KIND_ABORT, &[]));
                return Err(e);
            }
            self.messenger
                .send(id, &numbered(transfer, KIND_DATA, sent as u32, &buffer[..n]))?;
            total += n as u64;
            sent += 1;
        }
        self.messenger
            .send(id, &numbered(transfer, KIND_END, sent as u32, &[]))?;
        Ok(total)
    }

    /// Sends a file to a node.
    pub fn send_file<P: AsRef<Path>>(&self, id: NodeId, path: P) -> Result<u64> {
        self.send_from(id, &mut File::open(path)?)
    }

    /// Writes the next transfer to a writer, and returns who sent it and how many
    /// bytes it had. `timeout` applies to each piece of data, not to the whole
    /// transfer. Transfers from different nodes can arrive at the same time, and
    /// they are received one by one, in the order in which they started.
    ///
    /// If writing fails, the sender is told to stop. If a piece of the transfer is
    /// missing, the sender is told to stop too, and the result is
    /// [`Error::TransferIncomplete`]: what was written is not the whole transfer.
    pub fn receive<W: Write>(&self, writer: &mut W, timeout: Duration) -> Result<(NodeId, u64)> {
        let _receiving = self.receiving.lock().unwrap();
        let mut total = 0u64;
        let mut next = 0u32;
        let mut deadline = Instant::now() + timeout;
        let mut inbox = self.inbox.0.lock().unwrap();
        loop {
            let Some(&key) = inbox.pending.arrival.front() else {
                inbox = self.wait(inbox, deadline)?;
                continue;
            };
            let Some((kind, data)) = inbox.pending.pop(key) else {
                inbox = self.wait(inbox, deadline)?;
                continue;
            };
            deadline = Instant::now() + timeout;
            match kind {
                KIND_DATA => {
                    // Writing may be slow, and the reading thread must not wait for it.
                    drop(inbox);
                    let Some((index, piece)) = split_index(&data).filter(|(i, _)| *i == next)
                    else {
                        self.give_up(key);
                        return Err(Error::TransferIncomplete);
                    };
                    if let Err(e) = writer.write_all(piece) {
                        self.give_up(key);
                        return Err(e.into());
                    }
                    next = index + 1;
                    total += piece.len() as u64;
                    let _ = self.messenger.send(key.0, &frame(key.1, KIND_ACK, &[]));
                    inbox = self.inbox.0.lock().unwrap();
                }
                KIND_END => {
                    inbox.pending.forget(key);
                    // Pieces lost at the end leave no gap, only a shorter count.
                    return match split_index(&data) {
                        Some((count, _)) if count == next => Ok((key.0, total)),
                        _ => Err(Error::TransferIncomplete),
                    };
                }
                _ => {
                    inbox.pending.forget(key);
                    return Err(Error::TransferAborted);
                }
            }
        }
    }

    /// Drops a transfer this node is receiving, and tells the sender to stop.
    fn give_up(&self, key: Key) {
        {
            let mut inbox = self.inbox.0.lock().unwrap();
            // Unless it is over, what is still on the way must be dropped.
            let over = inbox
                .pending
                .transfers
                .get(&key)
                .is_some_and(|frames| frames.iter().any(|(kind, _)| *kind != KIND_DATA));
            inbox.pending.forget(key);
            if !over {
                inbox.discarded.insert(key);
            }
        }
        let _ = self.messenger.send(key.0, &frame(key.1, KIND_REJECT, &[]));
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
