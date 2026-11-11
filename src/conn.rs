use crate::error::{Error, Result};
#[cfg(unix)]
use crate::transport::sctp_local_addr;
use crate::transport::Stream;
use std::collections::VecDeque;
use std::io::{self, Write};
use std::net::IpAddr;
#[cfg(unix)]
use std::net::Ipv4Addr;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// Without this, a client whose server stops reading would block forever.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a server's writing thread waits for a receiver that has stopped reading.
/// It only holds back the packets of that connection.
const QUEUED_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// How much a server may hold for one connection. A receiver that falls this far
/// behind is disconnected, as it would otherwise take all the memory.
pub(crate) const MAX_QUEUED_BYTES: usize = 64 * 1024 * 1024;

/// The sending side of a connection.
pub(crate) struct Conn {
    output: Output,
    /// Another handle to the socket, to shut it down without waiting for a write.
    closer: Stream,
    local_ip: IpAddr,
}

enum Output {
    /// The caller writes, and waits for it: what a client does, so that a fast sender
    /// is slowed down by the network.
    Direct(Mutex<Stream>),
    /// A thread writes what is queued: what a server does, so that a slow receiver
    /// only delays its own packets.
    Queued(Arc<Queue>),
}

#[derive(Default)]
struct Queue {
    state: Mutex<QueueState>,
    changed: Condvar,
}

#[derive(Default)]
struct QueueState {
    frames: VecDeque<Vec<u8>>,
    bytes: usize,
    /// No more frames are taken. The queued ones are still written.
    closing: bool,
    /// Writing failed, so what is queued will never be written.
    failed: bool,
}

impl Conn {
    /// A connection whose writes block the caller. Returns it and a separate handle
    /// for the reading thread.
    pub(crate) fn new(stream: Stream) -> Result<(Arc<Conn>, Stream)> {
        stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
        let writer = stream.try_clone()?;
        Conn::build(stream, Output::Direct(Mutex::new(writer)))
    }

    /// A connection with its own writing thread, which takes the packets from a queue.
    pub(crate) fn new_queued(stream: Stream) -> Result<(Arc<Conn>, Stream)> {
        stream.set_write_timeout(Some(QUEUED_WRITE_TIMEOUT))?;
        let writer = stream.try_clone()?;
        let queue = Arc::new(Queue::default());
        {
            let queue = queue.clone();
            thread::spawn(move || write_queue(&queue, writer));
        }
        Conn::build(stream, Output::Queued(queue))
    }

    fn build(stream: Stream, output: Output) -> Result<(Arc<Conn>, Stream)> {
        stream.set_nodelay()?;
        let reader = stream.try_clone()?;
        let local_ip = match &stream {
            Stream::Tcp(s) => s.local_addr()?.ip(),
            #[cfg(unix)]
            Stream::Sctp(s) => sctp_local_addr(s)?.ip(),
            #[cfg(unix)]
            Stream::Unix(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            #[cfg(feature = "quic")]
            Stream::Quic(s) => s.local_ip(),
        };
        let conn = Conn {
            output,
            closer: stream,
            local_ip,
        };
        Ok((Arc::new(conn), reader))
    }

    /// The address of this machine on the network used by this connection.
    pub(crate) fn local_ip(&self) -> IpAddr {
        self.local_ip
    }

    /// Writes a whole packet, or queues it. On failure the connection is closed,
    /// because a partial write would corrupt the framing of the next packets.
    pub(crate) fn send(&self, frame: &[u8]) -> Result<()> {
        match &self.output {
            Output::Direct(writer) => {
                let mut writer = writer.lock().unwrap();
                if let Err(e) = writer.write_all(frame) {
                    let _ = self.closer.shutdown();
                    return Err(e.into());
                }
                Ok(())
            }
            Output::Queued(queue) => {
                let mut state = queue.state.lock().unwrap();
                if state.closing || state.failed {
                    return Err(Error::Disconnected);
                }
                if state.bytes + frame.len() > MAX_QUEUED_BYTES {
                    state.failed = true;
                    state.frames.clear();
                    drop(state);
                    queue.changed.notify_all();
                    let _ = self.closer.shutdown();
                    let slow = io::Error::new(io::ErrorKind::TimedOut, "the receiver is too slow");
                    return Err(slow.into());
                }
                state.bytes += frame.len();
                state.frames.push_back(frame.to_vec());
                drop(state);
                queue.changed.notify_all();
                Ok(())
            }
        }
    }

    /// Closes both directions, waking up any thread reading from it. What is already
    /// queued is written first.
    pub(crate) fn close(&self) {
        match &self.output {
            Output::Direct(_) => {
                let _ = self.closer.shutdown();
            }
            Output::Queued(queue) => {
                queue.state.lock().unwrap().closing = true;
                queue.changed.notify_all();
            }
        }
    }
}

impl Drop for Conn {
    /// Lets the writing thread finish, after what is queued.
    fn drop(&mut self) {
        self.close();
    }
}

/// Writes the queued packets of a connection until it closes or fails, and then shuts
/// the socket down.
fn write_queue(queue: &Queue, mut stream: Stream) {
    loop {
        let frame = {
            let mut state = queue.state.lock().unwrap();
            loop {
                if state.failed {
                    return;
                }
                if let Some(frame) = state.frames.pop_front() {
                    state.bytes -= frame.len();
                    break Some(frame);
                }
                if state.closing {
                    break None;
                }
                state = queue.changed.wait(state).unwrap();
            }
        };
        let Some(frame) = frame else {
            let _ = stream.shutdown();
            return;
        };
        if let Err(e) = stream.write_all(&frame) {
            log::debug!("closing a connection that could not be written: {e}");
            let mut state = queue.state.lock().unwrap();
            state.failed = true;
            state.frames.clear();
            drop(state);
            let _ = stream.shutdown();
            return;
        }
    }
}
