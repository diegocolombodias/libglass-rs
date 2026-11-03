use crate::error::Result;
use std::io::Write;
use std::net::{IpAddr, Shutdown, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Without this, a client that stops reading would block its senders forever.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// The sending side of a TCP connection.
pub(crate) struct Conn {
    writer: Mutex<TcpStream>,
    local_ip: IpAddr,
}

impl Conn {
    /// Returns the connection and a separate handle for the reading thread.
    pub(crate) fn new(stream: TcpStream) -> Result<(Arc<Conn>, TcpStream)> {
        stream.set_nodelay(true)?;
        stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
        let reader = stream.try_clone()?;
        let local_ip = stream.local_addr()?.ip();
        let conn = Conn {
            writer: Mutex::new(stream),
            local_ip,
        };
        Ok((Arc::new(conn), reader))
    }

    /// The address of this machine on the network used by this connection.
    pub(crate) fn local_ip(&self) -> IpAddr {
        self.local_ip
    }

    /// Writes a whole packet. On failure the connection is closed, because a
    /// partial write would corrupt the framing of the next packets.
    pub(crate) fn send(&self, frame: &[u8]) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        if let Err(e) = writer.write_all(frame) {
            let _ = writer.shutdown(Shutdown::Both);
            return Err(e.into());
        }
        Ok(())
    }

    /// Closes both directions, waking up any thread reading from it.
    pub(crate) fn close(&self) {
        let _ = self.writer.lock().unwrap().shutdown(Shutdown::Both);
    }
}
