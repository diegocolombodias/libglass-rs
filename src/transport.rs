//! The stream connections the library can run on.

#[cfg(unix)]
use socket2::{Domain, Protocol, Socket, Type};
use std::io::{self, ErrorKind, Read, Write};
#[cfg(unix)]
use std::net::SocketAddr;
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(unix)]
const LISTEN_BACKLOG: i32 = 128;
#[cfg(unix)]
const IPPROTO_SCTP: i32 = 132;
#[cfg(target_os = "linux")]
const SCTP_NODELAY: i32 = 3;
/// Linux refuses a write larger than the send buffer of an SCTP socket, instead of
/// sending part of it as with TCP. Writes close to the size of that buffer also work
/// badly: on Linux 6.6, 48 KiB and 64 KiB often fell to a few MB/s, waiting for delayed
/// acknowledgements, while this size kept hundreds of MB/s.
#[cfg(unix)]
const SCTP_MAX_WRITE: usize = 16 * 1024;

/// The protocol a server uses for its clients and for other servers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Transport {
    Tcp,
    #[cfg(unix)]
    Sctp,
}

impl Transport {
    /// Connects to the first address that answers.
    pub(crate) fn connect<A: ToSocketAddrs>(self, addr: A) -> io::Result<Stream> {
        let mut last_error = None;
        for addr in addr.to_socket_addrs()? {
            let stream = match self {
                Transport::Tcp => {
                    TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT).map(Stream::Tcp)
                }
                #[cfg(unix)]
                Transport::Sctp => connect_sctp(addr).map(Stream::Sctp),
            };
            match stream {
                Ok(stream) => return Ok(stream),
                Err(e) => last_error = Some(e),
            }
        }
        Err(last_error.unwrap_or_else(|| ErrorKind::NotFound.into()))
    }
}

#[cfg(unix)]
fn sctp_socket(addr: SocketAddr) -> io::Result<Socket> {
    let protocol = Some(Protocol::from(IPPROTO_SCTP));
    Socket::new(Domain::for_address(addr), Type::STREAM, protocol)
}

#[cfg(unix)]
fn connect_sctp(addr: SocketAddr) -> io::Result<Socket> {
    let socket = sctp_socket(addr)?;
    socket.connect_timeout(&addr.into(), CONNECT_TIMEOUT)?;
    Ok(socket)
}

/// The address of an SCTP socket, which is never a Unix one.
#[cfg(unix)]
pub(crate) fn sctp_local_addr(socket: &Socket) -> io::Result<SocketAddr> {
    socket
        .local_addr()?
        .as_socket()
        .ok_or_else(|| ErrorKind::InvalidData.into())
}

#[cfg(target_os = "linux")]
fn set_sctp_nodelay(socket: &Socket) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let on: libc::c_int = 1;
    // SAFETY: the pointer and the size describe `on`, which outlives the call.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            IPPROTO_SCTP,
            SCTP_NODELAY,
            (&raw const on).cast(),
            std::mem::size_of_val(&on) as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Elsewhere, the number of this option varies, and small packets may just take longer.
#[cfg(all(unix, not(target_os = "linux")))]
fn set_sctp_nodelay(_socket: &Socket) -> io::Result<()> {
    Ok(())
}

/// A connected stream socket.
#[derive(Debug)]
pub(crate) enum Stream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Sctp(Socket),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Stream {
    pub(crate) fn try_clone(&self) -> io::Result<Stream> {
        match self {
            Stream::Tcp(s) => s.try_clone().map(Stream::Tcp),
            #[cfg(unix)]
            Stream::Sctp(s) => s.try_clone().map(Stream::Sctp),
            #[cfg(unix)]
            Stream::Unix(s) => s.try_clone().map(Stream::Unix),
        }
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_read_timeout(timeout),
            #[cfg(unix)]
            Stream::Sctp(s) => s.set_read_timeout(timeout),
            #[cfg(unix)]
            Stream::Unix(s) => s.set_read_timeout(timeout),
        }
    }

    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_write_timeout(timeout),
            #[cfg(unix)]
            Stream::Sctp(s) => s.set_write_timeout(timeout),
            #[cfg(unix)]
            Stream::Unix(s) => s.set_write_timeout(timeout),
        }
    }

    pub(crate) fn shutdown(&self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.shutdown(Shutdown::Both),
            #[cfg(unix)]
            Stream::Sctp(s) => s.shutdown(Shutdown::Both),
            #[cfg(unix)]
            Stream::Unix(s) => s.shutdown(Shutdown::Both),
        }
    }

    /// Sends small packets right away. It does not apply to Unix sockets.
    pub(crate) fn set_nodelay(&self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_nodelay(true),
            #[cfg(unix)]
            Stream::Sctp(s) => set_sctp_nodelay(s),
            #[cfg(unix)]
            Stream::Unix(_) => Ok(()),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
            #[cfg(unix)]
            Stream::Sctp(s) => s.read(buf),
            #[cfg(unix)]
            Stream::Unix(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
            #[cfg(unix)]
            Stream::Sctp(s) => s.write(&buf[..buf.len().min(SCTP_MAX_WRITE)]),
            #[cfg(unix)]
            Stream::Unix(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.flush(),
            #[cfg(unix)]
            Stream::Sctp(s) => s.flush(),
            #[cfg(unix)]
            Stream::Unix(s) => s.flush(),
        }
    }
}

#[cfg(unix)]
fn listen_sctp(addr: SocketAddr) -> io::Result<Socket> {
    let socket = sctp_socket(addr)?;
    socket.set_reuse_address(true)?;
    socket.bind(&addr.into())?;
    socket.listen(LISTEN_BACKLOG)?;
    Ok(socket)
}

/// A socket accepting connections.
pub(crate) enum Listener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Sctp(Socket),
    #[cfg(unix)]
    Unix(UnixListener),
}

impl Listener {
    /// Listens for SCTP on the first address that works, and tells which one it was.
    #[cfg(unix)]
    pub(crate) fn bind_sctp(addrs: &[SocketAddr]) -> io::Result<(Listener, SocketAddr)> {
        let mut last_error = None;
        for &addr in addrs {
            match listen_sctp(addr) {
                Ok(socket) => {
                    let local = sctp_local_addr(&socket)?;
                    return Ok((Listener::Sctp(socket), local));
                }
                Err(e) => last_error = Some(e),
            }
        }
        Err(last_error.unwrap_or_else(|| ErrorKind::NotFound.into()))
    }

    /// Waits for connections without blocking, so that it can be stopped.
    pub(crate) fn set_nonblocking(&self) -> io::Result<()> {
        match self {
            Listener::Tcp(l) => l.set_nonblocking(true),
            #[cfg(unix)]
            Listener::Sctp(l) => l.set_nonblocking(true),
            #[cfg(unix)]
            Listener::Unix(l) => l.set_nonblocking(true),
        }
    }

    /// Accepts a connection, which will be in blocking mode.
    pub(crate) fn accept(&self) -> io::Result<Stream> {
        match self {
            Listener::Tcp(l) => {
                let (stream, peer) = l.accept()?;
                log::debug!("connection from {peer}");
                // On Windows, it would inherit non-blocking mode from the listener.
                stream.set_nonblocking(false)?;
                Ok(Stream::Tcp(stream))
            }
            #[cfg(unix)]
            Listener::Sctp(l) => {
                let (stream, _) = l.accept()?;
                stream.set_nonblocking(false)?;
                Ok(Stream::Sctp(stream))
            }
            #[cfg(unix)]
            Listener::Unix(l) => {
                let (stream, _) = l.accept()?;
                stream.set_nonblocking(false)?;
                Ok(Stream::Unix(stream))
            }
        }
    }
}
