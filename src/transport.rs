//! The stream connections the library can run on.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

/// A connected stream socket.
#[derive(Debug)]
pub(crate) enum Stream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Stream {
    pub(crate) fn try_clone(&self) -> io::Result<Stream> {
        match self {
            Stream::Tcp(s) => s.try_clone().map(Stream::Tcp),
            #[cfg(unix)]
            Stream::Unix(s) => s.try_clone().map(Stream::Unix),
        }
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_read_timeout(timeout),
            #[cfg(unix)]
            Stream::Unix(s) => s.set_read_timeout(timeout),
        }
    }

    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_write_timeout(timeout),
            #[cfg(unix)]
            Stream::Unix(s) => s.set_write_timeout(timeout),
        }
    }

    pub(crate) fn shutdown(&self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.shutdown(Shutdown::Both),
            #[cfg(unix)]
            Stream::Unix(s) => s.shutdown(Shutdown::Both),
        }
    }

    /// Sends small packets right away. It only makes sense for TCP.
    pub(crate) fn set_nodelay(&self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_nodelay(true),
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
            Stream::Unix(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
            #[cfg(unix)]
            Stream::Unix(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.flush(),
            #[cfg(unix)]
            Stream::Unix(s) => s.flush(),
        }
    }
}

/// A socket accepting connections.
pub(crate) enum Listener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(UnixListener),
}

impl Listener {
    /// Waits for connections without blocking, so that it can be stopped.
    pub(crate) fn set_nonblocking(&self) -> io::Result<()> {
        match self {
            Listener::Tcp(l) => l.set_nonblocking(true),
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
            Listener::Unix(l) => {
                let (stream, _) = l.accept()?;
                stream.set_nonblocking(false)?;
                Ok(Stream::Unix(stream))
            }
        }
    }
}
