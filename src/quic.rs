//! QUIC, through quinn. Each connection carries a single bidirectional stream, which
//! the rest of the library uses as it uses a TCP socket: the blocking calls run the
//! asynchronous ones on a runtime that has threads of its own.

use crate::error::{Error, Result};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{Connection, Endpoint, IdleTimeout, RecvStream, SendStream, TransportConfig, VarInt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, SignatureScheme};
use std::fmt;
use std::future::Future;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;

/// The name in generated certificates. Clients do not check it, only the fingerprint.
const SERVER_NAME: &str = "libglass";
const ALPN: &[u8] = b"libglass";
/// The client writes this as soon as it opens the stream, because the server only
/// learns about a QUIC stream when its first bytes arrive.
const PREAMBLE: &[u8] = b"GLASS\x01";
const RUNTIME_THREADS: usize = 2;
/// How long a new connection may take, on the server, to open its stream.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(10);
/// Closing a QUIC connection discards whatever is still in flight, so closing first
/// waits this long for the other side to acknowledge what was sent.
const CLOSE_LINGER: Duration = Duration::from_secs(2);
const KEEP_ALIVE: Duration = Duration::from_secs(5);
/// A connection that hears nothing from the other side for this long is over.
const IDLE_TIMEOUT_MS: u32 = 20_000;
/// The default of quinn, about 1.25 MB, is meant for many streams at a time. With a
/// single stream, a larger window keeps big packets flowing between machines.
const WINDOW: u32 = 16 * 1024 * 1024;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn runtime() -> io::Result<&'static Runtime> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(RUNTIME_THREADS)
        .thread_name("libglass-quic")
        .enable_all()
        .build()?;
    Ok(RUNTIME.get_or_init(|| runtime))
}

fn invalid(e: impl fmt::Display) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, e.to_string())
}

/// The SHA-256 hash of a certificate, which identifies it.
///
/// It is shown and parsed as 64 hexadecimal digits, optionally separated by `:`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuicFingerprint(pub [u8; 32]);

impl QuicFingerprint {
    /// The fingerprint of a certificate in DER.
    pub fn of(certificate_der: &[u8]) -> QuicFingerprint {
        let digest = ring::digest::digest(&ring::digest::SHA256, certificate_der);
        QuicFingerprint(digest.as_ref().try_into().unwrap())
    }
}

impl fmt::Display for QuicFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

impl fmt::Debug for QuicFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QuicFingerprint({self})")
    }
}

impl FromStr for QuicFingerprint {
    type Err = Error;

    fn from_str(text: &str) -> Result<QuicFingerprint> {
        let digits: Vec<u8> = text.bytes().filter(|&c| c != b':').collect();
        let mut bytes = [0u8; 32];
        if digits.len() != 2 * bytes.len() || !digits.iter().all(u8::is_ascii_hexdigit) {
            return Err(invalid("a fingerprint has 64 hexadecimal digits").into());
        }
        for (byte, pair) in bytes.iter_mut().zip(digits.chunks(2)) {
            // Both are ASCII hexadecimal digits, checked above.
            *byte = u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap();
        }
        Ok(QuicFingerprint(bytes))
    }
}

/// Which certificate a QUIC client accepts from the server it connects to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuicTrust {
    /// Only the certificate with this fingerprint, which the server must also prove
    /// it holds the private key of. Get it from
    /// [`GlassServer::quic_fingerprint`](crate::GlassServer::quic_fingerprint) or
    /// [`QuicIdentity::fingerprint`], and pass it to clients by a channel you trust.
    Fingerprint(QuicFingerprint),
    /// Any certificate. The connection is encrypted, but whoever can intercept it can
    /// pretend to be the server, and read and change everything: there is no
    /// protection against a man in the middle. Use it only on a network you trust.
    AnyCertificate,
}

/// The certificate and private key a QUIC server presents.
///
/// All the servers of a group, and any gateways linked to them, must use the same
/// one, because each server accepts only this certificate from the others. Anyone
/// with the private key can pretend to be one of these servers.
pub struct QuicIdentity {
    certificate: CertificateDer<'static>,
    key: PrivatePkcs8KeyDer<'static>,
}

impl QuicIdentity {
    /// A new self-signed certificate, with a new key.
    pub fn generate() -> Result<QuicIdentity> {
        let generated = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_owned()])
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(QuicIdentity {
            certificate: generated.cert.der().clone(),
            key: PrivatePkcs8KeyDer::from(generated.signing_key.serialize_der()),
        })
    }

    /// A certificate and its private key in PKCS#8, both in DER, as saved from
    /// [`certificate_der`](Self::certificate_der) and
    /// [`private_key_der`](Self::private_key_der).
    pub fn from_der(certificate: Vec<u8>, private_key: Vec<u8>) -> Result<QuicIdentity> {
        let identity = QuicIdentity {
            certificate: CertificateDer::from(certificate),
            key: PrivatePkcs8KeyDer::from(private_key),
        };
        identity.server_config()?;
        Ok(identity)
    }

    pub fn certificate_der(&self) -> &[u8] {
        &self.certificate
    }

    /// The private key, in PKCS#8. Keep it secret.
    pub fn private_key_der(&self) -> &[u8] {
        self.key.secret_pkcs8_der()
    }

    pub fn fingerprint(&self) -> QuicFingerprint {
        QuicFingerprint::of(&self.certificate)
    }

    fn server_config(&self) -> io::Result<quinn::ServerConfig> {
        let key = PrivateKeyDer::Pkcs8(self.key.clone_key());
        let mut tls = rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(invalid)?
            .with_no_client_auth()
            .with_single_cert(vec![self.certificate.clone()], key)
            .map_err(invalid)?;
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let crypto = QuicServerConfig::try_from(tls).map_err(invalid)?;
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(transport_config());
        Ok(config)
    }
}

impl Clone for QuicIdentity {
    fn clone(&self) -> Self {
        QuicIdentity {
            certificate: self.certificate.clone(),
            key: self.key.clone_key(),
        }
    }
}

impl fmt::Debug for QuicIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicIdentity")
            .field("fingerprint", &self.fingerprint())
            .finish_non_exhaustive()
    }
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn transport_config() -> Arc<TransportConfig> {
    let mut config = TransportConfig::default();
    config
        .max_concurrent_bidi_streams(VarInt::from_u32(1))
        .max_concurrent_uni_streams(VarInt::from_u32(0))
        .keep_alive_interval(Some(KEEP_ALIVE))
        .max_idle_timeout(Some(IdleTimeout::from(VarInt::from_u32(IDLE_TIMEOUT_MS))))
        .stream_receive_window(VarInt::from_u32(WINDOW))
        .receive_window(VarInt::from_u32(WINDOW))
        .send_window(WINDOW.into());
    Arc::new(config)
}

fn client_config(trust: QuicTrust) -> io::Result<quinn::ClientConfig> {
    let provider = provider();
    let verifier = Verifier {
        trust,
        algorithms: provider.signature_verification_algorithms,
    };
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(invalid)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let crypto = QuicClientConfig::try_from(tls).map_err(invalid)?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(transport_config());
    Ok(config)
}

/// Checks the certificate against a fingerprint instead of a certificate authority.
#[derive(Debug)]
struct Verifier {
    trust: QuicTrust,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        match self.trust {
            QuicTrust::Fingerprint(expected) if QuicFingerprint::of(end_entity) != expected => Err(
                rustls::Error::InvalidCertificate(CertificateError::ApplicationVerificationFailure),
            ),
            _ => Ok(ServerCertVerified::assertion()),
        }
    }

    // The signatures prove that the server holds the private key of the certificate.
    // Without them, anyone could present a certificate with the right fingerprint.
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

async fn within<F: Future>(timeout: Option<Duration>, future: F) -> io::Result<F::Output> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| ErrorKind::TimedOut.into()),
        None => Ok(future.await),
    }
}

fn unspecified_like(addr: SocketAddr) -> IpAddr {
    match addr {
        SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
        SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
    }
}

/// The address this machine uses to reach `remote`. Connecting a UDP socket sends
/// nothing.
fn route_to(remote: SocketAddr) -> IpAddr {
    let unspecified = unspecified_like(remote);
    UdpSocket::bind((unspecified, 0))
        .and_then(|socket| {
            socket.connect(remote)?;
            socket.local_addr()
        })
        .map_or(unspecified, |addr| addr.ip())
}

/// Opens a connection and its stream.
pub(crate) fn connect(
    addr: SocketAddr,
    trust: QuicTrust,
    timeout: Duration,
) -> io::Result<QuicStream> {
    let config = client_config(trust)?;
    runtime()?.block_on(async {
        // An endpoint of its own, so that closing this connection can wait until the
        // other side has been told, which happens when the endpoint becomes idle.
        let endpoint = Endpoint::client(SocketAddr::new(unspecified_like(addr), 0))?;
        let opening = async {
            let connecting = endpoint
                .connect_with(config, addr, SERVER_NAME)
                .map_err(io::Error::other)?;
            let connection = connecting.await?;
            let (mut send, recv) = connection.open_bi().await?;
            send.write_all(PREAMBLE).await?;
            io::Result::Ok((connection, send, recv))
        };
        match within(Some(timeout), opening).await {
            Ok(Ok((connection, send, recv))) => {
                Ok(QuicStream::new(connection, Some(endpoint), send, recv))
            }
            Ok(Err(e)) | Err(e) => {
                endpoint.close(VarInt::from_u32(0), b"");
                Err(e)
            }
        }
    })
}

/// A server endpoint. Connections are accepted by a task on the runtime, and wait
/// here until [`accept`](Self::accept) takes them.
pub(crate) struct QuicListener {
    endpoint: Endpoint,
    accepted: Receiver<QuicStream>,
    task: JoinHandle<()>,
}

impl QuicListener {
    /// Listens on the first address that works, and tells which one it was.
    pub(crate) fn bind(
        addrs: &[SocketAddr],
        identity: &QuicIdentity,
    ) -> io::Result<(QuicListener, SocketAddr)> {
        let config = identity.server_config()?;
        let runtime = runtime()?;
        let _context = runtime.enter();
        let mut last_error = None;
        for &addr in addrs {
            match Endpoint::server(config.clone(), addr) {
                Ok(endpoint) => {
                    let local = endpoint.local_addr()?;
                    let (tx, accepted) = mpsc::channel();
                    let task = runtime.spawn(accept_loop(endpoint.clone(), tx));
                    let listener = QuicListener {
                        endpoint,
                        accepted,
                        task,
                    };
                    return Ok((listener, local));
                }
                Err(e) => last_error = Some(e),
            }
        }
        Err(last_error.unwrap_or_else(|| ErrorKind::NotFound.into()))
    }

    /// Returns `WouldBlock` if no connection is ready.
    pub(crate) fn accept(&self) -> io::Result<QuicStream> {
        match self.accepted.try_recv() {
            Ok(stream) => Ok(stream),
            Err(TryRecvError::Empty) => Err(ErrorKind::WouldBlock.into()),
            Err(TryRecvError::Disconnected) => Err(ErrorKind::NotConnected.into()),
        }
    }
}

impl Drop for QuicListener {
    /// Refuses new connections, but keeps those already accepted, as with TCP.
    fn drop(&mut self) {
        self.task.abort();
        self.endpoint.set_server_config(None);
    }
}

async fn accept_loop(endpoint: Endpoint, accepted: Sender<QuicStream>) {
    while let Some(incoming) = endpoint.accept().await {
        let accepted = accepted.clone();
        tokio::spawn(async move {
            match tokio::time::timeout(ACCEPT_TIMEOUT, open(incoming)).await {
                Ok(Ok(stream)) => {
                    let _ = accepted.send(stream);
                }
                Ok(Err(e)) => log::debug!("QUIC connection failed: {e}"),
                Err(_) => log::debug!("QUIC connection did not open its stream in time"),
            }
        });
    }
}

async fn open(incoming: quinn::Incoming) -> io::Result<QuicStream> {
    let connection = incoming.await?;
    log::debug!("connection from {}", connection.remote_address());
    let (send, mut recv) = connection.accept_bi().await?;
    let mut preamble = [0u8; PREAMBLE.len()];
    recv.read_exact(&mut preamble)
        .await
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
    if preamble != PREAMBLE {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "not a libglass connection",
        ));
    }
    Ok(QuicStream::new(connection, None, send, recv))
}

/// What closing needs, which outlives the handles of the stream.
struct Link {
    connection: Connection,
    /// The endpoint of a client, which only this connection uses.
    endpoint: Option<Endpoint>,
    send: AsyncMutex<SendStream>,
}

struct Shared {
    link: Arc<Link>,
    recv: AsyncMutex<RecvStream>,
    read_timeout: Mutex<Option<Duration>>,
    write_timeout: Mutex<Option<Duration>>,
    shut_down: AtomicBool,
    wake: Notify,
    local_ip: IpAddr,
}

impl Shared {
    async fn until_shut_down(&self) {
        // Created before checking, so that it cannot miss the notification.
        let notified = self.wake.notified();
        if !self.shut_down.load(Ordering::SeqCst) {
            notified.await;
        }
    }

    fn shut_down(&self) {
        if self.shut_down.swap(true, Ordering::SeqCst) {
            return;
        }
        self.wake.notify_waiters();
        if let Some(runtime) = RUNTIME.get() {
            runtime.spawn(close(self.link.clone()));
        }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.shut_down();
    }
}

async fn close(link: Arc<Link>) {
    {
        let mut send = link.send.lock().await;
        if send.finish().is_ok() {
            let _ = tokio::time::timeout(CLOSE_LINGER, send.stopped()).await;
        }
    }
    link.connection.close(VarInt::from_u32(0), b"");
    if let Some(endpoint) = &link.endpoint {
        let _ = tokio::time::timeout(CLOSE_LINGER, endpoint.wait_idle()).await;
    }
}

/// Both halves of a QUIC stream. Clones share them, so that one thread can read
/// while another writes.
#[derive(Clone)]
pub(crate) struct QuicStream(Arc<Shared>);

impl QuicStream {
    fn new(
        connection: Connection,
        endpoint: Option<Endpoint>,
        send: SendStream,
        recv: RecvStream,
    ) -> QuicStream {
        let local_ip = connection
            .local_ip()
            .unwrap_or_else(|| route_to(connection.remote_address()));
        let link = Link {
            connection,
            endpoint,
            send: AsyncMutex::new(send),
        };
        QuicStream(Arc::new(Shared {
            link: Arc::new(link),
            recv: AsyncMutex::new(recv),
            read_timeout: Mutex::new(None),
            write_timeout: Mutex::new(None),
            shut_down: AtomicBool::new(false),
            wake: Notify::new(),
            local_ip,
        }))
    }

    pub(crate) fn local_ip(&self) -> IpAddr {
        self.0.local_ip
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        *self.0.read_timeout.lock().unwrap() = checked(timeout)?;
        Ok(())
    }

    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        *self.0.write_timeout.lock().unwrap() = checked(timeout)?;
        Ok(())
    }

    /// Closes both directions. Blocked reads return the end of the stream, and
    /// blocked writes fail, right away; what was written is still delivered.
    pub(crate) fn shutdown(&self) -> io::Result<()> {
        self.0.shut_down();
        Ok(())
    }
}

/// A zero timeout is an error, as for sockets.
fn checked(timeout: Option<Duration>) -> io::Result<Option<Duration>> {
    match timeout {
        Some(Duration::ZERO) => Err(invalid("a timeout cannot be zero")),
        _ => Ok(timeout),
    }
}

impl fmt::Debug for QuicStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicStream")
            .field("remote", &self.0.link.connection.remote_address())
            .finish_non_exhaustive()
    }
}

impl Read for QuicStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let shared = &*self.0;
        let timeout = *shared.read_timeout.lock().unwrap();
        runtime()?.block_on(async {
            tokio::select! {
                biased;
                () = shared.until_shut_down() => Ok(0),
                read = within(timeout, async { shared.recv.lock().await.read(buf).await }) => {
                    Ok(read?.map_err(io::Error::from)?.unwrap_or(0))
                }
            }
        })
    }
}

impl Write for QuicStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let shared = &*self.0;
        let timeout = *shared.write_timeout.lock().unwrap();
        runtime()?.block_on(async {
            tokio::select! {
                biased;
                () = shared.until_shut_down() => Err(ErrorKind::NotConnected.into()),
                written = within(timeout, async { shared.link.send.lock().await.write(buf).await }) => {
                    Ok(written?.map_err(io::Error::from)?)
                }
            }
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Instant;

    const TIMEOUT: Duration = Duration::from_secs(10);
    const SHORT: Duration = Duration::from_millis(200);

    fn listen(identity: &QuicIdentity) -> (QuicListener, SocketAddr) {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        QuicListener::bind(&[addr], identity).unwrap()
    }

    /// The two ends of a connection: the client's and the server's.
    fn pair() -> (QuicStream, QuicStream, QuicListener) {
        let identity = QuicIdentity::generate().unwrap();
        let (listener, addr) = listen(&identity);
        let trust = QuicTrust::Fingerprint(identity.fingerprint());
        let client = connect(addr, trust, TIMEOUT).unwrap();
        let deadline = Instant::now() + TIMEOUT;
        loop {
            match listener.accept() {
                Ok(server) => return (client, server, listener),
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "no connection arrived");
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("accept failed: {e}"),
            }
        }
    }

    #[test]
    fn accept_does_not_block() {
        let (listener, _) = listen(&QuicIdentity::generate().unwrap());
        let start = Instant::now();
        let e = listener.accept().unwrap_err();
        assert_eq!(e.kind(), ErrorKind::WouldBlock);
        assert!(start.elapsed() < SHORT);
    }

    #[test]
    fn halves_work_from_different_threads() {
        let (client, server, _listener) = pair();
        let mut reader = server.clone();
        let mut writer = server;
        let echo = thread::spawn(move || {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf).unwrap();
            writer.write_all(&buf).unwrap();
        });
        client.clone().write_all(b"ping").unwrap();
        let mut buf = [0u8; 4];
        client.clone().read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"ping");
        echo.join().unwrap();
    }

    /// Fails, instead of hanging, if what runs in the other thread never returns.
    fn in_time<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(TIMEOUT).expect("it did not return in time")
    }

    #[test]
    fn a_read_times_out() {
        let (client, _server, _listener) = pair();
        client.set_read_timeout(Some(SHORT)).unwrap();
        let start = Instant::now();
        let mut reader = client.clone();
        let e = in_time(move || reader.read(&mut [0u8; 8]).unwrap_err());
        assert_eq!(e.kind(), ErrorKind::TimedOut);
        assert!(start.elapsed() >= SHORT);
    }

    /// When the other side does not read, the flow control window fills up.
    #[test]
    fn a_write_times_out() {
        let (client, _server, _listener) = pair();
        client.set_write_timeout(Some(SHORT)).unwrap();
        let mut writer = client.clone();
        let e = in_time(move || {
            let chunk = vec![0u8; 1024 * 1024];
            loop {
                if let Err(e) = writer.write_all(&chunk) {
                    return e;
                }
            }
        });
        assert_eq!(e.kind(), ErrorKind::TimedOut);
    }

    #[test]
    fn shutdown_wakes_a_blocked_reader_and_the_other_side() {
        let (client, mut server, _listener) = pair();
        let mut reader = client.clone();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(reader.read(&mut [0u8; 8]).unwrap());
        });
        thread::sleep(SHORT);
        client.clone().write_all(b"last").unwrap();
        client.shutdown().unwrap();
        assert_eq!(
            rx.recv_timeout(SHORT).expect("the reader is still blocked"),
            0
        );

        // What was written before still arrives, and then the end of the stream.
        let mut received = Vec::new();
        server.set_read_timeout(Some(TIMEOUT)).unwrap();
        server.read_to_end(&mut received).unwrap();
        assert_eq!(received, b"last");
    }

    #[test]
    fn dropping_every_handle_closes_the_connection() {
        let (client, mut server, _listener) = pair();
        drop(client);
        server.set_read_timeout(Some(TIMEOUT)).unwrap();
        assert_eq!(server.read(&mut [0u8; 8]).unwrap(), 0);
    }

    #[test]
    fn a_wrong_certificate_is_refused_right_away() {
        let (_listener, addr) = listen(&QuicIdentity::generate().unwrap());
        let other = QuicIdentity::generate().unwrap().fingerprint();
        let start = Instant::now();
        assert!(connect(addr, QuicTrust::Fingerprint(other), TIMEOUT).is_err());
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}
