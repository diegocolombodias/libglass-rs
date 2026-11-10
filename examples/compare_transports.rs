//! Compares TCP, UDP, SCTP and, with the feature `quic`, QUIC with payloads of different
//! sizes, through the library: two clients exchange an `Event`, and each message goes
//! from a client to the server and from there to the other client.
//!
//! It measures the round trip of a message and the rate of a burst. It also checks every
//! message, and fails if TCP, SCTP or QUIC lose, change or reorder any.
//!
//! ```text
//! cargo run --release --features quic --example compare_transports -- [--repeats N] [--quick] [--only tcp|udp|sctp|quic]
//!
//! machine1$ cargo run --release --features quic --example compare_transports -- serve
//! machine2$ cargo run --release --features quic --example compare_transports -- --remote machine1 --fingerprint F
//! ```
//!
//! `serve` shows the fingerprint `F` of its QUIC certificate. Without `--fingerprint`, the
//! clients accept any certificate.
//!
//! Payloads that do not fit in a datagram are not measured for UDP, as the library would
//! send them through TCP. Without `--remote`, everything runs in one process, so the
//! clients and the server share the CPU, and there is no real network to lose packets.

#[cfg(feature = "quic")]
use libglass::QuicTrust;
use libglass::{Bytes, Error as GlassError, Event, GlassClient, GlassServer, Mode, NodeId};
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// Time for the server to know about new objects.
const SETTLE: Duration = Duration::from_millis(300);
const POLL: Duration = Duration::from_millis(50);
/// How long a message of a reliable protocol may take, which is a failure if exceeded.
const RELIABLE_TIMEOUT: Duration = Duration::from_secs(60);
/// After this, a message through UDP is counted as lost.
const UDP_REPLY_TIMEOUT: Duration = Duration::from_millis(200);
/// With no message for this long, a burst through UDP is over.
const UDP_IDLE_TIMEOUT: Duration = Duration::from_secs(1);
/// What fits in a datagram, with room for the headers of the library.
const MAX_UDP_PAYLOAD: usize = 60_000;
const BASE_PORT: u16 = 9100;
const SEQUENCE_LEN: usize = 8;
const DEFAULT_REPEATS: usize = 5;

struct Load {
    label: &'static str,
    size: usize,
    /// Messages of the round trip test.
    samples: usize,
    /// Messages of the burst.
    messages: usize,
}

const LOADS: [Load; 6] = [
    Load {
        label: "64 B",
        size: 64,
        samples: 2000,
        messages: 20_000,
    },
    Load {
        label: "1 KiB",
        size: 1024,
        samples: 2000,
        messages: 20_000,
    },
    Load {
        label: "16 KiB",
        size: 16 * 1024,
        samples: 1000,
        messages: 2000,
    },
    Load {
        label: "60 KB",
        size: MAX_UDP_PAYLOAD,
        samples: 500,
        messages: 500,
    },
    Load {
        label: "1 MiB",
        size: 1024 * 1024,
        samples: 30,
        messages: 32,
    },
    Load {
        label: "8 MiB",
        size: 8 * 1024 * 1024,
        samples: 6,
        messages: 4,
    },
];

#[derive(Clone, Copy, PartialEq)]
enum Protocol {
    Tcp,
    Udp,
    #[cfg(unix)]
    Sctp,
    #[cfg(feature = "quic")]
    Quic,
}

impl Protocol {
    fn all() -> Vec<Protocol> {
        vec![
            Protocol::Tcp,
            Protocol::Udp,
            #[cfg(unix)]
            Protocol::Sctp,
            #[cfg(feature = "quic")]
            Protocol::Quic,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Protocol::Tcp => "TCP",
            Protocol::Udp => "UDP",
            #[cfg(unix)]
            Protocol::Sctp => "SCTP",
            #[cfg(feature = "quic")]
            Protocol::Quic => "QUIC",
        }
    }

    fn port(self) -> u16 {
        match self {
            Protocol::Tcp => BASE_PORT,
            Protocol::Udp => BASE_PORT + 1,
            #[cfg(unix)]
            Protocol::Sctp => BASE_PORT + 2,
            #[cfg(feature = "quic")]
            Protocol::Quic => BASE_PORT + 3,
        }
    }

    fn is_reliable(self) -> bool {
        self != Protocol::Udp
    }

    fn fits(self, size: usize) -> bool {
        self != Protocol::Udp || size <= MAX_UDP_PAYLOAD
    }

    /// Only SCTP may be missing from a system, and only if nobody requires it.
    fn may_be_skipped(self) -> bool {
        #[cfg(unix)]
        if self == Protocol::Sctp {
            return std::env::var_os("LIBGLASS_REQUIRE_SCTP").is_none();
        }
        false
    }

    fn bind(self, addr: &str) -> Result<GlassServer> {
        Ok(match self {
            Protocol::Tcp => GlassServer::bind(addr)?,
            Protocol::Udp => GlassServer::bind_with_udp(addr)?,
            #[cfg(unix)]
            Protocol::Sctp => GlassServer::bind_sctp(addr)?,
            #[cfg(feature = "quic")]
            Protocol::Quic => GlassServer::bind_quic(addr)?,
        })
    }

    /// `fingerprint` is that of the certificate of a QUIC server.
    #[cfg_attr(not(feature = "quic"), allow(unused_variables))]
    fn connect(self, addr: &str, nickname: &str, fingerprint: Option<&str>) -> Result<GlassClient> {
        Ok(match self {
            Protocol::Tcp => GlassClient::connect(addr, nickname)?,
            Protocol::Udp => {
                let client = GlassClient::connect_with_udp(addr, nickname)?;
                // Otherwise, this would be measuring TCP without saying so.
                if !client.uses_udp() {
                    return Err("the client could not set up UDP".into());
                }
                client
            }
            #[cfg(unix)]
            Protocol::Sctp => GlassClient::connect_sctp(addr, nickname)?,
            #[cfg(feature = "quic")]
            Protocol::Quic => {
                let trust = match fingerprint {
                    Some(fingerprint) => QuicTrust::Fingerprint(fingerprint.parse()?),
                    None => QuicTrust::AnyCertificate,
                };
                GlassClient::connect_quic(addr, nickname, trust)?
            }
        })
    }

    fn send(self, event: &Event<Bytes>, message: &Bytes, target: NodeId) -> libglass::Result<()> {
        match self {
            Protocol::Udp => event.enqueue_unreliable(message, target),
            _ => event.enqueue(message, target, false),
        }
    }
}

/// Two clients and, unless it is on another machine, their server.
struct Pair {
    a: GlassClient,
    b: GlassClient,
    _server: Option<GlassServer>,
}

impl Pair {
    fn new(protocol: Protocol, remote: Option<&str>, fingerprint: Option<&str>) -> Result<Pair> {
        let (server, addr, fingerprint) = match remote {
            Some(host) => (
                None,
                format!("{host}:{}", protocol.port()),
                fingerprint.map(str::to_owned),
            ),
            None => {
                let server = protocol.bind("127.0.0.1:0")?;
                let addr = server.local_addr().to_string();
                let fingerprint = quic_fingerprint(&server);
                (Some(server), addr, fingerprint)
            }
        };
        Ok(Pair {
            a: protocol.connect(&addr, "a", fingerprint.as_deref())?,
            b: protocol.connect(&addr, "b", fingerprint.as_deref())?,
            _server: server,
        })
    }

    /// The same event in both clients. A server on another machine outlives the
    /// clients, so each pair of events gets its own name.
    fn events(&self) -> Result<(Event<Bytes>, Event<Bytes>)> {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let name = format!("ev{}", NEXT.fetch_add(1, Ordering::SeqCst));
        Ok((
            Event::new(&self.a, &name, Mode::Local)?,
            Event::new(&self.b, &name, Mode::Local)?,
        ))
    }
}

#[cfg(feature = "quic")]
fn quic_fingerprint(server: &GlassServer) -> Option<String> {
    server.quic_fingerprint().map(|f| f.to_string())
}

#[cfg(not(feature = "quic"))]
fn quic_fingerprint(_server: &GlassServer) -> Option<String> {
    None
}

fn template(size: usize) -> Bytes {
    Bytes((0..size).map(|i| (i % 251) as u8).collect())
}

fn stamp(message: &mut Bytes, sequence: u64) {
    message.0[..SEQUENCE_LEN].copy_from_slice(&sequence.to_be_bytes());
}

fn sequence_of(message: &Bytes) -> u64 {
    u64::from_be_bytes(message.0[..SEQUENCE_LEN].try_into().unwrap())
}

/// The sequence number of a message, if the rest of it is what was sent.
fn check(message: &Bytes, expected: &Bytes) -> std::result::Result<u64, String> {
    if message.0.len() != expected.0.len() {
        return Err(format!(
            "a message of {} bytes arrived with {}",
            expected.0.len(),
            message.0.len()
        ));
    }
    if message.0[SEQUENCE_LEN..] != expected.0[SEQUENCE_LEN..] {
        return Err(format!(
            "a message of {} bytes arrived changed",
            expected.0.len()
        ));
    }
    Ok(sequence_of(message))
}

#[derive(Default)]
struct Measures {
    round_trips: Vec<Duration>,
    pings: usize,
    pings_lost: usize,
    rates: Vec<f64>,
    sent: usize,
    received: usize,
}

/// Sends one message at a time and waits for the other client to send it back.
fn measure_round_trips(
    protocol: Protocol,
    pair: &Pair,
    samples: usize,
    size: usize,
    measures: &mut Measures,
) -> Result<()> {
    let (ping_a, ping_b) = pair.events()?;
    let (pong_a, pong_b) = pair.events()?;
    thread::sleep(SETTLE);
    let (a_id, b_id) = (pair.a.id(), pair.b.id());
    let expected = template(size);
    let reply_timeout = if protocol.is_reliable() {
        RELIABLE_TIMEOUT
    } else {
        UDP_REPLY_TIMEOUT
    };
    let stop = AtomicBool::new(false);

    thread::scope(|scope| -> Result<()> {
        let echo = scope.spawn(|| -> std::result::Result<(), String> {
            while !stop.load(Ordering::SeqCst) {
                match ping_b.get_timeout(POLL) {
                    Ok(message) => protocol
                        .send(&pong_b, &message, a_id)
                        .map_err(|e| e.to_string())?,
                    Err(GlassError::Timeout) => {}
                    Err(e) => return Err(e.to_string()),
                }
            }
            Ok(())
        });

        let mut message = expected.clone();
        let outcome = (|| -> Result<()> {
            for sequence in 0..samples as u64 {
                stamp(&mut message, sequence);
                let start = Instant::now();
                protocol.send(&ping_a, &message, b_id)?;
                measures.pings += 1;
                loop {
                    let remaining = reply_timeout.saturating_sub(start.elapsed());
                    match pong_a.get_timeout(remaining) {
                        Ok(reply) => {
                            // A late reply to a message already counted as lost.
                            if check(&reply, &expected)? == sequence {
                                measures.round_trips.push(start.elapsed());
                                break;
                            }
                        }
                        Err(GlassError::Timeout) if !protocol.is_reliable() => {
                            measures.pings_lost += 1;
                            break;
                        }
                        Err(e) => return Err(format!("no reply from the other client: {e}").into()),
                    }
                }
            }
            Ok(())
        })();

        stop.store(true, Ordering::SeqCst);
        echo.join().expect("the echo thread panicked")?;
        outcome
    })
}

/// Sends everything as fast as it can, and sees what arrives and how fast.
fn measure_burst(
    protocol: Protocol,
    pair: &Pair,
    messages: usize,
    size: usize,
    measures: &mut Measures,
) -> Result<()> {
    let (event_a, event_b) = pair.events()?;
    thread::sleep(SETTLE);
    let b_id = pair.b.id();
    let expected = template(size);
    let timeout = if protocol.is_reliable() {
        RELIABLE_TIMEOUT
    } else {
        UDP_IDLE_TIMEOUT
    };
    let start = Instant::now();

    let (received, last_arrival) = thread::scope(|scope| -> Result<(usize, Option<Instant>)> {
        let receiver = scope.spawn(|| -> std::result::Result<_, String> {
            let mut received = 0;
            let mut last_arrival = None;
            while received < messages {
                match event_b.get_timeout(timeout) {
                    Ok(message) => {
                        let sequence = check(&message, &expected)?;
                        if protocol.is_reliable() && sequence != received as u64 {
                            return Err(format!(
                                "{}: message {sequence} arrived instead of {received}",
                                protocol.name()
                            ));
                        }
                        received += 1;
                        last_arrival = Some(Instant::now());
                    }
                    Err(GlassError::Timeout) if !protocol.is_reliable() => break,
                    Err(e) => {
                        return Err(format!(
                            "{}: only {received} of {messages} messages arrived: {e}",
                            protocol.name()
                        ))
                    }
                }
            }
            Ok((received, last_arrival))
        });

        let mut message = expected.clone();
        for sequence in 0..messages as u64 {
            stamp(&mut message, sequence);
            protocol.send(&event_a, &message, b_id)?;
        }
        Ok(receiver.join().expect("the receiver thread panicked")?)
    })?;

    measures.sent += messages;
    measures.received += received;
    let bytes = (received * size) as f64;
    measures.rates.push(match last_arrival {
        Some(last) => bytes / 1e6 / last.duration_since(start).as_secs_f64(),
        None => 0.0,
    });
    Ok(())
}

fn percentile(sorted: &[Duration], percent: usize) -> Option<Duration> {
    let last = sorted.len().checked_sub(1)?;
    sorted.get(last * percent / 100).copied()
}

fn format_time(time: Option<Duration>) -> String {
    match time {
        Some(time) if time < Duration::from_secs(1) => {
            format!("{:.3} ms", time.as_secs_f64() * 1e3)
        }
        Some(time) => format!("{:.2} s", time.as_secs_f64()),
        None => "-".to_owned(),
    }
}

fn percent_lost(total: usize, lost: usize) -> f64 {
    match total {
        0 => 0.0,
        _ => lost as f64 * 100.0 / total as f64,
    }
}

fn print_row(load: &Load, protocol: Protocol, mut measures: Measures) {
    measures.round_trips.sort();
    measures.rates.sort_by(f64::total_cmp);
    let rates = &measures.rates;
    println!(
        "{:<7} {:<5} {:>11} {:>11} {:>8.1}% {:>9.1} {:>9.1} {:>9.1} {:>8.1}%",
        load.label,
        protocol.name(),
        format_time(percentile(&measures.round_trips, 50)),
        format_time(percentile(&measures.round_trips, 99)),
        percent_lost(measures.pings, measures.pings_lost),
        rates[0],
        rates[rates.len() / 2],
        rates[rates.len() - 1],
        percent_lost(measures.sent, measures.sent - measures.received),
    );
}

fn serve() -> Result<()> {
    let mut servers = Vec::new();
    for protocol in Protocol::all() {
        match protocol.bind(&format!("0.0.0.0:{}", protocol.port())) {
            Ok(server) => {
                print!("{} on port {}", protocol.name(), protocol.port());
                match quic_fingerprint(&server) {
                    Some(fingerprint) => println!(", certificate {fingerprint}"),
                    None => println!(),
                }
                servers.push(server);
            }
            Err(e) if protocol.may_be_skipped() => {
                println!("{} skipped: {e}", protocol.name());
            }
            Err(e) => return Err(e),
        }
    }
    servers[0].wait();
    Ok(())
}

fn run(
    repeats: usize,
    quick: bool,
    remote: Option<&str>,
    fingerprint: Option<&str>,
    only: Option<&str>,
) -> Result<()> {
    let divisor = if quick { 10 } else { 1 };
    let mut protocols = Vec::new();
    let chosen = Protocol::all()
        .into_iter()
        .filter(|p| only.is_none_or(|name| p.name().eq_ignore_ascii_case(name)));
    for protocol in chosen {
        match Pair::new(protocol, remote, fingerprint) {
            Ok(_) => protocols.push(protocol),
            Err(e) if protocol.may_be_skipped() => {
                println!("{} skipped: {e}", protocol.name());
            }
            Err(e) => return Err(format!("{}: {e}", protocol.name()).into()),
        }
    }

    println!(
        "{} run(s) on {}. Rates are of delivered payload, in MB/s.\n",
        repeats,
        remote.unwrap_or("this process")
    );
    println!(
        "{:<7} {:<5} {:>11} {:>11} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "payload",
        "proto",
        "rtt median",
        "rtt p99",
        "rtt lost",
        "rate min",
        "median",
        "max",
        "lost"
    );
    if protocols.is_empty() {
        return Err("no protocol to measure".into());
    }
    for load in &LOADS {
        for &protocol in &protocols {
            if !protocol.fits(load.size) {
                println!(
                    "{:<7} {:<5} does not fit in a datagram",
                    load.label,
                    protocol.name()
                );
                continue;
            }
            let mut measures = Measures::default();
            for _ in 0..repeats {
                let samples = (load.samples / divisor).max(2);
                let messages = (load.messages / divisor).max(2);
                let pair = Pair::new(protocol, remote, fingerprint)?;
                measure_round_trips(protocol, &pair, samples, load.size, &mut measures)?;
                measure_burst(protocol, &pair, messages, load.size, &mut measures)?;
            }
            print_row(load, protocol, measures);
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    const USAGE: &str = "usage: compare_transports [--repeats N] [--quick] [--remote HOST [--fingerprint F]] [--only PROTOCOL] | serve";
    let mut args = std::env::args().skip(1);
    let mut repeats = None;
    let mut quick = false;
    let mut remote = None;
    let mut fingerprint = None;
    let mut only = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "serve" => return serve(),
            "--quick" => quick = true,
            "--repeats" => repeats = Some(args.next().ok_or(USAGE)?.parse()?),
            "--remote" => remote = Some(args.next().ok_or(USAGE)?),
            "--fingerprint" => fingerprint = Some(args.next().ok_or(USAGE)?),
            "--only" => only = Some(args.next().ok_or(USAGE)?),
            _ => return Err(USAGE.into()),
        }
    }
    let repeats = repeats.unwrap_or(if quick { 1 } else { DEFAULT_REPEATS });
    if repeats == 0 {
        return Err(USAGE.into());
    }
    run(
        repeats,
        quick,
        remote.as_deref(),
        fingerprint.as_deref(),
        only.as_deref(),
    )
}
