//! A wall of tiles: several windows, on one machine or many, show parts of a single
//! animated scene. Translation and scale can be controlled from any of them, and they
//! all change frames together. It is a way to test a distributed rendering environment.
//!
//! All at once, on this machine:
//!
//! ```text
//! cargo run --example render_wall -- demo 4
//! ```
//!
//! On several machines:
//!
//! ```text
//! machine1$ cargo run --example render_wall -- server 0.0.0.0:9191 --wait-for 2
//! machine2$ cargo run --example render_wall -- node machine1:9191
//! machine3$ cargo run --example render_wall -- node machine1:9191
//! ```
//!
//! With `--sctp` on the server and on every node, everything goes through SCTP.
//!
//! See `usage` below for the keys and the options, and `sync.rs` for how it works.

mod scene;
mod sync;
mod window;

use libglass::GlassServer;
use std::collections::BTreeSet;
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;
use std::thread;
use std::time::Duration;
use sync::{coordinate, BoxError, Options, Protocol};
use window::{run_node, NodeOptions};

const DEFAULT_ADDRESS: &str = "127.0.0.1:9191";
const DEFAULT_SIZE: (u32, u32) = (640, 480);

fn usage() -> &'static str {
    "\
usage:
  render_wall demo [nodes] [--size WxH] [server options]
  render_wall server [address] [server options]
  render_wall node [address] [node options]

options of all:
  --sctp            uses SCTP instead of TCP, in the server and in every node

server options:
  --wait-for N      does not start until N nodes have joined
  --fps F           frames per second, at most (default 60; 0 is no limit)
  --cubes N         cubes around the center: more cubes, more work (default 12)
  --autopilot       moves and zooms on its own
  --no-tiles        hides the borders of the tiles
  --frames N        closes the wall at this frame
  --fixed-step S    advances the animation S seconds per frame, not by the clock

node options:
  --size WxH        size of the window (default 640x480)
  --position X,Y    position of the window
  --auto-place      moves the window to the place of its tile
  --vsync           waits for the monitor to swap
  --delay-ms N      spends N more milliseconds per frame, like a slower machine
  --log FILE        writes frame, state hash, time of the swap and view, for each frame
  --dump-dir DIR    saves the image of the frames in --dump-frames, in PPM
  --dump-frames A,B

keys, in any window:
  arrows or WASD    translate          mouse drag     translate
  + -               scale              mouse wheel    scale
  space             pause animation    T              show or hide tile borders
  R                 reset view         Esc            close the whole wall
"
}

/// The arguments still to be read.
struct Args(Vec<String>);

impl Args {
    fn flag(&mut self, name: &str) -> bool {
        match self.0.iter().position(|arg| arg == name) {
            Some(position) => {
                self.0.remove(position);
                true
            }
            None => false,
        }
    }

    fn text(&mut self, name: &str) -> Result<Option<String>, BoxError> {
        let Some(position) = self.0.iter().position(|arg| arg == name) else {
            return Ok(None);
        };
        self.0.remove(position);
        if position >= self.0.len() {
            return Err(format!("{name} needs a value").into());
        }
        Ok(Some(self.0.remove(position)))
    }

    fn value<T: FromStr>(&mut self, name: &str) -> Result<Option<T>, BoxError> {
        match self.text(name)? {
            Some(text) => match text.parse() {
                Ok(value) => Ok(Some(value)),
                Err(_) => Err(format!("invalid value for {name}: {text}").into()),
            },
            None => Ok(None),
        }
    }

    fn pair<T: FromStr>(
        &mut self,
        name: &str,
        separator: char,
    ) -> Result<Option<(T, T)>, BoxError> {
        let Some(text) = self.text(name)? else {
            return Ok(None);
        };
        let pair = text
            .split_once(separator)
            .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)));
        match pair {
            Some(pair) => Ok(Some(pair)),
            None => Err(format!("invalid value for {name}: {text}").into()),
        }
    }

    /// The next argument, if it is not an option.
    fn positional(&mut self) -> Option<String> {
        let is_positional = self.0.first().is_some_and(|arg| !arg.starts_with("--"));
        is_positional.then(|| self.0.remove(0))
    }

    fn finish(&self) -> Result<(), BoxError> {
        match self.0.first() {
            Some(arg) => Err(format!("unknown argument: {arg}").into()),
            None => Ok(()),
        }
    }
}

fn protocol(args: &mut Args) -> Protocol {
    if args.flag("--sctp") {
        Protocol::Sctp
    } else {
        Protocol::Tcp
    }
}

fn server_options(args: &mut Args) -> Result<Options, BoxError> {
    let defaults = Options::default();
    let options = Options {
        protocol: protocol(args),
        fps: args.value("--fps")?.unwrap_or(defaults.fps),
        fixed_step: args.value("--fixed-step")?,
        max_frames: args.value("--frames")?,
        wait_for: args.value("--wait-for")?.unwrap_or(defaults.wait_for),
        autopilot: args.flag("--autopilot"),
        show_tiles: !args.flag("--no-tiles"),
        cubes: args.value("--cubes")?.unwrap_or(defaults.cubes),
    };
    if options.fps < 0.0 || options.fps.is_nan() {
        return Err("--fps cannot be negative".into());
    }
    Ok(options)
}

/// The address that a client on this machine should use. Listening on all interfaces
/// gives an address that cannot be connected to.
fn local_address(server: &GlassServer) -> SocketAddr {
    let addr = server.local_addr();
    if addr.ip().is_unspecified() {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
    } else {
        addr
    }
}

fn run_server(mut args: Args) -> Result<(), BoxError> {
    let address = args.positional().unwrap_or_else(|| DEFAULT_ADDRESS.into());
    let options = server_options(&mut args)?;
    args.finish()?;

    let server = options.protocol.bind(&address)?;
    println!("listening on {}", server.local_addr());
    coordinate(local_address(&server), &options)
}

fn run_demo(mut args: Args) -> Result<(), BoxError> {
    let nodes: usize = match args.positional() {
        Some(text) => text.parse().map_err(|_| "invalid number of nodes")?,
        None => 4,
    };
    if nodes == 0 {
        return Err("at least one node is needed".into());
    }
    let (width, height) = args.pair("--size", 'x')?.unwrap_or(DEFAULT_SIZE);
    let mut options = server_options(&mut args)?;
    options.wait_for = nodes;
    args.finish()?;

    let server = options.protocol.bind("127.0.0.1:0")?;
    let address = local_address(&server);
    println!("listening on {address}");

    let program = env::current_exe()?;
    let mut children = Vec::with_capacity(nodes);
    for _ in 0..nodes {
        children.push(
            Command::new(&program)
                .args(["node", &address.to_string(), "--auto-place"])
                .args(["--size", &format!("{width}x{height}")])
                .args((options.protocol == Protocol::Sctp).then_some("--sctp"))
                .spawn()?,
        );
    }
    let coordinator = thread::spawn(move || coordinate(address, &options));
    for mut child in children {
        child.wait()?;
    }
    if coordinator.is_finished() {
        coordinator.join().expect("the coordinator panicked")?;
    }
    Ok(())
}

fn run_node_role(mut args: Args) -> Result<(), BoxError> {
    let address = args.positional().unwrap_or_else(|| DEFAULT_ADDRESS.into());
    let server = address
        .to_socket_addrs()?
        .next()
        .ok_or("the address of the server was not found")?;
    let dump_frames = match args.text("--dump-frames")? {
        Some(text) => text
            .split(',')
            .map(str::parse)
            .collect::<Result<BTreeSet<u64>, _>>()?,
        None => BTreeSet::new(),
    };
    let options = NodeOptions {
        protocol: protocol(&mut args),
        size: args.pair("--size", 'x')?.unwrap_or(DEFAULT_SIZE),
        position: args.pair("--position", ',')?,
        auto_place: args.flag("--auto-place"),
        vsync: args.flag("--vsync"),
        delay: Duration::from_millis(args.value("--delay-ms")?.unwrap_or(0)),
        log: args.value::<PathBuf>("--log")?,
        dump_dir: args.value::<PathBuf>("--dump-dir")?,
        dump_frames,
    };
    args.finish()?;
    run_node(server, options)
}

fn main() {
    let mut args = Args(env::args().skip(1).collect());
    let result = match args.positional().as_deref() {
        Some("demo") => run_demo(args),
        Some("server") => run_server(args),
        Some("node") => run_node_role(args),
        _ => {
            eprint!("{}", usage());
            std::process::exit(2);
        }
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
