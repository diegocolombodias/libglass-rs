//! The synchronization of the wall, without any graphics.
//!
//! A coordinator, which has no window, decides what each frame shows. Each frame goes
//! like this:
//!
//! ```text
//! coordinator                     every node
//! apply the commands received
//! send the frame (Event)  ----->  wait for the frame
//!                                 render it, and wait for the GPU to finish
//! Barrier::sync           <---->  Barrier::sync
//! pace, and go to the next frame  swap the buffers
//! ```
//!
//! Nodes never compute the scene state on their own, so they cannot diverge, and no
//! node swaps before all the others have finished rendering the same frame.
//!
//! A node can only enter the barrier between two frames, or it would be released along
//! with a frame that it has not rendered. So it asks to join, and the coordinator, once
//! the last frame has been released, holds the next one until the node is in the barrier.

use libglass::{
    Barrier, Error, Event, GlassClient, GlassServer, Messenger, Mode, NodeId, Wire, EVERYBODY_ID,
};
use std::error::Error as StdError;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

pub type BoxError = Box<dyn StdError + Send + Sync>;

const COORDINATOR: &str = "coordinator";
const FRAMES: &str = "wall-frames";
const CONTROL: &str = "wall-control";

const JOIN_RETRY: Duration = Duration::from_millis(100);
const JOIN_TIMEOUT: Duration = Duration::from_secs(30);
const JOINED_TIMEOUT: Duration = Duration::from_secs(5);
const GOODBYE_TIMEOUT: Duration = Duration::from_secs(2);
const REPORT_INTERVAL: Duration = Duration::from_secs(2);
const WAIT_FOR_NODES_INTERVAL: Duration = Duration::from_millis(10);
/// Longest step of the animation, so that it does not jump after a stall.
const MAX_STEP: f64 = 0.1;

pub const MIN_SCALE: f64 = 0.2;
pub const MAX_SCALE: f64 = 8.0;
/// The shape of one tile. The canvas is as many of these as the grid has.
pub const TILE_ASPECT: f64 = 4.0 / 3.0;

/// Which part of the canvas a node shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tile {
    pub col: u32,
    pub row: u32,
    pub cols: u32,
    pub rows: u32,
}

impl Tile {
    /// The canvas is as wide as `cols` tiles and as tall as `rows` tiles.
    pub fn canvas_aspect(&self) -> f64 {
        TILE_ASPECT * f64::from(self.cols) / f64::from(self.rows)
    }
}

/// Everything needed to draw a frame.
#[derive(Clone, Debug, PartialEq)]
pub struct FrameState {
    pub frame: u64,
    /// Seconds of animation. It stops while paused.
    pub time: f64,
    pub offset: [f64; 2],
    pub scale: f64,
    pub cubes: u32,
    pub paused: bool,
    pub show_tiles: bool,
    /// There is nothing to draw: the wall is closing.
    pub quit: bool,
    /// The nodes that show this frame. The position in the list gives the tile.
    pub nodes: Vec<NodeId>,
}

impl FrameState {
    fn new(options: &Options) -> FrameState {
        FrameState {
            frame: 0,
            time: 0.0,
            offset: [0.0, 0.0],
            scale: 1.0,
            cubes: options.cubes,
            paused: false,
            show_tiles: options.show_tiles,
            quit: false,
            nodes: Vec::new(),
        }
    }

    /// The canvas is split in as square a grid as possible.
    fn grid(&self) -> (u32, u32) {
        let nodes = self.nodes.len().max(1) as u32;
        let cols = f64::from(nodes).sqrt().ceil() as u32;
        (cols, nodes.div_ceil(cols))
    }

    pub fn tile_of(&self, id: NodeId) -> Option<Tile> {
        let position = self.nodes.iter().position(|&node| node == id)? as u32;
        let (cols, rows) = self.grid();
        Some(Tile {
            col: position % cols,
            row: position / cols,
            cols,
            rows,
        })
    }

    /// A number that is the same in all nodes if they have the same state.
    pub fn hash(&self) -> u64 {
        self.pack().iter().fold(0xcbf29ce484222325, |hash, &byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        })
    }

    fn apply(&mut self, message: &Message) {
        match *message {
            Message::Move(dx, dy) => {
                self.offset[0] += dx;
                self.offset[1] += dy;
            }
            Message::Zoom(factor) => self.scale = (self.scale * factor).clamp(MIN_SCALE, MAX_SCALE),
            Message::TogglePause => self.paused = !self.paused,
            Message::ToggleTiles => self.show_tiles = !self.show_tiles,
            Message::Reset => {
                self.offset = [0.0, 0.0];
                self.scale = 1.0;
            }
            Message::Quit => self.quit = true,
            Message::Join | Message::Admit(_) | Message::Joined => {}
        }
    }
}

const FLAG_PAUSED: u8 = 1;
const FLAG_SHOW_TILES: u8 = 2;
const FLAG_QUIT: u8 = 4;

/// Reads little-endian values from the front of a slice.
struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, tail) = self.0.split_first_chunk::<N>()?;
        self.0 = tail;
        Some(*head)
    }

    fn u32(&mut self) -> Option<u32> {
        self.take().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Option<u64> {
        self.take().map(u64::from_le_bytes)
    }

    fn f64(&mut self) -> Option<f64> {
        self.take().map(f64::from_le_bytes)
    }
}

impl Wire for FrameState {
    fn pack(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(53 + 4 * self.nodes.len());
        out.extend(self.frame.to_le_bytes());
        out.extend(self.time.to_le_bytes());
        out.extend(self.offset[0].to_le_bytes());
        out.extend(self.offset[1].to_le_bytes());
        out.extend(self.scale.to_le_bytes());
        out.extend(self.cubes.to_le_bytes());
        let flag = |on, flag| if on { flag } else { 0 };
        out.push(
            flag(self.paused, FLAG_PAUSED)
                | flag(self.show_tiles, FLAG_SHOW_TILES)
                | flag(self.quit, FLAG_QUIT),
        );
        out.extend((self.nodes.len() as u32).to_le_bytes());
        for node in &self.nodes {
            out.extend(node.to_le_bytes());
        }
        out
    }

    fn unpack(bytes: &[u8]) -> Option<FrameState> {
        let mut reader = Reader(bytes);
        let frame = reader.u64()?;
        let time = reader.f64()?;
        let offset = [reader.f64()?, reader.f64()?];
        let scale = reader.f64()?;
        let cubes = reader.u32()?;
        let [flags] = reader.take()?;
        let count = reader.u32()? as usize;
        if reader.0.len() != count.checked_mul(4)? {
            return None;
        }
        let nodes = (0..count).map(|_| reader.u32()).collect::<Option<_>>()?;
        Some(FrameState {
            frame,
            time,
            offset,
            scale,
            cubes,
            paused: flags & FLAG_PAUSED != 0,
            show_tiles: flags & FLAG_SHOW_TILES != 0,
            quit: flags & FLAG_QUIT != 0,
            nodes,
        })
    }
}

/// Messages between a node and the coordinator.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Message {
    /// A node asks to take part in the wall.
    Join,
    /// The coordinator tells it to enter the barrier, and which frame will be its first.
    Admit(u64),
    /// The node is in the barrier.
    Joined,
    Move(f64, f64),
    Zoom(f64),
    TogglePause,
    ToggleTiles,
    Reset,
    Quit,
}

impl Wire for Message {
    fn pack(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(17);
        match *self {
            Message::Join => out.push(0),
            Message::Admit(frame) => {
                out.push(1);
                out.extend(frame.to_le_bytes());
            }
            Message::Joined => out.push(2),
            Message::Move(dx, dy) => {
                out.push(3);
                out.extend(dx.to_le_bytes());
                out.extend(dy.to_le_bytes());
            }
            Message::Zoom(factor) => {
                out.push(4);
                out.extend(factor.to_le_bytes());
            }
            Message::TogglePause => out.push(5),
            Message::ToggleTiles => out.push(6),
            Message::Reset => out.push(7),
            Message::Quit => out.push(8),
        }
        out
    }

    fn unpack(bytes: &[u8]) -> Option<Message> {
        let mut reader = Reader(bytes);
        let [kind] = reader.take()?;
        let message = match kind {
            0 => Message::Join,
            1 => Message::Admit(reader.u64()?),
            2 => Message::Joined,
            3 => Message::Move(reader.f64()?, reader.f64()?),
            4 => Message::Zoom(reader.f64()?),
            5 => Message::TogglePause,
            6 => Message::ToggleTiles,
            7 => Message::Reset,
            8 => Message::Quit,
            _ => return None,
        };
        reader.0.is_empty().then_some(message)
    }
}

/// How the nodes and the coordinator reach the server.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Protocol {
    #[default]
    Tcp,
    Sctp,
}

impl Protocol {
    pub fn bind(self, address: &str) -> Result<GlassServer, BoxError> {
        match self {
            Protocol::Tcp => Ok(GlassServer::bind(address)?),
            #[cfg(unix)]
            Protocol::Sctp => Ok(GlassServer::bind_sctp(address)?),
            #[cfg(not(unix))]
            Protocol::Sctp => Err(NO_SCTP.into()),
        }
    }

    /// Keeps trying until `timeout`, as the server may not be up yet.
    fn connect(
        self,
        server: SocketAddr,
        nickname: &str,
        timeout: Duration,
    ) -> Result<GlassClient, BoxError> {
        match self {
            Protocol::Tcp => Ok(GlassClient::connect_retry(server, nickname, timeout)?),
            #[cfg(unix)]
            Protocol::Sctp => Ok(GlassClient::connect_retry_sctp(server, nickname, timeout)?),
            #[cfg(not(unix))]
            Protocol::Sctp => Err(NO_SCTP.into()),
        }
    }
}

#[cfg(not(unix))]
const NO_SCTP: &str = "SCTP is not available on this system";

/// How the coordinator runs the wall.
#[derive(Clone, Debug)]
pub struct Options {
    /// Frames per second, at most. Zero means as fast as the nodes can go.
    pub fps: f64,
    /// Advances the animation by this many seconds per frame, instead of by the real
    /// time, so that each frame is the same in every run.
    pub fixed_step: Option<f64>,
    /// Closes the wall at this frame.
    pub max_frames: Option<u64>,
    /// Does not start until this many nodes have joined.
    pub wait_for: usize,
    pub protocol: Protocol,
    /// Moves and zooms the scene on its own, always in the same way.
    pub autopilot: bool,
    pub show_tiles: bool,
    /// Number of cubes around the center: more cubes, more work for each node.
    pub cubes: u32,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            fps: 60.0,
            fixed_step: None,
            max_frames: None,
            wait_for: 0,
            protocol: Protocol::Tcp,
            autopilot: false,
            show_tiles: true,
            cubes: 12,
        }
    }
}

/// Where the autopilot wants the scene to be at a given frame.
fn autopilot_at(frame: u64) -> ([f64; 2], f64) {
    let f = frame as f64;
    let offset = [0.5 * (f * 0.031).sin(), 0.3 * (f * 0.047).sin()];
    (offset, 1.0 + 0.5 * (f * 0.023).sin())
}

/// Waits for a node that was admitted to say that it is in the barrier.
fn wait_joined(inbox: &Messenger<Message>, node: NodeId) -> libglass::Result<bool> {
    let deadline = Instant::now() + JOINED_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match inbox.get_from_timeout(node, remaining) {
            Ok(Message::Joined) => return Ok(true),
            // It repeats the request until it gets an answer.
            Ok(_) => {}
            Err(Error::Timeout) => return Ok(false),
            Err(e) => return Err(e),
        }
    }
}

/// Runs the wall until it is told to quit, or until `max_frames`. The server must be
/// running, and it decides nothing: this could be on any machine.
pub fn coordinate(server: SocketAddr, options: &Options) -> Result<(), BoxError> {
    // The server runs in this process, and is already up.
    let client = options
        .protocol
        .connect(server, COORDINATOR, Duration::ZERO)?;
    let frames = Event::<FrameState>::new(&client, FRAMES, Mode::Local)?;
    let inbox = Messenger::<Message>::new(&client, CONTROL, Mode::Local)?;
    let barrier = Barrier::new(&client, 0, 0, true, Mode::Local)?;

    let mut state = FrameState::new(options);
    let mut started = false;
    let mut last_frame = Instant::now();
    let mut last_report = (Instant::now(), 0);
    let interval = (options.fps > 0.0).then(|| Duration::from_secs_f64(1.0 / options.fps));

    loop {
        let clients = client.client_list();
        state.nodes.retain(|id| clients.contains_key(id));

        // All nodes have gone past the barrier, so this is the place for them to enter.
        while let Some((from, message)) = inbox.try_get()? {
            if message != Message::Join {
                state.apply(&message);
            } else if !state.nodes.contains(&from) {
                inbox.send(from, &Message::Admit(state.frame + 1))?;
                if wait_joined(&inbox, from)? {
                    state.nodes.push(from);
                    println!("node {from:#x} joined: {} nodes", state.nodes.len());
                } else {
                    println!("node {from:#x} did not enter the barrier");
                }
            }
        }

        if !started && !state.quit {
            if state.nodes.len() < options.wait_for {
                thread::sleep(WAIT_FOR_NODES_INTERVAL);
                continue;
            }
            started = true;
            last_frame = Instant::now();
        }

        if let Some(interval) = interval {
            let next = last_frame + interval;
            thread::sleep(next.saturating_duration_since(Instant::now()));
        }
        let now = Instant::now();
        let step = options
            .fixed_step
            .unwrap_or_else(|| (now - last_frame).as_secs_f64().min(MAX_STEP));
        last_frame = now;

        if options.autopilot {
            let (from, from_scale) = autopilot_at(state.frame);
            let (to, to_scale) = autopilot_at(state.frame + 1);
            state.apply(&Message::Move(to[0] - from[0], to[1] - from[1]));
            state.apply(&Message::Zoom(to_scale / from_scale));
        }
        if !state.paused {
            state.time += step;
        }
        state.frame += 1;
        if options.max_frames.is_some_and(|max| state.frame >= max) {
            state.quit = true;
        }

        frames.enqueue(&state, EVERYBODY_ID, false)?;
        if state.quit {
            break;
        }
        barrier.sync()?;

        if now - last_report.0 >= REPORT_INTERVAL {
            let fps = (state.frame - last_report.1) as f64 / (now - last_report.0).as_secs_f64();
            println!(
                "frame {}: {} nodes, {fps:.1} fps",
                state.frame,
                state.nodes.len()
            );
            last_report = (now, state.frame);
        }
    }

    // The nodes have nothing else to do, and the last frame still has to reach them.
    let deadline = Instant::now() + GOODBYE_TIMEOUT;
    while client.client_list().len() > 1 && Instant::now() < deadline {
        thread::sleep(WAIT_FOR_NODES_INTERVAL);
    }
    Ok(())
}

/// What a node uses to take part in the wall.
pub struct NodeLink {
    frames: Event<FrameState>,
    control: Messenger<Message>,
    barrier: Barrier,
    coordinator: NodeId,
    first_frame: u64,
    client: GlassClient,
}

impl NodeLink {
    /// Connects and joins the wall. It returns when the node is in the barrier.
    pub fn join(
        server: SocketAddr,
        protocol: Protocol,
        nickname: &str,
    ) -> Result<NodeLink, BoxError> {
        let deadline = Instant::now() + JOIN_TIMEOUT;
        let client = protocol.connect(server, nickname, JOIN_TIMEOUT)?;
        let frames = Event::<FrameState>::new(&client, FRAMES, Mode::Local)?;
        let control = Messenger::<Message>::new(&client, CONTROL, Mode::Local)?;

        let past_deadline = || -> Result<(), BoxError> {
            if Instant::now() >= deadline {
                return Err("no answer from the coordinator".into());
            }
            Ok(())
        };
        let coordinator = loop {
            match client.nickname_to_id(COORDINATOR) {
                Ok(id) => break id,
                Err(_) => {
                    past_deadline()?;
                    thread::sleep(WAIT_FOR_NODES_INTERVAL);
                }
            }
        };

        // The coordinator may not have its messenger yet, or may be busy with another
        // node, so the request is repeated.
        let first_frame = loop {
            control.send(coordinator, &Message::Join)?;
            match control.get_from_timeout(coordinator, JOIN_RETRY) {
                Ok(Message::Admit(frame)) => break frame,
                Ok(_) | Err(Error::Timeout) => past_deadline()?,
                Err(e) => return Err(e.into()),
            }
            if !client.client_list().contains_key(&coordinator) {
                return Err("the wall has already closed".into());
            }
        };
        let barrier = Barrier::new(&client, 0, 0, true, Mode::Local)?;
        control.send(coordinator, &Message::Joined)?;

        Ok(NodeLink {
            frames,
            control,
            barrier,
            coordinator,
            first_frame,
            client,
        })
    }

    pub fn id(&self) -> NodeId {
        self.client.id()
    }

    /// Waits for the next frame of this node. `None` means that it has not arrived yet.
    pub fn next_frame(&self, timeout: Duration) -> Result<Option<FrameState>, BoxError> {
        loop {
            match self.frames.get_timeout(timeout) {
                Ok(state) if state.quit || state.frame >= self.first_frame => {
                    return Ok(Some(state));
                }
                Ok(_) => {}
                Err(Error::Timeout) => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Sends a command to the coordinator, which applies it to the next frame.
    pub fn send(&self, message: Message) -> Result<(), BoxError> {
        Ok(self.control.send(self.coordinator, &message)?)
    }

    /// Waits until all the nodes have rendered this frame.
    pub fn finish_frame(&self) -> Result<(), BoxError> {
        Ok(self.barrier.sync()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, Mutex};

    const TIMEOUT: Duration = Duration::from_secs(30);

    /// Who has rendered each frame, as seen right before each barrier.
    type Rendered = Arc<Mutex<BTreeMap<u64, BTreeSet<NodeId>>>>;

    fn start(options: Options) -> (GlassServer, SocketAddr, thread::JoinHandle<()>) {
        let server = GlassServer::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr();
        let coordinator = thread::spawn(move || coordinate(addr, &options).unwrap());
        (server, addr, coordinator)
    }

    fn fast() -> Options {
        Options {
            fps: 0.0,
            ..Options::default()
        }
    }

    /// Plays the role of a node, without rendering. It checks that no frame gets past the
    /// barrier before all the nodes of that frame have rendered it, except for `leaving`.
    /// It returns all the frames it has seen.
    fn play(link: &NodeLink, rendered: &Rendered, leaving: &[NodeId]) -> Vec<FrameState> {
        let mut seen = Vec::new();
        loop {
            let state = link.next_frame(TIMEOUT).unwrap().expect("no frame arrived");
            if state.quit {
                return seen;
            }
            assert!(state.tile_of(link.id()).is_some(), "it must have a tile");
            let frame = state.frame;
            let nodes = state.nodes.clone();
            seen.push(state);
            let mut map = rendered.lock().unwrap();
            map.entry(frame).or_default().insert(link.id());
            drop(map);
            link.finish_frame().unwrap();

            let map = rendered.lock().unwrap();
            for node in nodes.iter().filter(|node| !leaving.contains(node)) {
                assert!(
                    map[&frame].contains(node),
                    "frame {frame} was released before {node:#x} rendered it"
                );
            }
        }
    }

    fn assert_consecutive(frames: &[FrameState]) {
        for pair in frames.windows(2) {
            assert_eq!(pair[0].frame + 1, pair[1].frame, "a frame was skipped");
        }
    }

    #[test]
    fn state_and_messages_survive_the_network_format() {
        let state = FrameState {
            frame: 7,
            time: 1.5,
            offset: [-0.25, 3.0],
            scale: 2.0,
            cubes: 9,
            paused: true,
            show_tiles: false,
            quit: true,
            nodes: vec![1, 2, 0x10000],
        };
        assert_eq!(FrameState::unpack(&state.pack()), Some(state.clone()));
        let packed = state.pack();
        assert_eq!(FrameState::unpack(&packed[..packed.len() - 1]), None);
        assert_eq!(FrameState::unpack(&[]), None);

        for message in [
            Message::Join,
            Message::Admit(9),
            Message::Joined,
            Message::Move(0.5, -1.0),
            Message::Zoom(1.5),
            Message::TogglePause,
            Message::ToggleTiles,
            Message::Reset,
            Message::Quit,
        ] {
            assert_eq!(Message::unpack(&message.pack()), Some(message));
        }
        assert_eq!(Message::unpack(&[200]), None);
        assert_eq!(Message::unpack(&[8, 0]), None);
    }

    #[test]
    fn tiles_cover_the_canvas_without_repeating() {
        for count in 1..=17u32 {
            let mut state = FrameState::new(&Options::default());
            state.nodes = (0..count).collect();
            let mut cells = BTreeSet::new();
            for id in 0..count {
                let tile = state.tile_of(id).unwrap();
                assert!(tile.col < tile.cols && tile.row < tile.rows);
                assert!(cells.insert((tile.col, tile.row)));
                assert!(tile.cols * tile.rows >= count);
            }
        }
        let state = FrameState {
            nodes: vec![5, 6, 7, 8],
            ..FrameState::new(&Options::default())
        };
        let last = state.tile_of(8).unwrap();
        assert_eq!((last.col, last.row, last.cols, last.rows), (1, 1, 2, 2));
        assert_eq!(state.tile_of(9), None);
    }

    #[test]
    fn zoom_stays_within_its_limits() {
        let mut state = FrameState::new(&Options::default());
        state.apply(&Message::Zoom(1000.0));
        assert_eq!(state.scale, MAX_SCALE);
        state.apply(&Message::Zoom(0.0001));
        assert_eq!(state.scale, MIN_SCALE);
        state.apply(&Message::Reset);
        assert_eq!(state.scale, 1.0);
    }

    #[test]
    fn all_nodes_see_the_same_frames() {
        let (_server, addr, coordinator) = start(Options {
            max_frames: Some(200),
            wait_for: 4,
            autopilot: true,
            fixed_step: Some(0.01),
            ..fast()
        });
        let rendered = Rendered::default();
        let nodes: Vec<_> = (0..4)
            .map(|i| {
                let rendered = rendered.clone();
                thread::spawn(move || {
                    let link = NodeLink::join(addr, Protocol::Tcp, &format!("n{i}")).unwrap();
                    play(&link, &rendered, &[])
                })
            })
            .collect();
        let seen: Vec<Vec<FrameState>> = nodes.into_iter().map(|n| n.join().unwrap()).collect();
        coordinator.join().unwrap();

        assert_eq!(seen[0].len(), 199);
        assert_eq!(seen[0][0].frame, 1);
        assert_consecutive(&seen[0]);
        assert!(seen.iter().all(|frames| frames == &seen[0]));
        assert!(seen[0].last().unwrap().time > 1.9);
        assert!(seen[0].iter().any(|state| state.offset != [0.0, 0.0]));
    }

    #[test]
    fn a_command_from_one_node_reaches_all_of_them_in_the_same_frame() {
        let (_server, addr, coordinator) = start(Options {
            wait_for: 3,
            ..fast()
        });
        let rendered = Rendered::default();
        let sender = thread::spawn({
            let rendered = rendered.clone();
            move || {
                let link = NodeLink::join(addr, Protocol::Tcp, "sender").unwrap();
                link.send(Message::Move(0.5, -0.25)).unwrap();
                link.send(Message::Zoom(2.0)).unwrap();
                link.send(Message::TogglePause).unwrap();
                link.send(Message::ToggleTiles).unwrap();
                let mut seen = Vec::new();
                loop {
                    let state = link.next_frame(TIMEOUT).unwrap().unwrap();
                    let frame = state.frame;
                    rendered
                        .lock()
                        .unwrap()
                        .entry(frame)
                        .or_default()
                        .insert(link.id());
                    seen.push(state);
                    if frame == 40 {
                        link.send(Message::Quit).unwrap();
                    }
                    if seen.last().unwrap().quit {
                        seen.pop();
                        return seen;
                    }
                    link.finish_frame().unwrap();
                }
            }
        });
        let others: Vec<_> = (0..2)
            .map(|i| {
                let rendered = rendered.clone();
                thread::spawn(move || {
                    let link = NodeLink::join(addr, Protocol::Tcp, &format!("n{i}")).unwrap();
                    play(&link, &rendered, &[])
                })
            })
            .collect();

        let sent = sender.join().unwrap();
        let seen: Vec<_> = others.into_iter().map(|n| n.join().unwrap()).collect();
        coordinator.join().unwrap();

        assert!(seen.iter().all(|frames| frames == &sent));
        let last = sent.last().unwrap();
        assert_eq!(last.offset, [0.5, -0.25]);
        assert_eq!(last.scale, 2.0);
        assert!(last.paused);
        assert!(!last.show_tiles);
        // Nothing moves in the animation while it is paused.
        assert_eq!(last.time, sent[sent.len() - 2].time);
    }

    #[test]
    fn a_node_can_join_while_the_wall_is_running() {
        let (_server, addr, coordinator) = start(Options {
            wait_for: 2,
            ..fast()
        });
        let rendered = Rendered::default();
        // Each late node waits for the wall to get to a given frame.
        let spawn = |name: &'static str, enter_at: u64, rendered: Rendered| {
            thread::spawn(move || {
                while rendered.lock().unwrap().keys().last().copied().unwrap_or(0) < enter_at {
                    thread::sleep(Duration::from_millis(1));
                }
                let link = NodeLink::join(addr, Protocol::Tcp, name).unwrap();
                play(&link, &rendered, &[])
            })
        };
        let nodes = [
            spawn("late1", 50, rendered.clone()),
            spawn("late2", 100, rendered.clone()),
        ];

        // These close the wall, but only after the late nodes have been there for a while.
        let founders: Vec<_> = (0..2)
            .map(|i| {
                let rendered = rendered.clone();
                thread::spawn(move || {
                    let link = NodeLink::join(addr, Protocol::Tcp, &format!("founder{i}")).unwrap();
                    let mut seen = Vec::new();
                    let mut frames_with_all = 0;
                    loop {
                        let state = link.next_frame(TIMEOUT).unwrap().unwrap();
                        if state.quit {
                            return seen;
                        }
                        if state.nodes.len() == 4 {
                            frames_with_all += 1;
                        }
                        if frames_with_all == 50 {
                            link.send(Message::Quit).unwrap();
                        }
                        rendered
                            .lock()
                            .unwrap()
                            .entry(state.frame)
                            .or_default()
                            .insert(link.id());
                        seen.push(state);
                        link.finish_frame().unwrap();
                    }
                })
            })
            .collect();

        let late: Vec<_> = nodes.into_iter().map(|n| n.join().unwrap()).collect();
        let founders: Vec<_> = founders.into_iter().map(|n| n.join().unwrap()).collect();
        coordinator.join().unwrap();

        assert_eq!(founders[0], founders[1]);
        assert_consecutive(&founders[0]);
        let last = founders[0].last().unwrap();
        assert_eq!(last.nodes.len(), 4);

        for frames in &late {
            assert_consecutive(frames);
            assert!(frames[0].frame > 50, "it must have joined late");
            // From the moment it joined, it has seen the same as the others.
            let skip = (frames[0].frame - founders[0][0].frame) as usize;
            assert!(founders[0][skip..] == *frames.as_slice());
        }
    }

    #[test]
    fn the_wall_goes_on_when_a_node_leaves_during_a_frame() {
        let (_server, addr, coordinator) = start(Options {
            wait_for: 3,
            ..fast()
        });
        let rendered = Rendered::default();
        let link = NodeLink::join(addr, Protocol::Tcp, "leaving").unwrap();
        let leaving = link.id();
        let survivors: Vec<_> = (0..2)
            .map(|i| {
                let rendered = rendered.clone();
                thread::spawn(move || {
                    let link = NodeLink::join(addr, Protocol::Tcp, &format!("n{i}")).unwrap();
                    let mut seen = Vec::new();
                    loop {
                        let state = link.next_frame(TIMEOUT).unwrap().unwrap();
                        if state.frame == 200 {
                            link.send(Message::Quit).unwrap();
                        }
                        if state.quit {
                            return seen;
                        }
                        rendered
                            .lock()
                            .unwrap()
                            .entry(state.frame)
                            .or_default()
                            .insert(link.id());
                        seen.push(state);
                        link.finish_frame().unwrap();
                    }
                })
            })
            .collect();

        // It stops in the middle of a frame: the others are held by it at the barrier.
        link.next_frame(TIMEOUT).unwrap().unwrap();
        link.finish_frame().unwrap();
        link.next_frame(TIMEOUT).unwrap().unwrap();
        thread::sleep(Duration::from_millis(100));
        drop(link);

        let seen: Vec<_> = survivors.into_iter().map(|n| n.join().unwrap()).collect();
        coordinator.join().unwrap();

        assert_eq!(seen[0], seen[1]);
        assert_consecutive(&seen[0]);
        assert_eq!(seen[0].first().unwrap().nodes.len(), 3);
        let last = seen[0].last().unwrap();
        assert!(!last.nodes.contains(&leaving));
        assert_eq!(last.nodes.len(), 2);
        // The canvas is split again among those who are left.
        assert_eq!(last.tile_of(last.nodes[1]).unwrap().cols, 2);
        assert_eq!(last.tile_of(last.nodes[1]).unwrap().rows, 1);
    }
}
