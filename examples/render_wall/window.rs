//! A node of the wall: a window, and a thread that renders in it.
//!
//! Rendering has its own thread because it blocks at the barrier, for as long as the
//! slowest node takes. The thread of the window keeps answering the system meanwhile.

use crate::scene::Scene;
use crate::sync::{BoxError, FrameState, Message, NodeLink, Tile};
use glutin::config::{Config, ConfigTemplateBuilder, GlConfig};
use glutin::context::{
    ContextApi, ContextAttributesBuilder, GlProfile, NotCurrentContext, Version,
};
use glutin::display::{GetGlDisplay, GlDisplay};
use glutin::prelude::*;
use glutin::surface::{Surface, SwapInterval, WindowSurface};
use glutin_winit::{DisplayBuilder, GlWindow};
use std::collections::BTreeSet;
use std::error::Error;
use std::fs::{self, File};
use std::io::{LineWriter, Write};
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use winit::application::ApplicationHandler;
use winit::dpi::{PhysicalPosition, PhysicalSize};
use winit::event::{ElementState, MouseButton, MouseScrollDelta, WindowEvent};
use winit::event_loop::{ActiveEventLoop, EventLoop, EventLoopProxy};
use winit::keyboard::{KeyCode, PhysicalKey};
use winit::raw_window_handle::HasWindowHandle;
use winit::window::{Window, WindowId};

const FRAME_WAIT: Duration = Duration::from_millis(200);
const TITLE_INTERVAL: Duration = Duration::from_millis(500);
/// Where the first tile goes, when the windows place themselves.
const FIRST_WINDOW: (i32, i32) = (40, 40);

/// World units per second, while a key is held.
const KEY_MOVE_SPEED: f64 = 1.0;
/// How much the scale changes per second, while a key is held.
const KEY_ZOOM_SPEED: f64 = 1.8;
/// How much the scale changes per line of the mouse wheel.
const WHEEL_ZOOM: f64 = 1.1;
const PIXELS_PER_WHEEL_LINE: f64 = 40.0;

pub struct NodeOptions {
    pub size: (u32, u32),
    pub position: Option<(i32, i32)>,
    /// Moves the window to the place of its tile, for when they are on the same screen.
    pub auto_place: bool,
    pub vsync: bool,
    /// Extra time in each frame, to simulate a slower machine.
    pub delay: Duration,
    /// Writes one line per frame: its number, the hash of its state, when it appeared (in
    /// microseconds), its translation, its scale and its animation time.
    pub log: Option<PathBuf>,
    pub dump_dir: Option<PathBuf>,
    pub dump_frames: BTreeSet<u64>,
}

/// What the window tells the render thread.
enum Input {
    Key(KeyCode, bool),
    Drag(f64, f64),
    Wheel(f64),
}

struct Renderer {
    server: SocketAddr,
    options: NodeOptions,
    window: Arc<Window>,
    context: NotCurrentContext,
    surface: Surface<WindowSurface>,
    input: Receiver<Input>,
    stop: Arc<AtomicBool>,
}

/// What a node has to do with the keys and the mouse, between two frames.
#[derive(Default)]
struct Controls {
    held: BTreeSet<KeyCode>,
    last_frame: Option<Instant>,
}

impl Controls {
    /// Turns what happened since the last frame into commands, and sends them. All the
    /// movement of a frame goes in one command.
    fn send_commands(
        &mut self,
        link: &NodeLink,
        input: &Receiver<Input>,
        tile: Tile,
        size: PhysicalSize<u32>,
    ) -> Result<(), BoxError> {
        let (mut dx, mut dy, mut zoom) = (0.0, 0.0, 1.0);

        // The canvas goes from -1 to 1 in height, so a tile is this large in the world.
        let tile_height = 2.0 / f64::from(tile.rows);
        let tile_width = 2.0 * tile.canvas_aspect() / f64::from(tile.cols);

        for event in input.try_iter() {
            match event {
                Input::Key(key, true) => {
                    if !self.held.insert(key) {
                        continue;
                    }
                    let command = match key {
                        KeyCode::Space => Some(Message::TogglePause),
                        KeyCode::KeyT => Some(Message::ToggleTiles),
                        KeyCode::KeyR => Some(Message::Reset),
                        KeyCode::Escape => Some(Message::Quit),
                        _ => None,
                    };
                    if let Some(command) = command {
                        link.send(command)?;
                    }
                }
                Input::Key(key, false) => {
                    self.held.remove(&key);
                }
                Input::Drag(x, y) => {
                    dx += x / f64::from(size.width.max(1)) * tile_width;
                    dy -= y / f64::from(size.height.max(1)) * tile_height;
                }
                Input::Wheel(lines) => zoom *= WHEEL_ZOOM.powf(lines),
            }
        }

        let now = Instant::now();
        let elapsed = self.last_frame.map_or(0.0, |t| (now - t).as_secs_f64());
        self.last_frame = Some(now);
        let held = |keys: &[KeyCode]| keys.iter().any(|key| self.held.contains(key));
        let step = KEY_MOVE_SPEED * elapsed;
        if held(&[KeyCode::ArrowLeft, KeyCode::KeyA]) {
            dx -= step;
        }
        if held(&[KeyCode::ArrowRight, KeyCode::KeyD]) {
            dx += step;
        }
        if held(&[KeyCode::ArrowDown, KeyCode::KeyS]) {
            dy -= step;
        }
        if held(&[KeyCode::ArrowUp, KeyCode::KeyW]) {
            dy += step;
        }
        if held(&[KeyCode::Equal, KeyCode::NumpadAdd]) {
            zoom *= KEY_ZOOM_SPEED.powf(elapsed);
        }
        if held(&[KeyCode::Minus, KeyCode::NumpadSubtract]) {
            zoom /= KEY_ZOOM_SPEED.powf(elapsed);
        }

        if dx != 0.0 || dy != 0.0 {
            link.send(Message::Move(dx, dy))?;
        }
        if zoom != 1.0 {
            link.send(Message::Zoom(zoom))?;
        }
        Ok(())
    }
}

/// Saves an image in the PPM format. OpenGL gives the rows from the bottom up.
fn write_ppm(path: &PathBuf, width: u32, height: u32, pixels: &[u8]) -> std::io::Result<()> {
    let mut data = format!("P6\n{width} {height}\n255\n").into_bytes();
    for row in pixels.chunks_exact(width as usize * 3).rev() {
        data.extend_from_slice(row);
    }
    fs::write(path, data)
}

fn since_epoch_micros() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| time.as_micros())
}

impl Renderer {
    fn run(self) -> Result<(), BoxError> {
        let Renderer {
            server,
            options,
            window,
            context,
            surface,
            input,
            stop,
        } = self;

        let context = context.make_current(&surface)?;
        let display = context.display();
        // SAFETY: the context is current on this thread for as long as this one is used,
        // and the functions come from its own display.
        let gl = unsafe {
            glow::Context::from_loader_function_cstr(|name| display.get_proc_address(name))
        };
        let scene = Scene::new(gl)?;

        let interval = if options.vsync {
            SwapInterval::Wait(NonZeroU32::MIN)
        } else {
            SwapInterval::DontWait
        };
        if let Err(e) = surface.set_swap_interval(&context, interval) {
            eprintln!("could not change vertical sync: {e}");
        }

        let mut log = match &options.log {
            Some(path) => Some(LineWriter::new(File::create(path)?)),
            None => None,
        };

        let nickname = format!("node-{}", std::process::id());
        let link = NodeLink::join(server, &nickname)?;

        let mut controls = Controls::default();
        let mut size = window.inner_size();
        let mut placed: Option<Tile> = None;
        let mut last_title = Instant::now();
        let mut frames_since_title = 0u32;

        while !stop.load(Ordering::Relaxed) {
            let Some(state) = link.next_frame(FRAME_WAIT)? else {
                continue;
            };
            if state.quit {
                break;
            }
            let tile = state
                .tile_of(link.id())
                .ok_or("the coordinator gave up on this node")?;
            controls.send_commands(&link, &input, tile, size)?;

            let new_size = window.inner_size();
            if new_size != size {
                size = new_size;
                if let (Some(width), Some(height)) =
                    (NonZeroU32::new(size.width), NonZeroU32::new(size.height))
                {
                    surface.resize(&context, width, height);
                }
            }
            if options.auto_place && placed != Some(tile) {
                let outer = window.outer_size();
                window.set_outer_position(PhysicalPosition::new(
                    FIRST_WINDOW.0 + tile.col as i32 * outer.width as i32,
                    FIRST_WINDOW.1 + tile.row as i32 * outer.height as i32,
                ));
                placed = Some(tile);
            }

            // A minimized window has nothing to draw, but it still takes part.
            let visible = size.width > 0 && size.height > 0;
            if visible {
                scene.draw(&state, tile, size.width, size.height);
                scene.finish();
                if let Some(dir) = &options.dump_dir {
                    if options.dump_frames.contains(&state.frame) {
                        let name = format!("frame{}_{}_{}.ppm", state.frame, tile.col, tile.row);
                        let pixels = scene.read_pixels(size.width, size.height);
                        write_ppm(&dir.join(name), size.width, size.height, &pixels)?;
                    }
                }
            }
            thread::sleep(options.delay);

            link.finish_frame()?;
            if visible {
                surface.swap_buffers(&context)?;
            }
            if let Some(log) = &mut log {
                writeln!(
                    log,
                    "{},{:x},{},{},{},{},{}",
                    state.frame,
                    state.hash(),
                    since_epoch_micros(),
                    state.offset[0],
                    state.offset[1],
                    state.scale,
                    state.time
                )?;
            }

            frames_since_title += 1;
            if last_title.elapsed() >= TITLE_INTERVAL {
                let fps = f64::from(frames_since_title) / last_title.elapsed().as_secs_f64();
                window.set_title(&title(&state, tile, fps));
                last_title = Instant::now();
                frames_since_title = 0;
            }
        }
        Ok(())
    }
}

fn title(state: &FrameState, tile: Tile, fps: f64) -> String {
    format!(
        "libglass wall | tile {},{} of {}x{} | frame {} | {fps:.0} fps | x{:.2}",
        tile.col, tile.row, tile.cols, tile.rows, state.frame, state.scale
    )
}

/// How a node ended.
enum Finished {
    Render(Result<(), String>),
}

struct App {
    server: SocketAddr,
    options: Option<NodeOptions>,
    proxy: EventLoopProxy<Finished>,
    input: Sender<Input>,
    receiver: Option<Receiver<Input>>,
    stop: Arc<AtomicBool>,
    window: Option<Arc<Window>>,
    dragging: bool,
    cursor: Option<PhysicalPosition<f64>>,
    result: Result<(), String>,
}

/// A configuration with depth and, if possible, without antialiasing, as tiles have to
/// match where they meet.
fn choose_config(configs: Box<dyn Iterator<Item = Config> + '_>) -> Config {
    configs
        .reduce(|best, config| {
            if config.num_samples() < best.num_samples() {
                config
            } else {
                best
            }
        })
        .expect("the system has no OpenGL configuration")
}

impl App {
    /// Creates the window and its OpenGL context, and starts rendering in another thread.
    fn start(&mut self, event_loop: &ActiveEventLoop) -> Result<(), Box<dyn Error>> {
        let options = self.options.take().ok_or("the window already exists")?;
        let mut attributes = Window::default_attributes()
            .with_title("libglass wall | waiting for the coordinator")
            .with_inner_size(PhysicalSize::new(options.size.0, options.size.1));
        if let Some((x, y)) = options.position {
            attributes = attributes.with_position(PhysicalPosition::new(x, y));
        }

        let template = ConfigTemplateBuilder::new().with_depth_size(24);
        let (window, config) = DisplayBuilder::new()
            .with_window_attributes(Some(attributes))
            .build(event_loop, template, choose_config)?;
        let window = Arc::new(window.ok_or("could not create the window")?);

        let handle = window.window_handle()?.as_raw();
        let context_attributes = ContextAttributesBuilder::new()
            .with_context_api(ContextApi::OpenGl(Some(Version::new(3, 3))))
            .with_profile(GlProfile::Core)
            .build(Some(handle));
        let display = config.display();
        // SAFETY: the window that the handle belongs to outlives the context, as both last
        // until the process ends.
        let context = unsafe { display.create_context(&config, &context_attributes)? };
        let surface_attributes = window.build_surface_attributes(Default::default())?;
        // SAFETY: as above, for the surface.
        let surface = unsafe { display.create_window_surface(&config, &surface_attributes)? };

        let renderer = Renderer {
            server: self.server,
            options,
            window: window.clone(),
            context,
            surface,
            input: self.receiver.take().ok_or("the window already exists")?,
            stop: self.stop.clone(),
        };
        let proxy = self.proxy.clone();
        thread::spawn(move || {
            let result = renderer.run().map_err(|e| e.to_string());
            // The window may be gone by now, and then nobody is waiting for this.
            let _ = proxy.send_event(Finished::Render(result));
        });
        self.window = Some(window);
        Ok(())
    }

    fn send(&self, input: Input) {
        // The render thread has ended: the window is about to close.
        let _ = self.input.send(input);
    }
}

impl ApplicationHandler<Finished> for App {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        if self.window.is_none() {
            if let Err(e) = self.start(event_loop) {
                self.result = Err(e.to_string());
                event_loop.exit();
            }
        }
    }

    fn user_event(&mut self, event_loop: &ActiveEventLoop, event: Finished) {
        let Finished::Render(result) = event;
        self.result = result;
        event_loop.exit();
    }

    fn window_event(&mut self, event_loop: &ActiveEventLoop, _: WindowId, event: WindowEvent) {
        match event {
            WindowEvent::CloseRequested => {
                self.stop.store(true, Ordering::Relaxed);
                event_loop.exit();
            }
            WindowEvent::KeyboardInput { event, .. } => {
                if let PhysicalKey::Code(key) = event.physical_key {
                    self.send(Input::Key(key, event.state == ElementState::Pressed));
                }
            }
            WindowEvent::MouseInput {
                state,
                button: MouseButton::Left,
                ..
            } => self.dragging = state == ElementState::Pressed,
            WindowEvent::CursorMoved { position, .. } => {
                if let (true, Some(last)) = (self.dragging, self.cursor) {
                    self.send(Input::Drag(position.x - last.x, position.y - last.y));
                }
                self.cursor = Some(position);
            }
            WindowEvent::MouseWheel { delta, .. } => {
                let lines = match delta {
                    MouseScrollDelta::LineDelta(_, y) => f64::from(y),
                    MouseScrollDelta::PixelDelta(position) => position.y / PIXELS_PER_WHEEL_LINE,
                };
                self.send(Input::Wheel(lines));
            }
            _ => {}
        }
    }
}

/// Runs a node until the wall closes or the window does.
pub fn run_node(server: SocketAddr, options: NodeOptions) -> Result<(), BoxError> {
    let event_loop = EventLoop::<Finished>::with_user_event().build()?;
    let (input, receiver) = mpsc::channel();
    let mut app = App {
        server,
        options: Some(options),
        proxy: event_loop.create_proxy(),
        input,
        receiver: Some(receiver),
        stop: Arc::new(AtomicBool::new(false)),
        window: None,
        dragging: false,
        cursor: None,
        result: Ok(()),
    };
    event_loop.run_app(&mut app)?;
    app.result.map_err(Into::into)
}
