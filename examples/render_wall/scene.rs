//! Draws the part of the scene that belongs to a tile, with OpenGL 3.3.
//!
//! Everything is computed from the frame state, so that all nodes agree. Each node
//! draws the whole scene and the projection crops it, which keeps the tiles aligned
//! to the pixel: this is the same as drawing once on a window as large as the wall.

use crate::sync::{FrameState, Tile};
use glow::HasContext;
use std::error::Error;

/// A matrix in the order that OpenGL expects, column by column.
pub type Mat4 = [f32; 16];

const IDENTITY: Mat4 = [
    1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0,
];

fn mul(a: &Mat4, b: &Mat4) -> Mat4 {
    let mut out = [0.0; 16];
    for col in 0..4 {
        for row in 0..4 {
            out[col * 4 + row] = (0..4).map(|k| a[k * 4 + row] * b[col * 4 + k]).sum();
        }
    }
    out
}

fn translation(x: f32, y: f32, z: f32) -> Mat4 {
    let mut out = IDENTITY;
    out[12] = x;
    out[13] = y;
    out[14] = z;
    out
}

fn scaling(x: f32, y: f32, z: f32) -> Mat4 {
    let mut out = IDENTITY;
    out[0] = x;
    out[5] = y;
    out[10] = z;
    out
}

fn rotation_x(angle: f32) -> Mat4 {
    let (sin, cos) = angle.sin_cos();
    let mut out = IDENTITY;
    out[5] = cos;
    out[6] = sin;
    out[9] = -sin;
    out[10] = cos;
    out
}

fn rotation_y(angle: f32) -> Mat4 {
    let (sin, cos) = angle.sin_cos();
    let mut out = IDENTITY;
    out[0] = cos;
    out[2] = -sin;
    out[8] = sin;
    out[10] = cos;
    out
}

fn perspective(fov_y: f32, aspect: f32, near: f32, far: f32) -> Mat4 {
    let f = 1.0 / (fov_y / 2.0).tan();
    let mut out = [0.0; 16];
    out[0] = f / aspect;
    out[5] = f;
    out[10] = (far + near) / (near - far);
    out[11] = -1.0;
    out[14] = 2.0 * far * near / (near - far);
    out
}

/// Makes the part of the canvas that belongs to a tile fill the window. The first row
/// is at the top.
fn crop(tile: Tile) -> Mat4 {
    let (cols, rows) = (tile.cols as f32, tile.rows as f32);
    let mut out = IDENTITY;
    out[0] = cols;
    out[5] = rows;
    out[12] = cols - 1.0 - 2.0 * tile.col as f32;
    out[13] = 2.0 * tile.row as f32 - (rows - 1.0);
    out
}

/// Field of view of the camera, in radians.
const FOV: f32 = std::f32::consts::FRAC_PI_3;

/// From the world to the whole canvas. At depth zero, one unit of the world has the same
/// size at any resolution: the canvas goes from -1 to 1 in height.
fn camera(aspect: f32) -> Mat4 {
    let distance = 1.0 / (FOV / 2.0).tan();
    mul(
        &perspective(FOV, aspect, 0.05, 100.0),
        &translation(0.0, 0.0, -distance),
    )
}

/// A color that is easy to tell apart from the ones next to it.
fn palette(index: u32) -> [f32; 3] {
    let hue = (index as f32 * 0.618_034).fract() * std::f32::consts::TAU;
    let channel = |shift: f32| 0.55 + 0.45 * (hue + shift).cos();
    [channel(0.0), channel(2.094), channel(4.189)]
}

const VERTEX_SHADER: &str = r#"#version 330 core
layout(location = 0) in vec3 position;
layout(location = 1) in vec3 normal;
uniform mat4 clip;
uniform mat4 model;
out vec3 world_normal;
void main() {
    world_normal = mat3(model) * normal;
    gl_Position = clip * model * vec4(position, 1.0);
}
"#;

const FRAGMENT_SHADER: &str = r#"#version 330 core
in vec3 world_normal;
uniform vec3 color;
uniform float lit;
out vec4 result;
void main() {
    float light = max(dot(normalize(world_normal), normalize(vec3(0.4, 0.7, 0.6))), 0.0);
    result = vec4(color * mix(1.0, 0.3 + 0.7 * light, lit), 1.0);
}
"#;

/// A cube from -0.5 to 0.5: position and normal of 36 vertices. Everything in the scene
/// is one of these, including the flat bars.
fn cube_vertices() -> Vec<f32> {
    let faces: [([f32; 3], [f32; 3], [f32; 3]); 6] = [
        ([1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]),
        ([-1.0, 0.0, 0.0], [0.0, 0.0, 1.0], [0.0, 1.0, 0.0]),
        ([0.0, 1.0, 0.0], [0.0, 0.0, 1.0], [1.0, 0.0, 0.0]),
        ([0.0, -1.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.0, 1.0]),
        ([0.0, 0.0, 1.0], [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]),
        ([0.0, 0.0, -1.0], [0.0, 1.0, 0.0], [1.0, 0.0, 0.0]),
    ];
    let corners = [
        (-0.5, -0.5),
        (0.5, -0.5),
        (0.5, 0.5),
        (-0.5, -0.5),
        (0.5, 0.5),
        (-0.5, 0.5),
    ];
    let mut out = Vec::with_capacity(36 * 6);
    for (normal, u, v) in faces {
        for (a, b) in corners {
            out.extend((0..3).map(|i| normal[i] * 0.5 + u[i] * a + v[i] * b));
            out.extend(normal);
        }
    }
    out
}

pub struct Scene {
    gl: glow::Context,
    clip: Option<glow::UniformLocation>,
    model: Option<glow::UniformLocation>,
    color: Option<glow::UniformLocation>,
    lit: Option<glow::UniformLocation>,
    vertices: i32,
}

impl Scene {
    /// A context must be current on this thread.
    pub fn new(gl: glow::Context) -> Result<Scene, Box<dyn Error + Send + Sync>> {
        // SAFETY: the shaders and the vertex data are valid for the calls that use them,
        // and the vertex attributes match the layout of `cube_vertices`.
        unsafe {
            let program = gl.create_program()?;
            for (kind, source) in [
                (glow::VERTEX_SHADER, VERTEX_SHADER),
                (glow::FRAGMENT_SHADER, FRAGMENT_SHADER),
            ] {
                let shader = gl.create_shader(kind)?;
                gl.shader_source(shader, source);
                gl.compile_shader(shader);
                if !gl.get_shader_compile_status(shader) {
                    return Err(gl.get_shader_info_log(shader).into());
                }
                gl.attach_shader(program, shader);
            }
            gl.link_program(program);
            if !gl.get_program_link_status(program) {
                return Err(gl.get_program_info_log(program).into());
            }
            gl.use_program(Some(program));

            let vertices = cube_vertices();
            let bytes: Vec<u8> = vertices.iter().flat_map(|v| v.to_ne_bytes()).collect();
            let array = gl.create_vertex_array()?;
            gl.bind_vertex_array(Some(array));
            let buffer = gl.create_buffer()?;
            gl.bind_buffer(glow::ARRAY_BUFFER, Some(buffer));
            gl.buffer_data_u8_slice(glow::ARRAY_BUFFER, &bytes, glow::STATIC_DRAW);
            gl.enable_vertex_attrib_array(0);
            gl.vertex_attrib_pointer_f32(0, 3, glow::FLOAT, false, 24, 0);
            gl.enable_vertex_attrib_array(1);
            gl.vertex_attrib_pointer_f32(1, 3, glow::FLOAT, false, 24, 12);

            gl.clear_color(0.04, 0.05, 0.09, 1.0);
            Ok(Scene {
                clip: gl.get_uniform_location(program, "clip"),
                model: gl.get_uniform_location(program, "model"),
                color: gl.get_uniform_location(program, "color"),
                lit: gl.get_uniform_location(program, "lit"),
                vertices: (vertices.len() / 6) as i32,
                gl,
            })
        }
    }

    fn cube(&self, model: &Mat4, color: [f32; 3]) {
        // SAFETY: the uniforms belong to the program in use, and the vertex array in use
        // has this many vertices.
        unsafe {
            let gl = &self.gl;
            gl.uniform_matrix_4_f32_slice(self.model.as_ref(), false, model);
            gl.uniform_3_f32(self.color.as_ref(), color[0], color[1], color[2]);
            gl.draw_arrays(glow::TRIANGLES, 0, self.vertices);
        }
    }

    fn set_clip(&self, clip: &Mat4, lit: f32) {
        // SAFETY: the uniforms belong to the program in use.
        unsafe {
            let gl = &self.gl;
            gl.uniform_matrix_4_f32_slice(self.clip.as_ref(), false, clip);
            gl.uniform_1_f32(self.lit.as_ref(), lit);
        }
    }

    /// A flat rectangle: its center, then its width and height.
    fn bar(&self, x: f32, y: f32, width: f32, height: f32, color: [f32; 3]) {
        let model = mul(&translation(x, y, 0.0), &scaling(width, height, 0.0));
        self.cube(&model, color);
    }

    /// Draws a frame on a window with this size.
    pub fn draw(&self, state: &FrameState, tile: Tile, width: u32, height: u32) {
        // SAFETY: changes only the state of the context, which is current on this thread.
        unsafe {
            let gl = &self.gl;
            gl.viewport(0, 0, width as i32, height as i32);
            gl.enable(glow::DEPTH_TEST);
            gl.clear(glow::COLOR_BUFFER_BIT | glow::DEPTH_BUFFER_BIT);
        }

        let crop = crop(tile);
        let clip = mul(&crop, &camera(tile.canvas_aspect() as f32));
        self.set_clip(&clip, 1.0);
        self.draw_world(state);

        // SAFETY: changes only the state of the context, which is current on this thread.
        unsafe { self.gl.disable(glow::DEPTH_TEST) };

        self.set_clip(&crop, 0.0);
        self.draw_sync_marks(state);
        if state.show_tiles {
            self.set_clip(&IDENTITY, 0.0);
            self.draw_tile_border(tile);
        }
    }

    /// What translation and scale act on: a grid, a large cube and the cubes around it.
    fn draw_world(&self, state: &FrameState) {
        let time = state.time as f32;
        let scale = state.scale as f32;
        let world = mul(
            &translation(state.offset[0] as f32, state.offset[1] as f32, 0.0),
            &scaling(scale, scale, scale),
        );

        const GRID_LINES: i32 = 12;
        const GRID_STEP: f32 = 0.5;
        const GRID_Z: f32 = -0.8;
        let length = GRID_LINES as f32 * GRID_STEP * 2.0;
        for line in -GRID_LINES..=GRID_LINES {
            let position = line as f32 * GRID_STEP;
            let (vertical, horizontal) = if line == 0 {
                ([0.9, 0.3, 0.3], [0.3, 0.9, 0.3])
            } else {
                ([0.22, 0.27, 0.38], [0.22, 0.27, 0.38])
            };
            let v = mul(
                &translation(position, 0.0, GRID_Z),
                &scaling(0.012, length, 0.001),
            );
            let h = mul(
                &translation(0.0, position, GRID_Z),
                &scaling(length, 0.012, 0.001),
            );
            self.cube(&mul(&world, &v), vertical);
            self.cube(&mul(&world, &h), horizontal);
        }

        let spin = mul(&rotation_y(time * 0.9), &rotation_x(time * 0.6));
        let center = mul(&spin, &scaling(0.55, 0.55, 0.55));
        self.cube(&mul(&world, &center), [0.9, 0.75, 0.3]);

        for i in 0..state.cubes {
            let share = i as f32 / state.cubes as f32;
            let angle = share * std::f32::consts::TAU + time * 0.5;
            let radius = 1.0 + 0.1 * (time * 1.3 + share * 12.0).sin();
            let place = translation(
                radius * angle.cos(),
                radius * angle.sin(),
                0.2 * (time * 2.0 + share * 18.0).sin(),
            );
            let own_spin = mul(&rotation_x(time * 1.7 + share), &rotation_y(time * 1.1));
            let model = mul(&place, &mul(&own_spin, &scaling(0.16, 0.16, 0.16)));
            self.cube(&mul(&world, &model), palette(i));
        }
    }

    /// Marks that cross the tiles and change all the time, and that ignore translation and
    /// scale. If nodes show different frames, they break where two tiles meet.
    fn draw_sync_marks(&self, state: &FrameState) {
        const SWEEP_SECONDS: f64 = 4.0;
        let sweep = (state.time / SWEEP_SECONDS).fract() as f32;
        self.bar(-1.0 + 2.0 * sweep, 0.0, 0.012, 2.0, [1.0, 0.95, 0.2]);
        self.bar(0.0, 2.0 * sweep - 1.0, 2.0, 0.016, [0.2, 0.95, 1.0]);

        // The number of the frame in binary, at the bottom of the canvas.
        const BITS: u32 = 24;
        const HEIGHT: f32 = 0.05;
        let cell = 2.0 / BITS as f32;
        for bit in 0..BITS {
            let on = state.frame >> bit & 1 == 1;
            let color = if on { [0.95; 3] } else { [0.12; 3] };
            let x = 1.0 - cell * (bit as f32 + 0.5);
            self.bar(x, HEIGHT / 2.0 - 1.0, cell * 0.9, HEIGHT, color);
        }
    }

    /// Shows where this tile ends. Each tile has its own color.
    fn draw_tile_border(&self, tile: Tile) {
        const THICKNESS: f32 = 0.02;
        let color = palette(tile.col + tile.row * tile.cols + 1);
        let edge = 1.0 - THICKNESS / 2.0;
        self.bar(0.0, edge, 2.0, THICKNESS, color);
        self.bar(0.0, -edge, 2.0, THICKNESS, color);
        self.bar(edge, 0.0, THICKNESS, 2.0, color);
        self.bar(-edge, 0.0, THICKNESS, 2.0, color);
    }

    /// Waits until the GPU has finished everything that was drawn.
    pub fn finish(&self) {
        // SAFETY: takes no arguments, and the context is current on this thread.
        unsafe { self.gl.finish() };
    }

    /// The pixels of the image being drawn, in RGB, starting from the bottom row.
    pub fn read_pixels(&self, width: u32, height: u32) -> Vec<u8> {
        let mut pixels = vec![0; width as usize * height as usize * 3];
        // SAFETY: the slice has room for exactly this many tightly packed RGB pixels.
        unsafe {
            let gl = &self.gl;
            gl.pixel_store_i32(glow::PACK_ALIGNMENT, 1);
            gl.read_pixels(
                0,
                0,
                width as i32,
                height as i32,
                glow::RGB,
                glow::UNSIGNED_BYTE,
                glow::PixelPackData::Slice(Some(&mut pixels)),
            );
        }
        pixels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Where a point of the world falls on a window, in pixels, from its bottom left.
    fn to_pixel(matrix: &Mat4, point: [f32; 3], width: f32, height: f32) -> Option<[f32; 2]> {
        let [x, y, z] = point;
        let clip = |row: usize| {
            matrix[row] * x + matrix[4 + row] * y + matrix[8 + row] * z + matrix[12 + row]
        };
        let w = clip(3);
        let (nx, ny) = (clip(0) / w, clip(1) / w);
        ((-1.0..=1.0).contains(&nx) && (-1.0..=1.0).contains(&ny))
            .then(|| [(nx + 1.0) / 2.0 * width, (ny + 1.0) / 2.0 * height])
    }

    /// Each point must fall on exactly one tile, and at the pixel where it would fall on a
    /// single window as large as all of them.
    #[test]
    fn tiles_put_each_point_where_the_whole_canvas_would() {
        let (cols, rows) = (3, 2);
        let (tile_width, tile_height) = (400.0, 300.0);
        let (total_width, total_height) = (tile_width * 3.0, tile_height * 2.0);
        let aspect = total_width / total_height;
        let whole = camera(aspect);

        for point in [
            [0.1, 0.2, 0.0],
            [-1.2, 0.7, 0.3],
            [1.4, -0.8, -0.5],
            [0.8, 0.6, 0.2],
        ] {
            let expected = to_pixel(&whole, point, total_width, total_height).unwrap();
            let mut found = 0;
            for row in 0..rows {
                for col in 0..cols {
                    let tile = Tile {
                        col,
                        row,
                        cols,
                        rows,
                    };
                    assert_eq!(tile.canvas_aspect() as f32, aspect);
                    let matrix = mul(&crop(tile), &whole);
                    let Some(pixel) = to_pixel(&matrix, point, tile_width, tile_height) else {
                        continue;
                    };
                    found += 1;
                    let x = pixel[0] + col as f32 * tile_width;
                    // The first row is at the top, and pixels count from the bottom.
                    let y = pixel[1] + (rows - 1 - row) as f32 * tile_height;
                    assert!((x - expected[0]).abs() < 0.01, "{x} {}", expected[0]);
                    assert!((y - expected[1]).abs() < 0.01, "{y} {}", expected[1]);
                }
            }
            assert_eq!(found, 1, "{point:?} must be on exactly one tile");
        }
    }

    #[test]
    fn matrices_multiply_in_the_order_of_opengl() {
        let moved_then_scaled = mul(&translation(1.0, 2.0, 3.0), &scaling(2.0, 2.0, 2.0));
        assert_eq!(moved_then_scaled[0], 2.0);
        assert_eq!(moved_then_scaled[12], 1.0);
        assert_eq!(moved_then_scaled[13], 2.0);
        assert_eq!(mul(&IDENTITY, &moved_then_scaled), moved_then_scaled);
    }

    #[test]
    fn the_cube_has_six_faces_facing_out() {
        let vertices = cube_vertices();
        assert_eq!(vertices.len(), 36 * 6);
        for vertex in vertices.chunks(6) {
            let (position, normal) = vertex.split_at(3);
            let along: f32 = (0..3).map(|i| position[i] * normal[i]).sum();
            assert_eq!(along, 0.5);
        }
    }
}
