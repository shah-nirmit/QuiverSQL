pub mod models;
pub mod engine;

pub use engine::DqlEngine;

pub fn init_core() {
    eprintln!("DQL Core initialized");
}
