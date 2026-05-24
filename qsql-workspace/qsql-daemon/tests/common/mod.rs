#![allow(dead_code)]
// Shared helpers for integration tests. Each tests/*.rs is its own crate, so
// individual files include this module via `mod common;` and pick the helpers
// they need. Items unused by a given test crate trigger dead-code warnings —
// the allow above silences them at the module boundary.

pub mod memory;
