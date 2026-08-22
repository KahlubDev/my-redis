// src/frame.rs
// We use the Frame type from the mini-redis crate for parsing logic
// but we wrap it in our own Connection struct for I/O control.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;