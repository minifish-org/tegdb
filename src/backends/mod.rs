//! Storage backends for different platforms

pub mod browser_log_backend;
pub mod file_log_backend;

// Re-export the appropriate backend for the current platform
#[cfg(not(target_arch = "wasm32"))]
pub use file_log_backend::DefaultLogBackend;

#[cfg(target_arch = "wasm32")]
pub use browser_log_backend::DefaultLogBackend;
