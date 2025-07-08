//! Storage backends for different platforms

pub mod browser_backend;
pub mod file_backend;

// Re-export the appropriate backend for the current platform
#[cfg(not(target_arch = "wasm32"))]
pub use file_backend::DefaultBackend;

#[cfg(target_arch = "wasm32")]
pub use browser_backend::DefaultBackend;
