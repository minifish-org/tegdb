use std::sync::atomic::{AtomicU64, Ordering};
use crate::engine::Engine;

const SNAPSHOT_KEY: &[u8] = b"__snapshot__";

// GLOBAL_SNAPSHOT is wrapped in an Arc<Mutex<_>> for optional persistence control if needed.
static GLOBAL_SNAPSHOT: AtomicU64 = AtomicU64::new(1);

// Call this once at startup to initialize the counter from the engine.
pub async fn init_snapshot(engine: &Engine) {
    let persisted = match engine.get(SNAPSHOT_KEY).await {
        Some(val) => match String::from_utf8(val) {
            Ok(s) => s.trim().parse::<u64>().unwrap_or(1),
            Err(_) => 1,
        },
        None => 1,
    };
    GLOBAL_SNAPSHOT.store(persisted, Ordering::Relaxed);
}

// Generates a new snapshot using the in-memory atomic counter.
pub fn get_atomic_snapshot() -> u64 {
    GLOBAL_SNAPSHOT.fetch_add(1, Ordering::Relaxed)
}

// Persists the current snapshot counter to the engine.
// This can be scheduled periodically or invoked during shutdown.
pub async fn persist_snapshot(engine: &Engine) {
    let current = GLOBAL_SNAPSHOT.load(Ordering::Relaxed);
    let _ = engine.set(SNAPSHOT_KEY, current.to_string().into_bytes()).await;
}
