use std::sync::atomic::{AtomicU64, Ordering};
use crate::engine::Engine;
use crate::types::Snapshot;
use std::io::Error;

pub const SNAPSHOT_KEY: &[u8] = b"__snapshot__";

// GLOBAL_SNAPSHOT is wrapped in an Arc<Mutex<_>> for optional persistence control if needed.
static GLOBAL_SNAPSHOT: AtomicU64 = AtomicU64::new(1);

// New: recover_snapshot scans for txn_marker keys to update the snapshot counter.
pub async fn recover_snapshot(engine: &Engine) -> Result<(), Error> {
    // Define range for keys starting with "txn_marker:"
    let lower_bound = b"txn_marker:".to_vec();
    let upper_bound = b"txn_marker;".to_vec();
    
    let mut iter = engine.reverse_scan(lower_bound.clone()..upper_bound.clone()).await?;
    let mut max_snapshot = 0;
    while let Some((key, _)) = iter.next() {
        if let Ok(key_str) = std::str::from_utf8(&key) {
            if key_str.starts_with("txn_marker:") {
                if let Ok(snap) = key_str["txn_marker:".len()..].parse::<u64>() {
                    if snap > max_snapshot {
                        max_snapshot = snap;
                    }
                }
            }
        }
    }
    // New snapshot value is max_snapshot + 1.
    let new_snapshot = max_snapshot + 1;
    engine.set(SNAPSHOT_KEY, new_snapshot.to_string().into_bytes()).await?;
    println!("Recovery updated snapshot counter to {}", new_snapshot);
    Ok(())
}

// Change synchronous init_snapshot to async.
pub async fn init_snapshot(engine: &Engine) {
    // Call recovery first.
    if let Err(e) = recover_snapshot(engine).await {
        eprintln!("Recovery failed with error: {:?}", e);
    }
    let persisted = match engine.get(SNAPSHOT_KEY).await {
        Some(val) => match String::from_utf8(val) {
            Ok(s) => s.trim().parse::<Snapshot>().unwrap_or(1),
            Err(_) => 1,
        },
        None => 1,
    };
    GLOBAL_SNAPSHOT.store(persisted, Ordering::Relaxed);
}

// Generates a new snapshot using the in-memory atomic counter.
pub fn get_atomic_snapshot() -> Snapshot {
    GLOBAL_SNAPSHOT.fetch_add(1, Ordering::Relaxed)
}

// Persists the current snapshot counter to the engine.
// This can be scheduled periodically or invoked during shutdown.
pub async fn persist_snapshot(engine: &Engine) {
    let current = GLOBAL_SNAPSHOT.load(Ordering::Relaxed);
    let _ = engine.set(SNAPSHOT_KEY, current.to_string().into_bytes()).await;
}
