use crate::constants::TXN_MARKER_PREFIX;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::engine::Engine;
use crate::types::Snapshot;
use std::io::Error;

pub const SNAPSHOT_KEY: &[u8] = b"__snapshot__";

// Global snapshot counter.
static GLOBAL_SNAPSHOT: AtomicU64 = AtomicU64::new(1);

/// Recovers the maximum snapshot from committed txn_marker keys and updates the snapshot.
pub async fn recover_snapshot(engine: &Engine) -> Result<(), Error> {
    // Build range for keys starting with TXN_MARKER_PREFIX.
    let lower_bound = TXN_MARKER_PREFIX.as_bytes().to_vec();
    // Here, we expect txn marker keys to come immediately after the prefix. Adjust upper_bound if needed.
    // For example, appending a high value character.
    let mut upper_bound = TXN_MARKER_PREFIX.as_bytes().to_vec();
    upper_bound.push(b';');
    
    let mut iter = engine.reverse_scan(lower_bound.clone()..upper_bound.clone()).await?;
    let mut max_snapshot = 0;
    while let Some((key, _)) = iter.next() {
        if let Ok(key_str) = std::str::from_utf8(&key) {
            if key_str.starts_with(TXN_MARKER_PREFIX) {
                if let Ok(snap) = key_str[TXN_MARKER_PREFIX.len()..].parse::<u64>() {
                    if snap > max_snapshot {
                        max_snapshot = snap;
                    }
                }
            }
        }
    }
    let new_snapshot = max_snapshot + 1;
    engine.set(SNAPSHOT_KEY, new_snapshot.to_string().into_bytes()).await?;
    // println!("Recovery updated snapshot counter to {}", new_snapshot);
    Ok(())
}

/// Initializes the global snapshot using recovery then persisted value.
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

/// Returns a new atomic snapshot.
pub fn get_atomic_snapshot() -> Snapshot {
    GLOBAL_SNAPSHOT.fetch_add(1, Ordering::Relaxed)
}

/// Persists the current snapshot counter asynchronously.
pub async fn persist_snapshot(engine: &Engine) {
    let current = GLOBAL_SNAPSHOT.load(Ordering::Relaxed);
    let _ = engine.set(SNAPSHOT_KEY, current.to_string().into_bytes()).await;
}
