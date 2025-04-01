use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;
use crossbeam_skiplist::SkipMap;
use crate::types::Snapshot;

/// Represents a write intent on a key
pub struct Intent {
    /// Snapshot timestamp of the transaction that created this intent
    pub snapshot: Snapshot,
    /// Notify channel for waiting transactions
    pub notify: Notify,
    /// Whether this intent has been resolved
    pub resolved: AtomicU64,
}

/// Manages write intents for serializable isolation
pub struct IntentManager {
    /// Map of key to intent
    pub intents: SkipMap<Vec<u8>, Arc<Intent>>,
    /// Map of transaction snapshot to set of keys it has intents on
    pub txn_intents: SkipMap<Snapshot, Vec<Vec<u8>>>,
}

impl IntentManager {
    pub fn new() -> Self {
        Self {
            intents: SkipMap::new(),
            txn_intents: SkipMap::new(),
        }
    }

    /// Places a write intent on a key
    pub async fn place_intent(&self, key: Vec<u8>, snapshot: Snapshot) -> Result<(), std::io::Error> {
        let intent = Arc::new(Intent {
            snapshot,
            notify: Notify::new(),
            resolved: AtomicU64::new(0),
        });

        // Check for existing intent
        if let Some(existing) = self.intents.get(&key) {
            let existing_intent = existing.value();
            let existing_snapshot = existing_intent.snapshot;
            
            if existing_snapshot != snapshot {
                // Wait for existing intent to be resolved
                existing_intent.notify.notified().await;
            }
        }

        // Place new intent
        self.intents.insert(key.clone(), intent);
        
        // Record intent for transaction
        if let Some(entry) = self.txn_intents.get(&snapshot) {
            let mut keys = entry.value().clone();
            keys.push(key);
            self.txn_intents.remove(&snapshot);
            self.txn_intents.insert(snapshot, keys);
        } else {
            self.txn_intents.insert(snapshot, vec![key]);
        }

        Ok(())
    }

    /// Resolves an intent (commits or aborts)
    pub fn resolve_intent(&self, key: &[u8], snapshot: Snapshot, commit: bool) {
        if let Some(entry) = self.intents.get(key) {
            let intent = entry.value();
            if intent.snapshot == snapshot {
                intent.resolved.store(if commit { 1 } else { 0 }, Ordering::Release);
                intent.notify.notify_waiters();
                self.intents.remove(key);
            }
        }
    }

    /// Cleans up all intents for a transaction
    pub fn cleanup_txn_intents(&self, snapshot: Snapshot) {
        if let Some(entry) = self.txn_intents.get(&snapshot) {
            let keys = entry.value();
            for key in keys {
                self.resolve_intent(key, snapshot, false);
            }
            self.txn_intents.remove(&snapshot);
        }
    }

    /// Checks if a key has an unresolved intent
    pub fn has_intent(&self, key: &[u8]) -> bool {
        self.intents.get(key).is_some()
    }

    /// Gets the intent for a key if it exists
    pub fn get_intent(&self, key: &[u8]) -> Option<Arc<Intent>> {
        self.intents.get(key).map(|entry| entry.value().clone())
    }
} 