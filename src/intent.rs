use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::VecDeque;
use tokio::sync::Notify;
use crossbeam_skiplist::SkipMap;
use crate::types::Snapshot;

/// Represents a write intent on a key
pub struct Intent {
    /// Snapshot timestamp of the transaction that created this intent
    pub snapshot: Snapshot,
    /// Notify channel for waiting transactions
    pub notify: Notify,
    /// Whether this intent has been aborted 
    pub aborted: AtomicU64,
}

/// Manages write intents for serializable isolation
pub struct IntentManager {
    /// Map of key to queue of intents
    pub intents: SkipMap<Vec<u8>, VecDeque<Arc<Intent>>>,
}

impl IntentManager {
    pub fn new() -> Self {
        Self {
            intents: SkipMap::new(),
        }
    }

    /// Places a write intent on a key
    pub async fn place_intent(&self, key: Vec<u8>, snapshot: Snapshot) -> Result<(), std::io::Error> {
        let intent = Arc::new(Intent {
            snapshot,
            notify: Notify::new(),
            aborted: AtomicU64::new(0),
        });

        // Check for existing intent queue
        if let Some(entry) = self.intents.get(&key) {
            let mut intents = entry.value().clone();
            
            // If there's an intent at the front with the same snapshot, do nothing
            if let Some(front_intent) = intents.front() {
                if front_intent.snapshot == snapshot {
                    return Ok(());
                }
            }
            
            // Add new intent to the back of the queue
            intents.push_back(intent.clone());
            self.intents.insert(key.clone(), intents);
            
            // Check if we're not at the front of the queue
            if let Some(entry) = self.intents.get(&key) {
                let queue = entry.value();
                if queue.len() > 1 && !Arc::ptr_eq(queue.front().unwrap(), &intent) {
                    // Wait until our intent becomes the front or gets resolved
                    intent.notify.notified().await;
                    
                    // After waking up, check if we were aborted while waiting
                    if intent.aborted.load(Ordering::Acquire) == 1 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other, 
                            "Transaction aborted due to serializability conflict"
                        ));
                    }
                }
            }
        } else {
            // No existing intents for this key, create new queue with this intent
            let mut intents = VecDeque::new();
            intents.push_back(intent);
            self.intents.insert(key, intents);
        }
        
        Ok(())
    }

    /// Resolves an intent (commits or aborts)
    pub fn resolve_intent(&self, key: &[u8], snapshot: Snapshot, commit: bool) {
        if let Some(entry) = self.intents.get(key) {
            let mut intents = entry.value().clone();
            
            // Check if the front intent matches our snapshot
            if let Some(intent) = intents.front() {
                if intent.snapshot == snapshot {
                    // Mark as resolved and notify waiters
                    intent.notify.notify_waiters();
                    
                    // Remove the front intent
                    intents.pop_front();
                    
                    if commit && !intents.is_empty() {
                        // If committing, check all remaining intents for serializability violations
                        if let Some(next_intent) = intents.front() {
                            // If the next intent's snapshot is smaller, abort it
                            // as it would break serializability guarantees
                            if next_intent.snapshot < snapshot {
                                // Mark the next transaction as aborted (0 = aborted)
                                next_intent.aborted.store(1, Ordering::Release);
                                next_intent.notify.notify_waiters();
                            }
                        }
                    } else if !commit && !intents.is_empty() {
                        // If rolling back (aborting), simply notify the next intent to continue
                        if let Some(next_intent) = intents.front() {
                            next_intent.notify.notify_waiters();
                        }
                    }
                    
                    if intents.is_empty() {
                        // No more intents, remove the entire key entry
                        self.intents.remove(key);
                    }
                }
            }
        }
    }

    /// Checks if a key has an unresolved intent
    pub fn has_intent(&self, key: &[u8]) -> bool {
        self.intents.get(key).map_or(false, |entry| !entry.value().is_empty())
    }

    /// Gets the intent for a key if it exists
    pub fn get_intent(&self, key: &[u8]) -> Option<Arc<Intent>> {
        self.intents.get(key).and_then(|intents| intents.front().cloned())
    }
}