use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use lazy_static::lazy_static;

struct Lock {
    locked: bool,
    // New: Owner identifies which transaction (by snapshot) owns this lock.
    owner: Option<u64>,
    notify: Arc<Notify>,
}

lazy_static! {
    static ref LOCKS: Mutex<HashMap<Vec<u8>, Arc<Mutex<Lock>>>> = Mutex::new(HashMap::new());
    // New: Global wait graph mapping waiting_txn_id -> set of txn_ids it is waiting on.
    static ref WAIT_GRAPH: Mutex<HashMap<u64, HashSet<u64>>> = Mutex::new(HashMap::new());
}

pub struct LockManager;

// New: Checks for a cycle in the wait graph starting from txn_id.
// Returns true if a cycle is detected.
fn has_deadlock(txn_id: u64, visited: &mut HashSet<u64>) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + '_>> {
    Box::pin(async move {
        if !visited.insert(txn_id) {
            return true; // cycle detected
        }
        let graph = WAIT_GRAPH.lock().await;
        if let Some(waiting_on) = graph.get(&txn_id) {
            for &other in waiting_on {
                if has_deadlock(other, visited).await {
                    return true;
                }
            }
        }
        false
    })
}

// New: Remove waiting edge from waiter to owner.
async fn remove_waiting_edge(waiter: u64, owner: u64) {
    let mut graph = WAIT_GRAPH.lock().await;
    if let Some(set) = graph.get_mut(&waiter) {
        set.remove(&owner);
        if set.is_empty() {
            graph.remove(&waiter);
        }
    }
}

impl LockManager {
    // Updated: Acquire a lock for the given key for transaction txn_id.
    // If a deadlock is detected, it returns early.
    pub async fn acquire_lock(key: Vec<u8>, txn_id: u64) {
        loop {
            let notify = {
                let mut locks = LOCKS.lock().await;
                if let Some(lock_arc) = locks.get(&key) {
                    let mut lock = lock_arc.lock().await;
                    if !lock.locked || lock.owner == Some(txn_id) {
                        lock.locked = true;
                        lock.owner = Some(txn_id);
                        // Notification of waiting tasks happens in release_lock.
                        remove_waiting_edge(txn_id, lock.owner.unwrap_or_default()).await;
                        return;
                    }
                    let owner = lock.owner.unwrap();
                    {
                        let mut graph = WAIT_GRAPH.lock().await;
                        graph.entry(txn_id).or_default().insert(owner);
                    }
                    let mut visited = HashSet::new();
                    if has_deadlock(txn_id, &mut visited).await {
                        remove_waiting_edge(txn_id, owner).await;
                        panic!("Deadlock detected between transactions.");
                    }
                    // The clone of notify is awaited below.
                    lock.notify.clone()
                } else {
                    let new_lock = Arc::new(Mutex::new(Lock {
                        locked: true,
                        owner: Some(txn_id),
                        notify: Arc::new(Notify::new()),
                    }));
                    locks.insert(key.clone(), new_lock);
                    return;
                }
            };
            notify.notified().await;
        }
    }

    // Updated: Releases the lock and notifies waiting tasks.
    pub async fn release_lock(key: Vec<u8>, txn_id: u64) {
        let locks = LOCKS.lock().await;
        if let Some(lock_arc) = locks.get(&key) {
            let mut lock = lock_arc.lock().await;
            if lock.owner == Some(txn_id) {
                lock.locked = false;
                lock.owner = None;
                lock.notify.notify_waiters();
            }
        }
    }
}
