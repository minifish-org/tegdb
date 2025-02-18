use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use lazy_static::lazy_static;

struct Lock {
    locked: bool,
    notify: Arc<Notify>,
}

lazy_static! {
    static ref LOCKS: Mutex<HashMap<Vec<u8>, Arc<Mutex<Lock>>>> = Mutex::new(HashMap::new());
}

pub struct LockManager;

impl LockManager {
    // Acquire a lock for the given key using Notify instead of loop-sleep.
    pub async fn acquire_lock(key: Vec<u8>) {
        loop {
            let notify_clone;
            {
                let mut locks = LOCKS.lock().await;
                if let Some(lock_arc) = locks.get(&key) {
                    let mut lock = lock_arc.lock().await;
                    if !lock.locked {
                        lock.locked = true;
                        return;
                    }
                    notify_clone = lock.notify.clone();
                } else {
                    let new_lock = Arc::new(Mutex::new(Lock {
                        locked: true,
                        notify: Arc::new(Notify::new()),
                    }));
                    locks.insert(key.clone(), new_lock);
                    return;
                }
            }
            // Wait until notified that the lock might be available.
            notify_clone.notified().await;
        }
    }

    // Releases the lock for the given key and notifies waiting tasks.
    pub async fn release_lock(key: Vec<u8>) {
        let locks = LOCKS.lock().await;
        if let Some(lock_arc) = locks.get(&key) {
            let mut lock = lock_arc.lock().await;
            lock.locked = false;
            lock.notify.notify_waiters();
        }
    }
}
