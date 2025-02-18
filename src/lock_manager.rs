use std::collections::HashMap;
use tokio::sync::Mutex;
use lazy_static::lazy_static;

lazy_static! {
    static ref LOCKS: Mutex<HashMap<Vec<u8>, bool>> = Mutex::new(HashMap::new());
}

pub struct LockManager;

impl LockManager {
    // Acquire a lock for the given key. Waits until the lock becomes available.
    pub async fn acquire_lock(key: Vec<u8>) {
        loop {
            {
                let mut locks = LOCKS.lock().await;
                if !locks.get(&key).copied().unwrap_or(false) {
                    locks.insert(key.clone(), true);
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    // Releases the lock for the given key.
    pub async fn release_lock(key: Vec<u8>) {
        let mut locks = LOCKS.lock().await;
        locks.remove(&key);
    }
}
