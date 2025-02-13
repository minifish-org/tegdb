use crate::engine::Engine;
use crossbeam_skiplist::SkipSet;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct Database {
    pub engine: Engine,
    active_transactions: Arc<SkipSet<u128>>,
}

impl Database {
    pub fn new(path: PathBuf) -> Self {
        Self {
            engine: Engine::new(path),
            active_transactions: Arc::new(SkipSet::new()),
        }
    }

    pub fn register_transaction(&self, snapshot: u128) {
        self.active_transactions.insert(snapshot);
    }

    pub fn unregister_transaction(&self, snapshot: u128) {
        self.active_transactions.remove(&snapshot);
    }

    pub fn active_transactions(&self) -> Vec<u128> {
        self.active_transactions.iter().map(|entry| *entry).collect()
    }
}
