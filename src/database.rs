use crate::constants::{KEY_SEPARATOR, MAX_KEY_BYTE, MIN_KEY_BYTE}; // New import for constants
use crate::engine::Engine;
use crate::transaction::Transaction;
use crate::types::Snapshot;
use crate::utils::make_marker_key;
use crate::intent::IntentManager;
// SkipList chosen for efficient concurrent operations and natural sorted order
use crossbeam_skiplist::{SkipMap, SkipSet};
use std::collections::HashSet;
use std::io::Error;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify; // new import for make_marker_key

/// TransactionManager implements the Database layer of the two-layer architecture.
/// Manages active transactions, intents, and garbage collection using SkipList for concurrent access.
pub struct TransactionManager {
    // SkipList for efficient concurrent access to active transactions
    pub active_transactions: Arc<SkipSet<Snapshot>>,
    pub stop_gc: Arc<AtomicBool>,
    // Global counters for committed modifications
    pub total_new: AtomicUsize,
    pub total_old: AtomicUsize,
    // Notify GC thread when thresholds are met
    pub gc_notify: Arc<Notify>,
    // Intent manager for serializable isolation
    pub intent_manager: Arc<IntentManager>,
    // SkipMap for wait graph
    pub wait_graph: SkipMap<Snapshot, HashSet<Snapshot>>,
}

impl Clone for TransactionManager {
    fn clone(&self) -> Self {
        Self {
            active_transactions: self.active_transactions.clone(),
            stop_gc: self.stop_gc.clone(),
            total_new: AtomicUsize::new(self.total_new.load(Ordering::Relaxed)),
            total_old: AtomicUsize::new(self.total_old.load(Ordering::Relaxed)),
            gc_notify: self.gc_notify.clone(),
            intent_manager: self.intent_manager.clone(),
            wait_graph: SkipMap::new(),
        }
    }
}

impl TransactionManager {
    /// Creates a new TransactionManager with empty state.
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(SkipSet::new()),
            stop_gc: Arc::new(AtomicBool::new(false)),
            total_new: AtomicUsize::new(0),
            total_old: AtomicUsize::new(0),
            gc_notify: Arc::new(Notify::new()),
            intent_manager: Arc::new(IntentManager::new()),
            wait_graph: SkipMap::new(),
        }
    }

    /// Registers a transaction by inserting its snapshot.
    pub fn register_transaction(&self, snapshot: Snapshot) {
        self.active_transactions.insert(snapshot);
    }

    /// Unregisters a transaction and cleans up its intents.
    pub fn unregister_transaction(&self, snapshot: Snapshot) {
        self.active_transactions.remove(&snapshot);
        // No need to clean up txn_intents as it has been removed
    }

    /// Returns the oldest active transaction snapshot or `Snapshot::MAX` if none.
    pub fn get_safe_snapshot(&self) -> Snapshot {
        self.active_transactions
            .iter()
            .next()
            .map(|entry| *entry.value())
            .unwrap_or(Snapshot::MAX)
    }

    /// Places a write intent for a transaction
    pub async fn place_intent(&self, key: Vec<u8>, snapshot: Snapshot) -> Result<(), std::io::Error> {
        self.intent_manager.place_intent(key, snapshot).await
    }

    /// Resolves a write intent
    pub fn resolve_intent(&self, key: &[u8], snapshot: Snapshot, commit: bool) {
        self.intent_manager.resolve_intent(key, snapshot, commit);
    }

    /// Checks if a key has an unresolved intent
    pub fn has_intent(&self, key: &[u8]) -> bool {
        self.intent_manager.has_intent(key)
    }

    /// Gets the intent for a key if it exists
    pub fn get_intent(&self, key: &[u8]) -> Option<Arc<crate::intent::Intent>> {
        self.intent_manager.get_intent(key)
    }

    /// Spawns the GC thread in a dedicated runtime.
    pub fn start_gc(&self, engine: Engine) {
        let tm = self.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            // Make engine mutable for compaction.
            let mut engine = engine;
            runtime.block_on(async move {
                loop {
                    // Wait indefinitely until notified or check stop flag.
                    tm.gc_notify.notified().await;
                    if tm.stop_gc.load(Ordering::Relaxed) {
                        break;
                    }
                    // println!("GC thread awakened by notify");
                    // Persist the snapshot key once per GC cycle.
                    crate::snapshot::persist_snapshot(&engine).await;
                    if let Err(e) = tm.garbage_collect(&engine).await {
                        eprintln!("GC error: {:?}", e);
                    } else {
                        // Trigger compaction at the end of the GC cycle.
                        if let Err(e) = engine.compact() {
                            eprintln!("Compaction error: {:?}", e);
                        }
                    }
                    // Reset counters after GC: total_new = total_new - total_old, total_old = 0.
                    let tn = tm.total_new.load(Ordering::Relaxed);
                    let to = tm.total_old.load(Ordering::Relaxed);
                    tm.total_new.store(tn.saturating_sub(to), Ordering::Relaxed);
                    tm.total_old.store(0, Ordering::Relaxed);
                }
            });
        });
    }

    /// Runs garbage collection based on the oldest snapshot using a single scan.
    pub async fn garbage_collect(&self, engine: &Engine) -> Result<(), Error> {
        let safe_snapshot = self.get_safe_snapshot();
        // println!("GC: Starting cycle. Oldest read snapshot: {}", oldest_read_snapshot);
        const DELETED_MARKER: &[u8] = b"__deleted__";

        let mut current_key: Option<Vec<u8>> = None;

        let iter = engine
            .reverse_scan(vec![MIN_KEY_BYTE]..vec![MAX_KEY_BYTE])
            .await?;
        for (key, value) in iter {
            if let Some(pos) = key.iter().rposition(|&b| b == KEY_SEPARATOR) {
                let logical_key = key[..pos].to_vec();
                if let Ok(snap_str) = std::str::from_utf8(&key[pos + 1..]) {
                    if let Ok(snapshot) = snap_str.parse::<Snapshot>() {
                        if snapshot < safe_snapshot {
                            match &current_key {
                                Some(current) if *current == logical_key => {
                                    // Not the first version, can be deleted
                                    engine.del(&key).await?;
                                }
                                _ => {
                                    // First version of a new key
                                    current_key = Some(logical_key);

                                    // Check transaction marker
                                    let txn_marker_key = make_marker_key(snapshot);
                                    match engine.get(txn_marker_key.as_bytes()).await {
                                        Some(marker) if marker == b"commit" => {
                                            // If committed but marked as deleted, remove it
                                            if value
                                                .windows(DELETED_MARKER.len())
                                                .any(|w| w == DELETED_MARKER)
                                            {
                                                engine.del(&key).await?;
                                            }
                                        }
                                        _ => {
                                            // Transaction not committed, delete the key
                                            engine.del(&key).await?;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Pushes counters and notifies the GC thread when GC thresholds are exceeded.
    pub fn push_counters(&self, new: usize, old: usize) {
        self.total_new.fetch_add(new, Ordering::Relaxed);
        self.total_old.fetch_add(old, Ordering::Relaxed);
        let tn = self.total_new.load(Ordering::Relaxed);
        let to = self.total_old.load(Ordering::Relaxed);
        if tn > crate::constants::GC_INSERT_THRESHOLD
            && (to as f64) / (tn as f64) >= crate::constants::GC_REMOVAL_RATIO_THRESHOLD
        {
            self.gc_notify.notify_one();
        }
    }

    /// Signals the GC thread to stop.
    pub fn stop_gc(&self) {
        self.stop_gc.store(true, Ordering::Relaxed);
        self.gc_notify.notify_one(); // notify GC thread to wake and exit
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Database encapsulating the Engine and TransactionManager.
#[derive(Clone)]
pub struct Database {
    pub engine: Engine,
    pub transaction_manager: TransactionManager,
}

impl Database {
    /// Initializes a new Database with Engine and starts the GC thread.
    pub async fn new(path: PathBuf) -> Self {
        let engine = Engine::new(path);
        crate::snapshot::init_snapshot(&engine).await;
        let tm = TransactionManager::new();
        tm.start_gc(engine.clone());
        Self {
            engine,
            transaction_manager: tm,
        }
    }

    /// Updates GC metrics from transactions.
    pub fn push_counters(&self, new: usize, old: usize) {
        self.transaction_manager.push_counters(new, old);
    }

    /// Stops the GC thread.
    pub fn stop_gc(&self) {
        self.transaction_manager.stop_gc();
    }

    /// Registers a new transaction.
    pub fn register_transaction(&self, snapshot: Snapshot) {
        self.transaction_manager.register_transaction(snapshot);
    }

    /// Unregisters a transaction.
    pub fn unregister_transaction(&self, snapshot: Snapshot) {
        self.transaction_manager.unregister_transaction(snapshot);
    }

    /// Begins a new transaction.
    pub async fn new_transaction(&self) -> Transaction {
        Transaction::begin(self.clone()).await
    }

    /// Returns all active transaction snapshots from the TransactionManager.
    pub fn get_active_transactions(&self) -> Vec<crate::types::Snapshot> {
        self.transaction_manager
            .active_transactions
            .iter()
            .map(|entry| *entry.value())
            .collect()
    }

    // Simplify shutdown: no atomic flag check.
    pub async fn shutdown(&self) {
        self.transaction_manager.stop_gc();
        crate::snapshot::persist_snapshot(&self.engine).await;
    }
}
