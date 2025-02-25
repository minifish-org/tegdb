use crate::engine::Engine;
use crate::transaction::Transaction;
use crossbeam_skiplist::{SkipSet, SkipMap}; // Changed from dashmap::DashMap
use std::path::PathBuf;
use std::sync::Arc;
use std::io::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;
use std::collections::HashSet;
use crate::types::Snapshot;
use crate::constants::{KEY_SEPARATOR, MAX_KEY_BYTE, MIN_KEY_BYTE}; // New import for constants
use std::sync::atomic::AtomicU64; // new import for AtomicU64
use crate::utils::make_marker_key; // new import for make_marker_key

/// Simplified Lock using an atomic owner field.
pub struct Lock {
    pub owner: AtomicU64, // 0 means unlocked
    pub notify: Notify,
}

/// Manages active transactions, locks and garbage collection.
pub struct TransactionManager {
    pub active_transactions: Arc<SkipSet<Snapshot>>,
    pub oldest_read_snapshot: Arc<SkipSet<Snapshot>>,
    pub stop_gc: Arc<AtomicBool>,
    // Global counters for committed modifications.
    pub total_new: AtomicUsize,
    pub total_old: AtomicUsize,
    // Notify GC thread when thresholds are met.
    pub gc_notify: Arc<Notify>,

    // New: Lock and wait graph now use SkipMap.
    pub locks: SkipMap<Vec<u8>, Arc<Lock>>,
    pub wait_graph: SkipMap<u64, HashSet<u64>>,
}

impl Clone for TransactionManager {
    fn clone(&self) -> Self {
        Self {
            active_transactions: self.active_transactions.clone(),
            oldest_read_snapshot: self.oldest_read_snapshot.clone(),
            stop_gc: self.stop_gc.clone(),
            total_new: AtomicUsize::new(self.total_new.load(Ordering::Relaxed)),
            total_old: AtomicUsize::new(self.total_old.load(Ordering::Relaxed)),
            gc_notify: self.gc_notify.clone(),
            locks: SkipMap::new(),
            wait_graph: SkipMap::new(),
        }
    }
}

impl TransactionManager {
    /// Creates a new TransactionManager with empty state.
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(SkipSet::new()),
            oldest_read_snapshot: Arc::new(SkipSet::new()),
            stop_gc: Arc::new(AtomicBool::new(false)),
            total_new: AtomicUsize::new(0),
            total_old: AtomicUsize::new(0),
            gc_notify: Arc::new(Notify::new()),
            locks: SkipMap::new(),
            wait_graph: SkipMap::new(),
        }
    }
    
    /// Registers a transaction by inserting its snapshot.
    pub fn register_transaction(&self, snapshot: Snapshot) {
        self.active_transactions.insert(snapshot);
        self.oldest_read_snapshot.insert(snapshot);
    }
    
    /// Unregisters a transaction.
    pub fn unregister_transaction(&self, snapshot: Snapshot) {
        self.active_transactions.remove(&snapshot);
        self.oldest_read_snapshot.remove(&snapshot);
    }
    
    /// Returns the oldest active transaction snapshot or `Snapshot::MAX` if none.
    pub fn get_oldest_read_snapshot(&self) -> Snapshot {
        self.oldest_read_snapshot.iter()
            .next()
            .map(|entry| *entry.value())
            .unwrap_or(Snapshot::MAX)
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
                    if tm.stop_gc.load(Ordering::Relaxed) { break; }
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
        let oldest_read_snapshot = self.get_oldest_read_snapshot();
        // println!("GC: Starting cycle. Oldest read snapshot: {}", oldest_read_snapshot);
        const DELETED_MARKER: &[u8] = b"__deleted__";
        
        let mut current_key: Option<Vec<u8>> = None;
        
        let mut iter = engine.reverse_scan(vec![MIN_KEY_BYTE]..vec![MAX_KEY_BYTE]).await?;
        while let Some((key, value)) = iter.next() {
            if let Some(pos) = key.iter().rposition(|&b| b == KEY_SEPARATOR) {
                let logical_key = key[..pos].to_vec();
                if let Ok(snap_str) = std::str::from_utf8(&key[pos + 1..]) {
                    if let Ok(snapshot) = snap_str.parse::<Snapshot>() {
                        if snapshot < oldest_read_snapshot {
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
                                            if value.windows(DELETED_MARKER.len()).any(|w| w == DELETED_MARKER) {
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
        if tn > crate::constants::GC_INSERT_THRESHOLD &&
           (to as f64) / (tn as f64) >= crate::constants::GC_REMOVAL_RATIO_THRESHOLD {
            self.gc_notify.notify_one();
        }
    }

    /// Signals the GC thread to stop.
    pub fn stop_gc(&self) {
        self.stop_gc.store(true, Ordering::Relaxed);
        self.gc_notify.notify_one(); // notify GC thread to wake and exit
    }

    /// Asynchronously acquires a lock for the given key and transaction.
    pub async fn acquire_lock(&self, key: Vec<u8>, txn_id: u64) {
        loop {
            if let Some(lock_entry) = self.locks.get(&key) {
                let lock_arc = lock_entry.value();
                let current = lock_arc.owner.load(Ordering::Acquire);
                if current == 0 || current == txn_id {
                    if lock_arc.owner.compare_exchange(
                        current, txn_id, Ordering::AcqRel, Ordering::Acquire
                    ).is_ok() {
                        return;
                    }
                }
                // Replace entry API: update wait_graph for txn_id.
                if let Some(entry) = self.wait_graph.get(&txn_id) {
                    let mut set = entry.value().clone();
                    set.insert(current);
                    self.wait_graph.remove(&txn_id);
                    self.wait_graph.insert(txn_id, set);
                } else {
                    let mut set = HashSet::new();
                    set.insert(current);
                    self.wait_graph.insert(txn_id, set);
                }
                let mut visited = HashSet::new();
                if Self::has_deadlock(&self.wait_graph, txn_id, &mut visited) {
                    Self::remove_waiting_edge(&self.wait_graph, txn_id, current);
                    panic!("Deadlock detected between transactions.");
                }
                lock_arc.notify.notified().await;
            } else {
                let new_lock = Arc::new(Lock {
                    owner: AtomicU64::new(txn_id),
                    notify: Notify::new(),
                });
                self.locks.insert(key.clone(), new_lock);
                return;
            }
        }
    }

    /// Releases the lock for the given key held by the transaction.
    pub async fn release_lock(&self, key: Vec<u8>, txn_id: u64) {
        if let Some(entry) = self.locks.get(&key) {
            let lock_arc = entry.value();
            if lock_arc.owner.load(Ordering::Acquire) == txn_id {
                lock_arc.owner.store(0, Ordering::Release);
                lock_arc.notify.notify_waiters();
            }
        }
    }

    /// Checks for deadlock cycles asynchronously.
    fn has_deadlock(
        wait_graph: &SkipMap<u64, HashSet<u64>>,
        txn_id: u64,
        visited: &mut HashSet<u64>
    ) -> bool {
        if !visited.insert(txn_id) {
            return true;
        }
        if let Some(entry) = wait_graph.get(&txn_id) {
            for &other in entry.value().iter() {
                if Self::has_deadlock(wait_graph, other, visited) {
                    return true;
                }
            }
        }
        false
    }

    /// Removes a waiting edge from the transaction dependency graph.
    fn remove_waiting_edge(wait_graph: &SkipMap<u64, HashSet<u64>>, waiter: u64, owner: u64) {
        if let Some(entry) = wait_graph.get(&waiter) {
            let mut set = entry.value().clone();
            set.remove(&owner);
            drop(entry);
            wait_graph.remove(&waiter);
            if !set.is_empty() {
                wait_graph.insert(waiter, set);
            }
        }
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

    /// Retrieves the oldest active snapshot.
    pub fn get_oldest_read_snapshot(&self) -> Snapshot {
        self.transaction_manager.get_oldest_read_snapshot()
    }

    /// Begins a new transaction.
    pub async fn new_transaction(&self) -> Transaction {
        Transaction::begin(self.clone()).await
    }

    // Simplify shutdown: no atomic flag check.
    pub async fn shutdown(&self) {
        self.transaction_manager.stop_gc();
        crate::snapshot::persist_snapshot(&self.engine).await;
    }
}
