/// Prefix used for transaction marker keys.
pub const TXN_MARKER_PREFIX: &str = "txn_marker:";

// New: Key separator constants.
pub const KEY_SEPARATOR: u8 = b':';

// New: Threshold constants for compaction.
pub const COMPACTION_INSERT_THRESHOLD: u64 = 10000;
pub const REMOVAL_RATIO_THRESHOLD: f64 = 0.3;

/// Threshold for triggering GC based on number of inserted records.
pub const GC_INSERT_THRESHOLD: usize = 10000;
/// Threshold ratio of removed to inserted records which triggers GC.
pub const GC_REMOVAL_RATIO_THRESHOLD: f64 = 0.3;

// New: Constants for key range boundaries.
pub const MIN_KEY_BYTE: u8 = 0;
pub const MAX_KEY_BYTE: u8 = 255;
