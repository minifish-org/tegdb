use crossbeam_skiplist::SkipMap;

/// A snapshot is represented as a 64-bit unsigned integer.
pub type Snapshot = u64;
/// Map of binary keys to values.
pub type KeyMap = SkipMap<Vec<u8>, Vec<u8>>;
