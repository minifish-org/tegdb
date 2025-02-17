pub type Snapshot = u64;
// New: Define a KeyMap type alias.
pub type KeyMap = dashmap::DashMap<Vec<u8>, Vec<u8>>;
