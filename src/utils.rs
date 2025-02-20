use crate::constants::TXN_MARKER_PREFIX;

pub fn make_marker_key(snapshot: u64) -> String {
    format!("{}{}", TXN_MARKER_PREFIX, snapshot)
}
