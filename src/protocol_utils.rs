//! Protocol utilities for storage identifier parsing
//!
//! TegDB now targets native platforms exclusively, so only the `file://` protocol
//! is supported when opening databases.

/// File-based storage protocol prefix
pub const PROTOCOL_FILE: &str = "file://";

/// Canonical protocol name
pub const PROTOCOL_NAME_FILE: &str = "file";

/// RPC-based storage protocol prefix
pub const PROTOCOL_RPC: &str = "rpc://";

/// Canonical protocol name
pub const PROTOCOL_NAME_RPC: &str = "rpc";

/// Parse a storage identifier to extract the protocol and path
///
/// Supports the following forms:
/// - `file://` absolute paths (preferred)
/// - Identifiers without a protocol, which default to `file`
///
/// # Examples
///
/// ```
/// use tegdb::protocol_utils::parse_storage_identifier;
///
/// let (protocol, path) = parse_storage_identifier("file:///path/to/db");
/// assert_eq!(protocol, "file");
/// assert_eq!(path, "/path/to/db");
///
/// let (protocol, path) = parse_storage_identifier("my_database.db");
/// assert_eq!(protocol, "file");
/// assert_eq!(path, "my_database.db");
/// ```
pub fn parse_storage_identifier(identifier: &str) -> (&str, &str) {
    if identifier.starts_with(PROTOCOL_FILE) {
        (
            PROTOCOL_NAME_FILE,
            identifier.trim_start_matches(PROTOCOL_FILE),
        )
    } else if identifier.starts_with(PROTOCOL_RPC) {
        (
            PROTOCOL_NAME_RPC,
            identifier.trim_start_matches(PROTOCOL_RPC),
        )
    } else {
        // Default to file protocol for backward compatibility
        (PROTOCOL_NAME_FILE, identifier)
    }
}

/// Check if an identifier uses the `file://` protocol explicitly
pub fn has_protocol(identifier: &str, protocol: &str) -> bool {
    match protocol {
        PROTOCOL_NAME_FILE => identifier.starts_with(PROTOCOL_FILE),
        PROTOCOL_NAME_RPC => identifier.starts_with(PROTOCOL_RPC),
        _ => false,
    }
}

/// Extract the path part from a storage identifier
pub fn extract_path(identifier: &str) -> &str {
    parse_storage_identifier(identifier).1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_file_protocol() {
        let (protocol, path) = parse_storage_identifier("file:///path/to/db");
        assert_eq!(protocol, "file");
        assert_eq!(path, "/path/to/db");
    }

    #[test]
    fn test_parse_no_protocol() {
        let (protocol, path) = parse_storage_identifier("my_database.db");
        assert_eq!(protocol, "file");
        assert_eq!(path, "my_database.db");
    }

    #[test]
    fn test_parse_rpc_protocol() {
        let (protocol, path) = parse_storage_identifier("rpc://127.0.0.1:9000");
        assert_eq!(protocol, "rpc");
        assert_eq!(path, "127.0.0.1:9000");
    }

    #[test]
    fn test_has_protocol() {
        assert!(has_protocol("file:///path/to/db", "file"));
        assert!(has_protocol("rpc://127.0.0.1:9000", "rpc"));
        assert!(!has_protocol("my_database.db", "file"));
    }

    #[test]
    fn test_extract_path() {
        assert_eq!(extract_path("file:///path/to/db"), "/path/to/db");
        assert_eq!(extract_path("my_database.db"), "my_database.db");
    }
}
