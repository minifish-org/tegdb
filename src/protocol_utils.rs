//! Protocol utilities for consistent backend identifier parsing
//!
//! This module provides utilities for parsing protocol-based identifiers
//! consistently across different storage backends.

/// Known backend protocol prefixes
pub const PROTOCOL_FILE: &str = "file://";
pub const PROTOCOL_BROWSER: &str = "browser://";
pub const PROTOCOL_LOCALSTORAGE: &str = "localstorage://";
pub const PROTOCOL_INDEXEDDB: &str = "indexeddb://";

/// Canonical protocol names (without ://)
pub const PROTOCOL_NAME_FILE: &str = "file";
pub const PROTOCOL_NAME_BROWSER: &str = "browser";
pub const PROTOCOL_NAME_LOCALSTORAGE: &str = "localstorage";
pub const PROTOCOL_NAME_INDEXEDDB: &str = "indexeddb";

/// Parse a storage identifier to extract the protocol and path
///
/// Supports the following protocols:
/// - `file://` - File-based storage (native platforms)
/// - `browser://` - Browser storage (WASM platforms)
/// - `localstorage://` - LocalStorage (WASM platforms)
/// - `indexeddb://` - IndexedDB (WASM platforms)
/// - No protocol - Defaults to file storage (backward compatibility)
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
/// let (protocol, path) = parse_storage_identifier("localstorage://my-app-db");
/// assert_eq!(protocol, "localstorage");
/// assert_eq!(path, "my-app-db");
///
/// let (protocol, path) = parse_storage_identifier("my_database.db");
/// assert_eq!(protocol, "file");
/// assert_eq!(path, "my_database.db");
/// ```
pub fn parse_storage_identifier(identifier: &str) -> (&str, &str) {
    if identifier.starts_with(PROTOCOL_FILE) {
        (PROTOCOL_NAME_FILE, identifier.trim_start_matches(PROTOCOL_FILE))
    } else if identifier.starts_with(PROTOCOL_BROWSER) {
        (PROTOCOL_NAME_BROWSER, identifier.trim_start_matches(PROTOCOL_BROWSER))
    } else if identifier.starts_with(PROTOCOL_LOCALSTORAGE) {
        (
            PROTOCOL_NAME_LOCALSTORAGE,
            identifier.trim_start_matches(PROTOCOL_LOCALSTORAGE),
        )
    } else if identifier.starts_with(PROTOCOL_INDEXEDDB) {
        (PROTOCOL_NAME_INDEXEDDB, identifier.trim_start_matches(PROTOCOL_INDEXEDDB))
    } else {
        // Default to file protocol for backward compatibility
        (PROTOCOL_NAME_FILE, identifier)
    }
}

/// Check if an identifier uses a specific protocol
pub fn has_protocol(identifier: &str, protocol: &str) -> bool {
    identifier.starts_with(&format!("{protocol}://"))
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
    fn test_parse_browser_protocol() {
        let (protocol, path) = parse_storage_identifier("browser://my-app-db");
        assert_eq!(protocol, "browser");
        assert_eq!(path, "my-app-db");
    }

    #[test]
    fn test_parse_localstorage_protocol() {
        let (protocol, path) = parse_storage_identifier("localstorage://my-app-db");
        assert_eq!(protocol, "localstorage");
        assert_eq!(path, "my-app-db");
    }

    #[test]
    fn test_parse_indexeddb_protocol() {
        let (protocol, path) = parse_storage_identifier("indexeddb://my-app-db");
        assert_eq!(protocol, "indexeddb");
        assert_eq!(path, "my-app-db");
    }

    #[test]
    fn test_parse_no_protocol() {
        let (protocol, path) = parse_storage_identifier("my_database.db");
        assert_eq!(protocol, "file");
        assert_eq!(path, "my_database.db");
    }

    #[test]
    fn test_has_protocol() {
        assert!(has_protocol("file:///path/to/db", "file"));
        assert!(has_protocol("browser://my-app-db", "browser"));
        assert!(!has_protocol("my_database.db", "file"));
    }

    #[test]
    fn test_extract_path() {
        assert_eq!(extract_path("file:///path/to/db"), "/path/to/db");
        assert_eq!(extract_path("localstorage://my-app-db"), "my-app-db");
        assert_eq!(extract_path("my_database.db"), "my_database.db");
    }
}
