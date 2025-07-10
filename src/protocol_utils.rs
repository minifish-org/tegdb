//! Protocol utilities for consistent backend identifier parsing
//!
//! This module provides utilities for parsing protocol-based identifiers
//! consistently across different storage backends.

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
    if identifier.starts_with("file://") {
        ("file", identifier.trim_start_matches("file://"))
    } else if identifier.starts_with("browser://") {
        ("browser", identifier.trim_start_matches("browser://"))
    } else if identifier.starts_with("localstorage://") {
        (
            "localstorage",
            identifier.trim_start_matches("localstorage://"),
        )
    } else if identifier.starts_with("indexeddb://") {
        ("indexeddb", identifier.trim_start_matches("indexeddb://"))
    } else {
        // Default to file protocol for backward compatibility
        ("file", identifier)
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


