use tegdb::utils::make_marker_key;
use tegdb::constants::TXN_MARKER_PREFIX;

#[test]
fn test_make_marker_key() {
    // Test with different snapshot numbers
    let test_cases = vec![
        (1, format!("{}1", TXN_MARKER_PREFIX)),
        (42, format!("{}42", TXN_MARKER_PREFIX)),
        (1000, format!("{}1000", TXN_MARKER_PREFIX)),
        (u64::MAX, format!("{}18446744073709551615", TXN_MARKER_PREFIX)),
    ];

    for (snapshot, expected) in test_cases {
        let result = make_marker_key(snapshot);
        assert_eq!(result, expected, 
            "Failed for snapshot {}: expected '{}', got '{}'", 
            snapshot, expected, result);
    }
}

#[test]
fn test_make_marker_key_format() {
    // Test that the marker key follows the expected format
    let snapshot = 123;
    let marker = make_marker_key(snapshot);
    
    // Check that it starts with the correct prefix
    assert!(marker.starts_with(TXN_MARKER_PREFIX),
        "Marker key should start with '{}', got '{}'", 
        TXN_MARKER_PREFIX, marker);
    
    // Check that the snapshot number follows the prefix
    let snapshot_str = marker[TXN_MARKER_PREFIX.len()..].parse::<u64>();
    assert!(snapshot_str.is_ok(),
        "Failed to parse snapshot number from marker key '{}'", marker);
    assert_eq!(snapshot_str.unwrap(), snapshot,
        "Snapshot number mismatch in marker key '{}'", marker);
} 