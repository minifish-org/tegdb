use std::fs;

use tegdb::tgstream::state::ReplicationState;

#[test]
fn test_state_roundtrip_and_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("state_test.teg");
    fs::write(&db_path, b"stub").unwrap();

    let state_path = dir.path().join("state.stream.toml");

    // New state
    let mut state = ReplicationState::new(&db_path);
    state.last_committed_offset = 128;
    state.save(&state_path).unwrap();

    // Load state
    let loaded = ReplicationState::load_or_create(&state_path, &db_path).unwrap();
    assert_eq!(loaded.last_committed_offset, 128);

    // Simulate rotation by replacing the file
    let db_path2 = dir.path().join("state_test_rotated.teg");
    fs::write(&db_path2, b"stub2").unwrap();

    let mut loaded2 = ReplicationState::load_or_create(&state_path, &db_path).unwrap();
    // Force rotation detection by changing db_path to a different file
    loaded2.db_path = db_path2.clone();
    let rotated = loaded2.check_rotation().unwrap();
    assert!(rotated);
    assert_eq!(loaded2.last_committed_offset, 64); // reset to header size
}
