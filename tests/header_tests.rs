use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use tempfile::TempDir;

const MAGIC: &[u8] = b"TEGDB\0";
const HEADER_SIZE: usize = 64;

#[test]
fn writes_header_on_create() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("tegdb").with_extension("teg");
    let identifier = format!("file://{}", db_path.to_string_lossy());

    {
        // Create database
        let mut db = tegdb::Database::open(&identifier).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT(32))")
            .unwrap();
    } // drop

    let mut f = File::open(&db_path).unwrap();
    let mut header = vec![0u8; HEADER_SIZE];
    f.read_exact(&mut header).unwrap();
    assert_eq!(&header[0..MAGIC.len()], MAGIC);
    // version == 1
    assert_eq!(u16::from_be_bytes([header[6], header[7]]), 1);
    assert_eq!(header.len(), HEADER_SIZE);
}

#[test]
fn rejects_missing_or_wrong_magic() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("bad").with_extension("teg");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        // Write bogus header of correct size but wrong magic (6 bytes)
        let mut bogus = vec![0u8; HEADER_SIZE];
        bogus[0..6].copy_from_slice(b"BADDB!");
        f.write_all(&bogus).unwrap();
        f.flush().unwrap();
    }
    let identifier = format!("file://{}", db_path.to_string_lossy());
    let res = tegdb::Database::open(&identifier);
    assert!(res.is_err());
    let msg = format!("{}", res.err().unwrap());
    assert!(msg.contains("Invalid storage file magic"));
}

#[test]
fn reads_entries_after_header() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("entries").with_extension("teg");
    let identifier = format!("file://{}", db_path.to_string_lossy());

    {
        let mut db = tegdb::Database::open(&identifier).unwrap();
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32))")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
    }

    {
        let mut db = tegdb::Database::open(&identifier).unwrap();
        let result = db.query("SELECT name FROM users WHERE id = 1").unwrap();
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.get_cell_text(0, 0).as_deref(), Some("Alice"));
    }
}

#[cfg(feature = "dev")]
#[test]
fn compaction_preserves_header() {
    use tegdb::storage_engine::StorageEngine;

    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("compact").with_extension("teg");

    // Use low-level engine API
    let mut engine = StorageEngine::new(db_path.clone()).unwrap();
    // write some data
    engine.set(b"k1", b"v1".to_vec()).unwrap();
    engine.set(b"k2", b"v2".to_vec()).unwrap();
    engine.del(b"k1").unwrap();

    // Compact
    engine.compact().unwrap();
    drop(engine);

    // Verify header still present and correct
    let mut f = File::open(&db_path).unwrap();
    let mut header = vec![0u8; HEADER_SIZE];
    f.read_exact(&mut header).unwrap();
    assert_eq!(&header[0..MAGIC.len()], MAGIC);
    assert_eq!(u16::from_be_bytes([header[6], header[7]]), 1);

    // Also ensure remaining data starts after header
    f.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
    let mut rest = Vec::new();
    f.read_to_end(&mut rest).unwrap();
    assert!(!rest.is_empty());
}
