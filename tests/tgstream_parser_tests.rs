use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use tegdb::log::{
    LENGTH_FIELD_BYTES, STORAGE_FORMAT_VERSION, STORAGE_HEADER_SIZE, STORAGE_MAGIC,
    TX_COMMIT_MARKER,
};
use tegdb::tgstream::parser::find_last_commit_offset;

fn write_header(file: &mut std::fs::File, valid_data_end: u64) {
    let mut header = vec![0u8; STORAGE_HEADER_SIZE];
    header[0..STORAGE_MAGIC.len()].copy_from_slice(STORAGE_MAGIC);
    header[6..8].copy_from_slice(&STORAGE_FORMAT_VERSION.to_be_bytes());
    header[8..12].copy_from_slice(&0u32.to_be_bytes());
    // max_key/max_val are not validated by parser, fill with defaults
    header[12..16].copy_from_slice(&(1024u32).to_be_bytes());
    header[16..20].copy_from_slice(&(256 * 1024u32).to_be_bytes());
    header[20] = 1u8; // BE
                      // Record valid_data_end [21..29)
    header[21..29].copy_from_slice(&valid_data_end.to_be_bytes());
    file.write_all(&header).unwrap();
}

fn write_record(file: &mut std::fs::File, key: &[u8], value: &[u8]) {
    let key_len = (key.len() as u32).to_be_bytes();
    let val_len = (value.len() as u32).to_be_bytes();
    file.write_all(&key_len).unwrap();
    file.write_all(&val_len).unwrap();
    file.write_all(key).unwrap();
    file.write_all(value).unwrap();
}

#[test]
fn test_find_last_commit_offset_simple() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("parser_test.teg");
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();

    // Write placeholder header first
    write_header(&mut f, STORAGE_HEADER_SIZE as u64);

    // Entry 1
    write_record(&mut f, b"k1", b"v1");
    // Commit 1
    write_record(&mut f, TX_COMMIT_MARKER, b"");
    // Entry 2 (after commit)
    write_record(&mut f, b"k2", b"v2");

    // Get current position as valid_data_end
    let valid_data_end = f.stream_position().unwrap();

    // Incomplete trailing record (key_len only) – should be ignored (outside valid_data_end)
    f.write_all(&(4u32.to_be_bytes())).unwrap();

    f.flush().unwrap();

    // Update header with correct valid_data_end
    f.seek(SeekFrom::Start(0)).unwrap();
    write_header(&mut f, valid_data_end);
    f.flush().unwrap();

    let mut rf = OpenOptions::new().read(true).open(&path).unwrap();
    let off = find_last_commit_offset(&mut rf).unwrap();

    // We expect offset to be right after the commit record written
    // The commit record layout: 4 + 4 + key_len + value_len = 8 + 12 + 0 = 20 for key "__TX_COMMIT__"
    // Compute expected position by re-parsing using the same logic: header + k1 + commit
    // Simpler: ensure it is at least after header and larger than first entry
    assert!(off > STORAGE_HEADER_SIZE as u64);

    // Seek to the offset and ensure we can continue reading the next full record (k2)
    rf.seek(SeekFrom::Start(off)).unwrap();
    use std::io::Read;
    let mut len_buf = [0u8; LENGTH_FIELD_BYTES];
    rf.read_exact(&mut len_buf).unwrap();
    let klen = u32::from_be_bytes(len_buf) as usize;
    rf.read_exact(&mut len_buf).unwrap();
    let vlen = u32::from_be_bytes(len_buf) as usize;
    let mut k = vec![0u8; klen];
    rf.read_exact(&mut k).unwrap();
    assert_eq!(&k, b"k2");
    let mut v = vec![0u8; vlen];
    rf.read_exact(&mut v).unwrap();
    assert_eq!(&v, b"v2");
}
