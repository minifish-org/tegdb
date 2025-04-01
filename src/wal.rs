use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::thread;

use crate::types::KeyMap;

pub struct Wal {
    pub path: PathBuf,
    pub writer: WalWriter,
}

// New: Helper function to read a big-endian u32 from the given reader.
fn read_u32(reader: &mut BufReader<&mut File>) -> Result<u32, std::io::Error> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

// New: Constant for length-sizing fields.
const LEN_FIELD_SIZE: u64 = 4;

impl Wal {
    pub fn new(path: PathBuf) -> Self {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).unwrap();
        }
        Self {
            path: path.clone(),
            writer: WalWriter::new(path),
        }
    }

    pub fn build_key_map(&self) -> (KeyMap, (u64, u64)) {
        // ...existing code...
        let key_map = KeyMap::new();
        let mut insert_count = 0;
        let mut remove_count = 0;
        let parent = self.path.parent().expect("Invalid directory");
        let mut wal_files: Vec<PathBuf> = Vec::new();
        let wal_old = parent.join("wal.old");
        if wal_old.exists() {
            wal_files.push(wal_old);
        }
        let wal_new = parent.join("wal.new");
        if wal_new.exists() {
            wal_files.push(wal_new);
        }
        for file_path in wal_files {
            let mut file = OpenOptions::new().read(true).open(&file_path).unwrap();
            let file_len = file.metadata().unwrap().len();
            let mut r = BufReader::new(&mut file);
            let mut pos = r.seek(SeekFrom::Start(0)).unwrap();
            while pos < file_len {
                let key_len = read_u32(&mut r).expect("Failed to read key length");
                let value_len = read_u32(&mut r).expect("Failed to read value length");
                let value_pos = pos + LEN_FIELD_SIZE + LEN_FIELD_SIZE + key_len as u64;
                let mut key = vec![0; key_len as usize];
                r.read_exact(&mut key).unwrap();
                let mut value = vec![0; value_len as usize];
                r.read_exact(&mut value).unwrap();
                if value_len == 0 {
                    key_map.remove(&key);
                    remove_count += 1;
                } else {
                    key_map.insert(key, value);
                    insert_count += 1;
                }
                pos = value_pos + value_len as u64;
            }
        }
        (key_map, (insert_count, remove_count))
    }

    pub fn write_entry(&self, key: &[u8], value: &[u8]) {
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let mut buffer = Vec::with_capacity(4 + 4 + key.len() + value.len());
        buffer.extend_from_slice(&key_len.to_be_bytes());
        buffer.extend_from_slice(&value_len.to_be_bytes());
        buffer.extend_from_slice(key);
        buffer.extend_from_slice(value);
        self.writer.write(buffer);
    }
}

impl Clone for Wal {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            writer: self.writer.clone(),
        }
    }
}

pub enum WalMessage {
    Write(Vec<u8>),
    Flush,
    Shutdown,
}

pub struct WalWriter {
    sender: Sender<WalMessage>,
}

impl WalWriter {
    pub fn new(path: PathBuf) -> Self {
        let file = File::options()
            .append(true)
            .create(true)
            .open(&path)
            .expect("failed to open wal file");
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let mut writer = BufWriter::new(file);
            while let Ok(msg) = receiver.recv() {
                match msg {
                    WalMessage::Write(data) => {
                        if let Err(e) = writer.write_all(&data) {
                            eprintln!("Failed to write wal: {}", e);
                        }
                    }
                    WalMessage::Flush => {
                        if let Err(e) = writer.flush() {
                            eprintln!("Failed to flush wal: {}", e);
                        }
                    }
                    WalMessage::Shutdown => break,
                }
            }
        });
        Self { sender }
    }

    pub fn write(&self, data: Vec<u8>) {
        let _ = self.sender.send(WalMessage::Write(data));
    }

    pub fn flush(&self) {
        let _ = self.sender.send(WalMessage::Flush);
    }

    pub fn shutdown(&self) {
        let _ = self.sender.send(WalMessage::Shutdown);
    }
}

impl Clone for WalWriter {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}
