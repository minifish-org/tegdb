use std::sync::mpsc::{self, Sender};
use std::thread;
use std::fs::File;
use std::io::{BufWriter, Write, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::fs::OpenOptions;

// The Log struct encapsulates a log writer for appending entries and enables log replay to rebuild the key map.
pub struct Log {
    pub path: PathBuf,
    pub writer: LogWriter,
}

impl Log {
    pub fn new(path: PathBuf) -> Self {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).unwrap();
        }
        Self {
            path: path.clone(),
            writer: LogWriter::new(path),
        }
    }

    pub fn build_key_map(&self) -> (dashmap::DashMap<Vec<u8>, Vec<u8>>, (u64, u64)) {
        let key_map = dashmap::DashMap::new();
        let mut insert_count = 0;
        let mut remove_count = 0;
        // Iterate through all log.N files
        let parent = self.path.parent().expect("Invalid directory");
        let mut log_files: Vec<(u32, PathBuf)> = std::fs::read_dir(parent)
            .expect("Failed to read log directory")
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_name = entry.file_name().to_string_lossy().into_owned();
                if let Some(num_str) = file_name.strip_prefix("log.") {
                    if let Ok(num) = num_str.parse::<u32>() {
                        return Some((num, entry.path()));
                    }
                }
                None
            })
            .collect();
        log_files.sort_by_key(|(num, _)| *num);
        for (_num, file_path) in log_files {
            let mut file = OpenOptions::new().read(true).open(&file_path).unwrap();
            let file_len = file.metadata().unwrap().len();
            let mut r = BufReader::new(&mut file);
            let mut pos = r.seek(SeekFrom::Start(0)).unwrap();
            let mut len_buf = [0u8; 4];
            while pos < file_len {
                r.read_exact(&mut len_buf).unwrap();
                let key_len = u32::from_be_bytes(len_buf);
                r.read_exact(&mut len_buf).unwrap();
                let value_len = u32::from_be_bytes(len_buf);
                let value_pos = pos + 4 + 4 + key_len as u64;
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
        if key.len() > 1024 || value.len() > 256 * 1024 {
            panic!("Key or value exceeds allowed limit");
        }
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

impl Clone for Log {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            writer: self.writer.clone(),
        }
    }
}

// Messages used to control the log writer thread.
pub enum LogMessage {
    Write(Vec<u8>),
    Flush,
    Shutdown,
}

pub struct LogWriter {
    sender: Sender<LogMessage>,
}

impl LogWriter {
    pub fn new(path: PathBuf) -> Self {
        let file = File::options()
            .append(true)
            .create(true)
            .open(&path)
            .expect("failed to open log file");
        let (sender, receiver) = mpsc::channel();
        // Spawn dedicated thread to process log messages.
        thread::spawn(move || {
            let mut writer = BufWriter::new(file);
            while let Ok(msg) = receiver.recv() {
                match msg {
                    LogMessage::Write(data) => {
                        if let Err(e) = writer.write_all(&data) {
                            eprintln!("Failed to write log: {}", e);
                        }
                    },
                    LogMessage::Flush => {
                        if let Err(e) = writer.flush() {
                            eprintln!("Failed to flush log: {}", e);
                        }
                    },
                    LogMessage::Shutdown => break,
                }
            }
        });
        Self { sender }
    }

    pub fn write(&self, data: Vec<u8>) {
        let _ = self.sender.send(LogMessage::Write(data));
    }

    pub fn flush(&self) {
        let _ = self.sender.send(LogMessage::Flush);
    }

    /// Initiates shutdown of the log writer thread.
    pub fn shutdown(&self) {
        let _ = self.sender.send(LogMessage::Shutdown);
    }
}

impl Clone for LogWriter {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}
