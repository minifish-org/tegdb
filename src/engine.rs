use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

pub struct Engine {
    log: Log,
    key_map: KeyMap,
}

type KeyMap = std::collections::BTreeMap<Vec<u8>, (u64, u32)>;

impl Engine {
    pub fn new(path: PathBuf) -> Self {
        let mut log = Log::new(path);
        let key_map = log.build_key_map();
        let mut s = Self {log, key_map};
        s.compact();
        s
    }

    pub fn get(&mut self, key: &[u8]) -> Vec<u8> {
        if let Some((value_pos, value_len)) = self.key_map.get(key) {
            self.log.read_value(*value_pos, *value_len)
        } else {
            Vec::new()
        }
    }

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) {
        let (pos, len) = self.log.write_entry(key, &*value);
        let value_len = value.len() as u32;
        self.key_map.insert(key.to_vec(), (pos + len as u64 - value_len as u64, value_len));
    }

    pub fn del(&mut self, key: &[u8]) {
        self.log.write_entry(key, &[]);
        self.key_map.remove(key);
    }

}

impl Engine {
    fn flush(&mut self) {
        self.log.file.sync_all().unwrap();
    }

    fn write_log(&mut self, path: PathBuf) -> (Log, KeyMap) {
        let mut new_key_map = KeyMap::new();
        let mut new_log = Log::new(path);
        new_log.file.set_len(0).unwrap();
        for (key, (value_pos, value_len)) in self.key_map.iter() {
            let value = self.log.read_value(*value_pos, *value_len);
            let (pos, len) = new_log.write_entry(key, &*value);
            new_key_map.insert(key.to_vec(), (pos + len as u64 - *value_len as u64, *value_len));
        
        }
        (new_log, new_key_map)
    }

    fn compact (&mut self) {
        let mut tmp_path = self.log.path.clone();
        tmp_path.set_extension("new");
        let (mut new_log, new_key_map) = self.write_log(tmp_path);

        std::fs::rename(&new_log.path, &self.log.path).unwrap();
        new_log.path = self.log.path.clone();

        self.log = new_log;
        self.key_map = new_key_map;
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.flush()
    }
}

struct Log {
    path: PathBuf,
    file: std::fs::File,
}

impl Log {
    fn new(path: PathBuf) -> Self {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).unwrap()
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path).unwrap();
        Self {path, file}
    }

    fn build_key_map(&mut self) -> KeyMap {
        let mut len_buf = [0u8; 4];
        let mut key_map = KeyMap::new();
        let file_len = self.file.metadata().unwrap().len();
        let mut r = BufReader::new(&mut self.file);
        let mut pos = r.seek(SeekFrom::Start(0)).unwrap();

        while pos < file_len {
            r.read_exact(&mut len_buf).unwrap();
            let key_len = u32::from_be_bytes(len_buf);
            r.read_exact(&mut len_buf).unwrap();
            let value_len = u32::from_be_bytes(len_buf);
            let value_pos = pos + 4 + 4 + key_len as u64;

            let mut key = vec![0; key_len as usize];
            r.read_exact(&mut key).unwrap();

            if value_len == 0 {
                key_map.remove(&key);
            } else {
                key_map.insert(key, (value_pos, value_len));
            }

            r.seek_relative(value_len as i64).unwrap();
            pos = value_pos + value_len as u64;
        }
        key_map
    }

    fn read_value(&mut self, value_pos: u64, value_len: u32) -> Vec<u8> {
        let mut value = vec![0; value_len as usize];
        self.file.seek(SeekFrom::Start(value_pos)).unwrap();
        self.file.read_exact(&mut value).unwrap();
        value
    }

    fn write_entry(&mut self, key: &[u8], value: &[u8]) -> (u64, u32) {
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let len = 4 + 4 + key_len + value_len;

        let pos = self.file.seek(SeekFrom::End(0)).unwrap();
        let mut w = BufWriter::with_capacity(len as usize,&mut self.file);
        w.write_all(&key_len.to_be_bytes()).unwrap();
        w.write_all(&value_len.to_be_bytes()).unwrap();
        w.write_all(key).unwrap();
        w.write_all(value).unwrap();
        w.flush().unwrap();

        (pos, len)
    }
}