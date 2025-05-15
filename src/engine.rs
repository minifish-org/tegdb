use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::marker::PhantomData;
use std::rc::Rc;
use fs2::FileExt;  // Add fs2 for file locking

pub struct Engine {
    log: Log,
    key_map: KeyMap,
    // This field makes Engine neither Send nor Sync
    _not_send_sync: PhantomData<Rc<()>>, 
}

// KeyMap is a BTreeMap that maps keys to a tuple of (position, value length, value).
type KeyMap = std::collections::BTreeMap<Vec<u8>, Vec<u8>>;

impl Engine {
    pub fn new(path: PathBuf) -> Self {
        let mut log = Log::new(path);
        let key_map = log.build_key_map();
        let mut s = Self {
            log,
            key_map,
            _not_send_sync: PhantomData,
        };
        s.compact().expect("Failed to compact log");
        s
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.key_map.get(key) {
            Some(value.clone())
        } else {
            None
        }
    }

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), std::io::Error> {
        if key.len() > 1024 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Key length exceeds 1k"));
        }
        if value.len() > 256 * 1024 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Value length exceeds 256k"));
        }

        if value.len() == 0 {
            return self.del(key);
        }

        if let Some(existing_value) = self.key_map.get(key) {
            if *existing_value == value {
                return Ok(()); // Value already exists, no need to write
            }
        }

        self.log.write_entry(key, &*value);
        self.key_map.insert(
            key.to_vec(),
            value,
        );
        Ok(())
    }

    pub fn del(&mut self, key: &[u8]) -> Result<(), std::io::Error> {
        if self.key_map.get(key).is_none() {
            return Ok(());
        }

        self.log.write_entry(key, &[]);
        self.key_map.remove(key);
        Ok(())
    }

    pub fn scan<'a>(
        &'a mut self,
        range: Range<Vec<u8>>,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>, std::io::Error> {
        let range = self.key_map.range(range);
        Ok(Box::new(range.map(move |(key, &ref value)| {
            (key.clone(), value.clone())
        })))
    }
}

impl Engine {
    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.log.file.sync_all()?;
        Ok(())
    }

    fn construct_log(&mut self, path: PathBuf) -> Result<(Log, KeyMap), std::io::Error> {
        let mut new_key_map = KeyMap::new();
        let mut new_log = Log::new(path);
        new_log.file.set_len(0)?;
        for (key, value) in self.key_map.iter() {
            new_log.write_entry(key, &*value);
            new_key_map.insert(
                key.to_vec(),
                value.clone(),
            );
        }
        Ok((new_log, new_key_map))
    }

    fn compact(&mut self) -> Result<(), std::io::Error> {
        let mut tmp_path = self.log.path.clone();
        tmp_path.set_extension("new");
        let (mut new_log, new_key_map) = self.construct_log(tmp_path)?;

        std::fs::rename(&new_log.path, &self.log.path)?;
        new_log.path = self.log.path.clone();

        self.log = new_log;
        self.key_map = new_key_map;
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.flush().unwrap();
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
            .open(&path)
            .unwrap();
        
        // Try to obtain an exclusive lock when opening the log file
        // This ensures only one process can access the file at a time
        file.try_lock_exclusive()
            .unwrap_or_else(|e| panic!("Could not obtain exclusive lock on database file: {}", e));
        
        Self { path, file }
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

            let mut value = vec![0; value_len as usize];
            r.read_exact(&mut value).unwrap();

            if value_len == 0 {
                key_map.remove(&key);
            } else {
                key_map.insert(key, value);
            }

            pos = value_pos + value_len as u64;
        }
        key_map
    }

    fn write_entry(&mut self, key: &[u8], value: &[u8]) {
        if key.len() > 1024 || value.len() > 256 * 1024 {
            panic!("Key or value length exceeds the allowed limit");
        }
        // Calculate the length of the entry. The structure of an entry is: key_len (4 bytes), value_len (4 bytes), key (key_len bytes), value (value_len bytes).
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let len = 4 + 4 + key_len + value_len;

        // Always append to the end of the file.
        _ = self.file.seek(SeekFrom::End(0)).unwrap();
        let mut w = BufWriter::with_capacity(len as usize, &mut self.file);

        let mut buffer = Vec::with_capacity(len as usize);

        // Write the length of the key and value, and then the key and value.
        buffer.extend_from_slice(&key_len.to_be_bytes());
        buffer.extend_from_slice(&value_len.to_be_bytes());
        buffer.extend_from_slice(key);
        buffer.extend_from_slice(&value);

        w.write_all(&buffer).unwrap();
        w.flush().unwrap();
    }
}
