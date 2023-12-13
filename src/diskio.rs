use std::{
    collections::BTreeMap,
    io::prelude::*,
    sync::{Arc, Mutex},
};
use flate2::{Compression, write::GzEncoder, read::GzDecoder};

const DATA_FILE: &str = "data.txt";

pub fn save_to_disk(kv_data: &Arc<Mutex<BTreeMap<String, String>>>) {
    let data = kv_data.lock().unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for (key, value) in data.iter() {
        writeln!(encoder, "{} {}", key, value).unwrap();
    }
    let compressed_data = encoder.finish().unwrap();
    // Write the compressed data to the file.
    std::fs::write(DATA_FILE, compressed_data).unwrap();
}

pub fn load_data_from_file(kv_data: &Arc<Mutex<BTreeMap<String, String>>>) {
    if let Ok(data) = std::fs::read(DATA_FILE) {
        let mut gz = GzDecoder::new(&data[..]);
        let mut s = String::new();
        gz.read_to_string(&mut s).unwrap();

        let mut kv_data = kv_data.lock().unwrap();
        for line in s.lines() {
            let data: Vec<&str> = line.split_whitespace().collect();
            if data.len() != 2 {
                continue;
            }
            let key = data[0];
            let value = data[1];
            kv_data.insert(key.to_string(), value.to_string());
        }
    }
}
