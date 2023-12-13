use std::{
    collections::BTreeMap,
    io::{prelude::*, BufReader},
    net::TcpStream,
    sync::{Arc, Mutex},
};

pub fn handle_connection (mut stream: TcpStream, kv_data: &Arc<Mutex<BTreeMap<String, String>>>) {
    let mut stream_for_reading = stream.try_clone().unwrap();
    let mut buffer = [0; 1024];
    let mut reader = BufReader::new(&mut stream_for_reading);
    loop {
        match reader.read(&mut buffer) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    // The client has disconnected.
                    println!("Client disconnected!");
                    break;
                }
                let request = String::from_utf8_lossy(&buffer[..]).to_string();
                let data: Vec<&str> = request.split_whitespace().collect();

                for i in 0..data.len() {
                    println!("{}: {}", i, data[i]);
                }

                let cmd = data[0];
                match cmd {
                    "g" => {
                        if data.len() != 3 {
                            let response = "NOT FOUND\n";
                            stream.write(response.as_bytes()).unwrap();
                            continue;
                        }
                        let key = data[1];
                        let response = format!("{}\n", kv_data.lock().unwrap().get(key).unwrap_or(&"NOT FOUND".to_string()));
                        stream.write(response.as_bytes()).unwrap();
                    },
                    "s" => {
                        if data.len() != 4 {
                            let response = "NOT FOUND\n";
                            stream.write(response.as_bytes()).unwrap();
                            continue;
                        }
                        let key = data[1];
                        let value = data[2];
                        kv_data.lock().unwrap().insert(key.to_string(), value.to_string());
                        let response = "OK\n";
                        stream.write(response.as_bytes()).unwrap();
                    },
                    _ => {
                        let response = "NOT FOUND\n";
                        stream.write(response.as_bytes()).unwrap();
                    }
                };

                stream.flush().unwrap();
            },
            Err(e) => {
                println!("Failed to read from connection: {}", e);
                break;
            }
        }
    }
}
