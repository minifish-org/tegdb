use std::{
    collections::BTreeMap,
    io::{prelude::*, BufReader},
    net::TcpStream,
    sync::{Arc, Mutex},
};

const NOT_FOUND: &str = "NOT FOUND\n";
const OK: &str = "OK\n";

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
                    "g" => handle_get_command(data, kv_data, &mut stream),
                    "s" => handle_set_command(data, kv_data, &mut stream),
                    _ => {
                        stream.write(NOT_FOUND.as_bytes()).unwrap();
                    }
                }
                stream.flush().unwrap();
            },
            Err(e) => {
                println!("Failed to read from connection: {}", e);
                break;
            }
        }
    }
}

fn handle_get_command(data: Vec<&str>, kv_data: &Arc<Mutex<BTreeMap<String, String>>>, stream: &mut TcpStream) {
    if data.len() != 3 {
        stream.write(NOT_FOUND.as_bytes()).unwrap();
        return;
    }
    let key = data[1];
    let response = format!("{}\n", kv_data.lock().unwrap().get(key).unwrap_or(&NOT_FOUND.to_string()));
    stream.write(response.as_bytes()).unwrap();
}

fn handle_set_command(data: Vec<&str>, kv_data: &Arc<Mutex<BTreeMap<String, String>>>, stream: &mut TcpStream) {
    if data.len() != 4 {
        stream.write(NOT_FOUND.as_bytes()).unwrap();
        return;
    }
    let key = data[1];
    let value = data[2];
    kv_data.lock().unwrap().insert(key.to_string(), value.to_string());
    stream.write(OK.as_bytes()).unwrap();
}   