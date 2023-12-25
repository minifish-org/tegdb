use std::{
    collections::BTreeMap,
    io::{prelude::*, BufReader},
    net::TcpStream,
    sync::{Arc, Mutex},
};

const NOT_FOUND: &str = "NOT FOUND\n";
const OK: &str = "OK\n";

pub fn handle_connection(mut stream: TcpStream, kv_data: &Arc<Mutex<BTreeMap<String, String>>>) {
    let mut stream_for_reading = stream.try_clone().unwrap();
    let mut buffer = [0; 1024];
    let mut reader = BufReader::new(&mut stream_for_reading);
    loop {
        let bytes_read = match reader.read(&mut buffer) {
            Ok(bytes_read) => bytes_read,
            Err(e) => {
                println!("Failed to read from connection: {}", e);
                break;
            }
        };
        if bytes_read == 0 {
            // The client has disconnected.
            println!("Client disconnected!");
            break;
        }
        let request = String::from_utf8_lossy(&buffer[..]).to_string();
        let data: Vec<&str> = request.split_whitespace().collect();
        let response = process_command(data, kv_data);
        write_response(&mut stream, response);
    }
}

fn process_command(data: Vec<&str>, kv_data: &Arc<Mutex<BTreeMap<String, String>>>) -> String {
    let cmd = data[0];
    match cmd {
        "g" => handle_get_command(data, kv_data),
        "s" => handle_set_command(data, kv_data),
        _ => NOT_FOUND.to_string(),
    }
}

fn write_response(stream: &mut TcpStream, response: String) {
    stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}

fn handle_get_command(data: Vec<&str>, kv_data: &Arc<Mutex<BTreeMap<String, String>>>) -> String {
    if data.len() != 3 {
        return NOT_FOUND.to_string();
    }
    let key = data[1];
    format!("{}\n", kv_data.lock().unwrap().get(key).unwrap_or(&NOT_FOUND.to_string()))
}

fn handle_set_command(data: Vec<&str>, kv_data: &Arc<Mutex<BTreeMap<String, String>>>) -> String {
    if data.len() != 4 {
        return NOT_FOUND.to_string();
    }
    let key = data[1];
    let value = data[2];
    kv_data.lock().unwrap().insert(key.to_string(), value.to_string());
    OK.to_string()
}