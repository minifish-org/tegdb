use std::{
    collections::BTreeMap,
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
};

fn main() {
    let mut kv_data = BTreeMap::new();

    kv_data.insert("key1", "value1");
    kv_data.insert("key2", "value2");

    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        println!("Connection established!");
        handle_connection(stream);
    }
    if kv_data.contains_key("key1") {
        println!("We've got {}", kv_data.get("key1").unwrap());
    }
    kv_data.remove("key1");
    println!("We've got {}", kv_data.get("key2").unwrap());
    println!("We've got {}", kv_data.get("key1").unwrap_or(&"default"));
    println!("We've got {}", kv_data.get("key1").unwrap_or(&"default").to_uppercase());
    println!("We've got {}", kv_data.get("key1").unwrap_or(&"default").to_ascii_lowercase());
    println!("We've got {} keys", kv_data.len());
}

// handle_connection is used to handle tcp request.
fn handle_connection (mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    let mut reader = BufReader::new(&stream);
    reader.read(&mut buffer).unwrap();
    println!("Request: {}", String::from_utf8_lossy(&buffer[..]));
    let response = "HTTP/1.1 200 OK\r\n\r\n";
    stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}