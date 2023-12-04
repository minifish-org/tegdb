use std::{
    collections::BTreeMap,
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
};

fn main() {
    let mut kv_data = BTreeMap::new();

    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        println!("Connection established!");
        handle_connection(stream, &mut kv_data);
    }
}

// handle_connection is used to handle tcp request.
fn handle_connection (mut stream: TcpStream, kv_data: &mut BTreeMap<String, String>) {
    let mut buffer = [0; 1024];
    let mut reader = BufReader::new(&stream);
    reader.read(&mut buffer).unwrap();
    println!("Request: {}", String::from_utf8_lossy(&buffer[..]));
    let request = String::from_utf8_lossy(&buffer[..]).to_string();
    process_request(&request, kv_data);
    let response = "HTTP/1.1 200 OK\r\n\r\n";
    stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}

fn process_request(request: &str, kv_data: &mut BTreeMap<String, String>) {
    kv_data.insert("key".to_string(), request.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_request() {
        let mut kv_data = BTreeMap::new();
        process_request("test request", &mut kv_data);
        assert_eq!(kv_data.get("key"), Some(&"test request".to_string()));
    }
}