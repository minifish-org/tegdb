use std::{
    io::{BufReader, Read, Write}, net::TcpListener
};
use crate::db::DB;

const UNKNOWN_CMD: &str = "Unknown command\n";

pub struct Server {
    c: TcpListener,
    db: DB,
}

impl Server {
    pub fn new(c: TcpListener, db: DB) -> Self {
        Self { c, db }
    }

    pub fn handle_connections(&mut self) {
        for stream in self.c.incoming() {
            let mut stream = stream.unwrap();
            println!("Connection established!");
            let mut buffer = [0; 1024];
            loop {
                let mut reader = BufReader::new(&mut stream);
                let bytes_read = reader.read(&mut buffer).unwrap();
                if bytes_read == 0 {
                    // The client has disconnected.
                    println!("Client disconnected!");
                    break;
                }
                // trim buffer to remove empty bytes
                let buffer = &mut buffer[..bytes_read];
                let request = String::from_utf8_lossy(&buffer[..]).to_string();
                let payload: Vec<&str> = request.split_whitespace().collect();
                let cmd = payload[0];
                let payload = payload[1..].to_vec();
                let response = match cmd {
                    "g" => self.db.handle_get_command(payload),
                    "s" => self.db.handle_set_command(payload),
                    "d" => self.db.handle_del_command(payload),
                    _ => UNKNOWN_CMD.to_string(),
                };
                stream.write(response.as_bytes()).unwrap();
                stream.flush().unwrap();
            }
        }
    }
}