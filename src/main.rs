mod engine;
mod server;
mod db;

use std::{env, net::TcpListener};

fn main() {
    // Create a server to store key-value data.
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    let engine = engine::Engine::new(
        env::current_dir()
        .expect("Failed to get current directory")
        .join("data.bc"));
    let mut s = server::Server::new(listener, db::DB::new(engine));

    // Handle incoming connections.
    s.handle_connections();
}
