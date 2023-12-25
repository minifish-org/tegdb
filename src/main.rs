mod diskio;
mod commands;
mod connections;

use std::{
    collections::BTreeMap,
    net::TcpListener,
    sync::{Arc, Mutex},
};
use ctrlc;

use diskio::{save_to_disk, load_data_from_disk};
use connections::handle_connections;


fn main() {
    // Create a map to store key-value data.
    let kv_data = Arc::new(Mutex::new(BTreeMap::new()));

    // Load data from file.
    load_data_from_disk(&kv_data);

    // Create a tcp listener.
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    // Set up a signal handler.
    setup_signal_handler(&kv_data);

    // Handle incoming connections.
    handle_connections(listener, kv_data);
}

fn setup_signal_handler(kv_data: &Arc<Mutex<BTreeMap<String, String>>>){
    let kv_data_for_singal = Arc::clone(&kv_data);
    ctrlc::set_handler(move || {
        println!("Ctrl-C received!");
        println!("Saving data...");
        save_to_disk(&kv_data_for_singal);
        println!("Data saved!");
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");
}   
