mod diskio;
mod commands;

use std::{
    collections::BTreeMap,
    net::TcpListener,
    sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}},
};
use ctrlc;
use rayon::ThreadPoolBuilder;

use diskio::{save_to_disk, load_data_from_file};
use commands::handle_connection;

const NUM_THREADS: usize = 8;

fn main() {
    // Create a map to store key-value data.
    let kv_data = Arc::new(Mutex::new(BTreeMap::new()));

    // Load data from file.
    load_data_from_file(&kv_data);

    // Create a tcp listener.
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    // Set up a signal handler.
    setup_signal_handler(&kv_data);

    // Create a thread pool with a maximum number of threads.
    let pool = ThreadPoolBuilder::new().num_threads(NUM_THREADS).build().unwrap();

    // Create a counter for active tasks.
    let active_tasks = Arc::new(AtomicUsize::new(0));

    // Listen for incoming tcp connections.
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        println!("Connection established!");
        let kv_data = Arc::clone(&kv_data);

        // Check if there are available threads in the pool.
        let current_tasks= active_tasks.load(Ordering::SeqCst);
        if current_tasks >= NUM_THREADS {
            println!("No available threads. Please try again later.");
            continue;
        }

        let active_tasks = Arc::clone(&active_tasks);
        pool.spawn(move || {
            active_tasks.fetch_add(1, Ordering::SeqCst);
            handle_connection(stream, &kv_data);
            active_tasks.fetch_sub(1, Ordering::SeqCst);
        });
    }
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
