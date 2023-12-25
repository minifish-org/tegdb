use std::{
    collections::BTreeMap,
    net::TcpListener,
    sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}},
};
use rayon::ThreadPoolBuilder;

use crate::commands::handle_request;

const NUM_THREADS: usize = 8;

pub fn handle_connections(listener: TcpListener, kv_data: Arc<Mutex<BTreeMap<String, String>>>) {
    // Create a thread pool with a maximum number of threads.
    let pool = ThreadPoolBuilder::new().num_threads(NUM_THREADS).build().unwrap();

    // Create a counter for active tasks.
    let active_tasks = Arc::new(AtomicUsize::new(0));

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
            handle_request(stream, &kv_data);
            active_tasks.fetch_sub(1, Ordering::SeqCst);
        });
    }
}
