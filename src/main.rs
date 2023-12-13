use std::{
    collections::BTreeMap,
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}},
};
use ctrlc;
use rayon::ThreadPoolBuilder;

const DATA_FILE: &str = "data.txt";
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
        let data = kv_data_for_singal.lock().unwrap();
        let mut file = std::fs::File::create(DATA_FILE).unwrap();
        for (key, value) in data.iter() {
            let line = format!("{} {}\n", key, value);
            file.write(line.as_bytes()).unwrap();
        }
        println!("Data saved!");
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");
}   

fn load_data_from_file(kv_data: &Arc<Mutex<BTreeMap<String, String>>>) {
    if let Ok(data) = std::fs::read_to_string(DATA_FILE) {
        let mut kv_data = kv_data.lock().unwrap();
        for line in data.lines() {
            let data: Vec<&str> = line.split_whitespace().collect();
            if data.len() != 2 {
                continue;
            }
            let key = data[0];
            let value = data[1];
            kv_data.insert(key.to_string(), value.to_string());
        }
    }
}

fn handle_connection (mut stream: TcpStream, kv_data: &Arc<Mutex<BTreeMap<String, String>>>) {
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
