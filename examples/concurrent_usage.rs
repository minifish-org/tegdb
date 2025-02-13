// Refined header comment:
// This example demonstrates concurrent, multi-threaded usage of the Tegdb Engine.
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tegdb::Engine;
use tokio::runtime::Builder;

fn main() {
    // Parse the number of threads; default is 4.
    let args: Vec<String> = env::args().collect();
    let thread_count: usize = if args.len() > 1 {
        args[1].parse().unwrap_or(4)
    } else {
        4
    };

    // Initialize the engine and remove any pre-existing data file.
    let path = PathBuf::from("test_concurrent.db");
    let _ = fs::remove_file(&path);
    let engine = Engine::new(path.clone());

    // Shared metrics for tracking set() and get() call counts.
    let set_metrics = Arc::new(Mutex::new(Vec::<usize>::new()));
    let get_metrics = Arc::new(Mutex::new(Vec::<usize>::new()));

    const RUN_DURATION: Duration = Duration::from_secs(10);

    // Spawn writer threads.
    let mut writer_handles = Vec::new();
    for thread_id in 0..thread_count {
        let engine_writer = engine.clone();
        let set_metrics_writer = Arc::clone(&set_metrics);
        writer_handles.push(thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let start = Instant::now();
                let mut count = 0;
                while start.elapsed() < RUN_DURATION {
                    // Use thread-specific keys for uniqueness.
                    let key = format!("thread_{}_key_{}", thread_id, count);
                    if let Err(e) = engine_writer.set(key.as_bytes(), b"value".to_vec()).await {
                        eprintln!("Error in set(): {}", e);
                    }
                    count += 1;
                }
                set_metrics_writer.lock().unwrap().push(count);
            });
        }));
    }

    // Wait for all writer threads.
    for handle in writer_handles {
        let _ = handle.join();
    }

    // Spawn reader threads.
    let mut reader_handles = Vec::new();
    for thread_id in 0..thread_count {
        let engine_reader = engine.clone();
        let get_metrics_reader = Arc::clone(&get_metrics);
        reader_handles.push(thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let start = Instant::now();
                let mut count = 0;
                while start.elapsed() < RUN_DURATION {
                    // Use matching thread-specific keys.
                    let key = format!("thread_{}_key_{}", thread_id, count);
                    let _ = engine_reader.get(key.as_bytes()).await;
                    count += 1;
                }
                get_metrics_reader.lock().unwrap().push(count);
            });
        }));
    }

    // Wait for all reader threads.
    for handle in reader_handles {
        let _ = handle.join();
    }

    // Calculate and display performance metrics.
    let set_metrics = set_metrics.lock().unwrap();
    let get_metrics = get_metrics.lock().unwrap();
    let total_set_calls: usize = set_metrics.iter().sum();
    let total_get_calls: usize = get_metrics.iter().sum();
    let total_run_secs = RUN_DURATION.as_secs_f64();
    let avg_set = Duration::from_secs_f64(RUN_DURATION.as_secs_f64() * (thread_count as f64) / (total_set_calls as f64));
    let avg_get = Duration::from_secs_f64(RUN_DURATION.as_secs_f64() * (thread_count as f64) / (total_get_calls as f64));
    let calls_set_per_sec = total_set_calls as f64 / total_run_secs;
    let calls_get_per_sec = total_get_calls as f64 / total_run_secs;

    println!("Performance over 10s runtime:");
    println!(" Number of threads: {}", thread_count);
    println!(" Average set() latency: {:?} (total calls: {}, calls/sec: {:.2})", avg_set, total_set_calls, calls_set_per_sec);
    println!(" Average get() latency: {:?} (total calls: {}, calls/sec: {:.2})", avg_get, total_get_calls, calls_get_per_sec);
    drop(engine);
    fs::remove_file(&path).unwrap();
}
