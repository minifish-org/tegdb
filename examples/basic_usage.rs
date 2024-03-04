// examples/basic_usage.rs
use tegdb::Engine;
use std::path::PathBuf;

fn main() {
    let path = PathBuf::from("test.db");
    let mut engine = Engine::new(path.clone());

    // Set a value
    let key = b"key";
    let value = b"value";
    engine.set(key, value.to_vec());

    // Get a value
    let get_value = engine.get(key);
    println!("Got value: {}", String::from_utf8_lossy(&get_value));

    // Delete a value
    engine.del(key);

    // Clean up
    drop(engine);
}