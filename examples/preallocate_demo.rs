use std::time::Instant;
/// Demonstration of memory and disk preallocation features in TegDB
///
/// This example shows:
/// 1. How to use memory preallocation (initial_capacity)
/// 2. How to use disk preallocation (preallocate_size)
/// 3. Performance comparison with and without preallocation
use tegdb::{EngineConfig, StorageEngine};

fn main() -> tegdb::Result<()> {
    println!("=== TegDB Preallocation Demo ===\n");

    // Test 1: Without preallocation (baseline)
    println!("Test 1: Without preallocation");
    let start = Instant::now();
    {
        let config = EngineConfig {
            initial_capacity: None,
            preallocate_size: None,
            ..Default::default()
        };
        let mut engine = StorageEngine::with_config("demo_no_prealloc.teg".into(), config)?;

        for i in 0..1000 {
            let key = format!("key_{:05}", i);
            let value = format!("value_{:05}", i);
            engine.set(key.as_bytes(), value.into_bytes())?;
        }
        engine.flush()?;
    }
    let elapsed_no_prealloc = start.elapsed();
    println!("  Time: {:?}", elapsed_no_prealloc);
    println!(
        "  File size: {} bytes\n",
        std::fs::metadata("demo_no_prealloc.teg")?.len()
    );

    // Test 2: With memory preallocation only
    println!("Test 2: With memory preallocation (capacity = 2000)");
    let start = Instant::now();
    {
        let config = EngineConfig {
            initial_capacity: Some(2000),
            preallocate_size: None,
            ..Default::default()
        };
        let mut engine = StorageEngine::with_config("demo_mem_prealloc.teg".into(), config)?;

        for i in 0..1000 {
            let key = format!("key_{:05}", i);
            let value = format!("value_{:05}", i);
            engine.set(key.as_bytes(), value.into_bytes())?;
        }
        engine.flush()?;
    }
    let elapsed_mem_prealloc = start.elapsed();
    println!("  Time: {:?}", elapsed_mem_prealloc);
    println!(
        "  File size: {} bytes\n",
        std::fs::metadata("demo_mem_prealloc.teg")?.len()
    );

    // Test 3: With disk preallocation only
    println!("Test 3: With disk preallocation (10MB)");
    let start = Instant::now();
    {
        let config = EngineConfig {
            preallocate_size: Some(10 * 1024 * 1024), // 10MB
            initial_capacity: None,
            ..Default::default()
        };
        let mut engine = StorageEngine::with_config("demo_disk_prealloc.teg".into(), config)?;

        for i in 0..1000 {
            let key = format!("key_{:05}", i);
            let value = format!("value_{:05}", i);
            engine.set(key.as_bytes(), value.into_bytes())?;
        }
        engine.flush()?;
    }
    let elapsed_disk_prealloc = start.elapsed();
    let file_size = std::fs::metadata("demo_disk_prealloc.teg")?.len();
    println!("  Time: {:?}", elapsed_disk_prealloc);
    println!("  File size: {} bytes (preallocated)", file_size);
    println!("  Note: File is preallocated to 10MB, but only ~30KB is used\n");

    // Test 4: With both memory and disk preallocation
    println!("Test 4: With both memory and disk preallocation");
    let start = Instant::now();
    {
        let config = EngineConfig {
            initial_capacity: Some(2000),
            preallocate_size: Some(10 * 1024 * 1024), // 10MB
            ..Default::default()
        };
        let mut engine = StorageEngine::with_config("demo_full_prealloc.teg".into(), config)?;

        for i in 0..1000 {
            let key = format!("key_{:05}", i);
            let value = format!("value_{:05}", i);
            engine.set(key.as_bytes(), value.into_bytes())?;
        }
        engine.flush()?;
    }
    let elapsed_full_prealloc = start.elapsed();
    println!("  Time: {:?}", elapsed_full_prealloc);
    println!(
        "  File size: {} bytes (preallocated)\n",
        std::fs::metadata("demo_full_prealloc.teg")?.len()
    );

    // Performance summary
    println!("=== Performance Summary ===");
    println!("No preallocation:        {:?}", elapsed_no_prealloc);
    println!(
        "Memory prealloc only:    {:?} ({:.1}x)",
        elapsed_mem_prealloc,
        elapsed_no_prealloc.as_secs_f64() / elapsed_mem_prealloc.as_secs_f64()
    );
    println!(
        "Disk prealloc only:      {:?} ({:.1}x)",
        elapsed_disk_prealloc,
        elapsed_no_prealloc.as_secs_f64() / elapsed_disk_prealloc.as_secs_f64()
    );
    println!(
        "Both preallocations:     {:?} ({:.1}x)",
        elapsed_full_prealloc,
        elapsed_no_prealloc.as_secs_f64() / elapsed_full_prealloc.as_secs_f64()
    );

    // Test 5: Demonstrate reopening preallocated database
    println!("\n=== Test 5: Reopening Preallocated Database ===");
    {
        let engine = StorageEngine::new("demo_disk_prealloc.teg".into())?;
        println!("Reopened database with {} entries", engine.len());

        // Verify some data
        let value = engine.get(b"key_00000").unwrap();
        assert_eq!(value.as_ref(), b"value_00000");
        println!("Data integrity verified ✓");
    }

    // Test 6: Show valid_data_end tracking
    println!("\n=== Test 6: Valid Data End Tracking ===");
    {
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open("demo_disk_prealloc.teg")?;
        let mut header = vec![0u8; 64];
        file.read_exact(&mut header)?;

        let version = u16::from_be_bytes([header[6], header[7]]);
        println!("File version: {}", version);

        if version >= 2 {
            let valid_data_end = u64::from_be_bytes([
                header[21], header[22], header[23], header[24], header[25], header[26], header[27],
                header[28],
            ]);
            let file_size = file.metadata()?.len();
            println!("Valid data end: {} bytes", valid_data_end);
            println!("File size: {} bytes", file_size);
            println!(
                "Efficiency: {:.1}% of file contains valid data",
                (valid_data_end as f64 / file_size as f64) * 100.0
            );
        }
    }

    // Cleanup
    println!("\n=== Cleanup ===");
    for file in &[
        "demo_no_prealloc.teg",
        "demo_mem_prealloc.teg",
        "demo_disk_prealloc.teg",
        "demo_full_prealloc.teg",
    ] {
        if std::fs::remove_file(file).is_ok() {
            println!("Removed {}", file);
        }
    }

    println!("\n=== Demo Complete ===");
    Ok(())
}
