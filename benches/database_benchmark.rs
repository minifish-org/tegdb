use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::fs;
use std::path::PathBuf;
use tegdb::Database;
use tokio::runtime::Runtime;

// Async function to run one transaction cycle: insert, update, select, delete then commit.
async fn transaction_cycle(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = db.new_transaction().await;
    let key = b"key";
    let initial_value = b"initial";
    let updated_value = b"updated";

    // Insert key.
    tx.insert(black_box(key), black_box(initial_value.to_vec())).await?;
    // Update key.
    tx.update(black_box(key), black_box(updated_value.to_vec())).await?;
    // Select key.
    let _ = tx.select(black_box(key)).await.unwrap_or(Vec::new());
    // Delete key.
    tx.delete(black_box(key)).await?;
    // Commit transaction.
    tx.commit().await?;
    Ok(())
}

// Benchmark function for the transaction cycle.
fn database_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = rt.block_on(async {
        let db = Database::new(PathBuf::from("bench_db")).await;
        db
    });
    let mut group = c.benchmark_group("database_transaction");
    group.warm_up_time(std::time::Duration::from_secs(5));
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(100);
    group.throughput(Throughput::Elements(1));

    group.bench_function("transaction_cycle", |b| {
        b.iter(|| {
            rt.block_on(async {
                transaction_cycle(&db).await.unwrap();
            })
        })
    });

    group.finish();
    rt.block_on(db.shutdown());
    fs::remove_dir_all("bench_db").ok();
}

criterion_group!(benches, database_benchmark);
criterion_main!(benches);
