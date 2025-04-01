use criterion::{
    black_box, criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, Criterion,
    Throughput,
};
use rand::Rng;
use std::fs;
use std::path::PathBuf;
use tegdb::Database;
use tokio::runtime::Runtime; // Changed: import random number generator

// Async function to run one transaction cycle with random inputs.
async fn transaction_cycle(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = rand::rng();
    let random: u32 = rng.random();
    let key = format!("tx_key_{}", random).into_bytes();
    let initial_value = format!("start_value_{}", random).into_bytes();
    let updated_value = format!("finish_value_{}", random).into_bytes();

    let mut tx = db.new_transaction().await;
    tx.insert(black_box(&key), black_box(initial_value)).await?;
    tx.update(black_box(&key), black_box(updated_value)).await?;
    let _ = tx
        .select(black_box(&key))
        .await
        .unwrap()
        .0
        .unwrap_or(Vec::new());
    tx.delete(black_box(&key)).await?;
    tx.commit().await?;
    Ok(())
}

// New benchmark functions

fn benchmark_select_only(rt: &Runtime, db: &Database, group: &mut BenchmarkGroup<WallTime>) {
    group.bench_function("select_only", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut rng = rand::rng();
                let random: u32 = rng.random();
                let key = format!("select_key_{}", random).into_bytes();
                let mut tx = db.new_transaction().await;
                tx.insert(black_box(&key), black_box(b"select_value".to_vec()))
                    .await
                    .unwrap();
                // Perform select.
                let _ = tx
                    .select(black_box(&key))
                    .await
                    .unwrap()
                    .0
                    .unwrap_or(Vec::new());
                tx.rollback().await.unwrap();
            })
        })
    });
}

fn benchmark_update(rt: &Runtime, db: &Database, group: &mut BenchmarkGroup<WallTime>) {
    group.bench_function("update", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut rng = rand::rng();
                let random: u32 = rng.random();
                let key = format!("update_key_{}", random).into_bytes();
                // Prepopulate key.
                let mut pre_tx = db.new_transaction().await;
                pre_tx
                    .insert(black_box(&key), black_box(b"old_data".to_vec()))
                    .await
                    .unwrap();
                pre_tx.commit().await.unwrap();
                // Update the key.
                let mut tx = db.new_transaction().await;
                tx.update(black_box(&key), black_box(b"new_data".to_vec()))
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            })
        })
    });
}

fn benchmark_insert(rt: &Runtime, db: &Database, group: &mut BenchmarkGroup<WallTime>) {
    group.bench_function("insert", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut rng = rand::rng();
                let random: u32 = rng.random();
                let key = format!("insert_key_{}", random).into_bytes();
                let mut tx = db.new_transaction().await;
                tx.insert(black_box(&key), black_box(b"inserted_data".to_vec()))
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            })
        })
    });
}

fn benchmark_delete(rt: &Runtime, db: &Database, group: &mut BenchmarkGroup<WallTime>) {
    group.bench_function("delete", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut rng = rand::rng();
                let random: u32 = rng.random();
                let key = format!("delete_key_{}", random).into_bytes();
                // Prepopulate key.
                let mut pre_tx = db.new_transaction().await;
                pre_tx
                    .insert(black_box(&key), black_box(b"to_delete_data".to_vec()))
                    .await
                    .unwrap();
                pre_tx.commit().await.unwrap();
                // Delete the key.
                let mut tx = db.new_transaction().await;
                tx.delete(black_box(&key)).await.unwrap();
                tx.commit().await.unwrap();
            })
        })
    });
}

fn benchmark_transaction_cycle(rt: &Runtime, db: &Database, group: &mut BenchmarkGroup<WallTime>) {
    group.bench_function("transaction_cycle", |b| {
        b.iter(|| rt.block_on(async { transaction_cycle(&db).await.unwrap() }))
    });
}

// Modified main benchmark function which now calls the separated functions.
fn database_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = rt.block_on(async { Database::new(PathBuf::from("bench_db")).await });
    let mut group = c.benchmark_group("database_transaction");
    group.warm_up_time(std::time::Duration::from_secs(5));
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(100);
    group.throughput(Throughput::Elements(1));

    benchmark_select_only(&rt, &db, &mut group);
    benchmark_update(&rt, &db, &mut group);
    benchmark_insert(&rt, &db, &mut group);
    benchmark_delete(&rt, &db, &mut group);
    benchmark_transaction_cycle(&rt, &db, &mut group);

    group.finish();
    rt.block_on(db.shutdown());
    fs::remove_dir_all("bench_db").ok();
}

criterion_group!(benches, database_benchmark);
criterion_main!(benches);
