use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use tegdb::vector_index::{HNSWIndex, IVFIndex, LSHIndex};

/// Generate random normalized vector
fn random_normalized_vector(dimension: usize) -> Vec<f64> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut vec: Vec<f64> = (0..dimension).map(|_| rng.gen_range(-1.0..1.0)).collect();
    let norm: f64 = vec.iter().map(|x| x * x).sum::<f64>().sqrt();
    if norm > 0.0 {
        for x in &mut vec {
            *x /= norm;
        }
    }
    vec
}

/// Generate test vectors for benchmarking
fn generate_test_vectors(num_vectors: usize, dimension: usize) -> Vec<(usize, Vec<f64>)> {
    (0..num_vectors)
        .map(|i| (i, random_normalized_vector(dimension)))
        .collect()
}

fn hnsw_index_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("HNSW Index Performance");

    // Test different dataset sizes
    for dataset_size in [100, 1000, 5000] {
        let vectors = generate_test_vectors(dataset_size, 128);

        group.bench_with_input(
            BenchmarkId::new("HNSW Build", dataset_size),
            &dataset_size,
            |b, &_size| {
                b.iter(|| {
                    let mut index = HNSWIndex::new(16, 32);
                    for (id, vector) in &vectors {
                        index.insert(*id, vector.clone()).unwrap();
                    }
                    black_box(index);
                });
            },
        );

        // Test search performance
        let mut index = HNSWIndex::new(16, 32);
        for (id, vector) in &vectors {
            index.insert(*id, vector.clone()).unwrap();
        }
        let query_vector = random_normalized_vector(128);

        group.bench_with_input(
            BenchmarkId::new("HNSW Search", dataset_size),
            &dataset_size,
            |b, &_size| {
                b.iter(|| {
                    let results = index.search(&query_vector, 10).unwrap();
                    black_box(results);
                });
            },
        );
    }

    group.finish();
}

fn ivf_index_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("IVF Index Performance");

    // Test different dataset sizes
    for dataset_size in [100, 1000, 5000] {
        let vectors = generate_test_vectors(dataset_size, 128);

        group.bench_with_input(
            BenchmarkId::new("IVF Build", dataset_size),
            &dataset_size,
            |b, &_size| {
                b.iter(|| {
                    let mut index = IVFIndex::new(8); // 8 clusters
                    index.build(vectors.clone()).unwrap();
                    black_box(index);
                });
            },
        );

        // Test search performance
        let mut index = IVFIndex::new(8);
        index.build(vectors).unwrap();
        let query_vector = random_normalized_vector(128);

        group.bench_with_input(
            BenchmarkId::new("IVF Search", dataset_size),
            &dataset_size,
            |b, &_size| {
                b.iter(|| {
                    let results = index.search(&query_vector, 10).unwrap();
                    black_box(results);
                });
            },
        );
    }

    group.finish();
}

fn lsh_index_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("LSH Index Performance");

    // Test different dataset sizes
    for dataset_size in [100, 1000, 5000] {
        let vectors = generate_test_vectors(dataset_size, 128);

        group.bench_with_input(
            BenchmarkId::new("LSH Build", dataset_size),
            &dataset_size,
            |b, &_size| {
                b.iter(|| {
                    let mut index = LSHIndex::new(128, 4, 8); // 128 dim, 4 hash functions, 8 tables
                    for (id, vector) in &vectors {
                        index.insert(*id, vector.clone()).unwrap();
                    }
                    black_box(index);
                });
            },
        );

        // Test search performance
        let mut index = LSHIndex::new(128, 4, 8);
        for (id, vector) in &vectors {
            index.insert(*id, vector.clone()).unwrap();
        }
        let query_vector = random_normalized_vector(128);

        group.bench_with_input(
            BenchmarkId::new("LSH Search", dataset_size),
            &dataset_size,
            |b, &_size| {
                b.iter(|| {
                    let results = index.search(&query_vector, 10).unwrap();
                    black_box(results);
                });
            },
        );
    }

    group.finish();
}

fn index_comparison_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Index Type Comparison");

    let dataset_size = 1000;
    let vectors = generate_test_vectors(dataset_size, 128);
    let query_vector = random_normalized_vector(128);

    // Build all indexes
    let mut hnsw_index = HNSWIndex::new(16, 32);
    for (id, vector) in &vectors {
        hnsw_index.insert(*id, vector.clone()).unwrap();
    }

    let mut ivf_index = IVFIndex::new(8);
    ivf_index.build(vectors.clone()).unwrap();

    let mut lsh_index = LSHIndex::new(128, 4, 8);
    for (id, vector) in &vectors {
        lsh_index.insert(*id, vector.clone()).unwrap();
    }

    // Compare search performance
    group.bench_function("HNSW Search", |b| {
        b.iter(|| {
            let results = hnsw_index.search(&query_vector, 10).unwrap();
            black_box(results);
        });
    });

    group.bench_function("IVF Search", |b| {
        b.iter(|| {
            let results = ivf_index.search(&query_vector, 10).unwrap();
            black_box(results);
        });
    });

    group.bench_function("LSH Search", |b| {
        b.iter(|| {
            let results = lsh_index.search(&query_vector, 10).unwrap();
            black_box(results);
        });
    });

    group.finish();
}

fn vector_dimension_scaling_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Vector Dimension Scaling");

    let dataset_size = 1000;

    // Test different vector dimensions
    for dimension in [64, 128, 256, 512] {
        let vectors = generate_test_vectors(dataset_size, dimension);
        let query_vector = random_normalized_vector(dimension);

        let mut hnsw_index = HNSWIndex::new(16, 32);
        for (id, vector) in &vectors {
            hnsw_index.insert(*id, vector.clone()).unwrap();
        }

        group.bench_with_input(
            BenchmarkId::new("HNSW Search", dimension),
            &dimension,
            |b, &_dim| {
                b.iter(|| {
                    let results = hnsw_index.search(&query_vector, 10).unwrap();
                    black_box(results);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    hnsw_index_benchmark,
    ivf_index_benchmark,
    lsh_index_benchmark,
    index_comparison_benchmark,
    vector_dimension_scaling_benchmark
);

criterion_main!(benches);
