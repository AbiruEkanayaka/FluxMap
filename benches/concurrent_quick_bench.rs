use criterion::{
    Criterion, Throughput, criterion_group, criterion_main, BenchmarkId,
};
use fluxmap::db::Database;
use rand::Rng;
use rand::prelude::{RngCore, SeedableRng};
use rand::rngs::StdRng;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Barrier;

const DATASET_SIZE: u64 = 10_000; // Smaller dataset for quick benches
const OPS_PER_TASK: u64 = 100; // Fewer operations per task for quick benches
const NUM_TASKS: usize = 32;

/// Pre-populates the database with a fixed set of keys.
async fn setup_db(db: &Arc<Database<String, u64>>) {
    let handle = db.handle();
    for i in 0..DATASET_SIZE {
        handle.insert(i.to_string(), i * 2).await.unwrap();
    }
}

/// --- Concurrent Reads Benchmark (32 tasks) ---
fn bench_concurrent_reads_32(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = Arc::new(
        rt.block_on(Database::<String, u64>::builder().build())
            .unwrap(),
    );

    // Pre-populate the database
    rt.block_on(setup_db(&db));

    let mut group = c.benchmark_group("Concurrent Reads (32 Tasks)");
    group.throughput(Throughput::Elements((OPS_PER_TASK * NUM_TASKS as u64) as u64));

    group.bench_with_input(
        BenchmarkId::from_parameter(NUM_TASKS),
        &NUM_TASKS,
        |b, &tasks_count| {
            b.to_async(&rt).iter(|| async {
                let db = db.clone();
                let barrier = Arc::new(Barrier::new(tasks_count));
                let mut handles = Vec::new();

                for i in 0..tasks_count {
                    let db_clone = db.clone();
                    let barrier_clone = barrier.clone();
                    let handle = tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(i as u64);
                        barrier_clone.wait().await;
                        let db_handle = db_clone.handle();
                        for _ in 0..OPS_PER_TASK {
                            let key = rng.random_range(0..DATASET_SIZE).to_string();
                            let _ = black_box(db_handle.get(&key));
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }
            });
        },
    );
    group.finish();
}

/// --- Concurrent Writes Benchmark (32 tasks) ---
fn bench_concurrent_writes_32(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("Concurrent Writes (32 Tasks)");
    group.throughput(Throughput::Elements((OPS_PER_TASK * NUM_TASKS as u64) as u64));

    group.bench_with_input(
        BenchmarkId::from_parameter(NUM_TASKS),
        &NUM_TASKS,
        |b, &tasks_count| {
            b.to_async(&rt).iter(|| async {
                // A new DB is created for each iteration to avoid it growing indefinitely
                let db = Arc::new(Database::<String, u64>::builder().build().await.unwrap());
                let barrier = Arc::new(Barrier::new(tasks_count));
                let mut handles = Vec::new();

                for i in 0..tasks_count {
                    let db_clone = db.clone();
                    let barrier_clone = barrier.clone();
                    let handle = tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(i as u64);
                        barrier_clone.wait().await;
                        let db_handle = db_clone.handle();
                        for _ in 0..OPS_PER_TASK {
                            let key = rng.random_range(0..DATASET_SIZE).to_string();
                            let value = rng.next_u64();
                            loop {
                                match db_handle.insert(key.clone(), value).await {
                                    Ok(_) => break,
                                    Err(fluxmap::error::FluxError::SerializationConflict) => {
                                        // Retry on conflict
                                        tokio::task::yield_now().await;
                                    }
                                    Err(e) => panic!("Unexpected error during insert: {:?}", e),
                                }
                            }
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }
            });
        },
    );
    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_reads_32,
    bench_concurrent_writes_32
);
criterion_main!(benches);
