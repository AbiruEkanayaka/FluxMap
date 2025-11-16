use criterion::{
    AxisScale, BenchmarkId, Criterion, PlotConfiguration, Throughput, criterion_group,
    criterion_main,
};
use fluxmap::db::Database;
use rand::RngCore;
use rand::prelude::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Barrier;

const DATASET_SIZE: u64 = 100_000;

/// Pre-populates the database with a fixed set of keys.
async fn setup_db(db: &Arc<Database<String, u64>>) {
    let handle = db.handle();
    for i in 0..DATASET_SIZE {
        handle.insert(i.to_string(), i * 2).await.unwrap();
    }
}

/// --- Concurrent Reads Benchmark ---
fn bench_concurrent_reads(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = Arc::new(
        rt.block_on(Database::<String, u64>::builder().build())
            .unwrap(),
    );

    // Pre-populate the database
    rt.block_on(setup_db(&db));

    let mut group = c.benchmark_group("Concurrent Reads (Get)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    // Benchmark across different numbers of concurrent tasks
    for &num_tasks in &[1, 2, 4, 8, 16, 32] {
        let ops_per_task = 1000 / num_tasks;
        group.throughput(Throughput::Elements((ops_per_task * num_tasks) as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_tasks),
            &num_tasks,
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
                            for _ in 0..ops_per_task {
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
    }
    group.finish();
}

/// --- Concurrent Writes Benchmark ---
fn bench_concurrent_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("Concurrent Writes (Insert)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for &num_tasks in &[1, 2, 4, 8, 16, 32] {
        let ops_per_task = 1000 / num_tasks;
        group.throughput(Throughput::Elements((ops_per_task * num_tasks) as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_tasks),
            &num_tasks,
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
                            for _ in 0..ops_per_task {
                                let key = rng.random_range(0..DATASET_SIZE).to_string();
                                let value = rng.next_u64();
                                black_box(db_handle.insert(key, value).await.unwrap());
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
    }
    group.finish();
}

/// --- Concurrent Mixed Workload Benchmark (80% Reads, 20% Writes) ---
fn bench_concurrent_mixed(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = Arc::new(
        rt.block_on(Database::<String, u64>::builder().build())
            .unwrap(),
    );

    rt.block_on(setup_db(&db));

    let mut group = c.benchmark_group("Concurrent Mixed (80% Read, 20% Write)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for &num_tasks in &[1, 2, 4, 8, 16, 32] {
        let ops_per_task = 1000 / num_tasks;
        group.throughput(Throughput::Elements((ops_per_task * num_tasks) as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_tasks),
            &num_tasks,
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
                            for _ in 0..ops_per_task {
                                let key = rng.random_range(0..DATASET_SIZE).to_string();
                                if rng.random_ratio(80, 100) {
                                    // 80% reads
                                    let _ = black_box(db_handle.get(&key));
                                } else {
                                    // 20% writes
                                    let value = rng.next_u64();
                                    black_box(db_handle.insert(key, value).await.unwrap());
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
    }
    group.finish();
}

/// --- Transaction Latency Benchmark ---
/// Measures the time to complete a small, contentious transaction.
fn bench_transaction_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("Transaction Latency (Contention)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for &num_tasks in &[1, 2, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(num_tasks as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_tasks),
            &num_tasks,
            |b, &tasks_count| {
                b.to_async(&rt).iter(|| async {
                    let db = Arc::new(Database::<String, u64>::builder().build().await.unwrap());

                    // Setup two contentious keys
                    let handle = db.handle();
                    handle.insert("1".to_string(), 100).await.unwrap();
                    handle.insert("2".to_string(), 100).await.unwrap();

                    let barrier = Arc::new(Barrier::new(tasks_count));
                    let mut handles = Vec::new();

                    // All tasks will try to atomically swap the values of key 1 and 2
                    for _ in 0..tasks_count {
                        let db_clone = db.clone();
                        let barrier_clone = barrier.clone();
                        let handle = tokio::spawn(async move {
                            let mut db_handle = db_clone.handle();
                            barrier_clone.wait().await;

                            // Retry loop for the transaction
                            loop {
                                let result = db_handle
                                    .transaction(|h| {
                                        Box::pin(async move {
                                            let val1 = h.get(&"1".to_string()).unwrap().unwrap();
                                            let val2 = h.get(&"2".to_string()).unwrap().unwrap();
                                            h.insert("1".to_string(), *val2).await?;
                                            h.insert("2".to_string(), *val1).await?;
                                            Ok::<_, fluxmap::error::FluxError>(())
                                        })
                                    })
                                    .await;

                                if result.is_ok() {
                                    break;
                                }
                                // If a serialization conflict occurs, the loop will retry.
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
    }
    group.finish();
}

/// --- Concurrent Updates Benchmark ---
fn bench_concurrent_updates(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = Arc::new(
        rt.block_on(Database::<String, u64>::builder().build())
            .unwrap(),
    );

    // Pre-populate the database
    rt.block_on(setup_db(&db));

    let mut group = c.benchmark_group("Concurrent Updates (Insert)");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for &num_tasks in &[1, 2, 4, 8, 16, 32] {
        let ops_per_task = 1000 / num_tasks;
        group.throughput(Throughput::Elements((ops_per_task * num_tasks) as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_tasks),
            &num_tasks,
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
                            for _ in 0..ops_per_task {
                                let key = rng.random_range(0..DATASET_SIZE).to_string();
                                let value = rng.next_u64();
                                black_box(db_handle.insert(key, value).await.unwrap());
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
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_reads,
    bench_concurrent_writes,
    bench_concurrent_mixed,
    bench_transaction_latency,
    bench_concurrent_updates
);
criterion_main!(benches);
