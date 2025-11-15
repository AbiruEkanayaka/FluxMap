//! Tests for the metrics layer.

use fluxmap::db::{Database, VacuumOptions};
use fluxmap::error::FluxError;
use fluxmap::mem::EvictionPolicy;
use fluxmap::persistence::PersistenceOptions;
use metrics::Label;
use metrics_util::debugging::{DebuggingRecorder, DebugValue, Snapshot};
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

/// Sets up a `DebuggingRecorder` to capture metrics emitted during a test.
/// This is wrapped in a `Lazy` to ensure it's only initialized once.
static SNAPSHOTTER: Lazy<Snapshotter> = Lazy::new(|| {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("failed to install recorder");
    snapshotter
});

// --- Assertion Helpers ---

fn assert_counter(snapshot: &Snapshot, name: &'static str, labels: &[(&'static str, &'static str)], expected: u64) {
    let labels: HashSet<Label> = labels.iter().map(|(k, v)| Label::new(*k, *v)).collect();

    let value = snapshot.iter().find_map(|(composite_key, _, _, v)| {
        let (_, key) = composite_key.clone().into_parts();
        let key_labels_set = key.labels().cloned().collect::<HashSet<_>>();
        if key.name() == name && key_labels_set == labels {
            if let DebugValue::Counter(c) = v {
                return Some(*c);
            }
        }
        None
    });

    assert_eq!(value.unwrap_or(0), expected, "Metric '{}' with labels {:?} did not match expected value", name, labels);
}

fn assert_counter_gt(snapshot: &Snapshot, name: &'static str, labels: &[(&'static str, &'static str)], floor: u64) {
    let labels: HashSet<Label> = labels.iter().map(|(k, v)| Label::new(*k, *v)).collect();

    let value = snapshot.iter().find_map(|(composite_key, _, _, v)| {
        let (_, key) = composite_key.clone().into_parts();
        let key_labels_set = key.labels().cloned().collect::<HashSet<_>>();
        if key.name() == name && key_labels_set == labels {
            if let DebugValue::Counter(c) = v {
                return Some(*c);
            }
        }
        None
    });

    assert!(value.unwrap_or(0) > floor, "Metric '{}' with labels {:?} was not greater than {}", name, labels, floor);
}


fn assert_gauge_gt(snapshot: &Snapshot, name: &'static str, labels: &[(&'static str, &'static str)], floor: f64) {
    let labels: HashSet<Label> = labels.iter().map(|(k, v)| Label::new(*k, *v)).collect();

    let value = snapshot.iter().find_map(|(composite_key, _, _, v)| {
        let (_, key) = composite_key.clone().into_parts();
        let key_labels_set = key.labels().cloned().collect::<HashSet<_>>();
        if key.name() == name && key_labels_set == labels {
            if let DebugValue::Gauge(g) = v {
                return Some(**g);
            }
        }
        None
    });

    assert!(value.unwrap_or(0.0) > floor, "Metric '{}' with labels {:?} was not greater than {}", name, labels, floor);
}

fn assert_histogram_count(snapshot: &Snapshot, name: &'static str, labels: &[(&'static str, &'static str)], expected_count: u64) {
    let labels: HashSet<Label> = labels.iter().map(|(k, v)| Label::new(*k, *v)).collect();

    let count = snapshot.iter().find_map(|(composite_key, _, _, v)| {
        let (_, key) = composite_key.clone().into_parts();
        let key_labels_set = key.labels().cloned().collect::<HashSet<_>>();
        if key.name() == name && key_labels_set == labels {
            if let DebugValue::Histogram(h) = v {
                return Some(h.len() as u64);
            }
        }
        None
    });

    assert_eq!(count.unwrap_or(0), expected_count, "Metric '{}' with labels {:?} did not have the expected count", name, labels);
}


#[tokio::test]
async fn test_transaction_metrics() {
    // Clear metrics from previous tests
    SNAPSHOTTER.snapshot();

    let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());

    // 1. Test successful commit
    let mut handle = db.handle();
    handle
        .transaction(|_h| Box::pin(async move { Ok::<_, FluxError>(()) }))
        .await
        .unwrap();

    let snapshot = SNAPSHOTTER.snapshot();
    assert_counter(&snapshot, "fluxmap_transactions_total", &[("status", "committed")], 1);

    // 2. Test aborted transaction
    let _ = handle
        .transaction(|_h| {
            Box::pin(async move {
                Err::<(), _>(FluxError::NoActiveTransaction) // Simulate an error
            })
        })
        .await;

    let snapshot = SNAPSHOTTER.snapshot();
    assert_counter(&snapshot, "fluxmap_transactions_total", &[("status", "aborted")], 1);
}

#[tokio::test]
async fn test_operation_metrics() {
    SNAPSHOTTER.snapshot();
    let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());
    let handle = db.handle();

    handle.insert("key".to_string(), 1).await.unwrap();
    let _ = handle.get(&"key".to_string()).unwrap();
    let _ = handle.remove(&"key".to_string()).await.unwrap();
    let _ = handle.range(&"a".to_string(), &"z".to_string()).unwrap();
    let _ = handle.prefix_scan("key").unwrap();

    let snapshot = SNAPSHOTTER.snapshot();
    assert_counter(&snapshot, "fluxmap_operations_total", &[("type", "insert")], 1);
    assert_counter(&snapshot, "fluxmap_operations_total", &[("type", "get")], 1);
    assert_counter(&snapshot, "fluxmap_operations_total", &[("type", "remove")], 1);
    assert_counter(&snapshot, "fluxmap_operations_total", &[("type", "range_scan")], 1);
    assert_counter(&snapshot, "fluxmap_operations_total", &[("type", "prefix_scan")], 1);
}

#[tokio::test]
async fn test_memory_and_eviction_metrics() {
    SNAPSHOTTER.snapshot();
    let db: Arc<Database<String, i32>> = Arc::new(
        Database::builder()
            .max_memory(450) // Small limit to force eviction
            .eviction_policy(EvictionPolicy::Lru)
            .build()
            .await
            .unwrap(),
    );
    let handle = db.handle();

    handle.insert("key1".to_string(), 1).await.unwrap();
    handle.insert("key2".to_string(), 2).await.unwrap();

    // This insert should trigger an eviction
    handle.insert("key3".to_string(), 3).await.unwrap();

    let snapshot = SNAPSHOTTER.snapshot();
    // Check eviction metric
    assert_counter(&snapshot, "fluxmap_evictions_total", &[("policy", "lru")], 1);

    // Check memory gauge
    assert_gauge_gt(&snapshot, "fluxmap_memory_usage_bytes", &[], 0.0);
}

#[tokio::test]
async fn test_vacuum_metrics() {
    SNAPSHOTTER.snapshot();
    let db: Arc<Database<String, i32>> = Arc::new(
        Database::builder()
            .auto_vacuum(VacuumOptions {
                interval: Duration::from_secs(60), // Long interval to prevent auto-run
            })
            .build()
            .await
            .unwrap(),
    );

    // Create a dead version
    let handle = db.handle();
    handle.insert("key".to_string(), 1).await.unwrap();
    handle.remove(&"key".to_string()).await.unwrap();

    // Manually run vacuum
    let (versions, keys) = db.vacuum().await.unwrap();
    assert_eq!(versions, 1);
    assert_eq!(keys, 1);

    let snapshot = SNAPSHOTTER.snapshot();
    // Check vacuum metrics
    assert_counter(&snapshot, "fluxmap_vacuum_versions_removed_total", &[], 1);
    assert_counter(&snapshot, "fluxmap_vacuum_keys_removed_total", &[], 1);
    assert_histogram_count(&snapshot, "fluxmap_vacuum_duration_seconds", &[], 1);
}

#[tokio::test]
async fn test_wal_metrics() {
    SNAPSHOTTER.snapshot();
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();

    let db: Arc<Database<String, i32>> = Arc::new(
        Database::builder()
            .durability_full(PersistenceOptions {
                wal_path,
                wal_pool_size: 2,
                wal_segment_size_bytes: 50, // Tiny segment to force rotation
            })
            .build()
            .await
            .unwrap(),
    );
    let handle = db.handle();

    // This write should be logged
    handle
        .insert("some_key".to_string(), 123)
        .await
        .unwrap();

    // These writes should force a rotation
    handle
        .insert("another_key".to_string(), 456)
        .await
        .unwrap();
    handle.insert("final_key".to_string(), 789).await.unwrap();

    let snapshot = SNAPSHOTTER.snapshot();
    // Check WAL metrics
    assert_counter_gt(&snapshot, "fluxmap_wal_bytes_written_total", &[], 0);
    assert_counter(&snapshot, "fluxmap_wal_rotations_total", &[], 1);
    assert_histogram_count(&snapshot, "fluxmap_wal_sync_duration_seconds", &[], 3);
}
