use fluxmap::persistence::{DurabilityLevel, PersistenceEngine, PersistenceOptions};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_initialization_in_memory() {
    let config = DurabilityLevel::InMemory;
    let fatal_error = Arc::new(Mutex::new(None));
    let engine = PersistenceEngine::<String, String>::new(config, fatal_error).unwrap();
    assert!(engine.is_none());
}

#[test]
fn test_durable_initialization_creates_files() {
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();
    let options = PersistenceOptions {
        wal_path: wal_path.clone(),
        wal_pool_size: 4,
        wal_segment_size_bytes: 1024, // 1KB for testing
    };
    let config = DurabilityLevel::Full {
        options: options.clone(),
    };

    let fatal_error = Arc::new(Mutex::new(None));
    let _engine = PersistenceEngine::<String, String>::new(config, fatal_error)
        .unwrap()
        .unwrap();

    for i in 0..options.wal_pool_size {
        let segment_path = wal_path.join(format!("wal.{}", i));
        assert!(segment_path.exists());
        let metadata = std::fs::metadata(segment_path).unwrap();
        // We can't assert equality because fallocate might not be supported on all test filesystems,
        // in which case the size will be 0.
        assert!(metadata.len() == options.wal_segment_size_bytes || metadata.len() == 0);
    }
}

#[test]
fn test_log_writes_to_wal() {
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();
    let config = DurabilityLevel::Full {
        options: PersistenceOptions::new(wal_path.clone()),
    };
    let fatal_error = Arc::new(Mutex::new(None));
    let engine = PersistenceEngine::<String, String>::new(config, fatal_error)
        .unwrap()
        .unwrap();

    // --- First Write ---
    let data1 = b"first write";
    engine.log(data1).unwrap();

    // Verify content
    let mut wal0_file = File::open(wal_path.join("wal.0")).unwrap();
    let mut buffer = vec![0; data1.len()];
    wal0_file.read_exact(&mut buffer).unwrap();
    assert_eq!(&buffer, data1);

    // --- Second Write ---
    let data2 = b"second write";
    engine.log(data2).unwrap();

    // Verify content
    wal0_file.seek(SeekFrom::Start(data1.len() as u64)).unwrap();
    let mut buffer = vec![0; data2.len()];
    wal0_file.read_exact(&mut buffer).unwrap();
    assert_eq!(&buffer, data2);
}

#[test]
fn test_log_triggers_rotation() {
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();
    let options = PersistenceOptions {
        wal_path: wal_path.clone(),
        wal_pool_size: 4,
        wal_segment_size_bytes: 100, // 100 bytes for testing
    };
    let config = DurabilityLevel::Full {
        options: options.clone(),
    };
    let fatal_error = Arc::new(Mutex::new(None));
    let engine = PersistenceEngine::<String, String>::new(config, fatal_error)
        .unwrap()
        .unwrap();

    // Almost fill the first segment
    let large_write = vec![0; (options.wal_segment_size_bytes - 10) as usize];
    engine.log(&large_write).unwrap();

    // This write should trigger the rotation
    let final_write = b"this should rotate";
    engine.log(final_write).unwrap();

    // Verify the data was written to the new segment
    let mut wal1_file = File::open(wal_path.join("wal.1")).unwrap();
    let mut buffer = vec![0; final_write.len()];
    wal1_file.read_exact(&mut buffer).unwrap();
    assert_eq!(&buffer, final_write);

    // Give snapshotter time to work
    std::thread::sleep(Duration::from_millis(200));

    // After rotation and snapshotting, the original WAL file should be truncated
    let wal0_meta = std::fs::metadata(wal_path.join("wal.0")).unwrap();
    assert_eq!(wal0_meta.len(), 0);
}

#[test]
fn test_relaxed_mode_flusher_time_based() {
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();
    let flush_interval = Duration::from_millis(50);
    let config = DurabilityLevel::Relaxed {
        options: PersistenceOptions::new(wal_path.clone()),
        flush_interval_ms: Some(flush_interval.as_millis() as u64),
        flush_after_n_commits: None,
        flush_after_m_bytes: None,
    };

    // Create the engine, which spawns the flusher thread
    let fatal_error = Arc::new(Mutex::new(None));
    let engine = PersistenceEngine::<String, String>::new(config, fatal_error)
        .unwrap()
        .unwrap();

    // Write some data. In relaxed mode, this only writes to the OS buffer.
    let data = b"some data to be flushed";
    engine.log(data).unwrap();

    // Wait for the flusher thread to run
    std::thread::sleep(flush_interval.saturating_add(Duration::from_millis(50)));

    // Verify the data was flushed to disk by reading the file directly.
    let mut wal0_file = File::open(wal_path.join("wal.0")).unwrap();
    let mut buffer = vec![0; data.len()];
    wal0_file.read_exact(&mut buffer).unwrap();
    assert_eq!(&buffer, data);
}

#[test]
fn test_relaxed_mode_flushes_on_commit_count() {
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();
    let config = DurabilityLevel::Relaxed {
        options: PersistenceOptions::new(wal_path.clone()),
        flush_interval_ms: Some(5000), // Long interval to ensure it's not time-based
        flush_after_n_commits: Some(3),
        flush_after_m_bytes: None,
    };
    let fatal_error = Arc::new(Mutex::new(None));
    let engine = PersistenceEngine::<String, String>::new(config, fatal_error)
        .unwrap()
        .unwrap();

    let data = b"commit";
    engine.log(data).unwrap(); // 1
    assert_eq!(engine.commits_since_flush.load(Ordering::Relaxed), 1);

    engine.log(data).unwrap(); // 2
    assert_eq!(engine.commits_since_flush.load(Ordering::Relaxed), 2);

    // Give some time to ensure the time-based flush has NOT run
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        engine.commits_since_flush.load(Ordering::Relaxed),
        2,
        "Counter should not reset before flush is triggered"
    );

    engine.log(data).unwrap(); // 3 - this should trigger the flush

    // Give the flusher thread a moment to run after being signaled
    thread::sleep(Duration::from_millis(100));

    // Now the counter should be reset
    assert_eq!(
        engine.commits_since_flush.load(Ordering::Relaxed),
        0,
        "Counter should reset after flush"
    );
}

#[test]
fn test_relaxed_mode_flushes_on_byte_count() {
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();
    let config = DurabilityLevel::Relaxed {
        options: PersistenceOptions::new(wal_path.clone()),
        flush_interval_ms: Some(5000), // Long interval
        flush_after_n_commits: None,
        flush_after_m_bytes: Some(20),
    };
    let fatal_error = Arc::new(Mutex::new(None));
    let engine = PersistenceEngine::<String, String>::new(config, fatal_error)
        .unwrap()
        .unwrap();

    let data1 = b"1234567890"; // 10 bytes
    engine.log(data1).unwrap();
    assert_eq!(engine.bytes_since_flush.load(Ordering::Relaxed), 10);

    // Give some time to ensure the time-based flush has NOT run
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        engine.bytes_since_flush.load(Ordering::Relaxed),
        10,
        "Byte counter should not reset before flush"
    );

    let data2 = b"12345678901"; // 11 bytes, total is 21, over the threshold of 20
    engine.log(data2).unwrap();

    // Give the flusher thread a moment to run after being signaled
    thread::sleep(Duration::from_millis(100));

    // Now the counter should be reset
    assert_eq!(
        engine.bytes_since_flush.load(Ordering::Relaxed),
        0,
        "Byte counter should reset after flush"
    );
}

#[test]
fn test_snapshotter_frees_segments() {
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();
    let options = PersistenceOptions {
        wal_path: wal_path.clone(),
        wal_pool_size: 4,
        wal_segment_size_bytes: 1024, // 1KB for testing
    };
    let config = DurabilityLevel::Full {
        options: options.clone(),
    };

    let fatal_error = Arc::new(Mutex::new(None));
    let engine = PersistenceEngine::<String, String>::new(config, fatal_error)
        .unwrap()
        .unwrap();

    let data_size = (options.wal_segment_size_bytes / 2) as usize; // Half a segment
    let data = vec![0u8; data_size];

    // Fill all segments and trigger rotations
    for _i in 0..options.wal_pool_size * 2 {
        engine.log(&data).unwrap();
        std::thread::sleep(Duration::from_millis(50)); // Give snapshotter time
    }

    // Try to write one more time to ensure a segment is available.
    // This will panic if rotation gets stuck because no segments are 'Available'.
    engine.log(&data).unwrap();
}
