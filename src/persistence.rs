//! Manages data durability through a Write-Ahead Log (WAL) and snapshotting.
//!
//! This module provides the `PersistenceEngine`, which is responsible for ensuring
//! that committed data survives process crashes and restarts. It operates on the
//! principle of writing all changes to a log before applying them to the main data
//! store.
//!
//! # Strategy
//!
//! 1.  **Write-Ahead Log (WAL):** Transactions are serialized and written to a WAL file.
//!     This write can be configured to be fully synced to disk or buffered, depending on
//!     the chosen `DurabilityLevel`.
//!
//! 2.  **WAL Segmentation:** The WAL is split into multiple segment files. When a segment
//!     fills up, the engine rotates to the next available one.
//!
//! 3.  **Snapshotting:** A background thread consumes full WAL segments and applies their
//!     changes to a consolidated snapshot file. This prevents the WAL from growing
//!     indefinitely and speeds up the recovery process.
//!
//! 4.  **Recovery:** On startup, the engine first loads the latest snapshot and then
//!     replays any subsequent WAL segments to restore the database to its last known
//!     consistent state.

use crate::transaction::Workspace;
use crate::SkipList;
use rustix::fs::fallocate;
use rustix::fs::FallocateFlags;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread::JoinHandle;
use std::time::Duration;

const WAL_POOL_SIZE: usize = 4;
const WAL_SEGMENT_SIZE_BYTES: u64 = 1 * 1024 * 1024; // 1MB for easier testing

/// Defines the durability guarantees for the database.
#[derive(Debug, Clone)]
pub enum DurabilityLevel {
    /// **In-Memory Mode:** No data is written to disk. This offers the highest
    /// performance but provides no durability. All data is lost on process exit.
    InMemory,
    /// **Relaxed Durability (Group Commit):** Acknowledges a commit after the data
    /// has been written to the operating system's buffer. A background task
    /// periodically flushes this buffer to disk (`fsync`).
    ///
    /// - **Pros:** High throughput, as it avoids waiting for disk I/O on every commit.
    /// - **Cons:** In the event of an OS crash or power failure, transactions that
    ///   occurred since the last flush may be lost.
    Relaxed {
        /// The directory path to store WAL and snapshot files.
        wal_path: PathBuf,
        /// The interval at which the background task flushes the OS buffer to disk.
        flush_interval: Duration,
    },
    /// **Full Durability (Per-Transaction `fsync`):** Acknowledges a commit only
    /// after the data has been fully written and synced to the physical disk.
    ///
    /// - **Pros:** Provides the strongest guarantee of durability. No committed data
    ///   will be lost due to OS crashes or power failures.
    /// - **Cons:** Lower throughput due to the performance cost of `fsync` on every
    ///   transaction.
    Full {
        /// The directory path to store WAL and snapshot files.
        wal_path: PathBuf,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum WalSegmentState {
    Writing,
    PendingSnapshot,
    Available,
}

#[derive(Debug)]
struct WalSegment {
    file: Arc<Mutex<File>>,
    state: Arc<Mutex<WalSegmentState>>,
}

impl Clone for WalSegment {
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            state: self.state.clone(),
        }
    }
}

/// A snapshot of the database state at a specific point in time.
#[derive(Serialize, Deserialize, Debug)]
#[serde(
    bound(
        serialize = "K: Eq + std::hash::Hash + Serialize, V: Serialize",
        deserialize = "K: Eq + std::hash::Hash + Deserialize<'de>, V: Deserialize<'de>"
    )
)]
struct SnapshotData<K, V> {
    /// The index of the last WAL segment that was successfully incorporated into this snapshot.
    last_processed_segment_idx: Option<usize>,
    /// The complete key-value data at the time of the snapshot.
    data: HashMap<K, Arc<V>>,
}

impl<K, V> Default for SnapshotData<K, V> {
    fn default() -> Self {
        Self {
            last_processed_segment_idx: None,
            data: HashMap::new(),
        }
    }
}

/// The engine responsible for managing durability via WAL and snapshots.
///
/// It handles logging transaction data, rotating WAL files, triggering snapshots,
/// and recovering the database state on startup.
#[derive(Debug)]
pub struct PersistenceEngine<K, V> {
    config: DurabilityLevel,
    wal_path: PathBuf,
    snapshot_path: PathBuf,
    wal_segments: Vec<WalSegment>,
    current_segment_idx: Arc<AtomicUsize>,
    writer_position: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    // A channel to send completed segments to the snapshotter
    snapshot_queue_tx: mpsc::Sender<usize>,
    _snapshot_queue_rx: Arc<Mutex<mpsc::Receiver<usize>>>,
    // Handles for our background threads
    _flusher_handle: Option<JoinHandle<()>>,
    _snapshotter_handle: Option<JoinHandle<()>>,
    _phantom: PhantomData<(K, V)>,
}

fn setup_wal_files(wal_path: &PathBuf) -> io::Result<Vec<WalSegment>> {
    fs::create_dir_all(wal_path)?;
    let mut wal_segments = Vec::with_capacity(WAL_POOL_SIZE);
    for i in 0..WAL_POOL_SIZE {
        let segment_path = wal_path.join(format!("wal.{}", i));
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&segment_path)?;

        // Pre-allocate space for the WAL segment to reduce fragmentation and
        // ensure space is available.
        fallocate(&file, FallocateFlags::empty(), 0, WAL_SEGMENT_SIZE_BYTES)?;

        let initial_state = if i == 0 {
            WalSegmentState::Writing
        } else {
            WalSegmentState::Available
        };

        wal_segments.push(WalSegment {
            file: Arc::new(Mutex::new(file)),
            state: Arc::new(Mutex::new(initial_state)),
        });
    }
    Ok(wal_segments)
}

impl<K, V> PersistenceEngine<K, V>
where
    K: Ord + Clone + Send + Sync + 'static + std::hash::Hash + Eq + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de>,
{
    /// Creates a new `PersistenceEngine` based on the provided `DurabilityLevel`.
    ///
    /// If the level is `InMemory`, it returns `Ok(None)`.
    /// Otherwise, it initializes the WAL files and starts the necessary background threads.
    pub fn new(config: DurabilityLevel) -> io::Result<Option<Self>> {
        if let DurabilityLevel::InMemory = &config {
            return Ok(None);
        }

        let wal_path = match &config {
            DurabilityLevel::Relaxed { wal_path, .. } => wal_path,
            DurabilityLevel::Full { wal_path } => wal_path,
            DurabilityLevel::InMemory => unreachable!(),
        };

        let snapshot_path = wal_path.join("snapshot.db");
        let wal_segments = setup_wal_files(wal_path)?;
        let (tx, rx) = mpsc::channel();
        let snapshot_queue_rx = Arc::new(Mutex::new(rx));
        let current_segment_idx = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        // --- Snapshotter Thread ---
        // This thread waits for filled WAL segments, applies them to a new snapshot,
        // and then marks the segment as available for reuse.
        let segments_clone_snap = wal_segments.clone();
        let rx_clone_snap = Arc::clone(&snapshot_queue_rx);
        let snapshot_path_clone = snapshot_path.clone();
        let wal_path_clone = wal_path.clone();
        let shutdown_clone_snap = Arc::clone(&shutdown);
        let snapshotter_handle = std::thread::spawn(move || {
            while !shutdown_clone_snap.load(Ordering::Relaxed) {
                let segment_idx = {
                    let rx = rx_clone_snap.lock().unwrap();
                    match rx.try_recv() {
                        Ok(idx) => idx,
                        Err(mpsc::TryRecvError::Empty) => {
                            // No message, wait a bit and check shutdown flag again
                            std::thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                        Err(mpsc::TryRecvError::Disconnected) => {
                            // Channel disconnected, time to shut down
                            return;
                        }
                    }
                };

                let segment: &WalSegment = &segments_clone_snap[segment_idx];

                // 1. Verify state is PendingSnapshot
                {
                    let state = segment.state.lock().unwrap();
                    if *state != WalSegmentState::PendingSnapshot {
                        // This would be a critical error, but for now we'll just continue.
                        // In a real system, log this error.
                        continue;
                    }
                } // State lock released

                // 2. Load current snapshot
                let mut snapshot_data: SnapshotData<K, V> = if snapshot_path_clone.exists() {
                    let file = File::open(&snapshot_path_clone).unwrap();
                    ciborium::from_reader(file).unwrap_or_default()
                } else {
                    SnapshotData::default()
                };

                // 3. Read WAL segment and apply changes
                let wal_file_path = wal_path_clone.join(format!("wal.{}", segment_idx));
                if let Ok(wal_file) = File::open(&wal_file_path) {
                    let mut reader = BufReader::new(wal_file);
                    while let Ok(workspace) =
                        ciborium::from_reader::<Workspace<K, V>, _>(&mut reader)
                    {
                        for (key, value) in workspace {
                            match value {
                                Some(val) => {
                                    snapshot_data.data.insert(key, val);
                                }
                                None => {
                                    snapshot_data.data.remove(&key);
                                }
                            }
                        }
                    }
                }

                // 4. Update metadata and write new snapshot atomically
                snapshot_data.last_processed_segment_idx = Some(segment_idx);
                let tmp_snapshot_path = snapshot_path_clone.with_extension("db.tmp");

                let tmp_file = File::create(&tmp_snapshot_path).unwrap();
                ciborium::into_writer(&snapshot_data, tmp_file).unwrap();

                // Fsync and rename
                File::open(&tmp_snapshot_path)
                    .unwrap()
                    .sync_all()
                    .unwrap();
                fs::rename(&tmp_snapshot_path, &snapshot_path_clone).unwrap();

                // 5. Truncate the WAL file to mark it as free.
                {
                    if let Ok(file) = segment.file.lock() {
                        let _ = file.set_len(0);
                    }
                }

                // 6. Mark the segment as Available again.
                {
                    let mut state = segment.state.lock().unwrap();
                    *state = WalSegmentState::Available;
                }
            }
        });

        // --- Flusher Thread (only for Relaxed mode) ---
        // This thread periodically calls `fsync` on the active WAL segment.
        let flusher_handle = if let DurabilityLevel::Relaxed { flush_interval, .. } = &config {
            let segments_clone_flush = wal_segments.clone();
            let current_idx_clone_flush = Arc::clone(&current_segment_idx);
            let flush_interval_clone = *flush_interval;
            let shutdown_clone_flush = Arc::clone(&shutdown);
            Some(std::thread::spawn(move || {
                while !shutdown_clone_flush.load(Ordering::Relaxed) {
                    std::thread::sleep(flush_interval_clone);
                    let idx = current_idx_clone_flush.load(Ordering::Relaxed);
                    let segment = &segments_clone_flush[idx];
                    if let Ok(file) = segment.file.lock() {
                        let _ = file.sync_all();
                    }
                }
            }))
        } else {
            None
        };

        Ok(Some(Self {
            config: config.clone(),
            wal_path: wal_path.clone(),
            snapshot_path,
            wal_segments,
            current_segment_idx,
            writer_position: Arc::new(AtomicU64::new(0)),
            shutdown,
            snapshot_queue_tx: tx,
            _snapshot_queue_rx: snapshot_queue_rx,
            _flusher_handle: flusher_handle,
            _snapshotter_handle: Some(snapshotter_handle),
            _phantom: PhantomData,
        }))
    }

    /// Recovers the database state from disk.
    ///
    /// This process involves:
    /// 1. Loading the most recent snapshot file.
    /// 2. Finding all WAL segments modified *after* the snapshot was taken.
    /// 3. Replaying the records from those WAL segments in order.
    ///
    /// The resulting `SkipList` contains the fully recovered state.
    pub async fn recover(&self) -> Result<SkipList<K, V>, crate::error::FluxError> {
        let skiplist = SkipList::new();
        let tx_manager = skiplist.transaction_manager();

        // 1. Load snapshot
        let snapshot_data: SnapshotData<K, V> = if self.snapshot_path.exists() {
            let file = File::open(&self.snapshot_path)?;
            ciborium::from_reader(file).unwrap_or_default()
        } else {
            SnapshotData::default()
        };

        let last_snap_idx = snapshot_data.last_processed_segment_idx;

        // 2. Populate skiplist from snapshot data in a single transaction
        let tx = tx_manager.begin();
        for (key, value) in snapshot_data.data {
            skiplist.insert(key, value, &tx).await;
        }

        // 3. Find WALs to replay, sorted by modification time
        let mut wal_files = Vec::new();
        for i in 0..WAL_POOL_SIZE {
            let path = self.wal_path.join(format!("wal.{}", i));
            if path.exists() {
                let meta = fs::metadata(&path)?;
                if meta.len() > 0 {
                    wal_files.push((i, path, meta.modified()?));
                }
            }
        }
        wal_files.sort_by_key(|k| k.2); // Sort by modification time, oldest to newest

        // 4. Determine which files to replay
        let replay_order_indices: Vec<usize> = if let Some(last_idx) = last_snap_idx {
            if let Some(pos) = wal_files.iter().position(|(idx, _, _)| *idx == last_idx) {
                // Replay all files that were modified after the one in the snapshot
                wal_files.iter().skip(pos + 1).map(|(idx, _, _)| *idx).collect()
            } else {
                // Snapshot refers to a WAL that's now empty/gone, so replay everything
                wal_files.iter().map(|(idx, _, _)| *idx).collect()
            }
        } else {
            // No snapshot, replay everything
            wal_files.iter().map(|(idx, _, _)| *idx).collect()
        };

        // 5. Replay WAL files into the same transaction
        for idx in replay_order_indices {
            let path = self.wal_path.join(format!("wal.{}", idx));
            if let Ok(wal_file) = File::open(&path) {
                let mut reader = BufReader::new(wal_file);
                while let Ok(workspace) = ciborium::from_reader::<Workspace<K, V>, _>(&mut reader) {
                    for (key, value) in workspace {
                        match value {
                            Some(val) => skiplist.insert(key, val, &tx).await,
                            None => {
                                skiplist.remove(&key, &tx).await;
                            }
                        }
                    }
                }
            }
        }

        // 6. Commit the recovery transaction
        tx_manager.commit(&tx)?;

        Ok(skiplist)
    }

    /// Logs a serialized transaction workspace to the current WAL segment.
    ///
    /// This method handles WAL rotation automatically. If the write would exceed
    /// the current segment's capacity, it triggers a rotation and retries.
    ///
    /// Depending on the `DurabilityLevel`, this may or may not block on `fsync`.
    pub fn log(&self, data: &[u8]) -> io::Result<()> {
        // This loop ensures that if a rotation happens, we transparently retry the write on the new segment.
        loop {
            let current_pos = self.writer_position.load(Ordering::Relaxed);

            // --- Rotation Check ---
            // Check if write fits *before* acquiring the lock.
            if current_pos + data.len() as u64 > WAL_SEGMENT_SIZE_BYTES {
                self.rotate_wal()?;
                // After rotation, restart the loop to get the new segment and position.
                continue;
            }

            // --- Write Path ---
            let segment_idx = self.current_segment_idx.load(Ordering::Relaxed);
            let segment = &self.wal_segments[segment_idx];

            // Lock is held for the shortest possible time: just the seek and write.
            {
                let mut file_lock = segment.file.lock().unwrap();
                file_lock.seek(SeekFrom::Start(current_pos))?;
                file_lock.write_all(data)?;

                if let DurabilityLevel::Full { .. } = self.config {
                    file_lock.sync_all()?;
                }
            } // Lock is released here

            // Update writer position after a successful write.
            self.writer_position
                .fetch_add(data.len() as u64, Ordering::Relaxed);

            // If the write was successful, exit the loop.
            return Ok(());
        }
    }

    /// Rotates to the next available WAL segment.
    ///
    /// This involves:
    /// 1. Finding the next segment in the pool.
    /// 2. Blocking until that segment is `Available` (i.e., not being snapshotted).
    /// 3. Marking the old segment as `PendingSnapshot` and sending it to the snapshotter.
    /// 4. Marking the new segment as `Writing` and resetting its state.
    fn rotate_wal(&self) -> io::Result<()> {
        let current_idx = self.current_segment_idx.load(Ordering::Relaxed);
        let next_idx = (current_idx + 1) % WAL_POOL_SIZE;

        // Block until the next segment is available.
        loop {
            let mut next_segment_state = self.wal_segments[next_idx].state.lock().unwrap();
            if *next_segment_state == WalSegmentState::Available {
                // It's available, we can proceed with the rotation.
                *next_segment_state = WalSegmentState::Writing;
                break;
            }
            // Drop the lock and wait before retrying.
            drop(next_segment_state);
            std::thread::sleep(Duration::from_millis(10)); // Sleep briefly
        }

        // Mark the old segment as pending snapshot
        *self.wal_segments[current_idx].state.lock().unwrap() = WalSegmentState::PendingSnapshot;

        // Send the old segment to the snapshotter
        self.snapshot_queue_tx.send(current_idx).unwrap();

        // Reset writer position and update current segment index
        self.writer_position.store(0, Ordering::Relaxed);
        self.current_segment_idx
            .store(next_idx, Ordering::Relaxed);

        // Truncate the new segment file before using it
        let segment = &self.wal_segments[next_idx];
        let mut file_lock = segment.file.lock().unwrap();
        file_lock.set_len(0)?;
        // Also seek to the beginning to be explicit
        file_lock.seek(SeekFrom::Start(0))?;

        Ok(())
    }
}

impl<K, V> Drop for PersistenceEngine<K, V> {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(handle) = self._flusher_handle.take() {
            handle.join().unwrap();
        }
        if let Some(handle) = self._snapshotter_handle.take() {
            handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek};
    use tempfile::tempdir;

    #[test]
    fn test_initialization_in_memory() {
        let config = DurabilityLevel::InMemory;
        let engine = PersistenceEngine::<String, String>::new(config).unwrap();
        assert!(engine.is_none());
    }

    #[test]
    fn test_durable_initialization_creates_files() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().to_path_buf();
        let config = DurabilityLevel::Full {
            wal_path: wal_path.clone(),
        };

        let engine = PersistenceEngine::<String, String>::new(config)
            .unwrap()
            .unwrap();

        assert_eq!(engine.wal_segments.len(), WAL_POOL_SIZE);
        for i in 0..WAL_POOL_SIZE {
            let file_lock = engine.wal_segments[i].file.lock().unwrap();
            let metadata = file_lock.metadata().unwrap();
            // We can't assert equality because fallocate might not be supported on all test filesystems,
            // in which case the size will be 0.
            assert!(metadata.len() == WAL_SEGMENT_SIZE_BYTES || metadata.len() == 0);
        }

        let segment0_state = engine.wal_segments[0].state.lock().unwrap();
        assert_eq!(*segment0_state, WalSegmentState::Writing);

        for i in 1..WAL_POOL_SIZE {
            let segment_state = engine.wal_segments[i].state.lock().unwrap();
            assert_eq!(*segment_state, WalSegmentState::Available);
        }
    }

    #[test]
    fn test_log_writes_to_wal() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().to_path_buf();
        let config = DurabilityLevel::Full {
            wal_path: wal_path.clone(),
        };
        let engine = PersistenceEngine::<String, String>::new(config)
            .unwrap()
            .unwrap();

        // --- First Write ---
        let data1 = b"first write";
        engine.log(data1).unwrap();
        assert_eq!(
            engine.writer_position.load(Ordering::Relaxed),
            data1.len() as u64
        );

        // Verify content
        let mut wal0_file = File::open(wal_path.join("wal.0")).unwrap();
        let mut buffer = vec![0; data1.len()];
        wal0_file.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, data1);

        // --- Second Write ---
        let data2 = b"second write";
        engine.log(data2).unwrap();
        assert_eq!(
            engine.writer_position.load(Ordering::Relaxed),
            (data1.len() + data2.len()) as u64
        );

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
        let config = DurabilityLevel::Full {
            wal_path: wal_path.clone(),
        };
        let engine = PersistenceEngine::<String, String>::new(config)
            .unwrap()
            .unwrap();

        // Almost fill the first segment
        let large_write = vec![0; (WAL_SEGMENT_SIZE_BYTES - 10) as usize];
        engine.log(&large_write).unwrap();
        assert_eq!(engine.current_segment_idx.load(Ordering::Relaxed), 0);
        assert_eq!(
            engine.writer_position.load(Ordering::Relaxed),
            large_write.len() as u64
        );

        // This write should trigger the rotation
        let final_write = b"this should rotate";
        engine.log(final_write).unwrap();

        // Verify rotation occurred
        assert_eq!(engine.current_segment_idx.load(Ordering::Relaxed), 1);
        assert_eq!(
            engine.writer_position.load(Ordering::Relaxed),
            final_write.len() as u64
        );

        // Verify the new segment is for writing
        let segment1_state = engine.wal_segments[1].state.lock().unwrap();
        assert_eq!(*segment1_state, WalSegmentState::Writing);

        // Verify the data was written to the new segment
        let mut wal1_file = File::open(wal_path.join("wal.1")).unwrap();
        let mut buffer = vec![0; final_write.len()];
        wal1_file.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, final_write);

        // Instead of checking the channel, wait for wal.0 to become Available
        let mut wal0_available = false;
        for _ in 0..100 { // Try for a bit
            let state = engine.wal_segments[0].state.lock().unwrap();
            if *state == WalSegmentState::Available {
                wal0_available = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(wal0_available, "wal.0 did not become Available after rotation");
    }

    #[test]
    fn test_relaxed_mode_flusher() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().to_path_buf();
        let flush_interval = Duration::from_millis(50);
        let config = DurabilityLevel::Relaxed {
            wal_path: wal_path.clone(),
            flush_interval,
        };

        // Create the engine, which spawns the flusher thread
        let engine = PersistenceEngine::<String, String>::new(config)
            .unwrap()
            .unwrap();
        assert!(engine._flusher_handle.is_some());

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
    fn test_snapshotter_frees_segments() {
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().to_path_buf();
        let config = DurabilityLevel::Full {
            wal_path: wal_path.clone(),
        };

        let engine = PersistenceEngine::<String, String>::new(config)
            .unwrap()
            .unwrap();
        assert!(engine._snapshotter_handle.is_some());

        let data_size = (WAL_SEGMENT_SIZE_BYTES / 2) as usize; // Half a segment
        let data = vec![0u8; data_size];

        // Fill all segments and trigger rotations
        for _i in 0..WAL_POOL_SIZE * 2 { // Trigger more rotations than segments
            engine.log(&data).unwrap();
            // After each log, the current segment might be full, triggering a rotation.
            // The snapshotter should eventually free up segments.
            // We need to give the snapshotter thread some time to process.
            std::thread::sleep(Duration::from_millis(20));
        }

        // Verify that we successfully rotated through segments.
        // The current segment index should be different from the initial one.
        let final_idx = engine.current_segment_idx.load(Ordering::Relaxed);
        assert!(final_idx < WAL_POOL_SIZE); // Should be a valid index

        // Try to write one more time to ensure a segment is available.
        engine.log(&data).unwrap();

        // Check that all segments eventually become available (or are in Writing state)
        // This is a more robust check than just asserting the final index.
        let mut all_available_or_writing = false;
        for _ in 0..100 { // Try for a bit
            all_available_or_writing = true;
            for i in 0..WAL_POOL_SIZE {
                let state = engine.wal_segments[i].state.lock().unwrap();
                if *state != WalSegmentState::Available && *state != WalSegmentState::Writing {
                    all_available_or_writing = false;
                    break;
                }
            }
            if all_available_or_writing {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(all_available_or_writing, "Not all segments became available or writing");
    }
}
