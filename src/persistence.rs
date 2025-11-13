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

use crate::SkipList;
use crate::mem::MemSize;
use crate::transaction::Workspace;
use rustix::fs::FallocateFlags;
use rustix::fs::fallocate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    mpsc,
};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::Notify;

pub const WAL_SEGMENT_SIZE_BYTES: u64 = 1 * 1024 * 1024; // 1MB for easier testing

/// Options for configuring data durability.
#[derive(Debug, Clone)]
pub struct PersistenceOptions {
    /// The directory path to store WAL and snapshot files.
    pub wal_path: PathBuf,
    /// The number of WAL segment files to cycle through.
    ///
    /// Defaults to 4.
    pub wal_pool_size: usize,
    /// The size of each WAL segment file in bytes.
    ///
    /// Defaults to 1MB.
    pub wal_segment_size_bytes: u64,
}

impl PersistenceOptions {
    /// Creates new persistence options with a specified path and default values for other settings.
    pub fn new(wal_path: PathBuf) -> Self {
        Self {
            wal_path,
            wal_pool_size: 4,
            wal_segment_size_bytes: 1 * 1024 * 1024, // 1MB
        }
    }
}

/// Defines the durability guarantees for the database.
#[derive(Debug, Clone)]
pub enum DurabilityLevel {
    /// **In-Memory Mode:** No data is written to disk. This offers the highest
    /// performance but provides no durability. All data is lost on process exit.
    InMemory,
    /// **Relaxed Durability (Group Commit):** Acknowledges a commit after the data
    /// has been written to the operating system's buffer. A background task
    /// flushes this buffer to disk when one of the configured conditions is met.
    ///
    /// At least one condition must be set.
    ///
    /// - **Pros:** High throughput, as it avoids waiting for disk I/O on every commit.
    /// - **Cons:** In the event of an OS crash or power failure, transactions that
    ///   occurred since the last flush may be lost.
    Relaxed {
        /// The configuration options for persistence.
        options: PersistenceOptions,
        /// Flushes after `T` milliseconds have passed since the last flush.
        /// This provides a latency bound for durability.
        flush_interval_ms: Option<u64>,
        /// Flushes after `N` commits have occurred since the last flush.
        /// This provides a deterministic bound on the number of transactions that can be lost.
        flush_after_n_commits: Option<usize>,
        /// Flushes after `M` bytes have been written to the WAL since the last flush.
        /// This helps control the size of the buffer and recovery time.
        flush_after_m_bytes: Option<u64>,
    },
    /// **Full Durability (Per-Transaction `fsync`):** Acknowledges a commit only
    /// after the data has been fully written and synced to the physical disk.
    ///
    /// - **Pros:** Provides the strongest guarantee of durability. No committed data
    ///   will be lost due to OS crashes or power failures.
    /// - **Cons:** Lower throughput due to the performance cost of `fsync` on every
    ///   transaction.
    Full {
        /// The configuration options for persistence.
        options: PersistenceOptions,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum WalSegmentState {
    Writing,
    PendingSnapshot,
    Available,
}

#[derive(Debug)]
struct WalSegment {
    file: Arc<Mutex<File>>,
    state: Arc<(Mutex<WalSegmentState>, Condvar)>,
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
#[serde(bound(
    serialize = "K: Eq + std::hash::Hash + Serialize + MemSize, V: Serialize + MemSize",
    deserialize = "K: Eq + std::hash::Hash + Deserialize<'de> + MemSize, V: Deserialize<'de> + MemSize"
))]
struct SnapshotData<K, V> {
    /// The index of the last WAL segment that was successfully incorporated into this snapshot.
    last_processed_segment_idx: Option<usize>,
    /// The complete key-value data at the time of the snapshot.
    data: HashMap<K, Arc<V>>,
}

impl<K: MemSize, V: MemSize> Default for SnapshotData<K, V> {
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
    // For Relaxed mode flushing
    flush_signal: Arc<Notify>,
    /// The number of commits since the last flush.
    pub commits_since_flush: Arc<AtomicUsize>,
    /// The number of bytes written to the WAL since the last flush.
    pub bytes_since_flush: Arc<AtomicU64>,
    // A channel to send completed segments to the snapshotter
    snapshot_queue_tx: mpsc::Sender<usize>,
    _snapshot_queue_rx: Arc<Mutex<mpsc::Receiver<usize>>>,
    // Handles for our background threads
    _flusher_handle: Option<JoinHandle<()>>,
    _snapshotter_handle: Option<JoinHandle<()>>,
    _phantom: PhantomData<(K, V)>,
}

fn setup_wal_files(options: &PersistenceOptions) -> io::Result<Vec<WalSegment>> {
    fs::create_dir_all(&options.wal_path)?;
    let mut wal_segments = Vec::with_capacity(options.wal_pool_size);
    for i in 0..options.wal_pool_size {
        let segment_path = options.wal_path.join(format!("wal.{}", i));
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&segment_path)?;

        // Pre-allocate space for the WAL segment to reduce fragmentation and
        // ensure space is available.
        fallocate(
            &file,
            FallocateFlags::empty(),
            0,
            options.wal_segment_size_bytes,
        )?;

        let initial_state = if i == 0 {
            WalSegmentState::Writing
        } else {
            WalSegmentState::Available
        };

        wal_segments.push(WalSegment {
            file: Arc::new(Mutex::new(file)),
            state: Arc::new((Mutex::new(initial_state), Condvar::new())),
        });
    }
    Ok(wal_segments)
}

impl<K, V> PersistenceEngine<K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + std::hash::Hash
        + Eq
        + Serialize
        + for<'de> Deserialize<'de>
        + MemSize
        + std::borrow::Borrow<str>,
    V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de> + MemSize,
{
    /// Creates a new `PersistenceEngine` based on the provided `DurabilityLevel`.
    ///
    /// If the level is `InMemory`, it returns `Ok(None)`.
    /// Otherwise, it initializes the WAL files and starts the necessary background threads.
    pub fn new(config: DurabilityLevel) -> io::Result<Option<Self>> {
        if let DurabilityLevel::InMemory = &config {
            return Ok(None);
        }

        let options = match &config {
            DurabilityLevel::Relaxed { options, .. } => options,
            DurabilityLevel::Full { options } => options,
            DurabilityLevel::InMemory => unreachable!(),
        };

        let snapshot_path = options.wal_path.join("snapshot.db");
        let wal_segments = setup_wal_files(options)?;
        let (tx, rx) = mpsc::channel();
        let snapshot_queue_rx = Arc::new(Mutex::new(rx));
        let current_segment_idx = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let flush_signal = Arc::new(Notify::new());
        let commits_since_flush = Arc::new(AtomicUsize::new(0));
        let bytes_since_flush = Arc::new(AtomicU64::new(0));

        // --- Snapshotter Thread ---
        // This thread waits for filled WAL segments, applies them to a new snapshot,
        // and then marks the segment as available for reuse.
        let segments_clone_snap = wal_segments.clone();
        let rx_clone_snap = Arc::clone(&snapshot_queue_rx);
        let snapshot_path_clone = snapshot_path.clone();
        let wal_path_clone = options.wal_path.clone();
        let shutdown_clone_snap = Arc::clone(&shutdown);
        let snapshotter_handle = std::thread::spawn(move || {
            while !shutdown_clone_snap.load(Ordering::Relaxed) {
                let segment_idx = match rx_clone_snap
                    .lock()
                    .expect("Snapshot queue mutex poisoned")
                    .recv_timeout(Duration::from_millis(100))
                {
                    Ok(idx) => idx,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue, // Loop to check shutdown flag again
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // Sender has been dropped, so we can shut down.
                        return;
                    }
                };

                let segment: &WalSegment = &segments_clone_snap[segment_idx];

                {
                    let (lock, _) = &*segment.state;
                    let state = lock.lock().expect("WalSegment state mutex poisoned");
                    if *state != WalSegmentState::PendingSnapshot {
                        eprintln!(
                            "Snapshotter: Segment {} in unexpected state: {:?}",
                            segment_idx, *state
                        );
                        continue;
                    }
                }

                let mut snapshot_data: SnapshotData<K, V> = if snapshot_path_clone.exists() {
                    match File::open(&snapshot_path_clone) {
                        Ok(file) => ciborium::from_reader(file).unwrap_or_else(|e| {
                            // A corrupted snapshot is a fatal error. We cannot safely proceed
                            // as it would mean losing all previously snapshotted data.
                            panic!(
                                "FATAL: Snapshot file at {:?} is corrupted and could not be deserialized: {}. Manual intervention required.",
                                snapshot_path_clone, e
                            );
                        }),
                        Err(e) => {
                            eprintln!("Snapshotter: Failed to open existing snapshot file {:?}: {}", snapshot_path_clone, e);
                            SnapshotData::default()
                        }
                    }
                } else {
                    SnapshotData::default()
                };

                let wal_file_path = wal_path_clone.join(format!("wal.{}", segment_idx));
                if let Ok(wal_file) = File::open(&wal_file_path) {
                    let mut reader = BufReader::new(wal_file);
                    loop {
                        match ciborium::from_reader::<Workspace<K, V>, _>(&mut reader) {
                            Ok(workspace) => {
                                for (key, value) in workspace {
                                    match value {
                                        Some(val) => {
                                            snapshot_data.data.insert(key, val);
                                        }
                                        None => {
                                            snapshot_data.data.remove((&key).borrow());
                                        }
                                    }
                                }
                            }
                            Err(ciborium::de::Error::Io(ref e))
                                if e.kind() == io::ErrorKind::UnexpectedEof =>
                            {
                                break; // Reached end of file
                            }
                            Err(e) => {
                                eprintln!(
                                    "Snapshotter: Failed to deserialize workspace from WAL segment {:?}: {}",
                                    wal_file_path, e
                                );
                                break; // Stop processing this WAL segment
                            }
                        }
                    }
                } else {
                    eprintln!(
                        "Snapshotter: Failed to open WAL segment file {:?}.",
                        wal_file_path
                    );
                }

                snapshot_data.last_processed_segment_idx = Some(segment_idx);
                let tmp_snapshot_path = snapshot_path_clone.with_extension("db.tmp");

                let write_result = (|| {
                    let tmp_file = File::create(&tmp_snapshot_path)?;
                    ciborium::into_writer(&snapshot_data, tmp_file).map_err(|e| match e {
                        ciborium::ser::Error::Io(io_err) => io_err,
                        ciborium::ser::Error::Value(msg) => {
                            io::Error::new(io::ErrorKind::InvalidData, msg)
                        }
                    })?;
                    let file_to_sync = File::open(&tmp_snapshot_path)?;
                    file_to_sync.sync_all()?;
                    fs::rename(&tmp_snapshot_path, &snapshot_path_clone)?;
                    Ok::<(), io::Error>(())
                })();

                if let Err(e) = write_result {
                    // This is a critical failure. If we can't write snapshots, we can't
                    // reclaim WAL space, and the system will eventually deadlock.
                    // Panicking is the safest option to alert the operator and prevent
                    // silent failure.
                    let _ = fs::remove_file(&tmp_snapshot_path); // Attempt to clean up
                    panic!(
                        "FATAL: Snapshotter failed to write or rename snapshot file {:?}: {}. Shutting down to prevent data loss or deadlock.",
                        snapshot_path_clone, e
                    );
                } else {
                    // Truncate the WAL file to mark it as free.
                    if let Ok(mut file) = segment.file.lock() {
                        if let Err(e) = file.set_len(0) {
                            eprintln!(
                                "Snapshotter: Failed to truncate WAL segment file {}: {}",
                                segment_idx, e
                            );
                        }
                        if let Err(e) = file.seek(SeekFrom::Start(0)) {
                            eprintln!(
                                "Snapshotter: Failed to seek WAL segment file {}: {}",
                                segment_idx, e
                            );
                        }
                    } else {
                        eprintln!(
                            "Snapshotter: Failed to lock WAL segment file {} for truncation.",
                            segment_idx
                        );
                    }

                    // Mark the segment as Available again.
                    let (lock, cvar) = &*segment.state;
                    let mut state = lock.lock().unwrap();
                    *state = WalSegmentState::Available;
                    cvar.notify_one();
                }
            }
        });

        // --- Flusher Thread (only for Relaxed mode) ---
        let flusher_handle = if let DurabilityLevel::Relaxed {
            flush_interval_ms, ..
        } = config
        {
            let segments_clone_flush = wal_segments.clone();
            let current_idx_clone_flush = Arc::clone(&current_segment_idx);
            let shutdown_clone_flush = Arc::clone(&shutdown);
            let flush_signal_clone = Arc::clone(&flush_signal);
            let commits_clone = Arc::clone(&commits_since_flush);
            let bytes_clone = Arc::clone(&bytes_since_flush);

            Some(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                runtime.block_on(async move {
                    loop {
                        if shutdown_clone_flush.load(Ordering::Relaxed) {
                            break;
                        }

                        let wait_future = flush_signal_clone.notified();
                        let timeout_future = if let Some(interval) = flush_interval_ms {
                            tokio::time::sleep(Duration::from_millis(interval))
                        } else {
                            // If no interval is set, wait indefinitely for a signal.
                            tokio::time::sleep(Duration::from_secs(u64::MAX))
                        };

                        tokio::select! {
                            _ = wait_future => {
                                // Woken by a signal (N commits or M bytes)
                            },
                            _ = timeout_future => {
                                // Woken by timeout
                            }
                        };

                        if shutdown_clone_flush.load(Ordering::Relaxed) {
                            break;
                        }

                        let idx = current_idx_clone_flush.load(Ordering::Relaxed);
                        let segment = &segments_clone_flush[idx];
                        if let Ok(file) = segment.file.lock() {
                            if file.sync_all().is_ok() {
                                // Reset counters on successful flush
                                commits_clone.store(0, Ordering::Relaxed);
                                bytes_clone.store(0, Ordering::Relaxed);
                            }
                        }
                    }
                });
            }))
        } else {
            None
        };

        Ok(Some(Self {
            config: config.clone(),
            wal_path: options.wal_path.clone(),
            snapshot_path,
            wal_segments,
            current_segment_idx,
            writer_position: Arc::new(AtomicU64::new(0)),
            shutdown,
            flush_signal,
            commits_since_flush,
            bytes_since_flush,
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
    pub async fn recover(
        &self,
        current_memory_bytes: Arc<AtomicU64>,
        access_clock: Arc<AtomicU64>,
    ) -> Result<SkipList<K, V>, crate::error::FluxError> {
        let skiplist = SkipList::new(current_memory_bytes, access_clock);
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
        let wal_pool_size = match &self.config {
            DurabilityLevel::Relaxed { options, .. } => options.wal_pool_size,
            DurabilityLevel::Full { options } => options.wal_pool_size,
            DurabilityLevel::InMemory => unreachable!(),
        };
        let mut wal_files = Vec::new();
        for i in 0..wal_pool_size {
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
                wal_files
                    .iter()
                    .skip(pos + 1)
                    .map(|(idx, _, _)| *idx)
                    .collect()
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
        tx_manager.commit(&tx, || Ok(()))?;

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

            let segment_size = match &self.config {
                DurabilityLevel::Relaxed { options, .. } => options.wal_segment_size_bytes,
                DurabilityLevel::Full { options } => options.wal_segment_size_bytes,
                DurabilityLevel::InMemory => unreachable!(),
            };

            // --- Rotation Check ---
            // Check if write fits *before* acquiring the lock.
            if current_pos + data.len() as u64 > segment_size {
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
            let bytes_written = data.len() as u64;
            self.writer_position
                .fetch_add(bytes_written, Ordering::Relaxed);

            // --- Trigger flush if in Relaxed mode and a condition is met ---
            if let DurabilityLevel::Relaxed {
                flush_after_n_commits,
                flush_after_m_bytes,
                ..
            } = &self.config
            {
                let old_commits = self.commits_since_flush.fetch_add(1, Ordering::Relaxed);
                let old_bytes = self
                    .bytes_since_flush
                    .fetch_add(bytes_written, Ordering::Relaxed);

                let mut should_flush = false;
                if let Some(n) = flush_after_n_commits {
                    if old_commits + 1 >= *n {
                        should_flush = true;
                    }
                }
                if let Some(m) = flush_after_m_bytes {
                    if old_bytes + bytes_written >= *m {
                        should_flush = true;
                    }
                }

                if should_flush {
                    self.flush_signal.notify_one();
                }
            }

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
        let wal_pool_size = match &self.config {
            DurabilityLevel::Relaxed { options, .. } => options.wal_pool_size,
            DurabilityLevel::Full { options } => options.wal_pool_size,
            DurabilityLevel::InMemory => unreachable!(),
        };
        let current_idx = self.current_segment_idx.load(Ordering::Relaxed);
        let next_idx = (current_idx + 1) % wal_pool_size;

        // Block until the next segment is available.
        let (next_lock, next_cvar) = &*self.wal_segments[next_idx].state;
        let mut state = next_lock.lock().unwrap();
        while *state != WalSegmentState::Available {
            state = next_cvar.wait(state).unwrap();
        }
        // It's available, we can proceed with the rotation.
        *state = WalSegmentState::Writing;
        drop(state); // Explicitly drop the lock before moving on.

        // Mark the old segment as pending snapshot
        let (old_lock, _) = &*self.wal_segments[current_idx].state;
        *old_lock.lock().unwrap() = WalSegmentState::PendingSnapshot;

        // Send the old segment to the snapshotter
        if let Err(e) = self.snapshot_queue_tx.send(current_idx) {
            eprintln!(
                "FATAL: Snapshotter thread has died. Cannot send segment {} for snapshotting. Error: {}",
                current_idx, e
            );
            // This is a critical error. The system can no longer guarantee durability
            // or reclaim space. A real-world implementation might need to panic,
            // enter a read-only mode, or shut down gracefully here.
        }

        // Reset writer position and update current segment index
        self.writer_position.store(0, Ordering::Relaxed);
        self.current_segment_idx.store(next_idx, Ordering::Relaxed);

        // Also reset the flush counters as rotation implies a flush of the old segment
        self.commits_since_flush.store(0, Ordering::Relaxed);
        self.bytes_since_flush.store(0, Ordering::Relaxed);

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
            if let Err(e) = handle.join() {
                eprintln!("Flusher thread panicked: {:?}", e);
            }
        }
        if let Some(handle) = self._snapshotter_handle.take() {
            if let Err(e) = handle.join() {
                eprintln!("Snapshotter thread panicked: {:?}", e);
            }
        }
    }
}
