//! # Examples
//!
//! ## Basic Usage (Autocommit)
//!
//! ```
//! # use fluxmap::db::Database;
//! # use std::sync::Arc;
//! #
//! # #[tokio::main]
//! # async fn main() {
//! // Create a new in-memory database using the builder.
//! let db: Arc<Database<String, String>> = Arc::new(Database::builder().build().await.unwrap());
//!
//! // Create a handle to interact with the database.
//! let handle = db.handle();
//!
//! // Each operation is a small, independent transaction.
//! handle.insert("key1".to_string(), "value1".to_string()).await.unwrap();
//! let value = handle.get(&"key1".to_string()).unwrap();
//!
//! assert_eq!(*value, "value1");
//! # }
//! ```
//!
//! ## Explicit Transactions
//!
//! For atomic, multi-statement operations, use the [`Handle::transaction`] helper.
//!
//! ```
//! # use fluxmap::db::Database;
//! # use fluxmap::error::FluxError;
//! # use std::sync::Arc;
//! #
//! # #[tokio::main]
//! # async fn main() -> Result<(), FluxError> {
//! # let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());
//! let mut handle = db.handle();
//! handle.insert("alice".to_string(), 100).await?;
//! handle.insert("bob".to_string(), 50).await?;
//!
//! // Atomically transfer 20 from Alice to Bob.
//! handle.transaction(|h| Box::pin(async move {
//!     let alice_balance = h.get(&"alice".to_string()).unwrap();
//!     let bob_balance = h.get(&"bob".to_string()).unwrap();
//!
//!     h.insert("alice".to_string(), *alice_balance - 20).await?;
//!     h.insert("bob".to_string(), *bob_balance + 20).await?;
//!
//!     Ok::<_, FluxError>(()) // Commit the changes
//! })).await?;
//!
//! let final_alice = handle.get(&"alice".to_string()).unwrap();
//! let final_bob = handle.get(&"bob".to_string()).unwrap();
//!
//! assert_eq!(*final_alice, 80);
//! assert_eq!(*final_bob, 70);
//! # Ok(())
//! # }
//! ```

use crate::error::{FluxError, PersistenceError};
use crate::mem::MemSize;
use crate::persistence::{DurabilityLevel, PersistenceEngine, PersistenceOptions};
use crate::transaction::Transaction;
use crate::SkipList;
use futures::stream::StreamExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// Options for configuring the automatic vacuuming process.
#[derive(Debug, Clone, Copy)]
pub struct VacuumOptions {
    /// The interval at which the vacuum process runs to clean up dead data versions.
    pub interval: Duration,
}

/// A builder for creating a [`Database`] instance with custom configurations.
///
/// # Examples
/// ```
/// # use fluxmap::db::{Database, VacuumOptions};
/// # use fluxmap::persistence::{DurabilityLevel, PersistenceOptions};
/// # use std::time::Duration;
/// # use tempfile::tempdir;
/// #
/// # #[tokio::main]
/// # async fn main() {
/// # let temp_dir = tempdir().unwrap();
/// # let wal_path = temp_dir.path().to_path_buf();
/// // A durable database with custom WAL settings and auto-vacuuming enabled.
/// let db = Database::<String, String>::builder()
///     .durability_full(PersistenceOptions {
///         wal_path,
///         wal_pool_size: 8,
///         wal_segment_size_bytes: 16 * 1024 * 1024, // 16MB
///     })
///     .auto_vacuum(VacuumOptions {
///         interval: Duration::from_secs(30),
///     })
///     .build()
///     .await
///     .unwrap();
/// # }
/// ```
pub struct DatabaseBuilder<K, V> {
    vacuum: Option<VacuumOptions>,
    persistence_options: Option<PersistenceOptions>,
    is_full_durability: bool,
    flush_interval: Option<Duration>,
    flush_after_n_commits: Option<usize>,
    flush_after_m_bytes: Option<u64>,
    max_memory_bytes: Option<u64>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Default for DatabaseBuilder<K, V> {
    fn default() -> Self {
        Self {
            vacuum: None,
            persistence_options: None,
            is_full_durability: false,
            flush_interval: None,
            flush_after_n_commits: None,
            flush_after_m_bytes: None,
            max_memory_bytes: None,
            _phantom: PhantomData,
        }
    }
}

impl<K, V> DatabaseBuilder<K, V>
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
        + MemSize,
    V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de> + MemSize,
{
    /// Configures the database for full durability (`fsync` per transaction).
    pub fn durability_full(mut self, options: PersistenceOptions) -> Self {
        self.persistence_options = Some(options);
        self.is_full_durability = true;
        self
    }

    /// Configures the database for relaxed durability (group commit).
    ///
    /// You must chain this with at least one flush condition (`flush_interval`,
    /// `flush_after_commits`, or `flush_after_bytes`).
    pub fn durability_relaxed(mut self, options: PersistenceOptions) -> Self {
        self.persistence_options = Some(options);
        self.is_full_durability = false;
        self
    }

    /// Sets the time-based flush condition for relaxed durability.
    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = Some(interval);
        self
    }

    /// Sets the commit-based flush condition for relaxed durability.
    pub fn flush_after_commits(mut self, n: usize) -> Self {
        self.flush_after_n_commits = Some(n);
        self
    }

    /// Sets the byte-based flush condition for relaxed durability.
    pub fn flush_after_bytes(mut self, m: u64) -> Self {
        self.flush_after_m_bytes = Some(m);
        self
    }

    /// Enables and configures automatic vacuuming.
    ///
    /// If not set, vacuuming must be performed manually by calling [`Database::vacuum`].
    pub fn auto_vacuum(mut self, vacuum: VacuumOptions) -> Self {
        self.vacuum = Some(vacuum);
        self
    }

    /// Sets a memory limit for the database.
    ///
    /// When the estimated memory usage exceeds this limit, the database will
    /// begin evicting the least recently used (LRU) keys to stay under the limit.
    pub fn max_memory(mut self, bytes: u64) -> Self {
        self.max_memory_bytes = Some(bytes);
        self
    }

    /// Builds the `Database` instance with the specified configurations.
    pub async fn build(self) -> Result<Database<K, V>, FluxError> {
        let durability = match self.persistence_options {
            Some(options) => {
                if self.is_full_durability {
                    DurabilityLevel::Full { options }
                } else {
                    let flush_interval_ms = self.flush_interval.map(|d| d.as_millis() as u64);
                    if flush_interval_ms.is_none()
                        && self.flush_after_n_commits.is_none()
                        && self.flush_after_m_bytes.is_none()
                    {
                        return Err(FluxError::Configuration("Relaxed durability mode requires at least one flush condition (interval, commits, or bytes).".to_string()));
                    }
                    DurabilityLevel::Relaxed {
                        options,
                        flush_interval_ms,
                        flush_after_n_commits: self.flush_after_n_commits,
                        flush_after_m_bytes: self.flush_after_m_bytes,
                    }
                }
            }
            None => DurabilityLevel::InMemory,
        };

        let persistence_engine = PersistenceEngine::new(durability)?
            .map(Arc::new);

        let current_memory_bytes = Arc::new(AtomicU64::new(0));
        let access_clock = Arc::new(AtomicU64::new(0));

        let skiplist = if let Some(engine) = &persistence_engine {
            Arc::new(
                engine
                    .recover(current_memory_bytes.clone(), access_clock.clone())
                    .await?,
            )
        } else {
            Arc::new(SkipList::new(
                current_memory_bytes.clone(),
                access_clock.clone(),
            ))
        };

        let shutdown = Arc::new(AtomicBool::new(false));
        let vacuum_handle = if let Some(vacuum_opts) = self.vacuum {
            let skiplist_clone = skiplist.clone();
            let shutdown_clone = shutdown.clone();
            let handle = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                runtime.block_on(async move {
                    while !shutdown_clone.load(Ordering::Relaxed) {
                        tokio::time::sleep(vacuum_opts.interval).await;
                        if shutdown_clone.load(Ordering::Relaxed) {
                            break;
                        }
                        if let Err(e) = skiplist_clone.vacuum().await {
                            eprintln!("Automatic vacuuming failed: {:?}", e);
                        }
                    }
                });
            });
            Some(handle)
        } else {
            None
        };

        Ok(Database {
            skiplist,
            persistence_engine,
            _vacuum_handle: vacuum_handle,
            shutdown,
            max_memory_bytes: self.max_memory_bytes,
            current_memory_bytes,
        })
    }
}

/// The central database object that owns the underlying data store.
///
/// A `Database` instance is thread-safe and can be shared across multiple threads,
/// typically by wrapping it in an `Arc`. It serves as the factory for creating
/// [`Handle`] instances, which are used to interact with the data.
///
/// Use the [`Database::builder()`] method to construct a new database.
pub struct Database<K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + std::hash::Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + MemSize,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned + MemSize,
{
    skiplist: Arc<SkipList<K, V>>,
    persistence_engine: Option<Arc<PersistenceEngine<K, V>>>,
    _vacuum_handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
    /// The configured memory limit in bytes.
    max_memory_bytes: Option<u64>,
    /// An atomic counter for the estimated current memory usage.
    current_memory_bytes: Arc<AtomicU64>,
}

impl<K, V> Drop for Database<K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + std::hash::Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + MemSize,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned + MemSize,
{
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self._vacuum_handle.take() {
            if let Err(e) = handle.join() {
                eprintln!("Automatic vacuum thread panicked: {:?}", e);
            }
        }
    }
}

impl<K, V> Database<K, V>
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
        + MemSize,
    V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de> + MemSize,
{
    /// Checks if memory usage exceeds the configured limit and evicts keys until it's under the limit.
    async fn evict_if_needed(&self) {
        if let Some(max_mem) = self.max_memory_bytes {
            while self.current_memory_bytes.load(Ordering::Relaxed) > max_mem {
                if self.evict_one().await.is_err() {
                    // Could not find a victim or eviction failed, stop trying for now.
                    break;
                }
            }
        }
    }

    /// Finds and evicts a single LRU victim.
    async fn evict_one(&self) -> Result<(), FluxError> {
        if let Some(victim_key) = self.skiplist.find_lru_victim_key() {
            // The eviction in the skiplist is synchronous and updates the counter.
            // We don't need to do anything with a transaction here, as the eviction
            // is a low-level administrative task that marks the node as dead.
            if self.skiplist.evict(&victim_key).is_some() {
                Ok(())
            } else {
                Err(FluxError::EvictionError)
            }
        } else {
            Err(FluxError::EvictionError) // No victim found
        }
    }
}

impl<K, V> Database<K, V>
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
        + std::borrow::Borrow<str>
        + MemSize,
    V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de> + MemSize,
{
    /// Creates a new `DatabaseBuilder` to configure and build a `Database`.
    pub fn builder() -> DatabaseBuilder<K, V> {
        DatabaseBuilder::default()
    }

    /// Creates a new, empty, in-memory `Database` with default settings.
    ///
    /// For more configuration options, use [`Database::builder`].
    pub async fn new_in_memory() -> Result<Self, FluxError> {
        Database::builder().build().await
    }

    /// Creates a new `Database` with a specified durability level and default settings.
    ///
    /// For more configuration options, use [`Database::builder`].
    pub async fn new(config: DurabilityLevel) -> Result<Self, FluxError> {
        let builder = Database::builder();
        let configured_builder = match config {
            DurabilityLevel::InMemory => builder,
            DurabilityLevel::Full { options } => builder.durability_full(options),
            DurabilityLevel::Relaxed {
                options,
                flush_interval_ms,
                flush_after_n_commits,
                flush_after_m_bytes,
            } => {
                let mut relaxed_builder = builder.durability_relaxed(options);
                if let Some(interval) = flush_interval_ms {
                    relaxed_builder =
                        relaxed_builder.flush_interval(Duration::from_millis(interval));
                }
                if let Some(n) = flush_after_n_commits {
                    relaxed_builder = relaxed_builder.flush_after_commits(n);
                }
                if let Some(m) = flush_after_m_bytes {
                    relaxed_builder = relaxed_builder.flush_after_bytes(m);
                }
                relaxed_builder
            }
        };
        configured_builder.build().await
    }

    /// Manually triggers the vacuum process to reclaim space from dead data versions.
    ///
    /// This is only necessary if automatic vacuuming is not configured.
    ///
    /// # Returns
    ///
    /// A `Result` containing a tuple of `(versions_removed, keys_removed)`.
    pub async fn vacuum(&self) -> Result<(usize, usize), ()> {
        self.skiplist.vacuum().await
    }

    /// Creates a new `Handle` for interacting with the database.
    ///
    /// Handles are lightweight, single-threaded session objects. They are not
    /// `Send` or `Sync` and should not be shared across threads. Create a new
    /// handle for each task that needs to interact with the database.
    pub fn handle(&self) -> Handle<'_, K, V> {
        Handle {
            db: self,
            skiplist: &self.skiplist,
            active_tx: None,
            persistence_engine: &self.persistence_engine,
        }
    }
}

/// A handle to the database, representing a single client session.
///
/// Handles are used to perform operations like `get`, `insert`, and `remove`.
/// They can operate in two modes:
///
/// 1.  **Autocommit Mode (default):** Each operation is a small, independent transaction.
///     This is simple and safe but can be inefficient for multiple operations.
///
/// 2.  **Explicit Transaction Mode:** You can use [`Handle::transaction`], or the
///     lower-level [`Handle::begin`], [`Handle::commit`], and [`Handle::rollback`] methods to control
///     transaction boundaries manually. This allows multiple operations to be
///     grouped into a single atomic unit, providing ACID guarantees.
///
/// If a `Handle` is dropped while a transaction is active, the transaction will be
/// automatically rolled back to ensure data consistency.
pub struct Handle<'db, K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + std::hash::Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + MemSize,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned + MemSize,
{
    db: &'db Database<K, V>,
    skiplist: &'db Arc<SkipList<K, V>>,
    /// The currently active transaction, if one has been explicitly started.
    #[cfg(test)]
    pub active_tx: Option<Arc<Transaction<K, V>>>,
    #[cfg(not(test))]
    active_tx: Option<Arc<Transaction<K, V>>>,
    persistence_engine: &'db Option<Arc<PersistenceEngine<K, V>>>,
}

impl<'db, K, V> Drop for Handle<'db, K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + std::hash::Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + MemSize,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned + MemSize,
{
    /// Ensures that any active transaction is rolled back when the handle is dropped.
    fn drop(&mut self) {
        if let Some(active_tx) = self.active_tx.take() {
            let tx_manager = self.skiplist.transaction_manager();
            tx_manager.abort(&active_tx);
        }
    }
}

impl<'db, K, V> Handle<'db, K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + std::hash::Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + MemSize,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned + MemSize,
{
    /// Retrieves a value from the database.
    ///
    /// - If an explicit transaction is active, this read is part of that transaction.
    ///   It respects Read-Your-Own-Writes (RYOW), meaning it will see changes made
    ///   earlier in the same transaction.
    /// - If no transaction is active, this operation runs in its own small, autocommitted transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fluxmap::db::Database;
    /// # use std::sync::Arc;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());
    /// let handle = db.handle();
    /// handle.insert("key".to_string(), 123).await.unwrap();
    ///
    /// let value = handle.get(&"key".to_string()).unwrap();
    /// assert_eq!(*value, 123);
    ///
    /// assert!(handle.get(&"non-existent-key".to_string()).is_none());
    /// # }
    /// ```
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path ---
            // 1. Check workspace for writes made within this transaction (RYOW).
            let workspace = active_tx.workspace.read().unwrap();
            if let Some(workspace_value) = workspace.get(key) {
                return workspace_value.clone();
            }
            // 2. Not in workspace, read from skiplist using the transaction's snapshot.
            self.skiplist.get(key, active_tx)
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();
            let result = self.skiplist.get(key, &tx);
            tx_manager.commit(&tx).unwrap(); // Autocommit always succeeds
            result
        }
    }

    /// Inserts or updates a key-value pair in the database.
    ///
    /// - If an explicit transaction is active, this write is buffered in the transaction's
    ///   private workspace and will only become visible to other transactions upon `commit()`.
    /// - If no transaction is active, this operation runs in its own small, autocommitted transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fluxmap::db::Database;
    /// # use std::sync::Arc;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());
    /// let handle = db.handle();
    ///
    /// // Insert a new key
    /// handle.insert("key".to_string(), 1).await.unwrap();
    ///
    /// // Update the key
    /// handle.insert("key".to_string(), 2).await.unwrap();
    ///
    /// let value = handle.get(&"key".to_string()).unwrap();
    /// assert_eq!(*value, 2);
    /// # }
    /// ```
    pub async fn insert(&self, key: K, value: V) -> Result<(), FluxError> {
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path ---
            active_tx
                .workspace
                .write()
                .unwrap()
                .insert(key, Some(Arc::new(value)));
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();

            // For durable autocommit, we must log the operation to the WAL.
            if let Some(engine) = self.persistence_engine {
                let mut workspace = crate::transaction::Workspace::new();
                workspace.insert(key.clone(), Some(Arc::new(value.clone())));
                let mut serialized_data = Vec::new();
                ciborium::into_writer(&workspace, &mut serialized_data)
                    .map_err(|e| FluxError::Persistence(PersistenceError::Serialization(e.to_string())))?;
                if let Err(e) = engine.log(&serialized_data) {
                    tx_manager.abort(&tx);
                    return Err(e.into());
                }
            }

            // In autocommit, we write directly to the skiplist's pending versions.
            self.skiplist.insert(key, Arc::new(value), &tx).await;
            tx_manager.commit(&tx).unwrap();
            self.db.evict_if_needed().await;
        }
        Ok(())
    }

    /// Removes a key from the database.
    ///
    /// - If an explicit transaction is active, this removal is buffered in the transaction's
    ///   private workspace and will only take effect upon `commit()`.
    /// - If no transaction is active, this operation runs in its own small, autocommitted transaction.
    ///
    /// # Returns
    ///
    /// The value that was removed, if it existed. In an explicit transaction, this will
    /// return a value written earlier in the same transaction (RYOW).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fluxmap::db::Database;
    /// # use std::sync::Arc;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());
    /// let handle = db.handle();
    /// handle.insert("key".to_string(), 123).await.unwrap();
    ///
    /// let removed_value = handle.remove(&"key".to_string()).await.unwrap().unwrap();
    /// assert_eq!(*removed_value, 123);
    ///
    /// assert!(handle.get(&"key".to_string()).is_none());
    /// # }
    /// ```
    pub async fn remove(&self, key: &K) -> Result<Option<Arc<V>>, FluxError> {
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path ---
            let mut workspace = active_tx.workspace.write().unwrap();
            // Mark the key for deletion in the workspace.
            let old_val_in_workspace = workspace.insert(key.clone(), None);
            if let Some(Some(val)) = old_val_in_workspace {
                // The key was present in the workspace, return its value.
                Ok(Some(val))
            } else {
                // The key was not in the workspace, so the value to be removed
                // is the one visible in the skiplist from our snapshot.
                Ok(self.skiplist.get(key, active_tx))
            }
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();

            // For durable autocommit, we must log the operation to the WAL.
            if let Some(engine) = self.persistence_engine {
                let mut workspace: crate::transaction::Workspace<K, V> =
                    crate::transaction::Workspace::new();
                workspace.insert(key.clone(), None); // None signifies removal
                let mut serialized_data = Vec::new();
                ciborium::into_writer(&workspace, &mut serialized_data)
                    .map_err(|e| FluxError::Persistence(PersistenceError::Serialization(e.to_string())))?;
                if let Err(e) = engine.log(&serialized_data) {
                    tx_manager.abort(&tx);
                    return Err(e.into());
                }
            }

            let result = self.skiplist.remove(key, &tx).await;
            tx_manager.commit(&tx).unwrap();
            Ok(result)
        }
    }

    /// Scans a range of keys and returns the visible versions as a `Vec`.
    ///
    /// This operation is performed within the handle's active transaction if one exists,
    /// or in a new, short-lived transaction if not. All keys returned are added to the
    /// transaction's read set to ensure serializability. This method respects
    /// Read-Your-Own-Writes (RYOW).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fluxmap::db::Database;
    /// # use std::sync::Arc;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());
    /// let mut handle = db.handle();
    /// handle.insert("a".to_string(), 1).await.unwrap();
    /// handle.insert("c".to_string(), 3).await.unwrap();
    ///
    /// handle.begin().unwrap();
    /// handle.insert("b".to_string(), 2).await.unwrap(); // Uncommitted
    ///
    /// let results = handle.range(&"a".to_string(), &"c".to_string());
    /// assert_eq!(results.len(), 3); // Sees the uncommitted "b"
    /// assert_eq!(results[1].0, "b");
    /// # handle.commit().await.unwrap();
    /// # }
    /// ```
    pub fn range(&self, start: &K, end: &K) -> Vec<(K, Arc<V>)> {
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path with RYOW ---
            // TODO: Implement full range-locking to prevent phantom reads for true SSI.
            // For now, we record every key we read, which prevents some but not all anomalies.

            // 1. Get results from the persistent skiplist.
            let mut results: BTreeMap<K, Arc<V>> = self
                .skiplist
                .range(start, end, active_tx)
                .into_iter()
                .collect();

            // 2. Merge with the transaction's workspace.
            let workspace = active_tx.workspace.read().unwrap();
            for (key, value) in workspace.iter() {
                if key >= start && key <= end {
                    match value {
                        Some(v) => {
                            // Insert/update from workspace
                            results.insert(key.clone(), v.clone());
                        }
                        None => {
                            // Deletion from workspace
                            results.remove(key);
                        }
                    }
                }
            }
            results.into_iter().collect()
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();
            let results = self.skiplist.range(start, end, &tx);
            tx_manager.commit(&tx).unwrap(); // Autocommit always succeeds
            results
        }
    }

    /// Returns a stream that yields visible key-value pairs within a given range.
    ///
    /// This operation is performed within the handle's active transaction if one exists,
    /// or in a new, short-lived transaction if not. All keys yielded by the stream are
    /// added to the transaction's read set. This method respects Read-Your-Own-Writes (RYOW).
    ///
    /// **Note:** In an explicit transaction, this method currently buffers all results
    /// internally to correctly merge changes from the transaction's workspace. It does
    /// not provide true end-to-end streaming in that case.
    pub fn range_stream<'a>(
        &'a self,
        start: &'a K,
        end: &'a K,
    ) -> impl futures::stream::Stream<Item = (K, Arc<V>)> + 'a {
        async_stream::stream! {
            if self.active_tx.is_some() {
                // --- Explicit Transaction Path with RYOW ---
                // Note: This is not a true stream as it buffers results to handle RYOW.
                // A true streaming implementation would require a complex merge iterator.
                let results = self.range(start, end);
                for item in results {
                    yield item;
                }
            } else {
                // --- Autocommit Path (can be a true stream) ---
                let tx_manager = self.skiplist.transaction_manager();
                let tx = tx_manager.begin();
                let stream = self.skiplist.range_stream(start, end, &tx);
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    yield item;
                }
                tx_manager.commit(&tx).unwrap();
            }
        }
    }

    /// Begins a new transaction on this handle.
    ///
    /// After calling `begin()`, all subsequent operations on this handle will be part
    /// of a single atomic transaction until `commit()` or `rollback()` is called.
    ///
    /// Returns an error if a transaction is already active on this handle.
    pub fn begin(&mut self) -> Result<(), FluxError> {
        if self.active_tx.is_some() {
            return Err(FluxError::TransactionAlreadyActive);
        }
        let tx_manager = self.skiplist.transaction_manager();
        let tx = tx_manager.begin();
        self.active_tx = Some(tx);
        Ok(())
    }

    /// Commits the active transaction.
    ///
    /// This will attempt to apply all changes made within the transaction to the database.
    /// If the commit fails due to a conflict (`SerializationConflict`), the transaction
    /// is automatically rolled back.
    ///
    /// Returns an error if no transaction is active.
    pub async fn commit(&mut self) -> Result<(), FluxError> {
        let active_tx = self
            .active_tx
            .take()
            .ok_or(FluxError::NoActiveTransaction)?;

        // --- Phase 1: Persistence (if durable) ---
        // Log the transaction's workspace to the WAL *before* applying changes to the
        // in-memory skiplist. This ensures that if we crash after this point, recovery
        // can replay the transaction.
        if let Some(engine) = self.persistence_engine {
            let workspace = active_tx.workspace.read().unwrap();
            if !workspace.is_empty() {
                let mut serialized_data = Vec::new();
                ciborium::into_writer(&*workspace, &mut serialized_data)
                    .map_err(|e| FluxError::Persistence(PersistenceError::Serialization(e.to_string())))?;

                if let Err(e) = engine.log(&serialized_data) {
                    // If logging fails, we must abort the transaction.
                    let tx_manager = self.skiplist.transaction_manager();
                    tx_manager.abort(&active_tx);
                    return Err(e.into());
                }
            }
        }

        // --- Phase 2: In-Memory Application ---
        // Apply changes from the workspace to the actual skiplist.
        let workspace = std::mem::take(&mut *active_tx.workspace.write().unwrap());
        for (key, value) in workspace {
            match value {
                Some(val) => {
                    self.skiplist.insert(key, val, &active_tx).await;
                }
                None => {
                    self.skiplist.remove(&key, &active_tx).await;
                }
            }
        }

        // --- Phase 3: Finalize Commit ---
        // Attempt to commit via the transaction manager, which performs SSI checks.
        let tx_manager = self.skiplist.transaction_manager();
        match tx_manager.commit(&active_tx) {
            Ok(()) => {
                self.db.evict_if_needed().await;
                Ok(())
            }
            Err(e) => {
                // If commit fails, the transaction is automatically aborted by the manager.
                Err(e)
            }
        }
    }

    /// Rolls back the active transaction.
    ///
    /// This will discard all changes made within the transaction.
    /// Returns an error if no transaction is active.
    pub fn rollback(&mut self) -> Result<(), FluxError> {
        let active_tx = self
            .active_tx
            .take()
            .ok_or(FluxError::NoActiveTransaction)?;

        let tx_manager = self.skiplist.transaction_manager();
        tx_manager.abort(&active_tx);
        Ok(())
    }

    /// Executes a closure within a managed transaction.
    ///
    /// This is the recommended, high-level way to run a transaction. It automatically
    /// handles beginning, committing, and rolling back the transaction.
    ///
    /// - If the closure returns `Ok(T)`, the transaction is committed.
    /// - If the closure returns `Err(E)`, the transaction is rolled back.
    /// - If the commit itself fails (e.g., due to a `SerializationConflict`), the
    ///   transaction is rolled back and the commit error is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fluxmap::db::Database;
    /// # use fluxmap::error::FluxError;
    /// # use std::sync::Arc;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), FluxError> {
    /// # let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());
    /// let mut handle = db.handle();
    ///
    /// let result = handle.transaction(|h| Box::pin(async move {
    ///     h.insert("key1".to_string(), 100).await?;
    ///     let val = h.get(&"key1".to_string()).unwrap();
    ///     assert_eq!(*val, 100); // Read-Your-Own-Writes
    ///     Ok::<_, FluxError>("Success!")
    /// })).await?;
    ///
    /// assert_eq!(result, "Success!");
    /// assert_eq!(*handle.get(&"key1".to_string()).unwrap(), 100);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn transaction<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: for<'a> FnOnce(&'a mut Self) -> Pin<Box<dyn Future<Output = Result<T, E>> + 'a>>,
        E: From<FluxError>,
    {
        self.begin()?;

        let result = f(self).await;

        if result.is_ok() {
            // The closure succeeded, so we attempt to commit.
            match self.commit().await {
                Ok(_) => result, // Return the closure's Ok result.
                Err(commit_err) => {
                    // Commit failed, and `commit` already rolled back.
                    Err(commit_err.into())
                }
            }
        } else {
            // The closure failed, so we must roll back.
            self.rollback().unwrap_or_else(|_| {
                // This would only fail if the transaction was already gone,
                // which shouldn't happen here. We prioritize the closure's error.
            });
            result // Return the closure's Err result.
        }
    }
}

impl<'db, K, V> Handle<'db, K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + std::hash::Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + MemSize
        + Borrow<str>,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned + MemSize,
{
    /// Scans for keys starting with a given prefix and returns the visible versions as a `Vec`.
    ///
    /// This operation is performed within the handle's active transaction if one exists,
    /// or in a new, short-lived transaction if not. All keys returned are added to the
    /// transaction's read set. This method respects Read-Your-Own-Writes (RYOW).
    pub fn prefix_scan(&self, prefix: &str) -> Vec<(K, Arc<V>)> {
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path with RYOW ---
            // 1. Get results from the persistent skiplist.
            let mut results: BTreeMap<K, Arc<V>> = self
                .skiplist
                .prefix_scan(prefix, active_tx)
                .into_iter()
                .collect();

            // 2. Merge with the transaction's workspace.
            let workspace = active_tx.workspace.read().unwrap();
            for (key, value) in workspace.iter() {
                if key.borrow().starts_with(prefix) {
                    match value {
                        Some(v) => {
                            // Insert/update from workspace
                            results.insert(key.clone(), v.clone());
                        }
                        None => {
                            // Deletion from workspace
                            results.remove(key.borrow());
                        }
                    }
                }
            }
            results.into_iter().collect()
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();
            let results = self.skiplist.prefix_scan(prefix, &tx);
            tx_manager.commit(&tx).unwrap();
            results
        }
    }

    /// Returns a stream that yields visible key-value pairs for keys starting with a given prefix.
    ///
    /// This operation is performed within the handle's active transaction if one exists,
    /// or in a new, short-lived transaction if not. All keys yielded by the stream are
    /// added to the transaction's read set. This method respects Read-Your-Own-Writes (RYOW).
    ///
    /// **Note:** In an explicit transaction, this method currently buffers all results
    /// internally to correctly merge changes from the transaction's workspace. It does
    /// not provide true end-to-end streaming in that case.
    pub fn prefix_scan_stream<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl futures::stream::Stream<Item = (K, Arc<V>)> + 'a {
        async_stream::stream! {
            if self.active_tx.is_some() {
                // --- Explicit Transaction Path with RYOW ---
                // Note: This is not a true stream as it buffers results to handle RYOW.
                let results = self.prefix_scan(prefix);
                for item in results {
                    yield item;
                }
            } else {
                // --- Autocommit Path (can be a true stream) ---
                let tx_manager = self.skiplist.transaction_manager();
                let tx = tx_manager.begin();
                let stream = self.skiplist.prefix_scan_stream(prefix, &tx);
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    yield item;
                }
                tx_manager.commit(&tx).unwrap();
            }
        }
    }
}
