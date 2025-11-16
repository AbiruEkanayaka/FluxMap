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
//! let value = handle.get(&"key1".to_string()).unwrap().unwrap();
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
//!     let alice_balance = h.get(&"alice".to_string()).unwrap().unwrap();
//!     let bob_balance = h.get(&"bob".to_string()).unwrap().unwrap();
//!
//!     h.insert("alice".to_string(), *alice_balance - 20).await?;
//!     h.insert("bob".to_string(), *bob_balance + 20).await?;
//!
//!     Ok::<_, FluxError>(()) // Commit the changes
//! })).await?;
//!
//! let final_alice = handle.get(&"alice".to_string()).unwrap().unwrap();
//! let final_bob = handle.get(&"bob".to_string()).unwrap().unwrap();
//!
//! assert_eq!(*final_alice, 80);
//! assert_eq!(*final_bob, 70);
//! # Ok(())
//! # }
//! ```

use crate::arc::ArcManager;
use crate::error::{FluxError, PersistenceError};
use crate::mem::{EvictionPolicy, MemSize};
use crate::persistence::{DurabilityLevel, PersistenceEngine, PersistenceOptions};
use crate::SkipList;
use crate::transaction::Transaction;
use futures::stream::StreamExt;
use log::error;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Options for configuring the automatic vacuuming process.
#[derive(Debug, Clone, Copy)]
pub struct VacuumOptions {
    /// The interval at which the vacuum process runs to clean up dead data versions.
    pub interval: Duration,
}

/// Contains information about the database's memory usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryInfo {
    /// The configured memory limit in bytes, if any.
    pub max_bytes: Option<u64>,
    /// The estimated current memory usage in bytes.
    pub current_bytes: u64,
}

/// Contains metadata about a specific key in the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMetadata<K> {
    pub key: K,
    pub last_accessed: u64,
    pub access_count: u64,
    pub size_bytes: u64,
}

// --- Builder Typestates ---

/// A marker trait for the states of the `DatabaseBuilder`.
pub trait BuilderState {}

/// The initial state of the builder. Can build an in-memory database or configure durability.
#[derive(Debug, Default)]
pub struct Initial;
impl BuilderState for Initial {}

/// State after `durability_relaxed` is called. Requires a flush condition to be buildable.
#[derive(Debug, Default)]
pub struct RelaxedDurability;
impl BuilderState for RelaxedDurability {}

/// A state where the configuration is valid and `build()` can be called.
#[derive(Debug, Default)]
pub struct Buildable;
impl BuilderState for Buildable {}

/// A builder for creating a [`Database`] instance with custom configurations.
///
/// It uses a typestate pattern to ensure valid configurations at compile time.
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
/// // A durable database with custom WAL settings, auto-vacuuming, and a memory limit.
/// let db = Database::<String, String>::builder()
///     .durability_full(PersistenceOptions {
///         wal_path,
///         wal_pool_size: 8,
///         wal_segment_size_bytes: 16 * 1024 * 1024, // 16MB
///     })
///     .auto_vacuum(VacuumOptions {
///         interval: Duration::from_secs(30),
///     })
///     .max_memory(512 * 1024 * 1024) // Set a 512MB memory limit
///     .build()
///     .await
///     .unwrap();
/// # }
/// ```
pub struct DatabaseBuilder<K, V, S: BuilderState = Initial> {
    vacuum: Option<VacuumOptions>,
    persistence_options: Option<PersistenceOptions>,
    is_full_durability: bool,
    flush_interval: Option<Duration>,
    flush_after_n_commits: Option<usize>,
    flush_after_m_bytes: Option<u64>,
    max_memory_bytes: Option<u64>,
    eviction_policy: EvictionPolicy,
    p_factor: Option<f64>,
    _phantom: PhantomData<(K, V, S)>,
}

/// Methods available on the builder in any state.
impl<K, V, S: BuilderState> DatabaseBuilder<K, V, S> {
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
    /// begin evicting keys to stay under the limit.
    pub fn max_memory(mut self, bytes: u64) -> Self {
        self.max_memory_bytes = Some(bytes);
        self
    }

    /// Sets the memory eviction policy.
    ///
    /// Defaults to `EvictionPolicy::Manual`.
    pub fn eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    /// Sets the skiplist probability factor `p`.
    ///
    /// This value, between 0.0 and 1.0, determines the probability of a node
    /// in the skiplist having a higher level. A higher `p` results in a taller
    /// skiplist, which can speed up searches but uses more memory.
    ///
    /// Defaults to `0.5`.
    pub fn skiplist_p(mut self, p: f64) -> Self {
        self.p_factor = Some(p);
        self
    }

    /// A helper function to transition the builder to a new state.
    fn transition<NewState: BuilderState>(self) -> DatabaseBuilder<K, V, NewState> {
        DatabaseBuilder {
            vacuum: self.vacuum,
            persistence_options: self.persistence_options,
            is_full_durability: self.is_full_durability,
            flush_interval: self.flush_interval,
            flush_after_n_commits: self.flush_after_n_commits,
            flush_after_m_bytes: self.flush_after_m_bytes,
            max_memory_bytes: self.max_memory_bytes,
            eviction_policy: self.eviction_policy,
            p_factor: self.p_factor,
            _phantom: PhantomData,
        }
    }
}

/// Methods available only on the initial builder state.
impl<K, V> DatabaseBuilder<K, V, Initial>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + Hash
        + Eq
        + Serialize
        + for<'de> Deserialize<'de>
        + MemSize
        + Borrow<str>,
    V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de> + MemSize,
{
    /// Configures the database for full durability (`fsync` per transaction).
    /// This makes the builder ready to build.
    pub fn durability_full(mut self, options: PersistenceOptions) -> DatabaseBuilder<K, V, Buildable> {
        self.persistence_options = Some(options);
        self.is_full_durability = true;
        self.transition()
    }

    /// Configures the database for relaxed durability (group commit).
    ///
    /// After calling this, you must chain it with at least one flush condition
    /// (`flush_interval`, `flush_after_commits`, or `flush_after_bytes`)
    /// before you can build.
    pub fn durability_relaxed(
        mut self,
        options: PersistenceOptions,
    ) -> DatabaseBuilder<K, V, RelaxedDurability> {
        self.persistence_options = Some(options);
        self.is_full_durability = false;
        self.transition()
    }

    /// Builds an in-memory `Database` instance.
    pub async fn build(self) -> Result<Database<K, V>, FluxError> {
        build_internal(self, DurabilityLevel::InMemory).await
    }
}

/// Methods available only when relaxed durability is configured but no flush condition is set.
impl<K, V> DatabaseBuilder<K, V, RelaxedDurability> {
    /// Sets the time-based flush condition for relaxed durability.
    /// This makes the builder ready to build.
    pub fn flush_interval(mut self, interval: Duration) -> DatabaseBuilder<K, V, Buildable> {
        self.flush_interval = Some(interval);
        self.transition()
    }

    /// Sets the commit-based flush condition for relaxed durability.
    /// This makes the builder ready to build.
    pub fn flush_after_commits(mut self, n: usize) -> DatabaseBuilder<K, V, Buildable> {
        self.flush_after_n_commits = Some(n);
        self.transition()
    }

    /// Sets the byte-based flush condition for relaxed durability.
    /// This makes the builder ready to build.
    pub fn flush_after_bytes(mut self, m: u64) -> DatabaseBuilder<K, V, Buildable> {
        self.flush_after_m_bytes = Some(m);
        self.transition()
    }
}

/// Methods available on a fully configured, buildable builder.
impl<K, V> DatabaseBuilder<K, V, Buildable>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + Hash
        + Eq
        + Serialize
        + for<'de> Deserialize<'de>
        + MemSize
        + Borrow<str>,
    V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de> + MemSize,
{
    /// Sets or adds a time-based flush condition for relaxed durability.
    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = Some(interval);
        self
    }

    /// Sets or adds a commit-based flush condition for relaxed durability.
    pub fn flush_after_commits(mut self, n: usize) -> Self {
        self.flush_after_n_commits = Some(n);
        self
    }

    /// Sets or adds a byte-based flush condition for relaxed durability.
    pub fn flush_after_bytes(mut self, m: u64) -> Self {
        self.flush_after_m_bytes = Some(m);
        self
    }

    /// Builds the `Database` instance with the specified configurations.
    pub async fn build(self) -> Result<Database<K, V>, FluxError> {
        let durability = if self.is_full_durability {
            DurabilityLevel::Full {
                options: self.persistence_options.clone().unwrap(), // Safe due to typestate
            }
        } else {
            DurabilityLevel::Relaxed {
                options: self.persistence_options.clone().unwrap(), // Safe due to typestate
                flush_interval_ms: self.flush_interval.map(|d| d.as_millis() as u64),
                flush_after_n_commits: self.flush_after_n_commits,
                flush_after_m_bytes: self.flush_after_m_bytes,
            }
        };
        build_internal(self, durability).await
    }
}

/// Internal build logic shared by all buildable states.
async fn build_internal<K, V, S: BuilderState>(
    builder: DatabaseBuilder<K, V, S>,
    durability: DurabilityLevel,
) -> Result<Database<K, V>, FluxError>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + Hash
        + Eq
        + Serialize
        + for<'de> Deserialize<'de>
        + MemSize
        + Borrow<str>,
    V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de> + MemSize,
{
    if builder.eviction_policy != EvictionPolicy::Manual && builder.max_memory_bytes.is_none() {
        return Err(FluxError::Configuration(
            "An automatic eviction policy requires max_memory to be set.".to_string(),
        ));
    }

    let fatal_error = Arc::new(Mutex::new(None));

    let persistence_engine = PersistenceEngine::new(durability, fatal_error.clone())?.map(Arc::new);

    let current_memory_bytes = Arc::new(AtomicU64::new(0));
    let access_clock = Arc::new(AtomicU64::new(0));

    let skiplist = if let Some(engine) = &persistence_engine {
        Arc::new(
            engine
                .recover(
                    current_memory_bytes.clone(),
                    access_clock.clone(),
                    builder.p_factor,
                )
                .await?,
        )
    } else {
        let list = match builder.p_factor {
            Some(p) => SkipList::with_p(p, current_memory_bytes.clone(), access_clock.clone()),
            None => SkipList::new(current_memory_bytes.clone(), access_clock.clone()),
        };
        Arc::new(list)
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let vacuum_handle = if let Some(vacuum_opts) = builder.vacuum {
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
                        error!("Automatic vacuuming failed: {:?}", e);
                    }
                }
            });
        });
        Some(handle)
    } else {
        None
    };

    let arc_manager = if builder.eviction_policy == EvictionPolicy::Arc {
        // The check at the start of the function ensures this unwrap is safe.
        let max_mem = builder.max_memory_bytes.unwrap();
        Some(Arc::new(Mutex::new(ArcManager::new(max_mem))))
    } else {
        None
    };

    Ok(Database {
        skiplist,
        persistence_engine,
        _vacuum_handle: vacuum_handle,
        shutdown,
        max_memory_bytes: builder.max_memory_bytes,
        current_memory_bytes,
        eviction_policy: builder.eviction_policy,
        arc_manager,
        fatal_error,
    })
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
        + Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + MemSize
        + Borrow<str>,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned + MemSize,
{
    #[cfg(test)]
    pub skiplist: Arc<SkipList<K, V>>,
    #[cfg(not(test))]
    skiplist: Arc<SkipList<K, V>>,
    persistence_engine: Option<Arc<PersistenceEngine<K, V>>>,
    _vacuum_handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
    /// The configured memory limit in bytes.
    max_memory_bytes: Option<u64>,
    /// An atomic counter for the estimated current memory usage.
    current_memory_bytes: Arc<AtomicU64>,
    /// The configured memory eviction policy.
    eviction_policy: EvictionPolicy,
    /// The manager for the ARC eviction policy, if enabled.
    arc_manager: Option<Arc<Mutex<ArcManager<K>>>>,
    /// A container for a fatal error message from a background thread.
    fatal_error: Arc<Mutex<Option<String>>>,
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
        + MemSize
        + Borrow<str>,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned + MemSize,
{
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self._vacuum_handle.take() {
            if let Err(e) = handle.join() {
                error!("Automatic vacuum thread panicked: {:?}", e);
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
        + MemSize
        + Borrow<str>,
    V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de> + MemSize,
{
    /// Checks if memory usage exceeds the configured limit and evicts keys until it's under the limit.
    async fn evict_if_needed(&self, spare_key: Option<&K>) {
        if self.eviction_policy == EvictionPolicy::Manual {
            return; // Manual policy means the user is responsible for eviction.
        }
        const EVICTION_BATCH_SIZE: usize = 10;

        if let Some(max_mem) = self.max_memory_bytes {
            // Loop until memory is under the limit.
            while self.current_memory_bytes.load(Ordering::Relaxed) > max_mem {
                // For ARC, its state changes with each eviction, so we must evict one-by-one.
                if self.eviction_policy == EvictionPolicy::Arc {
                    if self.evict_one(spare_key).await.is_err() {
                        break; // Stop if we can't find/evict a victim.
                    }
                    continue; // Continue the while loop to check memory again.
                }

                // For other policies, fetch a batch of candidates.
                let victim_candidates = self.skiplist.find_victim_keys(
                    self.eviction_policy,
                    spare_key,
                    EVICTION_BATCH_SIZE,
                );

                if victim_candidates.is_empty() {
                    break; // No more victims can be found.
                }

                // Evict from the batch one by one until we are under the limit.
                let mut evicted_in_batch = false;
                for victim_key in victim_candidates {
                    if self.skiplist.evict(&victim_key).is_some() {
                        evicted_in_batch = true;
                    }
                    // After each eviction in the batch, check if we're done.
                    if self.current_memory_bytes.load(Ordering::Relaxed) <= max_mem {
                        break;
                    }
                }

                // If we couldn't evict anything from the candidate batch, break to avoid an infinite loop.
                if !evicted_in_batch {
                    break;
                }
            }
        }
    }

    /// Finds and evicts a single victim based on the configured eviction policy.
    async fn evict_one(&self, spare_key: Option<&K>) -> Result<(), FluxError> {
        let victim_key = if let Some(manager) = &self.arc_manager {
            manager.lock().unwrap().find_victim()
        } else {
            // Call the new batch method to get a single victim.
            self.skiplist
                .find_victim_keys(self.eviction_policy, spare_key, 1)
                .into_iter()
                .next()
        };

        if let Some(victim_key) = victim_key {
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

    /// Creates a new `DatabaseBuilder` to configure and build a `Database`.
    pub fn builder() -> DatabaseBuilder<K, V, Initial> {
        DatabaseBuilder {
            vacuum: None,
            persistence_options: None,
            is_full_durability: false,
            flush_interval: None,
            flush_after_n_commits: None,
            flush_after_m_bytes: None,
            max_memory_bytes: None,
            eviction_policy: EvictionPolicy::default(),
            p_factor: None,
            _phantom: PhantomData,
        }
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
    pub async fn new(config: DurabilityLevel) -> Result<Self, FluxError>
    where
        K: Ord
            + Clone
            + Send
            + Sync
            + 'static
            + Hash
            + Eq
            + Serialize
            + for<'de> Deserialize<'de>
            + MemSize
            + Borrow<str>,
        V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de> + MemSize,
    {
        match config {
            DurabilityLevel::InMemory => Database::builder().build().await,
            DurabilityLevel::Full { options } => Database::builder().durability_full(options).build().await,
            DurabilityLevel::Relaxed {
                options,
                flush_interval_ms,
                flush_after_n_commits,
                flush_after_m_bytes,
            } => {
                let relaxed_builder = Database::builder().durability_relaxed(options);

                if let Some(interval) = flush_interval_ms {
                    let mut buildable =
                        relaxed_builder.flush_interval(Duration::from_millis(interval));
                    if let Some(n) = flush_after_n_commits {
                        buildable = buildable.flush_after_commits(n);
                    }
                    if let Some(m) = flush_after_m_bytes {
                        buildable = buildable.flush_after_bytes(m);
                    }
                    buildable.build().await
                } else if let Some(n) = flush_after_n_commits {
                    let mut buildable = relaxed_builder.flush_after_commits(n);
                    if let Some(m) = flush_after_m_bytes {
                        buildable = buildable.flush_after_bytes(m);
                    }
                    buildable.build().await
                } else if let Some(m) = flush_after_m_bytes {
                    relaxed_builder.flush_after_bytes(m).build().await
                } else {
                    Err(FluxError::Configuration("Relaxed durability mode requires at least one flush condition (interval, commits, or bytes).".to_string()))
                }
            }
        }
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

    /// Returns information about the database's current memory usage.
    ///
    /// # Examples
    /// ```
    /// # use fluxmap::db::Database;
    /// # #[tokio::main]
    /// # async fn main() {
    /// let db = Database::<String, String>::builder().max_memory(1024).build().await.unwrap();
    /// let info = db.memory_info();
    /// assert_eq!(info.max_bytes, Some(1024));
    /// # }
    /// ```
    pub fn memory_info(&self) -> MemoryInfo {
        MemoryInfo {
            max_bytes: self.max_memory_bytes,
            current_bytes: self.current_memory_bytes.load(Ordering::Relaxed),
        }
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
            arc_manager: &self.arc_manager,
            fatal_error: &self.fatal_error,
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
        + Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + MemSize
        + Borrow<str>,
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
    arc_manager: &'db Option<Arc<Mutex<ArcManager<K>>>>,
    fatal_error: &'db Arc<Mutex<Option<String>>>,
}

impl<'db, K, V> Drop for Handle<'db, K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + MemSize
        + Borrow<str>,
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
        + Hash
        + Eq
        + Serialize
        + DeserializeOwned
        + MemSize
        + Borrow<str>,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned + MemSize,
{
    /// Manually evicts a key from the database.
    ///
    /// This is primarily useful when using an `EvictionPolicy::Manual` policy,
    /// allowing the application to control eviction directly.
    ///
    /// Note that this performs a logical deletion. The memory will be reclaimed
    /// by the next vacuum cycle. The database's internal memory counter is updated
    /// immediately.
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if the key was found and evicted, `Ok(false)` otherwise.
    pub async fn evict(&self, key: &K) -> Result<bool, FluxError> {
        self.check_fatal_error()?;
        if self.db.skiplist.evict(key).is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns a stream that yields metadata for every key in the database.
    ///
    /// This is intended for use with `EvictionPolicy::Manual`, allowing an application
    /// to inspect key metadata (like access time, frequency, and size) to decide
    /// which keys to evict.
    ///
    /// **Warning:** This scans the entire database and can be a slow operation on
    /// large datasets.
    pub fn scan_metadata<'a>(&'a self) -> impl futures::stream::Stream<Item = KeyMetadata<K>> + 'a {
        self.skiplist.scan_metadata()
    }

    /// Checks if a fatal error has occurred in a background thread.
    fn check_fatal_error(&self) -> Result<(), FluxError> {
        if let Some(err_msg) = self.fatal_error.lock().unwrap().as_ref() {
            return Err(FluxError::FatalPersistenceError(err_msg.clone()));
        }
        Ok(())
    }

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
    /// let value = handle.get(&"key".to_string()).unwrap().unwrap();
    /// assert_eq!(*value, 123);
    ///
    /// assert!(handle.get(&"non-existent-key".to_string()).unwrap().is_none());
    /// # }
    /// ```
    pub fn get(&self, key: &K) -> Result<Option<Arc<V>>, FluxError> {
        self.check_fatal_error()?;
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path ---
            // 1. Check workspace for writes made within this transaction (RYOW).
            let workspace = active_tx.workspace.read().unwrap();
            if let Some(workspace_value) = workspace.get(key.borrow()) {
                return Ok(workspace_value.clone());
            }
            // 2. Not in workspace, read from skiplist using the transaction's snapshot.
            Ok(self.skiplist.get(key, active_tx))
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();
            let result = self.skiplist.get(key, &tx);
            if let (Some(manager), Some(_)) = (&self.arc_manager, &result) {
                manager.lock().unwrap().hit(key);
            }
            tx_manager.commit(&tx, || Ok(())).unwrap(); // Autocommit for reads cannot fail.
            Ok(result)
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
    /// let value = handle.get(&"key".to_string()).unwrap().unwrap();
    /// assert_eq!(*value, 2);
    /// # }
    /// ```
    pub async fn insert(&self, key: K, value: V) -> Result<(), FluxError> {
        self.check_fatal_error()?;
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path ---
            active_tx
                .workspace
                .write()
                .unwrap()
                .insert(key, Some(Arc::new(value)));
            Ok(())
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();

            let key_for_eviction = key.clone();
            let allocated_size = self
                .skiplist
                .insert(key.clone(), Arc::new(value.clone()), &tx)
                .await;

            // Define the pre-commit hook for WAL logging.
            let on_pre_commit = || {
                if let Some(engine) = self.persistence_engine {
                    let mut workspace = crate::transaction::Workspace::new();
                    workspace.insert(key, Some(Arc::new(value)));
                    let mut serialized_data = Vec::new();
                    ciborium::into_writer(&workspace, &mut serialized_data).map_err(|e| {
                        FluxError::Persistence(PersistenceError::Serialization(e.to_string()))
                    })?;
                    engine.log(&serialized_data)?;
                }
                Ok(())
            };

            // Attempt to commit with the WAL hook.
            match tx_manager.commit(&tx, on_pre_commit) {
                Ok(()) => {
                    if let Some(manager) = &self.arc_manager {
                        manager
                            .lock()
                            .unwrap()
                            .miss(key_for_eviction.clone(), allocated_size as usize);
                    }

                    if self.db.eviction_policy == EvictionPolicy::Manual {
                        if let Some(max_mem) = self.db.max_memory_bytes {
                            if self.db.current_memory_bytes.load(Ordering::Relaxed) > max_mem {
                                return Err(FluxError::MemoryLimitExceeded);
                            }
                        }
                    } else {
                        self.db.evict_if_needed(Some(&key_for_eviction)).await;
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
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
    /// assert!(handle.get(&"key".to_string()).unwrap().is_none());
    /// # }
    /// ```
    pub async fn remove(&self, key: &K) -> Result<Option<Arc<V>>, FluxError> {
        self.check_fatal_error()?;
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

            // Provisionally apply the remove.
            let result = self.skiplist.remove(key, &tx).await;

            // Define the pre-commit hook for WAL logging.
            let key_clone = key.clone();
            let on_pre_commit = || {
                if let Some(engine) = self.persistence_engine {
                    let mut workspace: crate::transaction::Workspace<K, V> =
                        crate::transaction::Workspace::new();
                    workspace.insert(key_clone, None); // None signifies removal
                    let mut serialized_data = Vec::new();
                    ciborium::into_writer(&workspace, &mut serialized_data).map_err(|e| {
                        FluxError::Persistence(PersistenceError::Serialization(e.to_string()))
                    })?;
                    engine.log(&serialized_data)?;
                }
                Ok(())
            };

            // Attempt to commit with the WAL hook.
            match tx_manager.commit(&tx, on_pre_commit) {
                Ok(()) => Ok(result),
                Err(e) => Err(e),
            }
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
    /// let results = handle.range(&"a".to_string(), &"c".to_string()).unwrap();
    /// assert_eq!(results.len(), 3); // Sees the uncommitted "b"
    /// assert_eq!(results[1].0, "b");
    /// # handle.commit().await.unwrap();
    /// # }
    /// ```
    pub fn range(&self, start: &K, end: &K) -> Result<Vec<(K, Arc<V>)>, FluxError> {
        self.check_fatal_error()?;
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path with RYOW ---
            active_tx
                .range_scans
                .write()
                .unwrap()
                .push((start.clone(), end.clone()));

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
                            results.remove(key.borrow());
                        }
                    }
                }
            }
            Ok(results.into_iter().collect())
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();
            let results = self.skiplist.range(start, end, &tx);
            tx_manager.commit(&tx, || Ok(())).unwrap(); // Autocommit for reads cannot fail.
            Ok(results)
        }
    }

    /// Returns a stream that yields visible key-value pairs within a given range.
    ///
    /// This operation is performed within the handle's active transaction if one exists,
    /// or in a new, short-lived transaction if not. All keys yielded by the stream are
    /// added to the transaction's read set. This method respects Read-Your-Own-Writes (RYOW)
    /// by merging changes from the transaction's workspace on-the-fly.
    pub fn range_stream<'a>(
        &'a self,
        start: &'a K,
        end: &'a K,
    ) -> impl futures::stream::Stream<Item = Result<(K, Arc<V>), FluxError>> + 'a {
        async_stream::stream! {
            if let Err(e) = self.check_fatal_error() {
                yield Err(e);
                return;
            }

            if let Some(active_tx) = &self.active_tx {
                // --- Explicit Transaction Path with RYOW (True Streaming Merge) ---
                active_tx
                    .range_scans
                    .write()
                    .unwrap()
                    .push((start.clone(), end.clone()));

                let skiplist_stream = self.skiplist.range_stream(start, end, active_tx).peekable();
                futures::pin_mut!(skiplist_stream);

                // Prepare sorted workspace items within the range
                let workspace = active_tx.workspace.read().unwrap();
                let mut workspace_items: BTreeMap<K, Option<Arc<V>>> = workspace
                    .iter()
                    .filter(|(k, _)| *k >= start && *k <= end)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                loop {
                    let skiplist_peek = skiplist_stream.as_mut().peek().await;

                    match skiplist_peek {
                        Some((skiplist_key, _)) => {
                            let workspace_key = workspace_items.keys().next().cloned();

                            if let Some(wk) = workspace_key {
                                if skiplist_key < &wk {
                                    // Skiplist item is smaller, yield it.
                                    let (key, value) = skiplist_stream.as_mut().next().await.unwrap();
                                    yield Ok((key, value));
                                } else if skiplist_key > &wk {
                                    // Workspace item is smaller, yield it (if not a delete).
                                    let (key, value_opt) = workspace_items.pop_first().unwrap();
                                    if let Some(value) = value_opt {
                                        yield Ok((key, value));
                                    }
                                } else { // Keys are equal
                                    // Workspace overrides. Yield workspace item (if not a delete).
                                    let (key, value_opt) = workspace_items.pop_first().unwrap();
                                    if let Some(value) = value_opt {
                                        yield Ok((key, value));
                                    }
                                    // And advance the skiplist stream, discarding its value.
                                    let _ = skiplist_stream.as_mut().next().await;
                                }
                            } else {
                                // Workspace is empty, drain the rest of the skiplist stream.
                                while let Some((key, value)) = skiplist_stream.as_mut().next().await {
                                    yield Ok((key, value));
                                }
                                break;
                            }
                        }
                        None => {
                            // Skiplist stream is empty, drain the rest of the workspace.
                            for (key, value_opt) in workspace_items {
                                if let Some(value) = value_opt {
                                    yield Ok((key, value));
                                }
                            }
                            break;
                        }
                    }
                }
            } else {
                // --- Autocommit Path (can be a true stream) ---
                let tx_manager = self.skiplist.transaction_manager();
                let tx = tx_manager.begin();
                let stream = self.skiplist.range_stream(start, end, &tx);
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    yield Ok(item);
                }
                tx_manager.commit(&tx, || Ok(())).unwrap();
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
        self.check_fatal_error()?;
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
        self.check_fatal_error()?;
        let active_tx = self
            .active_tx
            .take()
            .ok_or(FluxError::NoActiveTransaction)?;

        // --- Phase 1: In-Memory Application ---
        // Apply changes from the workspace to the actual skiplist.
        // This is provisional; the versions are not visible until the transaction status is Committed.
        let workspace = active_tx.workspace.read().unwrap().clone();
        for (key, value) in workspace.iter() {
            match value {
                Some(val) => {
                    self.skiplist
                        .insert(key.clone(), val.clone(), &active_tx)
                        .await;
                }
                None => {
                    self.skiplist.remove(key, &active_tx).await;
                }
            }
        }

        // --- Phase 2: Finalize Commit (with WAL write hook) ---
        // The WAL write is wrapped in a closure and passed to the transaction manager.
        // It will only be executed if the SSI checks pass.
        let tx_manager = self.skiplist.transaction_manager();
        let persistence_engine = self.persistence_engine;

        let on_pre_commit = || {
            if let Some(engine) = persistence_engine {
                if !workspace.is_empty() {
                    let mut serialized_data = Vec::new();
                    ciborium::into_writer(&workspace, &mut serialized_data).map_err(|e| {
                        FluxError::Persistence(PersistenceError::Serialization(e.to_string()))
                    })?;

                    engine.log(&serialized_data)?;
                }
            }
            Ok(())
        };

        match tx_manager.commit(&active_tx, on_pre_commit) {
            Ok(()) => {
                // For explicit transactions, we don't spare any specific key,
                // as multiple keys could have been inserted.
                if self.db.eviction_policy == EvictionPolicy::Manual {
                    if let Some(max_mem) = self.db.max_memory_bytes {
                        if self.db.current_memory_bytes.load(Ordering::Relaxed) > max_mem {
                            return Err(FluxError::MemoryLimitExceeded);
                        }
                    }
                } else {
                    self.db.evict_if_needed(None).await;
                }
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
        self.check_fatal_error()?;
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
    ///     let val = h.get(&"key1".to_string()).unwrap().unwrap();
    ///     assert_eq!(*val, 100); // Read-Your-Own-Writes
    ///     Ok::<_, FluxError>("Success!")
    /// })).await?;
    ///
    /// assert_eq!(result, "Success!");
    /// assert_eq!(*handle.get(&"key1".to_string()).unwrap().unwrap(), 100);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn transaction<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: for<'a> FnOnce(&'a mut Self) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>,
        E: From<FluxError>,
    {
        self.check_fatal_error()?;
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

    /// Scans for keys starting with a given prefix and returns the visible versions as a `Vec`.
    ///
    /// This operation is performed within the handle's active transaction if one exists,
    /// or in a new, short-lived transaction if not. All keys returned are added to the
    /// transaction's read set. This method respects Read-Your-Own-Writes (RYOW).
    pub fn prefix_scan(&self, prefix: &str) -> Result<Vec<(K, Arc<V>)>, FluxError> {
        self.check_fatal_error()?;
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path with RYOW ---
            active_tx
                .prefix_scans
                .write()
                .unwrap()
                .push(prefix.to_string());

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
            Ok(results.into_iter().collect())
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();
            let results = self.skiplist.prefix_scan(prefix, &tx);
            tx_manager.commit(&tx, || Ok(())).unwrap();
            Ok(results)
        }
    }

    /// Returns a stream that yields visible key-value pairs for keys starting with a given prefix.
    ///
    /// This operation is performed within the handle's active transaction if one exists,
    /// or in a new, short-lived transaction if not. All keys yielded by the stream are
    /// added to the transaction's read set. This method respects Read-Your-Own-Writes (RYOW)
    /// by merging changes from the transaction's workspace on-the-fly.
    pub fn prefix_scan_stream<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl futures::stream::Stream<Item = Result<(K, Arc<V>), FluxError>> + 'a {
        async_stream::stream! {
            if let Err(e) = self.check_fatal_error() {
                yield Err(e);
                return;
            }

            if let Some(active_tx) = &self.active_tx {
                // --- Explicit Transaction Path with RYOW (True Streaming Merge) ---
                active_tx
                    .prefix_scans
                    .write()
                    .unwrap()
                    .push(prefix.to_string());

                let skiplist_stream = self.skiplist.prefix_scan_stream(prefix, active_tx).peekable();
                futures::pin_mut!(skiplist_stream);

                // Prepare sorted workspace items matching the prefix
                let workspace = active_tx.workspace.read().unwrap();
                let mut workspace_items: BTreeMap<K, Option<Arc<V>>> = workspace
                    .iter()
                    .filter(|(k, _)| <K as Borrow<str>>::borrow(k).starts_with(prefix))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                loop {
                    let skiplist_peek = skiplist_stream.as_mut().peek().await;

                    match skiplist_peek {
                        Some((skiplist_key, _)) => {
                            let workspace_key = workspace_items.keys().next().cloned();

                            if let Some(wk) = workspace_key {
                                if skiplist_key < &wk {
                                    // Skiplist item is smaller, yield it.
                                    let (key, value) = skiplist_stream.as_mut().next().await.unwrap();
                                    yield Ok((key, value));
                                } else if skiplist_key > &wk {
                                    // Workspace item is smaller, yield it (if not a delete).
                                    let (key, value_opt) = workspace_items.pop_first().unwrap();
                                    if let Some(value) = value_opt {
                                        yield Ok((key, value));
                                    }
                                } else { // Keys are equal
                                    // Workspace overrides. Yield workspace item (if not a delete).
                                    let (key, value_opt) = workspace_items.pop_first().unwrap();
                                    if let Some(value) = value_opt {
                                        yield Ok((key, value));
                                    }
                                    // And advance the skiplist stream, discarding its value.
                                    let _ = skiplist_stream.as_mut().next().await;
                                }
                            } else {
                                // Workspace is empty, drain the rest of the skiplist stream.
                                while let Some((key, value)) = skiplist_stream.as_mut().next().await {
                                    yield Ok((key, value));
                                }
                                break;
                            }
                        }
                        None => {
                            // Skiplist stream is empty, drain the rest of the workspace.
                            for (key, value_opt) in workspace_items {
                                if let Some(value) = value_opt {
                                    yield Ok((key, value));
                                }
                            }
                            break;
                        }
                    }
                }
            } else {
                // --- Autocommit Path (can be a true stream) ---
                let tx_manager = self.skiplist.transaction_manager();
                let tx = tx_manager.begin();
                let stream = self.skiplist.prefix_scan_stream(prefix, &tx);
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    yield Ok(item);
                }
                tx_manager.commit(&tx, || Ok(())).unwrap();
            }
        }
    }
}
