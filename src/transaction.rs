//! Manages transactions, snapshots, and versions for MVCC.
//!
//! This module is the heart of FluxMap's concurrency control system. It implements
//! a Serializable Snapshot Isolation (SSI) protocol, which provides the highest
//! level of isolation defined by the SQL standard, preventing all common read/write
//! anomalies, including write skew.
//!
//! # Core Components
//!
//! -   **`TransactionManager`**: A global service that creates new transactions,
//!     tracks their status (`Active`, `Committed`, `Aborted`), and validates
//!     them upon commit.
//!
//! -   **`Transaction`**: Represents a single, isolated operation. It holds a
//!     `Snapshot` of the database at the time it began, and it tracks its own
//!     reads and writes in `read_set` and `write_set` to detect conflicts.
//!     Uncommitted writes are stored in a private `workspace`.
//!
//! -   **`Snapshot`**: An immutable view of the database state at a single point
//!     in time. It contains the information needed to determine which data
//!     versions are "visible" to its transaction.
//!
//! -   **`Version`**: A single version of a value, containing the value itself,
//!     the ID of the transaction that created it (`creator_txid`), and the ID
//!     of the transaction that expired it (`expirer_txid`).

use crate::error::FluxError;
use dashmap::{DashMap, DashSet};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// --- Core MVCC Types ---

/// A unique identifier for a transaction.
pub type TxId = u64;

/// The status of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// The transaction is currently in progress.
    Active,
    /// The transaction has successfully committed.
    Committed,
    /// The transaction has been aborted and its changes were discarded.
    Aborted,
}

/// Represents a transaction's private workspace for uncommitted changes.
///
/// A `Some(Arc<V>)` indicates an insertion or update, while `None` indicates a deletion.
pub type Workspace<K, V> = HashMap<K, Option<Arc<V>>>;

/// Manages the lifecycle and status of all transactions in the system.
///
/// This is the central authority for creating transactions, managing their
/// state, and ensuring serializability by detecting and resolving conflicts.
pub struct TransactionManager<K: Eq + std::hash::Hash, V> {
    /// The next transaction ID to be allocated.
    next_txid: AtomicU64,
    /// A map tracking the status of every transaction (Active, Committed, or Aborted).
    statuses: DashMap<TxId, TransactionStatus>,
    /// A map of currently active transactions.
    active_transactions: DashMap<TxId, Arc<Transaction<K, V>>>,
    /// The minimum transaction ID that must be kept in the `statuses` map.
    /// This is used by the vacuum process to prune old transaction statuses.
    pub min_retainable_txid: AtomicU64,
    /// Tracks which active transactions have read which keys.
    /// This is a key part of the SSI conflict detection mechanism.
    /// `DashMap<Key, DashSet<TxId>>`
    pub read_trackers: DashMap<K, DashSet<TxId>>,
    _phantom: std::marker::PhantomData<(K, V)>,
}

// ...

impl<K, V> TransactionManager<K, V>
where
    K: Eq + std::hash::Hash + Clone + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
{
    /// Creates a new `TransactionManager`.
    pub fn new() -> Self {
        // TXID 0 is reserved for pre-existing data from snapshots.
        Self {
            next_txid: AtomicU64::new(1),
            statuses: DashMap::new(),
            active_transactions: DashMap::new(),
            min_retainable_txid: AtomicU64::new(0), // Initialize to 0
            read_trackers: DashMap::new(),          // Initialize new field
            _phantom: std::marker::PhantomData,
        }
    }

    /// Allocates a new, unique transaction ID.
    pub fn new_txid(&self) -> TxId {
        self.next_txid.fetch_add(1, Ordering::SeqCst)
    }

    /// Begins a new transaction.
    ///
    /// This creates a new transaction ID, generates a snapshot for it,
    /// and registers it as active.
    pub fn begin(&self) -> Arc<Transaction<K, V>> {
        let txid = self.new_txid();
        self.statuses.insert(txid, TransactionStatus::Active);
        let snapshot = self.create_snapshot(txid);
        let tx = Arc::new(Transaction::new(txid, snapshot));
        self.active_transactions.insert(txid, tx.clone());
        tx
    }

    /// Attempts to commit a transaction.
    ///
    /// This performs the Serializable Snapshot Isolation (SSI) conflict checks.
    /// If a conflict is detected, the transaction is aborted and a `SerializationConflict`
    /// error is returned. Otherwise, the transaction is marked as committed.
    pub fn commit(&self, tx: &Transaction<K, V>) -> Result<(), FluxError> {
        // SSI: Incoming conflict check.
        // If another transaction has written to a key that this transaction read,
        // and that other transaction committed, then this transaction must abort.
        if tx.in_conflict.load(Ordering::Acquire) {
            self.abort(tx);
            return Err(FluxError::SerializationConflict);
        }

        // SSI: Outgoing conflict check.
        // Notify other transactions that read keys we are now writing to.
        // This prevents write-skew anomalies.
        // 1. Check for Read-Write conflicts (T1 writes K, T2 reads K)
        for written_key in tx.write_set.iter() {
            if let Some(reader_tx_ids) = self.read_trackers.get(written_key.key()) {
                for reader_tx_id in reader_tx_ids.iter() {
                    // Only signal other active transactions, not ourselves.
                    if *reader_tx_id == tx.id {
                        continue;
                    }
                    if let Some(reader_tx_entry) = self.active_transactions.get(&reader_tx_id) {
                        reader_tx_entry
                            .value()
                            .in_conflict
                            .store(true, Ordering::Release);
                    }
                }
            }
        }

        // 2. Check for Write-Write conflicts (T1 writes K, T2 writes K)
        // This is a simplified check. A more robust SSI would handle this differently,
        // but this helps prevent some races.
        for other_tx_entry in self.active_transactions.iter() {
            let other_tx = other_tx_entry.value();

            // We only care about other active transactions, not ourselves.
            if tx.id == other_tx.id {
                continue;
            }

            // If other_tx wrote any key that tx also wrote, mark other_tx in conflict.
            for other_written_key in other_tx.write_set.iter() {
                if tx.write_set.contains(other_written_key.key()) {
                    other_tx.in_conflict.store(true, Ordering::Release);
                    break; // No need to check other writes for this other_tx
                }
            }
        }

        self.cleanup_read_trackers(tx);
        self.statuses.insert(tx.id, TransactionStatus::Committed);
        self.active_transactions.remove(&tx.id);
        Ok(())
    }

    /// Aborts a transaction, marking it as aborted and cleaning up its state.
    pub fn abort(&self, tx: &Transaction<K, V>) {
        self.cleanup_read_trackers(tx);
        self.statuses.insert(tx.id, TransactionStatus::Aborted);
        self.active_transactions.remove(&tx.id);
    }

    /// Returns a set of all currently active transaction IDs.
    pub fn get_active_txids(&self) -> HashSet<TxId> {
        self.active_transactions
            .iter()
            .map(|entry| *entry.key())
            .collect()
    }

    /// Returns the next transaction ID that will be allocated.
    pub fn get_current_txid(&self) -> TxId {
        self.next_txid.load(Ordering::SeqCst)
    }

    /// Retrieves the status of a given transaction ID.
    pub fn get_status(&self, txid: TxId) -> Option<TransactionStatus> {
        self.statuses.get(&txid).map(|s| *s)
    }

    /// Removes transaction statuses that are older than the `min_retainable_txid`.
    /// This is called by the vacuum process to prevent unbounded growth of the `statuses` map.
    pub fn prune_statuses(&self) {
        let min_txid = self.min_retainable_txid.load(Ordering::Acquire);
        self.statuses.retain(|&txid, _| txid >= min_txid);
    }

    /// Returns the number of transaction statuses currently stored. Used for metrics/testing.
    pub fn statuses_len(&self) -> usize {
        self.statuses.len()
    }

    /// Removes the given transaction from all read trackers it was a part of.
    fn cleanup_read_trackers(&self, tx: &Transaction<K, V>) {
        use dashmap::mapref::entry::Entry;

        for read_key in tx.read_set.iter() {
            // Use the entry API to ensure the check-and-remove is atomic.
            if let Entry::Occupied(mut o) = self.read_trackers.entry(read_key.key().clone()) {
                o.get_mut().remove(&tx.id);
                if o.get().is_empty() {
                    o.remove();
                }
            }
        }
    }

    /// Creates a new snapshot for a transaction.
    fn create_snapshot(&self, txid: TxId) -> Snapshot {
        let xmax = self.next_txid.load(Ordering::SeqCst);
        let active_txids: HashSet<u64> = self
            .active_transactions
            .iter()
            .map(|entry| *entry.key())
            .collect();
        let xmin = active_txids.iter().min().copied().unwrap_or(xmax);

        Snapshot {
            txid,
            xmin,
            xmax,
            xip: active_txids,
        }
    }
}

/// Represents a transaction's consistent view of the database.
///
/// The snapshot defines which versions of data are visible to the transaction.
/// The visibility rules are based on the transaction IDs of the versions
/// relative to the state of the system when the snapshot was created.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The ID of the transaction this snapshot belongs to.
    pub txid: TxId,
    /// The oldest active transaction ID at the time of snapshot creation.
    /// Any transaction with an ID less than `xmin` is considered committed.
    pub xmin: TxId,
    /// The next transaction ID to be handed out. Any transaction with an ID
    /// greater than or equal to `xmax` was not yet started when the snapshot
    /// was created and is therefore invisible.
    pub xmax: TxId,
    /// The set of in-progress (active) transaction IDs at the time of snapshot creation.
    pub xip: HashSet<TxId>,
}

impl Snapshot {
    /// Checks if a given version is visible to the transaction owning this snapshot.
    ///
    /// # Visibility Rules
    ///
    /// A version is visible if and only if:
    /// 1. The transaction that created it (`creator_txid`) had committed before this
    ///    snapshot was taken.
    ///    - `creator_txid` is not in `xip`.
    ///    - `creator_txid` is less than `xmax`.
    ///    - `creator_txid` is not our own `txid`.
    ///
    /// 2. The transaction that expired it (`expirer_txid`), if any, had *not* yet
    ///    committed when this snapshot was taken.
    ///    - `expirer_txid` is 0 (not expired), OR
    ///    - `expirer_txid` belongs to a transaction that was still in progress (`xip`)
    ///      or had not yet started (`>= xmax`).
    pub fn is_visible<K, V, F>(
        &self,
        version: &Version<V>,
        tx_manager: &TransactionManager<K, F>,
    ) -> bool
    where
        K: Eq + std::hash::Hash + Clone + Serialize + DeserializeOwned,
        F: Clone + Serialize + DeserializeOwned,
    {
        // Rule 1: The creating transaction must be visible to us.
        let creator_visible = if version.creator_txid == self.txid {
            // Our own writes are not visible through the snapshot. They are read from the workspace.
            false
        } else if version.creator_txid >= self.xmax {
            // Created by a transaction that started after our snapshot. Not visible.
            false
        } else if self.xip.contains(&version.creator_txid) {
            // Created by a transaction that was concurrent and has not yet committed. Not visible.
            false
        } else {
            // It's an older transaction. Check its status.
            // If its status is pruned, we can assume it was committed because it's older
            // than the oldest active transaction (`xmin`).
            match tx_manager.get_status(version.creator_txid) {
                Some(status) => status == TransactionStatus::Committed,
                None => version.creator_txid < self.xmin, // Assume committed if pruned and older than oldest active
            }
        };

        if !creator_visible {
            return false;
        }

        // Rule 2: The expiring transaction (if any) must NOT be visible to us.
        let expirer_id = version.expirer_txid.load(Ordering::Acquire);

        if expirer_id == 0 {
            return true; // Not expired, so the version is visible.
        }

        if expirer_id == self.txid {
            return false; // Expired by our own transaction. Not visible.
        }

        let expirer_visible = if expirer_id >= self.xmax {
            // Expired by a transaction that started after our snapshot. Expiration is not visible.
            false
        } else if self.xip.contains(&expirer_id) {
            // Expired by a transaction that was concurrent. Expiration is not visible.
            false
        } else {
            // Expired by an older transaction. Check its status.
            match tx_manager.get_status(expirer_id) {
                Some(status) => status == TransactionStatus::Committed,
                None => expirer_id < self.xmin, // Assume committed if pruned and older than oldest active
            }
        };

        // If the expirer is visible, the deletion is visible, so the version is NOT visible.
        // Therefore, we return the inverse of `expirer_visible`.
        !expirer_visible
    }
}

/// Represents a single, isolated transaction.
#[derive(Debug)]
pub struct Transaction<K: Eq + std::hash::Hash, V> {
    /// The unique ID of this transaction.
    pub id: TxId,
    /// The consistent snapshot of the database state for this transaction.
    pub snapshot: Snapshot,
    /// The set of keys read by this transaction, used for SSI conflict detection.
    /// Maps Key -> Creator TxId of the version that was read.
    pub read_set: DashMap<K, TxId>,
    /// The set of keys written to by this transaction, used for SSI conflict detection.
    pub write_set: DashSet<K>,
    /// A flag indicating if a read-write conflict has been detected by another committing transaction.
    /// If true, this transaction will be forced to abort.
    pub in_conflict: AtomicBool,
    /// The transaction's private workspace for pending changes (inserts, updates, deletes).
    /// These changes are only applied to the main database upon a successful commit.
    pub workspace: Mutex<Workspace<K, V>>, // Changed for thread-safety
    _phantom: std::marker::PhantomData<V>,
}

impl<K, V> Transaction<K, V>
where
    K: Eq + std::hash::Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    /// Creates a new transaction with a given ID and snapshot.
    pub fn new(id: TxId, snapshot: Snapshot) -> Self {
        Self {
            id,
            snapshot,
            read_set: DashMap::new(),
            write_set: DashSet::new(),
            in_conflict: AtomicBool::new(false),
            workspace: Mutex::new(HashMap::new()), // Initialize workspace
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Represents a single version of a value in the multi-version store.
#[derive(Debug, Serialize, Deserialize)]
pub struct Version<V> {
    /// The actual value.
    pub value: V,
    /// The ID of the transaction that created this version.
    pub creator_txid: TxId,
    /// The ID of the transaction that expired (deleted) this version.
    /// A value of `0` means this version is not expired.
    pub expirer_txid: AtomicU64,
}
