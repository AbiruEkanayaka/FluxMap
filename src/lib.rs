#![doc = include_str!("../README.md")]
//! The core, concurrent, multi-version skiplist implementation.
//!
//! This module provides `SkipList`, a highly concurrent data structure that serves
//! as the foundation for FluxMap. It uses Multi-Version Concurrency Control (MVCC)
//! to allow for non-blocking reads and high-performance writes.
//!
//! # Internals
//!
//! -   **Nodes:** The skiplist is composed of `Node`s, each representing a key.
//! -   **Version Chains:** Each `Node` points to a linked list of `VersionNode`s.
//!     Each `VersionNode` represents a specific version of the value for that key,
//!     created by a specific transaction.
//! -   **MVCC:** When a value is updated, a new `VersionNode` is prepended to the
//!     chain. When a value is deleted, the most recent `VersionNode` is marked as

use std::borrow::Borrow;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use crossbeam_utils::CachePadded;
use dashmap::DashSet; // Added for read_trackers
use futures::stream::Stream;
use serde::{Serialize, de::DeserializeOwned};

pub mod db;
pub mod error;
pub mod persistence;
pub mod transaction;
pub mod vacuum;
pub use crate::transaction::{Snapshot, Transaction, TransactionManager, TxId, Version};
pub use persistence::{DurabilityLevel, PersistenceEngine, PersistenceOptions};

const DEFAULT_MAX_LEVEL: usize = 32;
const DEFAULT_P: f64 = 0.5;

/// A node in the version chain for a single key.
struct VersionNode<V> {
    version: Version<V>,
    next: Atomic<VersionNode<V>>,
}

impl<V> VersionNode<V> {
    fn new(version: Version<V>) -> Owned<Self> {
        Owned::new(Self {
            version,
            next: Atomic::null(),
        })
    }
}

/// A node in the skiplist, representing a key and its chain of versions.
struct Node<K, V> {
    key: Option<K>,
    /// An atomically-managed pointer to the head of the version chain.
    value: Atomic<VersionNode<Arc<V>>>,
    /// The forward pointers for each level of the skiplist.
    next: Vec<Atomic<Node<K, V>>>,
    /// A flag indicating that this node is logically deleted and awaiting physical removal.
    deleted: AtomicBool,
}

impl<K, V> Node<K, V> {
    /// Creates a new head node for a skiplist.
    fn head(max_level: usize) -> Owned<Self> {
        Owned::new(Node {
            key: None,
            value: Atomic::null(),
            next: (0..max_level).map(|_| Atomic::null()).collect(),
            deleted: AtomicBool::new(false),
        })
    }

    /// Creates a new data node with a single version.
    fn new(key: K, value: Arc<V>, level: usize, txid: TxId) -> Owned<Self> {
        let version = Version {
            value,
            creator_txid: txid,
            expirer_txid: AtomicU64::new(0), // 0 means not expired.
        };
        let version_node = VersionNode::new(version);

        Owned::new(Node {
            key: Some(key),
            value: Atomic::from(version_node),
            next: (0..level + 1).map(|_| Atomic::null()).collect(),
            deleted: AtomicBool::new(false),
        })
    }
}

/// A concurrent, multi-version, transactional skiplist.
///
/// `SkipList` is the core data structure that stores key-value pairs. It supports
/// highly concurrent reads and writes using Multi-Version Concurrency Control (MVCC)
/// and Serializable Snapshot Isolation (SSI).
pub struct SkipList<K: Eq + std::hash::Hash, V> {
    head: CachePadded<Atomic<Node<K, V>>>,
    max_level: CachePadded<usize>,
    level: CachePadded<AtomicUsize>,
    len: CachePadded<AtomicUsize>,
    p: CachePadded<f64>,
    tx_manager: Arc<TransactionManager<K, V>>,
}

enum InsertAction {
    YieldAndRetry,
    Return,
}

impl<K, V> Default for SkipList<K, V>
where
    K: Ord + Clone + Send + Sync + 'static + std::hash::Hash + Eq + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> SkipList<K, V>
where
    K: Ord + Clone + Send + Sync + 'static + std::hash::Hash + Eq + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned,
{
    /// Creates a new, empty `SkipList` with the default max level.
    pub fn new() -> Self {
        Self::with_max_level(DEFAULT_MAX_LEVEL)
    }

    /// Creates a new, empty `SkipList` with a specified max level.
    pub fn with_max_level(max_level: usize) -> Self {
        Self::with_max_level_and_p(max_level, DEFAULT_P)
    }

    /// Creates a new, empty `SkipList` with a specified max level and probability factor.
    pub fn with_max_level_and_p(max_level: usize, p: f64) -> Self {
        let head = Node::head(max_level);
        SkipList {
            head: CachePadded::new(Atomic::from(head)),
            max_level: CachePadded::new(max_level),
            level: CachePadded::new(AtomicUsize::new(0)),
            len: CachePadded::new(AtomicUsize::new(0)),
            p: CachePadded::new(p),
            tx_manager: Arc::new(TransactionManager::<K, V>::new()),
        }
    }

    /// Returns a reference to the associated `TransactionManager`.
    pub fn transaction_manager(&self) -> &Arc<TransactionManager<K, V>> {
        &self.tx_manager
    }

    /// Returns the approximate number of keys in the skiplist.
    ///
    /// This is an approximation because it may not reflect in-flight additions or removals.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Returns `true` if the skiplist contains no keys.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Generates a random level for a new node based on the probability factor `p`.
    fn random_level(&self) -> usize {
        let mut level = 0;
        while fastrand::f64() < *self.p && level < *self.max_level - 1 {
            level += 1;
        }
        level
    }

    /// Finds the predecessor node for a given key at a specific level.
    /// This function also helps with physical removal of logically deleted nodes it encounters.
    fn find_predecessor_at_level<'guard, Q: ?Sized>(
        &self,
        key: &Q,
        mut current: Shared<'guard, Node<K, V>>,
        level: usize,
        guard: &'guard Guard,
    ) -> Shared<'guard, Node<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        loop {
            let next = unsafe {
                // SAFETY: `current` is a `Shared` pointer obtained from `Atomic::from` or `load`
                // operations, ensuring it's a valid, non-null pointer to a `Node<K, V>`.
                // The `guard` ensures that the memory pointed to by `current` is protected
                // from reclamation during this operation.
                current.deref().next[level].load(Ordering::Relaxed, guard)
            };

            if let Some(next_node) = unsafe {
                // SAFETY: `next` is a `Shared` pointer. `as_ref()` is safe as `next` is checked for null.
                // The `guard` ensures the memory is protected.
                next.as_ref()
            } {
                if next_node.deleted.load(Ordering::Acquire) {
                    // Node is logically deleted, help physically remove it.
                    let next_of_next = next_node.next[level].load(Ordering::Relaxed, guard);
                    if unsafe {
                        // SAFETY: `current` is a valid pointer as established above.
                        // The `compare_exchange` operation is atomic and ensures memory safety.
                        // We are attempting to swing the `next` pointer of `current` to skip over
                        // the logically deleted `next` node.
                        current.deref().next[level].compare_exchange(
                            next,
                            next_of_next,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                            guard,
                        )
                    }
                    .is_ok()
                    {
                        // Physical removal was successful.
                        // Only decrement len and schedule for destruction at the base level
                        // to ensure it happens exactly once per node.
                        if level == 0 {
                            self.len.fetch_sub(1, Ordering::Relaxed);
                            unsafe {
                                // SAFETY: `next` points to the unlinked node. Since we have successfully
                                // unlinked it, no other thread will be able to reach it through the skiplist.
                                // We can now safely schedule its memory to be reclaimed by the epoch-based
                                // garbage collector.
                                guard.defer_destroy(next);
                            }
                        }
                    }
                    // Retry finding the predecessor from `current`, as the list has changed.
                    continue;
                }

                // If there is a next node and its key is less than the target key, move forward.
                // SAFETY: `next_node` is a valid reference. `key` is `Some` for all non-head nodes.
                // `unwrap_unchecked` is safe here because we know `next_node` is not the head node,
                // which is the only node type where `key` is `None`.
                if <K as Borrow<Q>>::borrow(unsafe { next_node.key.as_ref().unwrap_unchecked() })
                    < key
                {
                    current = next;
                    continue;
                }
            }

            // Otherwise, `current` is the predecessor at this level.
            break;
        }
        current
    }

    /// Finds the predecessor node for a given key by searching from the top level down.
    fn find_optimistic_predecessor<'guard, Q: ?Sized>(
        &self,
        key: &Q,
        guard: &'guard Guard,
    ) -> Shared<'guard, Node<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        let head = self.head.load(Ordering::Relaxed, guard);
        let mut predecessor = head;
        for i in (0..*self.max_level).rev() {
            predecessor = self.find_predecessor_at_level(key, predecessor, i, guard);
        }
        predecessor
    }

    /// Finds all predecessor nodes for a given key, one for each level of the skiplist.
    fn find_predecessors<'guard, Q: ?Sized>(
        &self,
        key: &Q,
        guard: &'guard Guard,
    ) -> Vec<Shared<'guard, Node<K, V>>>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        let head = self.head.load(Ordering::Relaxed, guard);
        let mut predecessors = vec![Shared::null(); *self.max_level];
        let mut current = head;

        for i in (0..*self.max_level).rev() {
            current = self.find_predecessor_at_level(key, current, i, guard);
            predecessors[i] = current;
        }
        predecessors
    }

    /// Retrieves the value associated with `key` that is visible to the given `transaction`.
    ///
    /// This method traverses the skiplist to find the node for the given `key`. It then
    /// walks the version chain for that node to find the most recent version that is
    /// visible according to the transaction's `Snapshot`.
    ///
    /// As part of the SSI protocol, this operation adds the key to the transaction's
    /// read set.
    pub fn get(&self, key: &K, transaction: &Transaction<K, V>) -> Option<Arc<V>> {
        let snapshot = &transaction.snapshot;
        let guard = &crossbeam_epoch::pin();
        let predecessor = self.find_optimistic_predecessor::<K>(key, guard);
        let current = unsafe {
            // SAFETY: `predecessor` is a `Shared` pointer to a valid `Node`. `deref()` is safe
            // because `find_optimistic_predecessor` ensures it's not null. The `guard` protects the memory.
            predecessor.deref().next[0].load(Ordering::Acquire, guard)
        };

        if let Some(node) = unsafe {
            // SAFETY: `current` is a `Shared` pointer. `as_ref()` is safe as `current` is checked for null.
            // The `guard` ensures the memory is protected.
            current.as_ref()
        } {
            if unsafe {
                // SAFETY: `node` is a valid reference. `key` is `Some` for all non-head nodes.
                // `unwrap_unchecked` is safe as we've confirmed `node` is not the head.
                node.key.as_ref().unwrap_unchecked()
            } == key
                && !node.deleted.load(Ordering::Acquire)
            {
                let mut current_version_ptr = node.value.load(Ordering::Acquire, guard);
                while let Some(version_node) = unsafe {
                    // SAFETY: `current_version_ptr` is a `Shared` pointer. `as_ref()` is safe as
                    // it's checked for null. The `guard` ensures the memory is protected.
                    current_version_ptr.as_ref()
                } {
                    let is_visible = snapshot.is_visible(&version_node.version, &*self.tx_manager);

                    if is_visible {
                        // Record the read for SSI conflict detection.
                        transaction
                            .read_set
                            .insert(key.clone(), version_node.version.creator_txid);
                        // Add this transaction to the read_trackers for this key
                        self.tx_manager
                            .read_trackers
                            .entry(key.clone())
                            .or_insert_with(DashSet::new)
                            .insert(transaction.id);
                        return Some(version_node.version.value.clone());
                    }

                    current_version_ptr = version_node.next.load(Ordering::Acquire, guard);
                }
            }
        }
        None
    }

    /// Checks if a key exists and is visible to the given `transaction`.
    pub fn contains_key(&self, key: &K, transaction: &Transaction<K, V>) -> bool {
        self.get(key, transaction).is_some()
    }

    /// Links a new node into the skiplist at all its levels.
    fn link_new_node<'guard>(
        &self,
        key: &K,
        mut predecessors: Vec<Shared<'guard, Node<K, V>>>,
        new_node_shared: Shared<'guard, Node<K, V>>,
        new_level: usize,
        guard: &'guard Guard,
    ) {
        // Link the node from level 1 up to its randomly determined level.
        // Level 0 is handled separately by the caller.
        for i in 1..=new_level {
            loop {
                let pred = predecessors[i];
                let next_at_level = unsafe {
                    // SAFETY: `pred` is a `Shared` pointer to a valid `Node`. `deref()` is safe
                    // because `find_predecessors` ensures it's valid. The `guard` protects the memory.
                    pred.deref().next[i].load(Ordering::Relaxed, guard)
                };
                unsafe {
                    // SAFETY: `new_node_shared` is a `Shared` pointer to a valid `Node`.
                    // `deref()` is safe. We are setting its forward pointer.
                    new_node_shared.deref().next[i].store(next_at_level, Ordering::Relaxed)
                };

                if unsafe {
                    // SAFETY: `pred` is a valid pointer. The `compare_exchange` is atomic and
                    // safely links the new node into the list at this level.
                    pred.deref().next[i].compare_exchange(
                        next_at_level,
                        new_node_shared,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                        guard,
                    )
                }
                .is_ok()
                {
                    break; // Success, move to the next level.
                }
                // CAS failed, contention. Re-find predecessors and retry for this level.
                predecessors = self.find_predecessors::<K>(key, guard);
            }
        }

        self.len.fetch_add(1, Ordering::Relaxed);
        self.level.fetch_max(new_level, Ordering::Release);
    }

    /// Inserts a key-value pair as part of a transaction.
    ///
    /// If the key already exists, this prepends a new version to its version chain.
    /// If the key does not exist, this creates a new `Node` and links it into the skiplist.
    ///
    /// This operation adds the key to the transaction's write set for SSI conflict detection.
    pub async fn insert(&self, key: K, value: Arc<V>, transaction: &Transaction<K, V>) {
        transaction.write_set.insert(key.clone());
        let new_level = self.random_level();

        loop {
            let action = {
                let guard = &crossbeam_epoch::pin();
                let predecessors = self.find_predecessors::<K>(&key, guard);
                let predecessor = predecessors[0];

                let next = unsafe {
                    // SAFETY: `predecessor` is a valid `Shared` pointer. `deref()` is safe.
                    // The `guard` protects the memory.
                    predecessor.deref().next[0].load(Ordering::Relaxed, guard)
                };

                if let Some(next_node) = unsafe {
                    // SAFETY: `next` is a `Shared` pointer. `as_ref()` is safe as `next` is checked for null.
                    // The `guard` ensures the memory is protected.
                    next.as_ref()
                } {
                    if unsafe {
                        // SAFETY: `next_node` is a valid reference. `key` is `Some` for all non-head nodes.
                        next_node.key.as_ref().unwrap_unchecked()
                    } == &key
                    {
                        // Key exists. Prepend a new version to the version chain.
                        let new_version = Version {
                            value: value.clone(),
                            creator_txid: transaction.id,
                            expirer_txid: AtomicU64::new(0),
                        };
                        let new_version_node = VersionNode::new(new_version);
                        let new_version_node_shared = new_version_node.into_shared(guard);

                        loop {
                            let current_head_ptr = next_node.value.load(Ordering::Acquire, guard);
                            unsafe {
                                // SAFETY: `new_version_node_shared` is a valid `Shared` pointer.
                                // `deref()` is safe. We are setting its `next` pointer to the current
                                // head of the version chain.
                                new_version_node_shared
                                    .deref()
                                    .next
                                    .store(current_head_ptr, Ordering::Relaxed)
                            };

                            // Atomically swing the `value` pointer to the new version node.
                            match next_node.value.compare_exchange(
                                current_head_ptr,
                                new_version_node_shared,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                                guard,
                            ) {
                                Ok(_) => break InsertAction::Return, // Success
                                Err(_) => continue,                  // Contention, retry CAS loop
                            }
                        }
                    } else {
                        // Key does not exist, create a new node.
                        let new_node =
                            Node::new(key.clone(), value.clone(), new_level, transaction.id);
                        let new_node_shared = new_node.into_shared(guard);

                        unsafe {
                            // SAFETY: `new_node_shared` is a valid `Shared` pointer. `deref()` is safe.
                            // We are setting its level 0 forward pointer.
                            new_node_shared.deref().next[0].store(next, Ordering::Relaxed)
                        };

                        if unsafe {
                            // SAFETY: `predecessor` is a valid pointer. The `compare_exchange` is atomic
                            // and safely links the new node into the base level of the list.
                            predecessor.deref().next[0].compare_exchange(
                                next,
                                new_node_shared,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                                guard,
                            )
                        }
                        .is_err()
                        {
                            InsertAction::YieldAndRetry // Contention, retry whole operation.
                        } else {
                            // Link the node at higher levels.
                            self.link_new_node(
                                &key,
                                predecessors,
                                new_node_shared,
                                new_level,
                                guard,
                            );
                            InsertAction::Return
                        }
                    }
                } else {
                    // List is empty or we are at the end. Create a new node.
                    let new_node = Node::new(key.clone(), value.clone(), new_level, transaction.id);
                    let new_node_shared = new_node.into_shared(guard);

                    unsafe {
                        // SAFETY: `new_node_shared` is a valid `Shared` pointer. `deref()` is safe.
                        new_node_shared.deref().next[0].store(next, Ordering::Relaxed)
                    };

                    if unsafe {
                        // SAFETY: `predecessor` is a valid pointer. The `compare_exchange` is atomic.
                        predecessor.deref().next[0].compare_exchange(
                            next,
                            new_node_shared,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            guard,
                        )
                    }
                    .is_err()
                    {
                        InsertAction::YieldAndRetry
                    } else {
                        self.link_new_node(&key, predecessors, new_node_shared, new_level, guard);
                        InsertAction::Return
                    }
                }
            };

            match action {
                InsertAction::YieldAndRetry => {
                    // Implement exponential backoff
                    let mut attempts = 0;
                    loop {
                        attempts += 1;
                        if attempts < 5 {
                            // Spin for a few attempts
                            std::thread::yield_now();
                        } else {
                            // Then yield to Tokio runtime with increasing delay
                            let delay_ms = 2u64.pow(attempts - 5); // Exponential delay
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms.min(100)))
                                .await; // Cap delay at 100ms
                        }
                        // Break from the inner loop to let the outer loop re-run the whole insert logic.
                        break;
                    }
                    continue; // Continue the outer loop to retry the insert operation
                }
                InsertAction::Return => return,
            }
        }
    }

    /// Logically removes a key as part of a transaction.
    ///
    /// This finds the latest visible version of the key and atomically sets its
    /// `expirer_txid` to the current transaction's ID. The actual data is not
    /// removed until the vacuum process runs.
    ///
    /// This operation adds the key to the transaction's write set.
    ///
    /// # Returns
    ///
    /// Returns the value that was removed if a visible version was found, otherwise `None`.
    pub async fn remove(&self, key: &K, transaction: &Transaction<K, V>) -> Option<Arc<V>> {
        transaction.write_set.insert(key.clone());
        let transaction_id = transaction.id;

        let guard = &crossbeam_epoch::pin();
        let predecessor = self.find_optimistic_predecessor::<K>(key, guard);
        let node_ptr = unsafe {
            // SAFETY: `predecessor` is a valid `Shared` pointer. `deref()` is safe.
            // The `guard` protects the memory.
            predecessor.deref().next[0].load(Ordering::Acquire, guard)
        };

        if let Some(node) = unsafe {
            // SAFETY: `node_ptr` is a `Shared` pointer. `as_ref()` is safe as it's checked for null.
            // The `guard` protects the memory.
            node_ptr.as_ref()
        } {
            if unsafe {
                // SAFETY: `node` is a valid reference. `key` is `Some` for all non-head nodes.
                node.key.as_ref().unwrap_unchecked()
            } != key
            {
                return None; // Key not found.
            }

            // If the node is already marked as deleted by the vacuum, we can't do anything.
            if node.deleted.load(Ordering::Acquire) {
                return None;
            }

            let mut version_ptr = node.value.load(Ordering::Acquire, guard);
            while let Some(version_node) = unsafe {
                // SAFETY: `version_ptr` is a `Shared` pointer. `as_ref()` is safe as it's checked for null.
                // The `guard` protects the memory.
                version_ptr.as_ref()
            } {
                // Check if the version is visible to the current transaction.
                let is_visible = transaction
                    .snapshot
                    .is_visible(&version_node.version, &*self.tx_manager);

                if is_visible {
                    // This is a version we can try to expire.
                    // Atomically set the expirer_txid from 0 to our transaction ID.
                    match version_node.version.expirer_txid.compare_exchange(
                        0,
                        transaction_id,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // Success! We expired this version.
                            return Some(version_node.version.value.clone());
                        }
                        Err(_) => {
                            // CAS failed. Another concurrent transaction just expired it.
                            // This version is no longer visible to us.
                            // We continue the loop to find the next visible version.
                        }
                    }
                }

                // Move to the next version in the chain.
                version_ptr = version_node.next.load(Ordering::Acquire, guard);
            }

            // If we reach here, no visible version was found or we lost all races.
            return None;
        } else {
            return None; // Node not found.
        }
    }

    /// Scans a range of keys and returns the visible versions as a `Vec`.
    pub fn range(&self, start: &K, end: &K, snapshot: &Snapshot) -> Vec<(K, Arc<V>)> {
        let guard = &crossbeam_epoch::pin();
        let mut results = Vec::new();

        let predecessor = self.find_optimistic_predecessor::<K>(start, guard);
        let mut current = unsafe {
            // SAFETY: `predecessor` is a valid `Shared` pointer. `deref()` is safe.
            // The `guard` protects the memory.
            predecessor.deref().next[0].load(Ordering::Acquire, guard)
        };

        loop {
            if let Some(node_ref) = unsafe {
                // SAFETY: `current` is a `Shared` pointer. `as_ref()` is safe as it's checked for null.
                // The `guard` protects the memory.
                current.as_ref()
            } {
                if node_ref.deleted.load(Ordering::Acquire) {
                    current = node_ref.next[0].load(Ordering::Acquire, guard);
                    continue;
                }
                let key = unsafe {
                    // SAFETY: `node_ref` is a valid reference. `key` is `Some` for all non-head nodes.
                    node_ref.key.as_ref().unwrap_unchecked()
                };
                if key > end {
                    break;
                }
                if key >= start {
                    let mut current_version_ptr = node_ref.value.load(Ordering::Acquire, guard);
                    while let Some(version_node) = unsafe {
                        // SAFETY: `current_version_ptr` is a `Shared` pointer. `as_ref()` is safe.
                        // The `guard` protects the memory.
                        current_version_ptr.as_ref()
                    } {
                        let is_visible =
                            snapshot.is_visible(&version_node.version, &*self.tx_manager);

                        if is_visible {
                            results.push((key.clone(), version_node.version.value.clone()));
                            break; // Found the visible version for this key, move to next key
                        }
                        current_version_ptr = version_node.next.load(Ordering::Acquire, guard);
                    }
                }
                current = node_ref.next[0].load(Ordering::Acquire, guard);
            } else {
                break;
            }
        }
        results
    }

    /// Returns a stream that yields visible key-value pairs within a given range.
    pub fn range_stream<'a>(
        &'a self,
        start: &'a K,
        end: &'a K,
        snapshot: &'a Snapshot,
    ) -> impl Stream<Item = (K, Arc<V>)> + 'a {
        // Use async_stream to create a true streaming iterator
        async_stream::stream! {
            let guard = &crossbeam_epoch::pin();
            let predecessor = self.find_optimistic_predecessor::<K>(start, guard);
            let mut current = unsafe {
                // SAFETY: `predecessor` is a valid `Shared` pointer. `deref()` is safe.
                // The `guard` protects the memory.
                predecessor.deref().next[0].load(Ordering::Acquire, guard)
            };

            loop {
                if let Some(node_ref) = unsafe {
                    // SAFETY: `current` is a `Shared` pointer. `as_ref()` is safe.
                    // The `guard` protects the memory.
                    current.as_ref()
                } {
                    if node_ref.deleted.load(Ordering::Acquire) {
                        current = node_ref.next[0].load(Ordering::Acquire, guard);
                        continue;
                    }
                    let key = unsafe {
                        // SAFETY: `node_ref` is a valid reference. `key` is `Some` for all non-head nodes.
                        node_ref.key.as_ref().unwrap_unchecked()
                    };
                    if key > end {
                        break;
                    }
                    if key >= start {
                        let mut current_version_ptr = node_ref.value.load(Ordering::Acquire, guard);
                        while let Some(version_node) = unsafe {
                            // SAFETY: `current_version_ptr` is a `Shared` pointer. `as_ref()` is safe.
                            // The `guard` protects the memory.
                            current_version_ptr.as_ref()
                        } {
                            let is_visible = snapshot.is_visible(&version_node.version, &*self.tx_manager);

                            if is_visible {
                                yield (key.clone(), version_node.version.value.clone());
                                break; // Found the visible version for this key, move to next key
                            }
                            current_version_ptr = version_node.next.load(Ordering::Acquire, guard);
                        }
                    }
                    current = node_ref.next[0].load(Ordering::Acquire, guard);
                } else {
                    break;
                }
            }
        }
    }
}

impl<K, V> SkipList<K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + Borrow<str>
        + std::hash::Hash
        + Eq
        + Serialize
        + DeserializeOwned,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned,
{
    /// Scans for keys starting with a given prefix and returns the visible versions as a `Vec`.
    pub fn prefix_scan(&self, prefix: &str, snapshot: &Snapshot) -> Vec<(K, Arc<V>)> {
        let guard = &crossbeam_epoch::pin();
        let mut results = Vec::new();

        let predecessor = self.find_optimistic_predecessor::<str>(prefix, guard);
        let mut current = unsafe {
            // SAFETY: `predecessor` is a valid `Shared` pointer. `deref()` is safe.
            // The `guard` protects the memory.
            predecessor.deref().next[0].load(Ordering::Acquire, guard)
        };

        loop {
            if let Some(node_ref) = unsafe {
                // SAFETY: `current` is a `Shared` pointer. `as_ref()` is safe.
                // The `guard` protects the memory.
                current.as_ref()
            } {
                if node_ref.deleted.load(Ordering::Acquire) {
                    current = node_ref.next[0].load(Ordering::Acquire, guard);
                    continue;
                }

                let key = unsafe {
                    // SAFETY: `node_ref` is a valid reference. `key` is `Some` for all non-head nodes.
                    node_ref.key.as_ref().unwrap_unchecked()
                };
                if key.borrow().starts_with(prefix) {
                    let mut current_version_ptr = node_ref.value.load(Ordering::Acquire, guard);
                    while let Some(version_node) = unsafe {
                        // SAFETY: `current_version_ptr` is a `Shared` pointer. `as_ref()` is safe.
                        // The `guard` protects the memory.
                        current_version_ptr.as_ref()
                    } {
                        let is_visible =
                            snapshot.is_visible(&version_node.version, &*self.tx_manager);

                        if is_visible {
                            results.push((key.clone(), version_node.version.value.clone()));
                            break; // Found the visible version for this key, move to next key
                        }
                        current_version_ptr = version_node.next.load(Ordering::Acquire, guard);
                    }
                } else {
                    // Since the skiplist is sorted, once we find a key that doesn't
                    // have the prefix, no subsequent keys will either.
                    break;
                }
                current = node_ref.next[0].load(Ordering::Acquire, guard);
            } else {
                break;
            }
        }
        results
    }

    /// Returns a stream that yields visible key-value pairs for keys starting with a given prefix.
    pub fn prefix_scan_stream<'a>(
        &'a self,
        prefix: &'a str,
        snapshot: &'a Snapshot,
    ) -> impl Stream<Item = (K, Arc<V>)> + 'a {
        // Use async_stream to create a true streaming iterator
        async_stream::stream! {
            let guard = &crossbeam_epoch::pin();
            let predecessor = self.find_optimistic_predecessor::<str>(prefix, guard);
            let mut current = unsafe {
                // SAFETY: `predecessor` is a valid `Shared` pointer. `deref()` is safe.
                // The `guard` protects the memory.
                predecessor.deref().next[0].load(Ordering::Acquire, guard)
            };

            loop {
                if let Some(node_ref) = unsafe {
                    // SAFETY: `current` is a `Shared` pointer. `as_ref()` is safe.
                    // The `guard` protects the memory.
                    current.as_ref()
                } {
                    if node_ref.deleted.load(Ordering::Acquire) {
                        current = node_ref.next[0].load(Ordering::Acquire, guard);
                        continue;
                    }

                    let key = unsafe {
                        // SAFETY: `node_ref` is a valid reference. `key` is `Some` for all non-head nodes.
                        node_ref.key.as_ref().unwrap_unchecked()
                    };
                    if key.borrow().starts_with(prefix) {
                        let mut current_version_ptr = node_ref.value.load(Ordering::Acquire, guard);
                        while let Some(version_node) = unsafe {
                            // SAFETY: `current_version_ptr` is a `Shared` pointer. `as_ref()` is safe.
                            // The `guard` protects the memory.
                            current_version_ptr.as_ref()
                        } {
                            let is_visible = snapshot.is_visible(&version_node.version, &*self.tx_manager);

                            if is_visible {
                                yield (key.clone(), version_node.version.value.clone());
                                break; // Found the visible version for this key, move to next key
                            }
                            current_version_ptr = version_node.next.load(Ordering::Acquire, guard);
                        }
                    } else {
                        break;
                    }
                    current = node_ref.next[0].load(Ordering::Acquire, guard);
                } else {
                    break;
                }
            }
        }
    }
}
