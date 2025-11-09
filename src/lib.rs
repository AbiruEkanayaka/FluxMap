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
pub use persistence::{DurabilityLevel, PersistenceEngine};

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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use futures::pin_mut;
    use rand::{Rng, SeedableRng};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_new_skip_list() {
        let skip_list: SkipList<String, String> = SkipList::new();
        assert_eq!(skip_list.len(), 0);
        assert!(skip_list.is_empty());
        assert_eq!(skip_list.level.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_default() {
        let skip_list: SkipList<String, String> = SkipList::default();
        assert!(skip_list.is_empty());
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let skip_list: SkipList<String, String> = SkipList::new();
        let tx_manager = skip_list.transaction_manager();

        // Writer Transaction
        let writer_tx = tx_manager.begin();
        skip_list
            .insert("c".to_string(), Arc::new("three".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("a".to_string(), Arc::new("one".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("b".to_string(), Arc::new("two".to_string()), &writer_tx)
            .await;
        tx_manager.commit(&writer_tx).unwrap();

        // Reader Transaction
        let reader_tx = tx_manager.begin();
        assert_eq!(skip_list.len(), 3);
        assert_eq!(
            *skip_list.get(&"a".to_string(), &reader_tx).unwrap(),
            "one".to_string()
        );
        assert_eq!(
            *skip_list.get(&"b".to_string(), &reader_tx).unwrap(),
            "two".to_string()
        );
        assert_eq!(
            *skip_list.get(&"c".to_string(), &reader_tx).unwrap(),
            "three".to_string()
        );
        assert!(skip_list.get(&"f".to_string(), &reader_tx).is_none());
    }

    #[tokio::test]
    async fn test_insert_duplicate_key() {
        let skip_list: SkipList<String, String> = SkipList::new();
        let tx_manager = skip_list.transaction_manager();

        let writer_tx = tx_manager.begin();
        skip_list
            .insert("a".to_string(), Arc::new("one".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("a".to_string(), Arc::new("one_new".to_string()), &writer_tx)
            .await; // Prepend new version
        tx_manager.commit(&writer_tx).unwrap();

        let reader_tx = tx_manager.begin();
        assert_eq!(skip_list.len(), 1);
        assert_eq!(
            *skip_list.get(&"a".to_string(), &reader_tx).unwrap(),
            "one_new".to_string()
        );
    }

    #[tokio::test]
    async fn test_update_value() {
        let skip_list: SkipList<String, String> = SkipList::new();
        let tx_manager = skip_list.transaction_manager();

        let writer_tx_1 = tx_manager.begin();
        skip_list
            .insert("a".to_string(), Arc::new("one".to_string()), &writer_tx_1)
            .await;
        tx_manager.commit(&writer_tx_1).unwrap();

        let reader_tx_1 = tx_manager.begin();
        assert_eq!(
            *skip_list.get(&"a".to_string(), &reader_tx_1).unwrap(),
            "one".to_string()
        );
        tx_manager.commit(&reader_tx_1).unwrap(); // Commit the reader to release dependencies

        let writer_tx_2 = tx_manager.begin();
        skip_list
            .insert(
                "a".to_string(),
                Arc::new("one_updated".to_string()),
                &writer_tx_2,
            )
            .await;
        tx_manager.commit(&writer_tx_2).unwrap();

        let reader_tx_2 = tx_manager.begin();
        assert_eq!(skip_list.len(), 1);
        assert_eq!(
            *skip_list.get(&"a".to_string(), &reader_tx_2).unwrap(),
            "one_updated".to_string()
        );
    }

    #[tokio::test]
    async fn test_get_empty() {
        let skip_list: SkipList<String, String> = SkipList::new();
        let tx = skip_list.transaction_manager().begin();
        assert!(skip_list.get(&"a".to_string(), &tx).is_none());
    }

    #[tokio::test]
    async fn test_remove() {
        let skip_list: SkipList<String, String> = SkipList::new();
        let tx_manager = skip_list.transaction_manager();

        let writer_tx = tx_manager.begin();
        skip_list
            .insert("a".to_string(), Arc::new("one".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("b".to_string(), Arc::new("two".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("c".to_string(), Arc::new("three".to_string()), &writer_tx)
            .await;
        tx_manager.commit(&writer_tx).unwrap();

        let reader_tx_1 = tx_manager.begin();
        assert_eq!(skip_list.len(), 3);

        let remover_tx = tx_manager.begin();
        assert_eq!(
            *skip_list
                .remove(&"b".to_string(), &remover_tx)
                .await
                .unwrap(),
            "two".to_string()
        );
        tx_manager.commit(&remover_tx).unwrap();

        let reader_tx_2 = tx_manager.begin();
        assert_eq!(skip_list.len(), 3);
        assert!(skip_list.get(&"b".to_string(), &reader_tx_2).is_none());

        // Verify that the old transaction still sees the old data
        assert!(skip_list.get(&"b".to_string(), &reader_tx_1).is_some());

        let remover_tx_2 = tx_manager.begin();
        assert!(
            skip_list
                .remove(&"d".to_string(), &remover_tx_2)
                .await
                .is_none()
        );
        assert_eq!(skip_list.len(), 3);
    }

    #[tokio::test]
    async fn test_contains_key() {
        let skip_list: SkipList<String, String> = SkipList::new();
        let tx_manager = skip_list.transaction_manager();

        let writer_tx = tx_manager.begin();
        skip_list
            .insert("a".to_string(), Arc::new("one".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("c".to_string(), Arc::new("three".to_string()), &writer_tx)
            .await;
        tx_manager.commit(&writer_tx).unwrap();

        let reader_tx = tx_manager.begin();
        assert!(skip_list.contains_key(&"a".to_string(), &reader_tx));
        assert!(skip_list.contains_key(&"c".to_string(), &reader_tx));
        assert!(!skip_list.contains_key(&"b".to_string(), &reader_tx));
        assert!(!skip_list.contains_key(&"d".to_string(), &reader_tx));
    }

    #[tokio::test]
    async fn test_range() {
        let skip_list: SkipList<String, String> = SkipList::new();
        let tx_manager = skip_list.transaction_manager();

        let writer_tx = tx_manager.begin();
        skip_list
            .insert("a".to_string(), Arc::new("1".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("b".to_string(), Arc::new("2".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("c".to_string(), Arc::new("3".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("d".to_string(), Arc::new("4".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("e".to_string(), Arc::new("5".to_string()), &writer_tx)
            .await;
        tx_manager.commit(&writer_tx).unwrap();

        let reader_tx = tx_manager.begin();
        let range = skip_list.range(&"b".to_string(), &"d".to_string(), &reader_tx.snapshot);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0, "b".to_string());
        assert_eq!(range[1].0, "c".to_string());
        assert_eq!(range[2].0, "d".to_string());

        let range_all = skip_list.range(&"a".to_string(), &"z".to_string(), &reader_tx.snapshot);
        assert_eq!(range_all.len(), 5);
    }

    #[tokio::test]
    async fn test_prefix_scan() {
        let skip_list: SkipList<String, String> = SkipList::new();
        let tx_manager = skip_list.transaction_manager();

        let writer_tx = tx_manager.begin();
        skip_list
            .insert("apple".to_string(), Arc::new("1".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("apply".to_string(), Arc::new("2".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("banana".to_string(), Arc::new("3".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("bandana".to_string(), Arc::new("4".to_string()), &writer_tx)
            .await;
        tx_manager.commit(&writer_tx).unwrap();

        let reader_tx = tx_manager.begin();
        let scan = skip_list.prefix_scan("app", &reader_tx.snapshot);
        assert_eq!(scan.len(), 2);
        assert_eq!(scan[0].0, "apple".to_string());
        assert_eq!(scan[1].0, "apply".to_string());

        let scan2 = skip_list.prefix_scan("ban", &reader_tx.snapshot);
        assert_eq!(scan2.len(), 2);
    }

    #[tokio::test]
    async fn test_range_stream() {
        let skip_list: SkipList<String, String> = SkipList::new();
        let tx_manager = skip_list.transaction_manager();

        let writer_tx = tx_manager.begin();
        skip_list
            .insert("a".to_string(), Arc::new("1".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("b".to_string(), Arc::new("2".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("c".to_string(), Arc::new("3".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("d".to_string(), Arc::new("4".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("e".to_string(), Arc::new("5".to_string()), &writer_tx)
            .await;
        tx_manager.commit(&writer_tx).unwrap();

        let reader_tx = tx_manager.begin();
        let start_key = "b".to_string();
        let end_key = "d".to_string();
        let range_stream = skip_list.range_stream(&start_key, &end_key, &reader_tx.snapshot);
        pin_mut!(range_stream);
        assert_eq!(range_stream.next().await.unwrap().0, "b".to_string());
        assert_eq!(range_stream.next().await.unwrap().0, "c".to_string());
        assert_eq!(range_stream.next().await.unwrap().0, "d".to_string());
        assert!(range_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_prefix_scan_stream() {
        let skip_list: SkipList<String, String> = SkipList::new();
        let tx_manager = skip_list.transaction_manager();

        let writer_tx = tx_manager.begin();
        skip_list
            .insert("apple".to_string(), Arc::new("1".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("apply".to_string(), Arc::new("2".to_string()), &writer_tx)
            .await;
        skip_list
            .insert("banana".to_string(), Arc::new("3".to_string()), &writer_tx)
            .await;
        tx_manager.commit(&writer_tx).unwrap();

        let reader_tx = tx_manager.begin();
        let prefix_key = "app".to_string();
        let scan_stream = skip_list.prefix_scan_stream(&prefix_key, &reader_tx.snapshot);
        pin_mut!(scan_stream);
        assert_eq!(scan_stream.next().await.unwrap().0, "apple".to_string());
        assert_eq!(scan_stream.next().await.unwrap().0, "apply".to_string());
        assert!(scan_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_concurrent_insert() {
        let skip_list = Arc::new(SkipList::new());
        let tx_manager = skip_list.transaction_manager().clone();
        let mut tasks = vec![];

        for i in 0..1000 {
            let skip_list = skip_list.clone();
            let tx_manager = tx_manager.clone();
            tasks.push(tokio::spawn(async move {
                let tx = tx_manager.begin();
                skip_list
                    .insert(i.to_string(), Arc::new(i.to_string()), &tx)
                    .await;
                tx_manager.commit(&tx).unwrap();
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(skip_list.len(), 1000);
    }

    #[tokio::test]
    async fn test_concurrent_insert_and_remove() {
        let skip_list = Arc::new(SkipList::new());
        let tx_manager = skip_list.transaction_manager().clone();

        // Insert 1000 items.
        let mut tasks = vec![];
        for i in 0..1000 {
            let skip_list = skip_list.clone();
            let tx_manager = tx_manager.clone();
            tasks.push(tokio::spawn(async move {
                let tx = tx_manager.begin();
                skip_list
                    .insert(i.to_string(), Arc::new(i.to_string()), &tx)
                    .await;
                tx_manager.commit(&tx).unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let _reader_tx = tx_manager.begin();
        assert_eq!(skip_list.len(), 1000);

        // Concurrently remove half of the items.
        let mut tasks = vec![];
        for i in 0..500 {
            let skip_list = skip_list.clone();
            let tx_manager = tx_manager.clone();
            tasks.push(tokio::spawn(async move {
                let tx = tx_manager.begin();
                skip_list.remove(&i.to_string(), &tx).await;
                tx_manager.commit(&tx).unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        let final_tx = tx_manager.begin();
        for i in 0..500 {
            assert!(!skip_list.contains_key(&i.to_string(), &final_tx));
        }
        for i in 500..1000 {
            assert!(skip_list.contains_key(&i.to_string(), &final_tx));
        }
    }

    #[tokio::test]
    async fn test_stress_concurrent_operations() {
        let skip_list = Arc::new(SkipList::<String, String>::new());
        let tx_manager = skip_list.transaction_manager().clone();
        let num_tasks = 100;
        let ops_per_task = 100;
        let key_range = 500;

        let mut tasks = vec![];
        for i in 0..num_tasks {
            let skip_list = skip_list.clone();
            let tx_manager = tx_manager.clone();
            tasks.push(tokio::spawn(async move {
                let mut rng = rand::rngs::StdRng::seed_from_u64(i as u64);
                let tx = tx_manager.begin();
                for _ in 0..ops_per_task {
                    let key = rng.gen_range(0..key_range);
                    match rng.gen_range(0..5) {
                        0 => {
                            skip_list
                                .insert(key.to_string(), Arc::new(key.to_string()), &tx)
                                .await;
                        }
                        1 => {
                            skip_list.get(&key.to_string(), &tx);
                        }
                        2 => {
                            skip_list.remove(&key.to_string(), &tx).await;
                        }
                        3 => {
                            let start = rng.gen_range(0..key_range).to_string();
                            let end = rng
                                .gen_range(start.parse::<i32>().unwrap()..key_range)
                                .to_string();
                            let range = skip_list.range(&start, &end, &tx.snapshot);
                            // Check that the snapshot is consistent.
                            for i in 0..range.len().saturating_sub(1) {
                                assert!(range[i].0 <= range[i + 1].0);
                            }
                        }
                        4 => {
                            let prefix = rng.gen_range(0..key_range).to_string();
                            let scan = skip_list.prefix_scan(&prefix, &tx.snapshot);
                            // Check that the snapshot is consistent.
                            for (key, _) in scan {
                                assert!(key.starts_with(&prefix));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                tx_manager.commit(&tx).unwrap();
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_concurrent_range_stream_modifications() {
        let skip_list = Arc::new(SkipList::<String, String>::new());
        let tx_manager = skip_list.transaction_manager().clone();
        let num_modifiers = 10;
        let ops_per_modifier = 100;
        let key_range = 100;

        // Spawn tasks that continuously modify the skip list
        let mut modifier_tasks = vec![];
        for i in 0..num_modifiers {
            let skip_list_clone = skip_list.clone();
            let tx_manager = tx_manager.clone();
            modifier_tasks.push(tokio::spawn(async move {
                let mut rng = rand::rngs::StdRng::seed_from_u64(i as u64);
                let tx = tx_manager.begin();
                for _ in 0..ops_per_modifier {
                    let key = rng.gen_range(0..key_range).to_string();
                    match rng.gen_range(0..2) {
                        0 => {
                            skip_list_clone
                                .insert(key.clone(), Arc::new(key.clone()), &tx)
                                .await;
                        }
                        1 => {
                            skip_list_clone.remove(&key, &tx).await;
                        }
                        _ => unreachable!(),
                    }
                }
                tx_manager.commit(&tx).unwrap();
            }));
        }

        // Run range stream on the main thread while modifications are happening
        let start_key = "10".to_string();
        let end_key = "50".to_string();
        let tx = tx_manager.begin();
        let stream = skip_list.range_stream(&start_key, &end_key, &tx.snapshot);
        pin_mut!(stream);

        let mut prev_key = None;
        while let Some((key, _)) = stream.next().await {
            // Assert that keys are within the range and sorted
            assert!(key >= start_key && key <= end_key);
            if let Some(pk) = prev_key {
                assert!(key >= pk);
            }
            prev_key = Some(key);
        }

        // Wait for all modifier tasks to complete
        for task in modifier_tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_concurrent_prefix_scan_modifications() {
        let skip_list = Arc::new(SkipList::<String, String>::new());
        let tx_manager = skip_list.transaction_manager().clone();
        let num_modifiers = 10;
        let ops_per_modifier = 100;
        let key_range = 100;

        // Spawn tasks that continuously modify the skip list
        let mut modifier_tasks = vec![];
        for i in 0..num_modifiers {
            let skip_list_clone = skip_list.clone();
            let tx_manager = tx_manager.clone();
            modifier_tasks.push(tokio::spawn(async move {
                let mut rng = rand::rngs::StdRng::seed_from_u64(i as u64);
                let tx = tx_manager.begin();
                for _ in 0..ops_per_modifier {
                    let key = rng.gen_range(0..key_range).to_string();
                    match rng.gen_range(0..2) {
                        0 => {
                            skip_list_clone
                                .insert(key.clone(), Arc::new(key.clone()), &tx)
                                .await;
                        }
                        1 => {
                            skip_list_clone.remove(&key, &tx).await;
                        }
                        _ => unreachable!(),
                    }
                }
                tx_manager.commit(&tx).unwrap();
            }));
        }

        // Run prefix scan stream on the main thread while modifications are happening
        let prefix = "1".to_string(); // Scan for keys starting with '1'
        let tx = tx_manager.begin();
        let stream = skip_list.prefix_scan_stream(&prefix, &tx.snapshot);
        pin_mut!(stream);

        let mut prev_key = None;
        while let Some((key, _)) = stream.next().await {
            // Assert that keys start with the prefix and are sorted
            assert!(key.starts_with(&prefix));
            if let Some(pk) = prev_key {
                assert!(key >= pk);
            }
            prev_key = Some(key);
        }

        // Wait for all modifier tasks to complete
        for task in modifier_tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_write_skew_prevention() {
        let skip_list = Arc::new(SkipList::<String, i32>::new());
        let tx_manager = skip_list.transaction_manager();

        // Initialize two keys, x and y, with a sum of 100.
        let setup_tx = tx_manager.begin();
        skip_list
            .insert("x".to_string(), Arc::new(60), &setup_tx)
            .await;
        skip_list
            .insert("y".to_string(), Arc::new(40), &setup_tx)
            .await;
        tx_manager.commit(&setup_tx).unwrap();

        // Transaction 1: Reads x and y, then writes to x.
        let tx1 = tx_manager.begin();
        let x1 = skip_list.get(&"x".to_string(), &tx1).unwrap();
        let y1 = skip_list.get(&"y".to_string(), &tx1).unwrap();
        assert_eq!(*x1 + *y1, 100);

        // Transaction 2: Reads x and y, then writes to y.
        let tx2 = tx_manager.begin();
        let x2 = skip_list.get(&"x".to_string(), &tx2).unwrap();
        let y2 = skip_list.get(&"y".to_string(), &tx2).unwrap();
        assert_eq!(*x2 + *y2, 100);

        // In a simple SI implementation, both transactions would be able to commit,
        // leading to a final state where x=10 and y=10, and the sum is 20, violating the invariant.

        // Tx1 writes based on its read.
        skip_list.insert("x".to_string(), Arc::new(10), &tx1).await;

        // Tx2 writes based on its read.
        skip_list.insert("y".to_string(), Arc::new(10), &tx2).await;

        // Now, attempt to commit both. One must fail.
        let commit1_result = tx_manager.commit(&tx1);
        let commit2_result = tx_manager.commit(&tx2);

        let success = commit1_result.is_ok() || commit2_result.is_ok();
        let failure = commit1_result.is_err() || commit2_result.is_err();

        assert!(success, "At least one transaction should succeed");
        assert!(
            failure,
            "At least one transaction should fail due to write skew"
        );

        // Further, check that the error is the one we expect.
        if let Err(e) = &commit1_result {
            assert_eq!(*e, error::FluxError::SerializationConflict);
        }
        if let Err(e) = &commit2_result {
            assert_eq!(*e, error::FluxError::SerializationConflict);
        }

        // Verify the final state is consistent.
        let final_tx = tx_manager.begin();
        let final_x = skip_list
            .get(&"x".to_string(), &final_tx)
            .unwrap_or(Arc::new(0));
        let final_y = skip_list
            .get(&"y".to_string(), &final_tx)
            .unwrap_or(Arc::new(0));

        if commit1_result.is_ok() {
            // Tx1 succeeded, Tx2 failed.
            assert_eq!(*final_x, 10);
            assert_eq!(*final_y, 40); // y should be unchanged
        } else {
            // Tx2 succeeded, Tx1 failed.
            assert_eq!(*final_x, 60); // x should be unchanged
            assert_eq!(*final_y, 10);
        }
        assert_ne!(*final_x + *final_y, 20);
    }

    #[tokio::test]
    async fn test_vacuum() {
        let skip_list = Arc::new(SkipList::<String, i32>::new());
        let tx_manager = skip_list.transaction_manager();

        // Tx1: Insert x=10, y=20
        let tx1 = tx_manager.begin();
        skip_list.insert("x".to_string(), Arc::new(10), &tx1).await;
        skip_list.insert("y".to_string(), Arc::new(20), &tx1).await;
        tx_manager.commit(&tx1).unwrap();

        // Tx2: Delete x, Update y to 30
        let tx2 = tx_manager.begin();
        skip_list.remove(&"x".to_string(), &tx2).await;
        skip_list.insert("y".to_string(), Arc::new(30), &tx2).await; // This prepends a new version, does not expire the old one.
        tx_manager.commit(&tx2).unwrap();

        // Run vacuum. Only the version of `x` was explicitly removed.
        let (versions_removed, _keys_removed) = skip_list.vacuum().await.unwrap();

        // Only the explicitly `remove`d version of 'x' should be cleaned up.
        assert_eq!(
            versions_removed, 1,
            "Vacuum should have removed one dead version."
        );

        // A final check to ensure the correct data is still visible.
        let final_tx = tx_manager.begin();
        assert!(skip_list.get(&"x".to_string(), &final_tx).is_none());
        assert_eq!(*skip_list.get(&"y".to_string(), &final_tx).unwrap(), 30);
        tx_manager.commit(&final_tx).unwrap();

        // --- Test multiple dead versions for the same key ---
        // Create a situation with multiple expired versions for a single key 'z'
        let tx3 = tx_manager.begin();
        skip_list.insert("z".to_string(), Arc::new(1), &tx3).await;
        tx_manager.commit(&tx3).unwrap();

        let tx4 = tx_manager.begin();
        skip_list.remove(&"z".to_string(), &tx4).await; // Expires z=1
        tx_manager.commit(&tx4).unwrap();

        let tx5 = tx_manager.begin();
        skip_list.insert("z".to_string(), Arc::new(2), &tx5).await;
        tx_manager.commit(&tx5).unwrap();

        let tx6 = tx_manager.begin();
        skip_list.remove(&"z".to_string(), &tx6).await; // Expires z=2
        tx_manager.commit(&tx6).unwrap();

        let tx7 = tx_manager.begin();
        skip_list.insert("z".to_string(), Arc::new(3), &tx7).await; // Current visible version
        tx_manager.commit(&tx7).unwrap();

        // The version chain for 'z' is now 3 -> 2(expired) -> 1(expired)
        // The vacuum should remove the two expired versions.
        let (versions_removed_z, _) = skip_list.vacuum().await.unwrap();
        assert_eq!(versions_removed_z, 2);

        // Check that the correct version of 'z' is visible.
        let final_tx_z = tx_manager.begin();
        assert_eq!(*skip_list.get(&"z".to_string(), &final_tx_z).unwrap(), 3);
    }

    #[tokio::test]
    async fn test_vacuum_removes_node() {
        let skip_list = Arc::new(SkipList::<String, i32>::new());
        let tx_manager = skip_list.transaction_manager();

        // Tx1: Insert "a"
        let tx1 = tx_manager.begin();
        skip_list.insert("a".to_string(), Arc::new(1), &tx1).await;
        tx_manager.commit(&tx1).unwrap();

        // Tx2: Remove "a"
        let tx2 = tx_manager.begin();
        assert_eq!(
            skip_list.remove(&"a".to_string(), &tx2).await.unwrap(),
            Arc::new(1)
        );
        tx_manager.commit(&tx2).unwrap();

        // Before vacuum, the node for "a" still exists but has no visible versions.
        let tx3 = tx_manager.begin();
        assert!(skip_list.get(&"a".to_string(), &tx3).is_none());
        tx_manager.commit(&tx3).unwrap();

        // Run vacuum. This should remove the dead version of "a" and then
        // mark the now-empty node as "deleted".
        let (versions_removed, keys_removed) = skip_list.vacuum().await.unwrap();
        assert_eq!(versions_removed, 1);
        assert_eq!(keys_removed, 1);

        // After vacuum, the key should still be gone. Traversal should physically
        // remove the node, but we can't easily verify that here. We just
        // confirm the key remains inaccessible.
        let tx4 = tx_manager.begin();
        assert!(skip_list.get(&"a".to_string(), &tx4).is_none());
        tx_manager.commit(&tx4).unwrap();
    }

    #[tokio::test]
    async fn test_remove_respects_snapshot() {
        let skip_list = Arc::new(SkipList::<String, i32>::new());
        let tx_manager = skip_list.transaction_manager();

        // Tx1: Insert "a" with value 1
        let tx1 = tx_manager.begin();
        skip_list.insert("a".to_string(), Arc::new(1), &tx1).await;
        tx_manager.commit(&tx1).unwrap();

        // Tx2 (Updater): Start a transaction to update "a" to 2, but DON'T commit yet.
        // This creates a new version that is not visible to other transactions.
        let tx2_updater = tx_manager.begin();
        skip_list
            .insert("a".to_string(), Arc::new(2), &tx2_updater)
            .await;

        // Tx3 (Remover): Start a new transaction. Its snapshot should only see "a" = 1.
        let tx3_remover = tx_manager.begin();

        // The `get` should see the old value.
        assert_eq!(
            *skip_list.get(&"a".to_string(), &tx3_remover).unwrap(),
            1,
            "Remover should see the initial value"
        );

        // Now, `remove` should find the visible version (value 1) and expire it.
        // The old, incorrect implementation would have tried to expire version 2.
        let removed_value = skip_list.remove(&"a".to_string(), &tx3_remover).await;
        assert_eq!(
            *removed_value.unwrap(),
            1,
            "Remove should act on the visible version"
        );

        // Commit the removal.
        tx_manager.commit(&tx3_remover).unwrap();

        // Tx4 (Final Reader): A new transaction should see the key as removed.
        let tx4_reader = tx_manager.begin();
        assert!(
            skip_list.get(&"a".to_string(), &tx4_reader).is_none(),
            "A new transaction should not see the key"
        );

        // The uncommitted updater transaction should now fail if it tries to commit.
        let commit_result = tx_manager.commit(&tx2_updater);
        assert!(
            commit_result.is_err(),
            "Updater transaction should fail to commit due to conflict"
        );
        assert_eq!(
            commit_result.unwrap_err(),
            error::FluxError::SerializationConflict
        );
    }

    #[tokio::test]
    async fn test_vacuum_handles_uncommitted_expirer() {
        let skip_list = Arc::new(SkipList::<String, i32>::new());
        let tx_manager = skip_list.transaction_manager();

        // Tx1: Insert "a" with value 1 and commit.
        let tx1 = tx_manager.begin();
        skip_list.insert("a".to_string(), Arc::new(1), &tx1).await;
        tx_manager.commit(&tx1).unwrap();

        // Tx2: Remove "a" but DO NOT commit yet.
        // This creates a version (value 1) expired by an uncommitted transaction (Tx2).
        let tx2_expirer = tx_manager.begin();
        let removed_val = skip_list.remove(&"a".to_string(), &tx2_expirer).await;
        assert_eq!(*removed_val.unwrap(), 1);

        // Run vacuum. It should NOT remove the version expired by tx2_expirer
        // because tx2_expirer is still active.
        let (versions_removed_before_commit, keys_removed_before_commit) =
            skip_list.vacuum().await.unwrap();
        assert_eq!(
            versions_removed_before_commit, 0,
            "No versions should be removed by vacuum before expirer commits"
        );
        assert_eq!(
            keys_removed_before_commit, 0,
            "No keys should be removed by vacuum before expirer commits"
        );

        // Verify that "a" is still visible to an old snapshot (if any) or not visible to new ones.
        let reader_tx_before_commit = tx_manager.begin();
        assert_eq!(
            *skip_list
                .get(&"a".to_string(), &reader_tx_before_commit)
                .unwrap(),
            1,
            "Key 'a' should still be visible with its original value to a new reader before expirer commits."
        );

        // Commit tx2_expirer. Now the version expired by tx2 is truly dead.
        tx_manager.commit(&tx2_expirer).unwrap();

        // Run vacuum again. Now it SHOULD remove the version.
        let (versions_removed_after_commit, keys_removed_after_commit) =
            skip_list.vacuum().await.unwrap();
        assert_eq!(
            versions_removed_after_commit, 1,
            "One version should be removed by vacuum after expirer commits"
        );
        assert_eq!(
            keys_removed_after_commit, 1,
            "One key should be removed by vacuum after expirer commits"
        );

        // Verify "a" is still gone.
        let final_reader_tx = tx_manager.begin();
        assert!(skip_list.get(&"a".to_string(), &final_reader_tx).is_none());
    }

    #[tokio::test]
    async fn test_transaction_status_pruning() {
        let skip_list = Arc::new(SkipList::<String, i32>::new());
        let tx_manager = skip_list.transaction_manager();

        let num_transactions = 100;
        let mut committed_tx_ids = Vec::new();

        // Create and commit a number of transactions
        for i in 0..num_transactions {
            let tx = tx_manager.begin();
            skip_list
                .insert(format!("key{}", i), Arc::new(i), &tx)
                .await;
            tx_manager.commit(&tx).unwrap();
            committed_tx_ids.push(tx.id);
        }

        // At this point, the `statuses` map should contain entries for all committed transactions.
        // The exact count might be `num_transactions` + 1 (for the initial txid 0 or 1).
        let initial_status_count = tx_manager.statuses_len();
        assert!(initial_status_count >= num_transactions as usize);

        let (_versions_removed, _keys_removed) = skip_list.vacuum().await.unwrap();
        // We expect some versions/keys to be removed if there were any logical deletions.
        // For this test, we just care about status pruning.

        // After vacuum, the `statuses` map should be pruned.
        // The number of statuses should be significantly less than the initial count,
        // ideally close to 0 if no active transactions remain and all versions are gone.
        let final_status_count = tx_manager.statuses_len();
        // The `min_retainable_txid` will be the current `next_txid` if no active transactions.
        // So, only statuses for transactions >= `min_retainable_txid` should remain.
        // In this simple case, it should be very small, possibly 0 or 1 (for the next_txid itself).
        assert!(
            final_status_count < initial_status_count,
            "Transaction statuses should have been pruned."
        );
        assert!(
            final_status_count <= 1,
            "Expected very few statuses remaining after pruning."
        ); // Should be 0 or 1 (for the next_txid)
    }
}
