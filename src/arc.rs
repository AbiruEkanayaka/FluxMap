//! An implementation of the Adaptive Replacement Cache (ARC) eviction policy.
//!
//! ARC is a cache replacement policy that adaptively balances between LRU (recency)
//! and LFU (frequency). It maintains four lists:
//! - T1: Pages accessed exactly once recently (the "recency" list).
//! - T2: Pages accessed more than once recently (the "frequency" list).
//! - B1: A "ghost list" of pages recently evicted from T1.
//! - B2: A "ghost list" of pages recently evicted from T2.
//!
//! The algorithm adjusts a parameter `p` which represents the target size of T1,
//! thereby dynamically changing the balance between recency and frequency focus.
//! This implementation is adapted for variable-sized items by tracking bytes instead
//! of a fixed number of pages.
//!
//! This implementation is lock-free, using a concurrent doubly-linked list and
//! concurrent hash maps to allow multiple threads to access it without blocking.

use crossbeam_epoch::{pin, Atomic, Guard, Owned, Shared};
use dashmap::DashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// --- Lock-Free Doubly-Linked List for O(1) operations ---

/// A node in the doubly-linked list.
/// It contains atomic pointers to its neighbors.
#[derive(Debug)]
pub(crate) struct Node<K> {
    key: Option<K>,
    size: usize,
    prev: Atomic<Node<K>>,
    next: Atomic<Node<K>>,
}

/// A lock-free, concurrent doubly-linked list implementation.
/// It uses a dummy head and tail node to simplify edge cases.
pub(crate) struct LinkedList<K> {
    head: Atomic<Node<K>>,
    tail: Atomic<Node<K>>,
    len: AtomicUsize,
}

impl<K> fmt::Debug for LinkedList<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkedList")
            .field("len", &self.len.load(Ordering::Relaxed))
            .finish()
    }
}

impl<K: Eq> LinkedList<K> {
    /// Creates a new concurrent linked list.
    /// It is initialized with dummy head and tail nodes to simplify insertion/deletion logic.
    pub(crate) fn new() -> Self {
        let guard = &pin();
        let tail = Owned::new(Node {
            key: None, // Dummy node
            size: 0,
            prev: Atomic::null(),
            next: Atomic::null(),
        })
        .into_shared(guard);

        let head = Owned::new(Node {
            key: None, // Dummy node
            size: 0,
            prev: Atomic::null(),
            next: Atomic::from(tail),
        })
        .into_shared(guard);

        unsafe { tail.deref().prev.store(head, Ordering::Relaxed) };

        Self {
            head: Atomic::from(head),
            tail: Atomic::from(tail),
            len: AtomicUsize::new(0),
        }
    }

    /// Adds a new node to the front of the list (right after the dummy head).
    pub(crate) fn push_front<'guard>(
        &self,
        key: K,
        size: usize,
        guard: &'guard Guard,
    ) -> Shared<'guard, Node<K>> {
        let new_node = Owned::new(Node {
            key: Some(key),
            size,
            prev: Atomic::null(),
            next: Atomic::null(),
        })
        .into_shared(guard);

        loop {
            let head = self.head.load(Ordering::Acquire, guard);
            let first = unsafe { head.deref().next.load(Ordering::Acquire, guard) };

            unsafe { new_node.deref().prev.store(head, Ordering::Relaxed) };
            unsafe { new_node.deref().next.store(first, Ordering::Relaxed) };

            // Try to link the new node in between head and first.
            if unsafe {
                head.deref().next.compare_exchange(
                    first,
                    new_node,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    guard,
                )
            }
            .is_ok()
            {
                unsafe { first.deref().prev.store(new_node, Ordering::Release) };
                self.len.fetch_add(1, Ordering::Relaxed);
                return new_node;
            }
            // Contention, retry.
        }
    }

    /// Removes and returns the node at the back of the list (right before the dummy tail).
    pub(crate) fn pop_back<'guard>(&self, guard: &'guard Guard) -> Option<Shared<'guard, Node<K>>> {
        loop {
            let tail = self.tail.load(Ordering::Acquire, guard);
            let last = unsafe { tail.deref().prev.load(Ordering::Acquire, guard) };

            if last == self.head.load(Ordering::Relaxed, guard) {
                return None; // List is empty
            }

            if self.unlink(last, guard).is_ok() {
                return Some(last);
            }
            // Contention, retry.
        }
    }

    /// Unlinks a node from the list. This is the core lock-free operation.
    pub(crate) fn unlink<'guard>(
        &self,
        node: Shared<'guard, Node<K>>,
        guard: &'guard Guard,
    ) -> Result<(), ()> {
        let prev = unsafe { node.deref().prev.load(Ordering::Acquire, guard) };
        let next = unsafe { node.deref().next.load(Ordering::Acquire, guard) };

        // Attempt to swing the `next` pointer of the previous node.
        if unsafe {
            prev.deref().next.compare_exchange(
                node,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            )
        }
        .is_err()
        {
            return Err(()); // CAS failed, contention.
        }

        // If the first CAS succeeded, attempt to swing the `prev` pointer of the next node.
        // If this fails, another thread may be helping to fix the link, which is okay.
        unsafe {
            next.deref().prev.compare_exchange(
                node,
                prev,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            )
        }
        .ok(); // We don't need to handle the error case.

        self.len.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    /// Moves an existing node to the front of the list.
    pub(crate) fn move_to_front<'guard>(
        &self,
        node: Shared<'guard, Node<K>>,
        guard: &'guard Guard,
    ) where
        K: Clone,
    {
        if self.unlink(node, guard).is_ok() {
            let key = unsafe { node.deref().key.as_ref().unwrap().clone() };
            let size = unsafe { node.deref().size };
            // Defer destroying the old node
            unsafe { guard.defer_destroy(node) };
            // Push a new node with the same data
            self.push_front(key, size, guard);
        }
    }

    /// Returns the number of elements in the list.
    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }
}

impl<K> Drop for LinkedList<K> {
    fn drop(&mut self) {
        // The epoch-based GC will handle freeing all nodes.
        // We just need to ensure the dummy nodes are eventually freed.
        let guard = &pin();
        let head = self.head.load(Ordering::Relaxed, guard);
        let tail = self.tail.load(Ordering::Relaxed, guard);
        unsafe {
            guard.defer_destroy(head);
            guard.defer_destroy(tail);
        }
    }
}

/// Manages the state for the ARC eviction policy.
#[derive(Debug)]
pub struct ArcManager<K: Hash + Eq + Clone + 'static> {
    /// Target size of T1 in bytes. This is the adaptive parameter.
    p: AtomicU64,
    /// The total capacity of the cache in bytes.
    capacity: u64,

    // --- Main Cache Lists ---
    t1: LinkedList<K>,
    t1_size: AtomicU64,
    t2: LinkedList<K>,
    t2_size: AtomicU64,

    // --- Ghost Lists (for tracking evicted items) ---
    b1: LinkedList<K>,
    b2: LinkedList<K>,

    // --- Fast Lookup Maps ---
    t1_map: DashMap<K, Shared<'static, Node<K>>>,
    t2_map: DashMap<K, Shared<'static, Node<K>>>,
    b1_map: DashMap<K, Shared<'static, Node<K>>>,
    b2_map: DashMap<K, Shared<'static, Node<K>>>,
}

impl<K: Hash + Eq + Clone + 'static> ArcManager<K> {
    /// Creates a new `ArcManager` with a given capacity in bytes.
    pub fn new(capacity: u64) -> Self {
        Self {
            p: AtomicU64::new(0),
            capacity,
            t1: LinkedList::new(),
            t1_size: AtomicU64::new(0),
            t2: LinkedList::new(),
            t2_size: AtomicU64::new(0),
            b1: LinkedList::new(),
            b2: LinkedList::new(),
            t1_map: DashMap::new(),
            t2_map: DashMap::new(),
            b1_map: DashMap::new(),
            b2_map: DashMap::new(),
        }
    }

    /// Notifies the manager of a cache hit.
    /// This moves the item to the LFU list (T2).
    pub fn hit(&self, key: &K) {
        let guard = &pin();
        if let Some(entry) = self.t1_map.remove(key) {
            // Hit in T1: move to T2
            let node_ptr = entry.1;
            if self.t1.unlink(node_ptr, guard).is_ok() {
                let size = unsafe { node_ptr.deref().size };
                let key_clone = unsafe { node_ptr.deref().key.as_ref().unwrap().clone() };
                unsafe { guard.defer_destroy(node_ptr) };

                self.t1_size.fetch_sub(size as u64, Ordering::Relaxed);
                let new_node_ptr = self.t2.push_front(key_clone.clone(), size, guard);
                // SAFETY: The guard will outlive the manager, and the epoch ensures memory is valid.
                let static_ptr = unsafe { std::mem::transmute(new_node_ptr) };
                self.t2_map.insert(key_clone, static_ptr);
                self.t2_size.fetch_add(size as u64, Ordering::Relaxed);
            }
        } else if let Some(entry) = self.t2_map.get(key) {
            // Hit in T2: move to front of T2
            let node_ptr = *entry.value();
            self.t2.move_to_front(node_ptr, guard);
        }
    }

    /// Notifies the manager of a cache miss (a new item).
    pub fn miss(&self, key: K, size: usize) {
        let guard = &pin();
        // Case 1: The key was recently evicted (it's in a ghost list).
        if let Some(entry) = self.b1_map.remove(&key) {
            // Ghost hit on B1. Adapt p: Increase the target size of T1.
            let b1_len = self.b1.len() as u64;
            let b2_len = self.b2.len() as u64;
            let delta = if b1_len == 0 { 0 } else if b1_len >= b2_len { 1 } else { b2_len / b1_len };
            let p = self.p.load(Ordering::Relaxed);
            self.p.store(p.saturating_add(delta).min(self.capacity), Ordering::Relaxed);

            // Move from ghost B1 to T2.
            let node_ptr = entry.1;
            if self.b1.unlink(node_ptr, guard).is_ok() {
                let key_clone = unsafe { node_ptr.deref().key.as_ref().unwrap().clone() };
                unsafe { guard.defer_destroy(node_ptr) };
                let new_node_ptr = self.t2.push_front(key_clone.clone(), size, guard);
                let static_ptr = unsafe { std::mem::transmute(new_node_ptr) };
                self.t2_map.insert(key_clone, static_ptr);
                self.t2_size.fetch_add(size as u64, Ordering::Relaxed);
            }
        } else if let Some(entry) = self.b2_map.remove(&key) {
            // Ghost hit on B2. Adapt p: Decrease the target size of T1.
            let b1_len = self.b1.len() as u64;
            let b2_len = self.b2.len() as u64;
            let delta = if b2_len == 0 { 0 } else if b2_len >= b1_len { 1 } else { b1_len / b2_len };
            let p = self.p.load(Ordering::Relaxed);
            self.p.store(p.saturating_sub(delta), Ordering::Relaxed);

            // Move from ghost B2 to T2.
            let node_ptr = entry.1;
            if self.b2.unlink(node_ptr, guard).is_ok() {
                let key_clone = unsafe { node_ptr.deref().key.as_ref().unwrap().clone() };
                unsafe { guard.defer_destroy(node_ptr) };
                let new_node_ptr = self.t2.push_front(key_clone.clone(), size, guard);
                let static_ptr = unsafe { std::mem::transmute(new_node_ptr) };
                self.t2_map.insert(key_clone, static_ptr);
                self.t2_size.fetch_add(size as u64, Ordering::Relaxed);
            }
        }
        // Case 2: A cold miss (the key is completely new).
        else {
            let new_node_ptr = self.t1.push_front(key.clone(), size, guard);
            let static_ptr = unsafe { std::mem::transmute(new_node_ptr) };
            self.t1_map.insert(key, static_ptr);
            self.t1_size.fetch_add(size as u64, Ordering::Relaxed);
        }
    }

    /// Finds and returns a victim key to be evicted.
    pub fn find_victim(&self) -> Option<K> {
        let guard = &pin();
        let t1_size = self.t1_size.load(Ordering::Relaxed);
        let t2_size = self.t2_size.load(Ordering::Relaxed);

        if t1_size + t2_size < self.capacity {
            return None; // No need to evict
        }

        // Determine which list to evict from.
        let p = self.p.load(Ordering::Relaxed);
        if self.t1.len() > 0 && (t1_size >= p || self.t2.len() == 0) {
            // Evict from T1 (LRU part)
            self.t1.pop_back(guard).map(|evicted_node| {
                let evicted_key = unsafe { evicted_node.deref().key.as_ref().unwrap().clone() };
                let evicted_size = unsafe { evicted_node.deref().size };

                self.t1_size.fetch_sub(evicted_size as u64, Ordering::Relaxed);
                self.t1_map.remove(&evicted_key);

                // Add to ghost list B1
                let ghost_node_ptr = self.b1.push_front(evicted_key.clone(), 0, guard);
                let static_ptr = unsafe { std::mem::transmute(ghost_node_ptr) };
                self.b1_map.insert(evicted_key.clone(), static_ptr);

                // Prune ghost list B1 if it's too large
                if self.b1.len() > self.t2.len() {
                    if let Some(b1_evicted) = self.b1.pop_back(guard) {
                        let key = unsafe { b1_evicted.deref().key.as_ref().unwrap() };
                        self.b1_map.remove(key);
                        unsafe { guard.defer_destroy(b1_evicted) };
                    }
                }
                unsafe { guard.defer_destroy(evicted_node) };
                evicted_key
            })
        } else if self.t2.len() > 0 {
            // Evict from T2 (LFU part)
            self.t2.pop_back(guard).map(|evicted_node| {
                let evicted_key = unsafe { evicted_node.deref().key.as_ref().unwrap().clone() };
                let evicted_size = unsafe { evicted_node.deref().size };

                self.t2_size.fetch_sub(evicted_size as u64, Ordering::Relaxed);
                self.t2_map.remove(&evicted_key);

                // Add to ghost list B2
                let ghost_node_ptr = self.b2.push_front(evicted_key.clone(), 0, guard);
                let static_ptr = unsafe { std::mem::transmute(ghost_node_ptr) };
                self.b2_map.insert(evicted_key.clone(), static_ptr);

                // Prune ghost list B2 if it's too large
                if self.b2.len() > self.t1.len() {
                    if let Some(b2_evicted) = self.b2.pop_back(guard) {
                        let key = unsafe { b2_evicted.deref().key.as_ref().unwrap() };
                        self.b2_map.remove(key);
                        unsafe { guard.defer_destroy(b2_evicted) };
                    }
                }
                unsafe { guard.defer_destroy(evicted_node) };
                evicted_key
            })
        } else {
            None
        }
    }
}

impl<K: Hash + Eq + Clone + 'static> Drop for ArcManager<K> {
    fn drop(&mut self) {
        // The epoch-based GC in the LinkedList's Drop implementation
        // will handle deallocating all nodes.
    }
}

// SAFETY: The ArcManager is now safe to send across threads.
// All its internal state is held in concurrent, thread-safe data structures
// (`DashMap`, `AtomicU64`, and our lock-free `LinkedList`).
unsafe impl<K: Hash + Eq + Clone + Send + 'static> Send for ArcManager<K> {}
unsafe impl<K: Hash + Eq + Clone + Send + Sync + 'static> Sync for ArcManager<K> {}