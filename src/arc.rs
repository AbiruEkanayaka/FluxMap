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
//! This implementation uses a custom doubly-linked list to achieve O(1) complexity
//! for all core cache operations (hit, miss, and victim selection).

use std::collections::HashMap;
use std::hash::Hash;
use std::ptr::NonNull;

// --- Doubly-Linked List for O(1) operations ---

/// A node in the doubly-linked list.
#[derive(Debug)]
struct Node<K> {
    key: K,
    size: usize,
    prev: Option<NonNull<Node<K>>>,
    next: Option<NonNull<Node<K>>>,
}

/// A doubly-linked list implementation tailored for the cache.
#[derive(Debug)]
struct LinkedList<K> {
    head: Option<NonNull<Node<K>>>,
    tail: Option<NonNull<Node<K>>>,
    len: usize,
}

impl<K> LinkedList<K> {
    fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,
        }
    }

    /// Adds a new node to the front of the list.
    fn push_front(&mut self, key: K, size: usize) -> NonNull<Node<K>> {
        let new_node = Box::new(Node {
            key,
            size,
            prev: None,
            next: self.head,
        });

        let new_node_ptr = NonNull::from(Box::leak(new_node));

        if let Some(mut head) = self.head {
            unsafe { head.as_mut().prev = Some(new_node_ptr) };
        } else {
            // List was empty, new node is both head and tail
            self.tail = Some(new_node_ptr);
        }
        self.head = Some(new_node_ptr);
        self.len += 1;
        new_node_ptr
    }

    /// Removes and returns the node at the back of the list.
    fn pop_back(&mut self) -> Option<Box<Node<K>>> {
        self.tail.map(|tail| {
            // Unlink and return the node
            self.remove(tail)
        })
    }

    /// Unlinks a node from the list given a pointer to it.
    fn remove(&mut self, mut node_ptr: NonNull<Node<K>>) -> Box<Node<K>> {
        let node = unsafe { node_ptr.as_mut() };

        // Update pointers of surrounding nodes
        if let Some(mut prev) = node.prev {
            unsafe { prev.as_mut().next = node.next };
        } else {
            // This was the head node
            self.head = node.next;
        }

        if let Some(mut next) = node.next {
            unsafe { next.as_mut().prev = node.prev };
        } else {
            // This was the tail node
            self.tail = node.prev;
        }

        self.len -= 1;
        unsafe { Box::from_raw(node_ptr.as_ptr()) }
    }

    /// Moves an existing node to the front of the list.
    fn move_to_front(&mut self, mut node_ptr: NonNull<Node<K>>) {
        let node = unsafe { node_ptr.as_mut() };

        if self.head == Some(node_ptr) {
            return; // Already at the front
        }

        // Unlink the node
        if let Some(mut prev) = node.prev {
            unsafe { prev.as_mut().next = node.next };
        }
        if let Some(mut next) = node.next {
            unsafe { next.as_mut().prev = node.prev };
        }

        // If it was the tail, update the tail
        if self.tail == Some(node_ptr) {
            self.tail = node.prev;
        }

        // Link it to the front
        node.prev = None;
        node.next = self.head;
        if let Some(mut head) = self.head {
            unsafe { head.as_mut().prev = Some(node_ptr) };
        }
        self.head = Some(node_ptr);
    }
}

impl<K> Drop for LinkedList<K> {
    fn drop(&mut self) {
        // Iterate and deallocate all remaining nodes
        while let Some(node) = self.pop_back() {
            drop(node);
        }
    }
}

/// Manages the state for the ARC eviction policy.
#[derive(Debug)]
pub struct ArcManager<K: Hash + Eq + Clone> {
    /// Target size of T1 in bytes. This is the adaptive parameter.
    p: u64,
    /// The total capacity of the cache in bytes.
    capacity: u64,

    // --- Main Cache Lists ---
    t1: LinkedList<K>,
    t1_size: u64,
    t2: LinkedList<K>,
    t2_size: u64,

    // --- Ghost Lists (for tracking evicted items) ---
    b1: LinkedList<K>,
    b2: LinkedList<K>,

    // --- Fast Lookup Maps ---
    t1_map: HashMap<K, NonNull<Node<K>>>,
    t2_map: HashMap<K, NonNull<Node<K>>>,
    b1_map: HashMap<K, NonNull<Node<K>>>,
    b2_map: HashMap<K, NonNull<Node<K>>>,
}

impl<K: Hash + Eq + Clone> ArcManager<K> {
    /// Creates a new `ArcManager` with a given capacity in bytes.
    pub fn new(capacity: u64) -> Self {
        Self {
            p: 0,
            capacity,
            t1: LinkedList::new(),
            t1_size: 0,
            t2: LinkedList::new(),
            t2_size: 0,
            b1: LinkedList::new(),
            b2: LinkedList::new(),
            t1_map: HashMap::new(),
            t2_map: HashMap::new(),
            b1_map: HashMap::new(),
            b2_map: HashMap::new(),
        }
    }

    /// Notifies the manager of a cache hit.
    /// This moves the item to the LFU list (T2).
    pub fn hit(&mut self, key: &K) {
        if let Some(&node_ptr) = self.t1_map.get(key) {
            // Hit in T1: move to T2
            let node = self.t1.remove(node_ptr);
            let size = node.size;
            let key = node.key;

            self.t1_map.remove(&key);
            self.t1_size -= size as u64;

            let new_node_ptr = self.t2.push_front(key.clone(), size);
            self.t2_map.insert(key, new_node_ptr);
            self.t2_size += size as u64;
        } else if let Some(&node_ptr) = self.t2_map.get(key) {
            // Hit in T2: move to front of T2
            self.t2.move_to_front(node_ptr);
        }
    }

    /// Notifies the manager of a cache miss (a new item).
    pub fn miss(&mut self, key: K, size: usize) {
        // Case 1: The key was recently evicted (it's in a ghost list).
        if let Some(&node_ptr) = self.b1_map.get(&key) {
            // Ghost hit on B1. Adapt p: Increase the target size of T1.
            let b1_len = self.b1.len as u64;
            let b2_len = self.b2.len as u64;
            let delta = if b1_len == 0 { 0 } else if b1_len >= b2_len { 1 } else { b2_len / b1_len };
            self.p = (self.p + delta).min(self.capacity);

            // Move from ghost B1 to T2.
            let node = self.b1.remove(node_ptr);
            self.b1_map.remove(&node.key);

            let new_node_ptr = self.t2.push_front(node.key, size);
            self.t2_map.insert(key, new_node_ptr);
            self.t2_size += size as u64;
        } else if let Some(&node_ptr) = self.b2_map.get(&key) {
            // Ghost hit on B2. Adapt p: Decrease the target size of T1.
            let b1_len = self.b1.len as u64;
            let b2_len = self.b2.len as u64;
            let delta = if b2_len == 0 { 0 } else if b2_len >= b1_len { 1 } else { b1_len / b2_len };
            self.p = self.p.saturating_sub(delta);

            // Move from ghost B2 to T2.
            let node = self.b2.remove(node_ptr);
            self.b2_map.remove(&node.key);

            let new_node_ptr = self.t2.push_front(node.key, size);
            self.t2_map.insert(key, new_node_ptr);
            self.t2_size += size as u64;
        }
        // Case 2: A cold miss (the key is completely new).
        else {
            let new_node_ptr = self.t1.push_front(key.clone(), size);
            self.t1_map.insert(key, new_node_ptr);
            self.t1_size += size as u64;
        }
    }

    /// Finds and returns a victim key to be evicted.
    pub fn find_victim(&mut self) -> Option<K> {
        let t1_plus_t2_size = self.t1_size + self.t2_size;

        if t1_plus_t2_size < self.capacity {
            return None; // No need to evict
        }

        // Determine which list to evict from.
        if self.t1.len > 0 && (self.t1_size >= self.p || self.t2.len == 0) {
            // Evict from T1 (LRU part)
            self.t1.pop_back().map(|evicted_node| {
                let evicted_key = evicted_node.key;
                let evicted_size = evicted_node.size;

                self.t1_size -= evicted_size as u64;
                self.t1_map.remove(&evicted_key);

                // Add to ghost list B1
                let ghost_node_ptr = self.b1.push_front(evicted_key.clone(), 0);
                self.b1_map.insert(evicted_key.clone(), ghost_node_ptr);

                // Prune ghost list B1 if it's too large
                if self.b1.len > self.t2.len {
                    if let Some(b1_evicted) = self.b1.pop_back() {
                        self.b1_map.remove(&b1_evicted.key);
                    }
                }
                evicted_key
            })
        } else if self.t2.len > 0 {
            // Evict from T2 (LFU part)
            self.t2.pop_back().map(|evicted_node| {
                let evicted_key = evicted_node.key;
                let evicted_size = evicted_node.size;

                self.t2_size -= evicted_size as u64;
                self.t2_map.remove(&evicted_key);

                // Add to ghost list B2
                let ghost_node_ptr = self.b2.push_front(evicted_key.clone(), 0);
                self.b2_map.insert(evicted_key.clone(), ghost_node_ptr);

                // Prune ghost list B2 if it's too large
                if self.b2.len > self.t1.len {
                    if let Some(b2_evicted) = self.b2.pop_back() {
                        self.b2_map.remove(&b2_evicted.key);
                    }
                }
                evicted_key
            })
        } else {
            None
        }
    }
}

impl<K: Hash + Eq + Clone> Drop for ArcManager<K> {
    fn drop(&mut self) {
        // The LinkedList drop implementation will handle deallocating all nodes
        // in t1, t2, b1, and b2, preventing memory leaks.
    }
}

// SAFETY: The ArcManager is safe to send across threads because all its internal state,
// including the linked lists with raw pointers, is protected by a Mutex within the
// Database struct. This ensures that only one thread can access and modify the manager
// at any given time.
unsafe impl<K: Hash + Eq + Clone + Send> Send for ArcManager<K> {}