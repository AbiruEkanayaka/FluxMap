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

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;

/// Manages the state for the ARC eviction policy.
#[derive(Debug)]
pub struct ArcManager<K: Hash + Eq + Clone> {
    /// Target size of T1 in bytes. This is the adaptive parameter.
    p: u64,
    /// The total capacity of the cache in bytes.
    capacity: u64,

    // --- Main Cache Queues ---
    /// T1: Cache entries accessed exactly once. Stores (key, size).
    t1: VecDeque<(K, usize)>,
    t1_size: u64,
    /// T2: Cache entries accessed at least twice. Stores (key, size).
    t2: VecDeque<(K, usize)>,
    t2_size: u64,

    // --- Ghost Lists (for tracking evicted items) ---
    /// B1: Keys recently evicted from T1.
    b1: VecDeque<K>,
    /// B2: Keys recently evicted from T2.
    b2: VecDeque<K>,

    // --- Fast Lookup Sets ---
    t1_set: HashMap<K, usize>,
    t2_set: HashMap<K, usize>,
    b1_set: HashSet<K>,
    b2_set: HashSet<K>,
}

impl<K: Hash + Eq + Clone> ArcManager<K> {
    /// Creates a new `ArcManager` with a given capacity in bytes.
    pub fn new(capacity: u64) -> Self {
        Self {
            p: 0,
            capacity,
            t1: VecDeque::new(),
            t1_size: 0,
            t2: VecDeque::new(),
            t2_size: 0,
            b1: VecDeque::new(),
            b2: VecDeque::new(),
            t1_set: HashMap::new(),
            t2_set: HashMap::new(),
            b1_set: HashSet::new(),
            b2_set: HashSet::new(),
        }
    }

    /// Notifies the manager of a cache hit.
    /// This moves the item to the LFU list (T2).
    pub fn hit(&mut self, key: &K) {
        // If it's a hit, it must be in either T1 or T2.
        // In both cases, it gets moved to the MRU position of T2.
        if let Some(size) = self.t1_set.get(key).copied() {
            self.move_from_t1_to_t2(key, size);
        } else if self.t2_set.contains_key(key) {
            // Just move it to the front of T2 to mark it as recently used.
            self.move_to_front_of_t2(key);
        }
    }

    /// Notifies the manager of a cache miss (a new item).
    pub fn miss(&mut self, key: K, size: usize) {
        // Case 1: The key was recently evicted (it's in a ghost list).
        if self.b1_set.contains(&key) {
            // Ghost hit on B1. Adapt p: Increase the target size of T1.
            let b1_len = self.b1.len() as u64;
            let b2_len = self.b2.len() as u64;
            let delta = if b1_len == 0 { 0 } else if b1_len >= b2_len { 1 } else { b2_len / b1_len };
            self.p = (self.p + delta).min(self.capacity);
            // Move from ghost to T2.
            self.move_from_ghost_to_t2(&key, size);
        } else if self.b2_set.contains(&key) {
            // Ghost hit on B2. Adapt p: Decrease the target size of T1.
            let b1_len = self.b1.len() as u64;
            let b2_len = self.b2.len() as u64;
            let delta = if b2_len == 0 { 0 } else if b2_len >= b1_len { 1 } else { b1_len / b2_len };
            self.p = self.p.saturating_sub(delta);
            // Move from ghost to T2.
            self.move_from_ghost_to_t2(&key, size);
        }
        // Case 2: A cold miss (the key is completely new).
        else {
            self.t1_size += size as u64;
            self.t1.push_front((key.clone(), size));
            self.t1_set.insert(key, size);
        }
    }

    /// Moves a key from T1 to the front of T2.
    fn move_from_t1_to_t2(&mut self, key: &K, size: usize) {
        // Remove from T1
        if self.t1_set.remove(key).is_some() {
            self.t1_size -= size as u64;
            if let Some(pos) = self.t1.iter().position(|(k, _)| k == key) {
                self.t1.remove(pos);
            }
        }
        // Add to T2
        self.t2_size += size as u64;
        self.t2.push_front((key.clone(), size));
        self.t2_set.insert(key.clone(), size);
    }

    /// Moves an existing key in T2 to the front.
    fn move_to_front_of_t2(&mut self, key: &K) {
        if let Some(pos) = self.t2.iter().position(|(k, _)| k == key) {
            if let Some(item) = self.t2.remove(pos) {
                self.t2.push_front(item);
            }
        }
    }

    /// Helper to move a key from a ghost list to T2.
    fn move_from_ghost_to_t2(&mut self, key: &K, size: usize) {
        if self.b1_set.remove(key) {
            if let Some(pos) = self.b1.iter().position(|k| k == key) {
                self.b1.remove(pos);
            }
        }
        if self.b2_set.remove(key) {
            if let Some(pos) = self.b2.iter().position(|k| k == key) {
                self.b2.remove(pos);
            }
        }
        self.t2_size += size as u64;
        self.t2.push_front((key.clone(), size));
        self.t2_set.insert(key.clone(), size);
    }

    /// Finds and returns a victim key to be evicted.
    pub fn find_victim(&mut self) -> Option<K> {
        let t1_plus_t2_size = self.t1_size + self.t2_size;

        if t1_plus_t2_size < self.capacity {
            return None; // No need to evict
        }

        // Determine which list to evict from.
        if self.t1_size > 0 && self.t1_size >= self.p {
            // Evict from T1 (LRU part)
            self.t1.pop_back().map(|(evicted_key, evicted_size)| {
                self.t1_size -= evicted_size as u64;
                self.t1_set.remove(&evicted_key);
                self.b1.push_front(evicted_key.clone());
                self.b1_set.insert(evicted_key.clone());
                // Prune ghost list B1 if it's too large
                if self.b1.len() > self.t2.len() {
                    if let Some(b1_evicted) = self.b1.pop_back() {
                        self.b1_set.remove(&b1_evicted);
                    }
                }
                evicted_key
            })
        } else if self.t2_size > 0 {
            // Evict from T2 (LFU part)
            self.t2.pop_back().map(|(evicted_key, evicted_size)| {
                self.t2_size -= evicted_size as u64;
                self.t2_set.remove(&evicted_key);
                self.b2.push_front(evicted_key.clone());
                self.b2_set.insert(evicted_key.clone());
                // Prune ghost list B2 if it's too large
                if self.b2.len() > self.t1.len() {
                    if let Some(b2_evicted) = self.b2.pop_back() {
                        self.b2_set.remove(&b2_evicted);
                    }
                }
                evicted_key
            })
        } else {
            None
        }
    }
}

