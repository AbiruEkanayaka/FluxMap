//! Implements the garbage collection (vacuum) process for FluxMap.
//!
//! The vacuum process is responsible for reclaiming memory from "dead" data versions
//! that are no longer visible to any active or future transaction. This prevents
//! unbounded memory growth in workloads with frequent updates or deletes.

use crate::SkipList;
use crate::transaction::TransactionStatus;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::atomic::Ordering;

impl<K, V> SkipList<K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + std::hash::Hash
        + Eq
        + std::borrow::Borrow<str>
        + Serialize
        + DeserializeOwned,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned,
{
    /// Scans the database and removes dead data versions to reclaim space.
    ///
    /// This function performs two main tasks:
    /// 1.  **Version Pruning:** It iterates through every key's version chain and removes
    ///     any version that is considered "dead". A version is dead if it was expired
    ///     by a transaction that has since committed, and that transaction is older
    ///     than any currently active transaction (i.e., it's before the "vacuum horizon").
    ///
    /// 2.  **Node Pruning:** If, after pruning versions, a key's version chain becomes
    ///     empty, the node itself is logically marked as deleted. A subsequent traversal
    ///     of the skiplist will then physically unlink and reclaim the memory for this node.
    ///
    /// Finally, it updates the `TransactionManager` to allow it to prune its own
    /// historical transaction status records, further saving memory.
    ///
    /// # Returns
    ///
    /// A `Result` containing a tuple of `(versions_removed, keys_removed)`.
    pub async fn vacuum(&self) -> Result<(usize, usize), ()> {
        let tx_manager = self.transaction_manager();

        // 1. Find the vacuum horizon. This is the oldest active transaction ID.
        // Any version expired by a transaction that committed *before* this horizon is safe to remove.
        let vacuum_horizon = tx_manager
            .get_active_txids()
            .iter()
            .min()
            .copied()
            .unwrap_or_else(|| tx_manager.get_current_txid());

        let mut versions_removed = 0;
        let mut keys_removed_count = 0;

        let guard = &crossbeam_epoch::pin();
        let mut current_node_ptr = self.head.load(Ordering::Relaxed, guard);

        // Iterate through all nodes in the base level of the skip list.
        while let Some(node) = unsafe {
            // SAFETY: `current_node_ptr` is a `Shared` pointer. `as_ref()` is safe as `current_node_ptr` is checked for null.
            // The `guard` ensures the memory is protected.
            current_node_ptr.as_ref()
        } {
            if node.key.is_some() {
                let mut prev_version_ptr = &node.value;
                let mut has_live_versions = false; // True if any version in this chain is not dead

                loop {
                    let version_head = prev_version_ptr.load(Ordering::Acquire, guard);

                    let version_node = match unsafe {
                        // SAFETY: `version_head` is a `Shared` pointer. `as_ref()` is safe as `version_head` is checked for null.
                        // The `guard` ensures the memory is protected.
                        version_head.as_ref()
                    } {
                        Some(v) => v,
                        None => break, // End of the version chain
                    };

                    let expirer_id = version_node.version.expirer_txid.load(Ordering::Relaxed);

                    // A version is dead if it was expired by a committed transaction
                    // that is older than the vacuum horizon.
                    let is_dead = if expirer_id != 0 {
                        if let Some(status) = tx_manager.get_status(expirer_id) {
                            status == TransactionStatus::Committed && expirer_id < vacuum_horizon
                        } else {
                            // If status is not found, it must be a very old transaction.
                            // We can consider it committed if it's older than the horizon.
                            expirer_id < vacuum_horizon
                        }
                    } else {
                        // If expirer_id is 0, it's not expired, thus not dead.
                        false
                    };

                    if is_dead {
                        // Atomically unlink the dead version from the chain.
                        let next_version_ptr = version_node.next.load(Ordering::Relaxed, guard);
                        if prev_version_ptr
                            .compare_exchange(
                                version_head,
                                next_version_ptr,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                                guard, // Re-added the missing guard argument
                            )
                            .is_ok()
                        {
                            unsafe {
                                // SAFETY: `version_head` is a `Shared` pointer to a `VersionNode` that has been
                                // successfully unlinked from the version chain via `compare_exchange`.
                                // `defer_destroy` schedules its reclamation after the current epoch.
                                guard.defer_destroy(version_head)
                            };
                            versions_removed += 1;
                            // Pointer has been swung, so we loop again on the same prev_version_ptr
                            continue;
                        }
                        // If CAS failed, this version is still "live" for now (could be a race),
                        // so we treat it as live and move to the next.
                        has_live_versions = true;
                    } else {
                        // This version is not dead, so it's a live version.
                        has_live_versions = true;
                    }

                    // Move to the next version.
                    prev_version_ptr = &version_node.next;
                }

                // If after checking all versions, no live versions remain in the chain,
                // and the node itself is not already marked for deletion,
                // then the node can be logically marked for removal.
                if !has_live_versions && !node.deleted.load(Ordering::Acquire) {
                    node.deleted.store(true, Ordering::Release);
                    keys_removed_count += 1;
                }
            }
            current_node_ptr = node.next[0].load(Ordering::Relaxed, guard);
        }

        // After vacuuming the skip list, update the minimum retainable TXID
        // and prune old transaction statuses from the manager.
        tx_manager.min_retainable_txid.store(vacuum_horizon, Ordering::Release);
        tx_manager.prune_statuses();

        Ok((versions_removed, keys_removed_count))
    }
}
