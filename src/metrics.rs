//! Defines the metric keys and labels used throughout FluxMap.
//!
//! Using a central module for these constants helps prevent typos and ensures
//! consistency across the codebase.

// --- Metric Keys ---

/// Tracks the total number of transactions, labeled by their final status.
///
/// Labels:
/// - `status`: "committed", "aborted", "conflict"
pub const TRANSACTIONS_TOTAL: &str = "fluxmap_transactions_total";

/// Tracks the total number of user-initiated operations.
///
/// Labels:
/// - `type`: "get", "insert", "remove", "range_scan", "prefix_scan"
pub const OPERATIONS_TOTAL: &str = "fluxmap_operations_total";

/// Tracks the total number of key-value pairs evicted from memory.
///
/// Labels:
/// - `policy`: "lru", "lfu", "random", "arc"
pub const EVICTIONS_TOTAL: &str = "fluxmap_evictions_total";

/// A gauge representing the current estimated memory usage of the database in bytes.
pub const MEMORY_USAGE_BYTES: &str = "fluxmap_memory_usage_bytes";

/// Tracks cache hits for policies that support it (currently ARC).
///
/// Labels:
/// - `policy`: "arc"
pub const CACHE_HITS_TOTAL: &str = "fluxmap_cache_hits_total";

/// Tracks cache misses for policies that support it (currently ARC).
///
/// Labels:
/// - `policy`: "arc"
pub const CACHE_MISSES_TOTAL: &str = "fluxmap_cache_misses_total";

/// Tracks the total number of dead versions removed by the vacuum process.
pub const VACUUM_VERSIONS_REMOVED_TOTAL: &str = "fluxmap_vacuum_versions_removed_total";

/// Tracks the total number of empty nodes removed by the vacuum process.
pub const VACUUM_KEYS_REMOVED_TOTAL: &str = "fluxmap_vacuum_keys_removed_total";

/// A histogram measuring the duration of the vacuum process in seconds.
pub const VACUUM_DURATION_SECONDS: &str = "fluxmap_vacuum_duration_seconds";

/// Tracks the total number of bytes written to the Write-Ahead Log.
pub const WAL_BYTES_WRITTEN_TOTAL: &str = "fluxmap_wal_bytes_written_total";

/// Tracks the total number of WAL segment rotations.
pub const WAL_ROTATIONS_TOTAL: &str = "fluxmap_wal_rotations_total";

/// A histogram measuring the duration of a WAL `fsync` operation in seconds.
pub const WAL_SYNC_DURATION_SECONDS: &str = "fluxmap_wal_sync_duration_seconds";

// --- Label Keys ---

pub const LABEL_STATUS: &str = "status";
pub const LABEL_OPERATION_TYPE: &str = "type";
pub const LABEL_POLICY: &str = "policy";
