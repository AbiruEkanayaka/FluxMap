//! Defines the error types used throughout FluxMap.
use std::fmt;
use std::io;

/// The primary error enum for all fallible operations in FluxMap.
#[derive(Debug, PartialEq, Eq)]
pub enum FluxError {
    /// Represents a serialization error, which occurs when a transaction cannot be
    /// committed because it conflicts with another concurrent transaction. This is
    /// essential for maintaining Serializable Snapshot Isolation (SSI).
    ///
    /// This error indicates that a transaction was rolled back to prevent a write-skew
    /// or other anomaly. The operation can typically be safely retried.
    SerializationConflict,
    /// Occurs when `begin()` is called on a `Handle` that already has an active transaction.
    /// A handle can only manage one transaction at a time.
    TransactionAlreadyActive,
    /// Occurs when `commit()` or `rollback()` is called on a `Handle` with no active transaction.
    NoActiveTransaction,
    /// Wraps an error originating from the persistence layer.
    Persistence(PersistenceError),
    /// Represents an error during the key eviction process, e.g., no victim could be found.
    EvictionError,
    /// Represents an error in the database configuration.
    Configuration(String),
    /// The database has exceeded its configured memory limit and requires manual eviction.
    /// This error is only returned when the `EvictionPolicy` is set to `Manual`.
    MemoryLimitExceeded,
    /// A fatal, unrecoverable error occurred in a background task, such as the persistence engine.
    /// The database is in an indeterminate state and cannot accept further operations.
    FatalPersistenceError(String),
}

/// A specific error originating from the persistence layer.
#[derive(Debug, PartialEq, Eq)]
pub enum PersistenceError {
    /// An underlying I/O error from the filesystem.
    Io(String),
    /// An error during data serialization or deserialization (e.g., for the WAL or snapshots).
    Serialization(String),
    /// An error that occurred during the database recovery process.
    Recovery(String),
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PersistenceError::Io(e) => write!(f, "I/O error: {}", e),
            PersistenceError::Serialization(e) => write!(f, "Serialization error: {}", e),
            PersistenceError::Recovery(e) => write!(f, "Recovery error: {}", e),
        }
    }
}

impl std::error::Error for PersistenceError {}

impl fmt::Display for FluxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FluxError::SerializationConflict => {
                write!(f, "Serialization conflict: transaction aborted")
            }
            FluxError::TransactionAlreadyActive => {
                write!(f, "A transaction is already active on this handle")
            }
            FluxError::NoActiveTransaction => {
                write!(f, "No active transaction on this handle")
            }
            FluxError::Persistence(e) => write!(f, "Persistence error: {}", e),
            FluxError::EvictionError => write!(f, "Eviction error: could not find or evict a key"),
            FluxError::MemoryLimitExceeded => {
                write!(f, "Memory limit exceeded, manual eviction required")
            }
            FluxError::Configuration(e) => write!(f, "Configuration error: {}", e),
            FluxError::FatalPersistenceError(e) => {
                write!(f, "Fatal persistence error: {}. The database is in a terminal state.", e)
            }
        }
    }
}

impl std::error::Error for FluxError {}

impl From<io::Error> for FluxError {
    fn from(err: io::Error) -> Self {
        FluxError::Persistence(PersistenceError::Io(err.to_string()))
    }
}
