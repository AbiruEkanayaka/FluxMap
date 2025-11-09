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
    /// Wraps an error originating from the persistence layer, such as an I/O error
    /// during WAL writing, snapshotting, or recovery.
    PersistenceError(String),
}

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
            FluxError::PersistenceError(e) => write!(f, "Persistence error: {}", e),
        }
    }
}

impl std::error::Error for FluxError {}

impl From<io::Error> for FluxError {
    fn from(err: io::Error) -> Self {
        FluxError::PersistenceError(err.to_string())
    }
}

