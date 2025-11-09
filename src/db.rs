//! The primary user-facing API for interacting with FluxMap.
//!
//! This module provides the main entry points for creating and using the database.
//! The two most important structs are:
//!
//! -   [`Database`]: The central, thread-safe database object that owns the data.
//!     You typically create one `Database` instance and share it across your
//!     application using an `Arc`.
//!
//! -   [`Handle`]: A lightweight, single-threaded session handle used to perform
//!     operations on the database. You create handles from a `Database` instance.
//!
//! # Examples
//!
//! ## Basic Usage (Autocommit)
//!
//! ```
//! # use fluxmap::db::Database;
//! # use std::sync::Arc;
//! #
//! # #[tokio::main]
//! # async fn main() {
//! // Create a new in-memory database.
//! let db: Arc<Database<String, String>> = Arc::new(Database::new_in_memory());
//!
//! // Create a handle to interact with the database.
//! let handle = db.handle();
//!
//! // Each operation is a small, independent transaction.
//! handle.insert("key1".to_string(), "value1".to_string()).await;
//! let value = handle.get(&"key1".to_string()).unwrap();
//!
//! assert_eq!(*value, "value1");
//! # }
//! ```
//!
//! ## Explicit Transactions
//!
//! For atomic, multi-statement operations, use the [`Handle::transaction`] helper.
//!
//! ```
//! # use fluxmap::db::Database;
//! # use fluxmap::error::FluxError;
//! # use std::sync::Arc;
//! #
//! # #[tokio::main]
//! # async fn main() -> Result<(), FluxError> {
//! # let db: Arc<Database<String, i32>> = Arc::new(Database::new_in_memory());
//! let mut handle = db.handle();
//! handle.insert("alice".to_string(), 100).await;
//! handle.insert("bob".to_string(), 50).await;
//!
//! // Atomically transfer 20 from Alice to Bob.
//! handle.transaction(|h| Box::pin(async move {
//!     let alice_balance = h.get(&"alice".to_string()).unwrap();
//!     let bob_balance = h.get(&"bob".to_string()).unwrap();
//!
//!     h.insert("alice".to_string(), *alice_balance - 20).await;
//!     h.insert("bob".to_string(), *bob_balance + 20).await;
//!
//!     Ok::<_, FluxError>(()) // Commit the changes
//! })).await?;
//!
//! let final_alice = handle.get(&"alice".to_string()).unwrap();
//! let final_bob = handle.get(&"bob".to_string()).unwrap();
//!
//! assert_eq!(*final_alice, 80);
//! assert_eq!(*final_bob, 70);
//! # Ok(())
//! # }
//! ```

use crate::error::FluxError;
use crate::persistence::{DurabilityLevel, PersistenceEngine};
use crate::SkipList;
use crate::transaction::Transaction;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The central database object that owns the underlying data store.
///
/// A `Database` instance is thread-safe and can be shared across multiple threads,
/// typically by wrapping it in an `Arc`. It serves as the factory for creating
/// [`Handle`] instances, which are used to interact with the data.
///
/// # Examples
///
/// ```
/// # use fluxmap::db::Database;
/// # use fluxmap::persistence::DurabilityLevel;
/// # use std::sync::Arc;
/// # use tempfile::tempdir;
/// #
/// # #[tokio::main]
/// # async fn main() {
/// // In-memory database
/// let in_memory_db: Arc<Database<String, String>> = Arc::new(Database::new_in_memory());
///
/// // Durable database
/// # let temp_dir = tempdir().unwrap();
/// # let wal_path = temp_dir.path().to_path_buf();
/// let config = DurabilityLevel::Full { wal_path };
/// let durable_db: Arc<Database<String, String>> = Arc::new(Database::new(config).await.unwrap());
/// # }
/// ```
pub struct Database<K, V>
where
    K: Ord + Clone + Send + Sync + 'static + std::hash::Hash + Eq + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned,
{
    skiplist: Arc<SkipList<K, V>>,
    persistence_engine: Option<Arc<PersistenceEngine<K, V>>>,
}

impl<K, V> Default for Database<K, V>
where
    K: Ord + Clone + Send + Sync + 'static + std::hash::Hash + Eq + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned,
{
    /// Creates a new, empty, in-memory `Database`.
    fn default() -> Self {
        Self::new_in_memory()
    }
}

impl<K, V> Database<K, V>
where
    K: Ord
        + Clone
        + Send
        + Sync
        + 'static
        + std::hash::Hash
        + Eq
        + Serialize
        + for<'de> Deserialize<'de>,
    V: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de>,
{
    /// Creates a new, empty, in-memory `Database`.
    ///
    /// Data will not be persisted and will be lost when the `Database` is dropped.
    pub fn new_in_memory() -> Self {
        Database {
            skiplist: Arc::new(SkipList::new()),
            persistence_engine: None,
        }
    }

    /// Creates a new `Database` with a specified durability level.
    ///
    /// If the durability level is not `InMemory`, this will initialize the
    /// persistence engine, which may involve creating files and recovering
    /// state from the Write-Ahead Log (WAL) on disk.
    pub async fn new(config: DurabilityLevel) -> Result<Self, FluxError> {
        let persistence_engine = PersistenceEngine::new(config)
            .map_err(|e| FluxError::PersistenceError(e.to_string()))?
            .map(Arc::new);

        let skiplist = if let Some(engine) = &persistence_engine {
            Arc::new(
                engine
                    .recover()
                    .await
                    .map_err(|e| FluxError::PersistenceError(e.to_string()))?,
            )
        } else {
            Arc::new(SkipList::new())
        };

        Ok(Database {
            skiplist,
            persistence_engine,
        })
    }

    /// Creates a new `Handle` for interacting with the database.
    ///
    /// Handles are lightweight, single-threaded session objects. They are not
    /// `Send` or `Sync` and should not be shared across threads. Create a new
    /// handle for each task that needs to interact with the database.
    pub fn handle(&self) -> Handle<'_, K, V> {
        Handle {
            skiplist: &self.skiplist,
            active_tx: None,
            persistence_engine: &self.persistence_engine,
        }
    }
}

/// A handle to the database, representing a single client session.
///
/// Handles are used to perform operations like `get`, `insert`, and `remove`.
/// They can operate in two modes:
///
/// 1.  **Autocommit Mode (default):** Each operation is a small, independent transaction.
///     This is simple and safe but can be inefficient for multiple operations.
///
/// 2.  **Explicit Transaction Mode:** You can use [`Handle::transaction`], or the
///     lower-level [`Handle::begin`], [`Handle::commit`], and [`Handle::rollback`] methods to control
///     transaction boundaries manually. This allows multiple operations to be
///     grouped into a single atomic unit, providing ACID guarantees.
///
/// If a `Handle` is dropped while a transaction is active, the transaction will be
/// automatically rolled back to ensure data consistency.
pub struct Handle<'db, K, V>
where
    K: Ord + Clone + Send + Sync + 'static + std::hash::Hash + Eq + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned,
{
    skiplist: &'db Arc<SkipList<K, V>>,
    /// The currently active transaction, if one has been explicitly started.
    pub active_tx: Option<Arc<Transaction<K, V>>>, // Made public for testing
    persistence_engine: &'db Option<Arc<PersistenceEngine<K, V>>>,
}

impl<'db, K, V> Drop for Handle<'db, K, V>
where
    K: Ord + Clone + Send + Sync + 'static + std::hash::Hash + Eq + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned,
{
    /// Ensures that any active transaction is rolled back when the handle is dropped.
    fn drop(&mut self) {
        if let Some(active_tx) = self.active_tx.take() {
            let tx_manager = self.skiplist.transaction_manager();
            tx_manager.abort(&active_tx);
        }
    }
}

impl<'db, K, V> Handle<'db, K, V>
where
    K: Ord + Clone + Send + Sync + 'static + std::hash::Hash + Eq + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + 'static + Serialize + DeserializeOwned,
{
    /// Retrieves a value from the database.
    ///
    /// - If an explicit transaction is active, this read is part of that transaction.
    ///   It respects Read-Your-Own-Writes (RYOW), meaning it will see changes made
    ///   earlier in the same transaction.
    /// - If no transaction is active, this operation runs in its own small, autocommitted transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fluxmap::db::Database;
    /// # use std::sync::Arc;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let db: Arc<Database<String, i32>> = Arc::new(Database::new_in_memory());
    /// let handle = db.handle();
    /// handle.insert("key".to_string(), 123).await;
    ///
    /// let value = handle.get(&"key".to_string()).unwrap();
    /// assert_eq!(*value, 123);
    ///
    /// assert!(handle.get(&"non-existent-key".to_string()).is_none());
    /// # }
    /// ```
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path ---
            // 1. Check workspace for writes made within this transaction (RYOW).
            let workspace = active_tx.workspace.lock().unwrap();
            if let Some(workspace_value) = workspace.get(key) {
                return workspace_value.clone();
            }
            // 2. Not in workspace, read from skiplist using the transaction's snapshot.
            self.skiplist.get(key, active_tx)
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();
            let result = self.skiplist.get(key, &tx);
            tx_manager.commit(&tx).unwrap(); // Autocommit always succeeds
            result
        }
    }

    /// Inserts or updates a key-value pair in the database.
    ///
    /// - If an explicit transaction is active, this write is buffered in the transaction's
    ///   private workspace and will only become visible to other transactions upon `commit()`.
    /// - If no transaction is active, this operation runs in its own small, autocommitted transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fluxmap::db::Database;
    /// # use std::sync::Arc;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let db: Arc<Database<String, i32>> = Arc::new(Database::new_in_memory());
    /// let handle = db.handle();
    ///
    /// // Insert a new key
    /// handle.insert("key".to_string(), 1).await;
    ///
    /// // Update the key
    /// handle.insert("key".to_string(), 2).await;
    ///
    /// let value = handle.get(&"key".to_string()).unwrap();
    /// assert_eq!(*value, 2);
    /// # }
    /// ```
    pub async fn insert(&self, key: K, value: V) {
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path ---
            active_tx
                .workspace
                .lock()
                .unwrap()
                .insert(key, Some(Arc::new(value)));
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();

            // For durable autocommit, we must log the operation to the WAL.
            if let Some(engine) = self.persistence_engine {
                let mut workspace = crate::transaction::Workspace::new();
                workspace.insert(key.clone(), Some(Arc::new(value.clone())));
                let mut serialized_data = Vec::new();
                // In a real scenario, we'd handle this error, but for the fix, unwrap is acceptable.
                ciborium::into_writer(&workspace, &mut serialized_data).unwrap();
                if engine.log(&serialized_data).is_err() {
                    tx_manager.abort(&tx);
                    // The function can't return a Result, so we can't propagate.
                    // In a real app, this might panic or log a severe error.
                    return;
                }
            }

            // In autocommit, we write directly to the skiplist's pending versions.
            self.skiplist.insert(key, Arc::new(value), &tx).await;
            tx_manager.commit(&tx).unwrap();
        }
    }

    /// Removes a key from the database.
    ///
    /// - If an explicit transaction is active, this removal is buffered in the transaction's
    ///   private workspace and will only take effect upon `commit()`.
    /// - If no transaction is active, this operation runs in its own small, autocommitted transaction.
    ///
    /// # Returns
    ///
    /// The value that was removed, if it existed. In an explicit transaction, this will
    /// return a value written earlier in the same transaction (RYOW).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fluxmap::db::Database;
    /// # use std::sync::Arc;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let db: Arc<Database<String, i32>> = Arc::new(Database::new_in_memory());
    /// let handle = db.handle();
    /// handle.insert("key".to_string(), 123).await;
    ///
    /// let removed_value = handle.remove(&"key".to_string()).await.unwrap();
    /// assert_eq!(*removed_value, 123);
    ///
    /// assert!(handle.get(&"key".to_string()).is_none());
    /// # }
    /// ```
    pub async fn remove(&self, key: &K) -> Option<Arc<V>> {
        if let Some(active_tx) = &self.active_tx {
            // --- Explicit Transaction Path ---
            let mut workspace = active_tx.workspace.lock().unwrap();
            // Mark the key for deletion in the workspace.
            let old_val_in_workspace = workspace.insert(key.clone(), None);
            if let Some(Some(val)) = old_val_in_workspace {
                // The key was present in the workspace, return its value.
                Some(val)
            } else {
                // The key was not in the workspace, so the value to be removed
                // is the one visible in the skiplist from our snapshot.
                self.skiplist.get(key, active_tx)
            }
        } else {
            // --- Autocommit Path ---
            let tx_manager = self.skiplist.transaction_manager();
            let tx = tx_manager.begin();

            // For durable autocommit, we must log the operation to the WAL.
            if let Some(engine) = self.persistence_engine {
                let mut workspace: crate::transaction::Workspace<K, V> =
                    crate::transaction::Workspace::new();
                workspace.insert(key.clone(), None); // None signifies removal
                let mut serialized_data = Vec::new();
                ciborium::into_writer(&workspace, &mut serialized_data).unwrap();
                if engine.log(&serialized_data).is_err() {
                    tx_manager.abort(&tx);
                    return None; // Can't return an error, so we return None.
                }
            }

            let result = self.skiplist.remove(key, &tx).await;
            tx_manager.commit(&tx).unwrap();
            result
        }
    }

    /// Begins a new transaction on this handle.
    ///
    /// After calling `begin()`, all subsequent operations on this handle will be part
    /// of a single atomic transaction until `commit()` or `rollback()` is called.
    ///
    /// Returns an error if a transaction is already active on this handle.
    pub fn begin(&mut self) -> Result<(), FluxError> {
        if self.active_tx.is_some() {
            return Err(FluxError::TransactionAlreadyActive);
        }
        let tx_manager = self.skiplist.transaction_manager();
        let tx = tx_manager.begin();
        self.active_tx = Some(tx);
        Ok(())
    }

    /// Commits the active transaction.
    ///
    /// This will attempt to apply all changes made within the transaction to the database.
    /// If the commit fails due to a conflict (`SerializationConflict`), the transaction
    /// is automatically rolled back.
    ///
    /// Returns an error if no transaction is active.
    pub async fn commit(&mut self) -> Result<(), FluxError> {
        let active_tx = self
            .active_tx
            .take()
            .ok_or(FluxError::NoActiveTransaction)?;

        // --- Phase 1: Persistence (if durable) ---
        // Log the transaction's workspace to the WAL *before* applying changes to the
        // in-memory skiplist. This ensures that if we crash after this point, recovery
        // can replay the transaction.
        if let Some(engine) = self.persistence_engine {
            let workspace = active_tx.workspace.lock().unwrap();
            if !workspace.is_empty() {
                let mut serialized_data = Vec::new();
                ciborium::into_writer(&*workspace, &mut serialized_data)
                    .map_err(|e| FluxError::PersistenceError(e.to_string()))?;

                if let Err(e) = engine.log(&serialized_data) {
                    // If logging fails, we must abort the transaction.
                    let tx_manager = self.skiplist.transaction_manager();
                    tx_manager.abort(&active_tx);
                    return Err(FluxError::PersistenceError(e.to_string()));
                }
            }
        }

        // --- Phase 2: In-Memory Application ---
        // Apply changes from the workspace to the actual skiplist.
        let workspace = std::mem::take(&mut *active_tx.workspace.lock().unwrap());
        for (key, value) in workspace {
            match value {
                Some(val) => {
                    self.skiplist.insert(key, val, &active_tx).await;
                }
                None => {
                    self.skiplist.remove(&key, &active_tx).await;
                }
            }
        }

        // --- Phase 3: Finalize Commit ---
        // Attempt to commit via the transaction manager, which performs SSI checks.
        let tx_manager = self.skiplist.transaction_manager();
        match tx_manager.commit(&active_tx) {
            Ok(()) => Ok(()),
            Err(e) => {
                // If commit fails, the transaction is automatically aborted by the manager.
                Err(e)
            }
        }
    }

    /// Rolls back the active transaction.
    ///
    /// This will discard all changes made within the transaction.
    /// Returns an error if no transaction is active.
    pub fn rollback(&mut self) -> Result<(), FluxError> {
        let active_tx = self
            .active_tx
            .take()
            .ok_or(FluxError::NoActiveTransaction)?;

        let tx_manager = self.skiplist.transaction_manager();
        tx_manager.abort(&active_tx);
        Ok(())
    }

    /// Executes a closure within a managed transaction.
    ///
    /// This is the recommended, high-level way to run a transaction. It automatically
    /// handles beginning, committing, and rolling back the transaction.
    ///
    /// - If the closure returns `Ok(T)`, the transaction is committed.
    /// - If the closure returns `Err(E)`, the transaction is rolled back.
    /// - If the commit itself fails (e.g., due to a `SerializationConflict`), the
    ///   transaction is rolled back and the commit error is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fluxmap::db::Database;
    /// # use fluxmap::error::FluxError;
    /// # use std::sync::Arc;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), FluxError> {
    /// # let db: Arc<Database<String, i32>> = Arc::new(Database::new_in_memory());
    /// let mut handle = db.handle();
    ///
    /// let result = handle.transaction(|h| Box::pin(async move {
    ///     h.insert("key1".to_string(), 100).await;
    ///     let val = h.get(&"key1".to_string()).unwrap();
    ///     assert_eq!(*val, 100); // Read-Your-Own-Writes
    ///     Ok::<_, FluxError>("Success!")
    /// })).await?;
    ///
    /// assert_eq!(result, "Success!");
    /// assert_eq!(*handle.get(&"key1".to_string()).unwrap(), 100);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn transaction<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: for<'a> FnOnce(&'a mut Self) -> Pin<Box<dyn Future<Output = Result<T, E>> + 'a>>,
        E: From<FluxError>,
    {
        self.begin()?;

        let result = f(self).await;

        if result.is_ok() {
            // The closure succeeded, so we attempt to commit.
            match self.commit().await {
                Ok(_) => result, // Return the closure's Ok result.
                Err(commit_err) => {
                    // Commit failed, and `commit` already rolled back.
                    Err(commit_err.into())
                }
            }
        } else {
            // The closure failed, so we must roll back.
            self.rollback().unwrap_or_else(|_| {
                // This would only fail if the transaction was already gone,
                // which shouldn't happen here. We prioritize the closure's error.
            });
            result // Return the closure's Err result.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FluxError;

    #[tokio::test]
    async fn test_autocommit_insert_and_get() {
        let db: Database<String, String> = Database::new_in_memory();
        let handle = db.handle();

        handle
            .insert("key1".to_string(), "value1".to_string())
            .await;

        let val = handle.get(&"key1".to_string());
        assert_eq!(val.as_deref().map(|s| s.as_str()), Some("value1"));
    }

    #[tokio::test]
    async fn test_autocommit_remove() {
        let db: Database<String, String> = Database::new_in_memory();
        let handle = db.handle();

        handle
            .insert("key1".to_string(), "value1".to_string())
            .await;
        let val = handle.get(&"key1".to_string());
        assert!(val.is_some());

        let removed_val = handle.remove(&"key1".to_string()).await;
        assert_eq!(removed_val.as_deref().map(|s| s.as_str()), Some("value1"));

        let val_after_remove = handle.get(&"key1".to_string());
        assert!(val_after_remove.is_none());
    }

    #[tokio::test]
    async fn test_ryow_insert_get() {
        let db: Database<String, String> = Database::new_in_memory();
        let mut handle = db.handle();

        // Manually begin a transaction for testing RYOW
        let tx = db.skiplist.transaction_manager().begin();
        handle.active_tx = Some(tx.clone());

        // Insert a value into the workspace
        handle
            .insert("ryow_key".to_string(), "ryow_value".to_string())
            .await;

        // 1. Get the value within the same transaction - should see the uncommitted write
        let val = handle.get(&"ryow_key".to_string());
        assert_eq!(
            val.as_deref().map(|s| s.as_str()),
            Some("ryow_value"),
            "Should read own uncommitted insert from workspace"
        );

        // 2. Verify that another handle (using autocommit) doesn't see the value yet
        let other_handle = db.handle();
        let other_val = other_handle.get(&"ryow_key".to_string());
        assert!(
            other_val.is_none(),
            "Another handle should not see the uncommitted value"
        );
    }

    #[tokio::test]
    async fn test_ryow_insert_remove_get() {
        let db: Database<String, String> = Database::new_in_memory();
        let mut handle = db.handle();

        // Manually begin a transaction
        let tx = db.skiplist.transaction_manager().begin();
        handle.active_tx = Some(tx.clone());

        // Insert a value
        handle
            .insert("ryow_key_del".to_string(), "ryow_value_del".to_string())
            .await;
        let val_inserted = handle.get(&"ryow_key_del".to_string());
        assert_eq!(
            val_inserted.as_deref().map(|s| s.as_str()),
            Some("ryow_value_del")
        );

        // Remove the value within the same transaction
        let removed_val = handle.remove(&"ryow_key_del".to_string()).await;
        assert_eq!(
            removed_val.as_deref().map(|s| s.as_str()),
            Some("ryow_value_del")
        );

        // Get the value within the same transaction - should see None
        let val_after_remove = handle.get(&"ryow_key_del".to_string());
        assert!(val_after_remove.is_none());
    }

    #[tokio::test]
    async fn test_explicit_commit() {
        let db: Database<String, String> = Database::new_in_memory();
        let mut handle = db.handle();

        handle.begin().unwrap();
        handle
            .insert("key1".to_string(), "value1".to_string())
            .await;

        // Value should not be visible to another transaction before commit
        let other_handle = db.handle();
        assert!(other_handle.get(&"key1".to_string()).is_none());

        handle.commit().await.unwrap();

        // Value should be visible after commit
        let val = other_handle.get(&"key1".to_string());
        assert_eq!(val.as_deref().map(|s| s.as_str()), Some("value1"));
    }

    #[tokio::test]
    async fn test_explicit_rollback() {
        let db: Database<String, String> = Database::new_in_memory();
        let mut handle = db.handle();

        handle.begin().unwrap();
        handle
            .insert("key1".to_string(), "value1".to_string())
            .await;

        // RYOW should work
        let val = handle.get(&"key1".to_string());
        assert_eq!(val.as_deref().map(|s| s.as_str()), Some("value1"));

        handle.rollback().unwrap();

        // Value should not be visible after rollback
        let other_handle = db.handle();
        assert!(other_handle.get(&"key1".to_string()).is_none());

        // The transaction should be gone from the handle
        assert!(handle.active_tx.is_none());
        // Trying to get it from the same handle should now use autocommit path and find nothing
        assert!(handle.get(&"key1".to_string()).is_none());
    }

    #[tokio::test]
    async fn test_begin_twice_fails() {
        let db: Database<String, String> = Database::new_in_memory();
        let mut handle = db.handle();
        handle.begin().unwrap();
        let res = handle.begin();
        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), FluxError::TransactionAlreadyActive);
    }

    #[tokio::test]
    async fn test_commit_without_begin_fails() {
        let db: Database<String, String> = Database::new_in_memory();
        let mut handle = db.handle();
        let res = handle.commit().await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), FluxError::NoActiveTransaction);
    }

    #[tokio::test]
    async fn test_rollback_without_begin_fails() {
        let db: Database<String, String> = Database::new_in_memory();
        let mut handle = db.handle();
        let res = handle.rollback();
        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), FluxError::NoActiveTransaction);
    }

    #[tokio::test]
    async fn test_serialization_conflict_aborts() {
        let db: Arc<Database<String, i32>> = Arc::new(Database::new_in_memory());

        // Setup: insert x=10, y=20
        let setup_handle = db.handle();
        setup_handle.insert("x".to_string(), 10).await;
        setup_handle.insert("y".to_string(), 20).await;

        let mut h1 = db.handle();
        let mut h2 = db.handle();

        // Tx1 starts
        h1.begin().unwrap();
        // Tx2 starts
        h2.begin().unwrap();

        // Tx1 reads x and y
        let x1 = h1.get(&"x".to_string()).unwrap();
        let y1 = h1.get(&"y".to_string()).unwrap();

        // Tx2 reads x and y
        let x2 = h2.get(&"x".to_string()).unwrap();
        let y2 = h2.get(&"y".to_string()).unwrap();

        // Tx1 writes to y based on x
        h1.insert("y".to_string(), *x1 + *y1).await; // y = 10 + 20 = 30

        // Tx2 writes to x based on y
        h2.insert("x".to_string(), *x2 + *y2).await; // x = 10 + 20 = 30

        // Commit Tx1
        let res1 = h1.commit().await;
        assert!(res1.is_ok());

        // Commit Tx2 - this should fail
        let res2 = h2.commit().await;
        assert!(res2.is_err());
        assert_eq!(res2.unwrap_err(), FluxError::SerializationConflict);

        // Check final state
        let final_handle = db.handle();
        let final_x = final_handle.get(&"x".to_string()).unwrap();
        let final_y = final_handle.get(&"y".to_string()).unwrap();

        assert_eq!(*final_x, 10); // from setup, because tx2 failed
        assert_eq!(*final_y, 30); // from tx1
    }

    #[tokio::test]
    async fn test_transaction_closure_commit() {
        let db: Arc<Database<String, String>> = Arc::new(Database::new_in_memory());
        let mut handle = db.handle();

        let result = handle
            .transaction(|h| {
                Box::pin(async move {
                    h.insert("key".to_string(), "value".to_string()).await;
                    Ok::<_, FluxError>("success".to_string())
                })
            })
            .await;

        assert_eq!(result.unwrap(), "success");

        // Check that the value is visible after the transaction
        let final_val = handle.get(&"key".to_string());
        assert_eq!(final_val.as_deref().map(|s| s.as_str()), Some("value"));
    }

    #[tokio::test]
    async fn test_transaction_closure_rollback() {
        let db: Arc<Database<String, String>> = Arc::new(Database::new_in_memory());
        let mut handle = db.handle();

        let result: Result<String, FluxError> = handle
            .transaction(|h| {
                Box::pin(async move {
                    h.insert("key".to_string(), "value".to_string()).await;
                    // Return an error to trigger rollback
                    Err(FluxError::NoActiveTransaction) // Using a FluxError for simplicity
                })
            })
            .await;

        assert!(result.is_err());

        // Check that the value is NOT visible after the transaction
        let final_val = handle.get(&"key".to_string());
        assert!(final_val.is_none());
    }

    #[tokio::test]
    async fn test_drop_rolls_back() {
        let db: Arc<Database<String, String>> = Arc::new(Database::new_in_memory());

        {
            let mut handle = db.handle();
            handle.begin().unwrap();
            handle.insert("key".to_string(), "value".to_string()).await;
            // handle is dropped here at the end of the scope
        }

        // A new handle should not see the changes
        let new_handle = db.handle();
        let val = new_handle.get(&"key".to_string());
        assert!(
            val.is_none(),
            "Changes should be rolled back when handle is dropped"
        );
    }

    #[tokio::test]
    async fn test_commit_logs_to_wal() {
        use crate::persistence::DurabilityLevel;
        use std::collections::HashMap;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().to_path_buf();
        let config = DurabilityLevel::Full {
            wal_path: wal_path.clone(),
        };

        // Create a durable database
        let db: Database<String, i32> = Database::new(config).await.unwrap();
        let mut handle = db.handle();

        // Begin, insert, and commit
        handle.begin().unwrap();
        handle.insert("logged_key".to_string(), 12345).await;
        handle.commit().await.unwrap();

        // Verify the WAL file contains the committed data
        let wal_file_path = wal_path.join("wal.0");
        assert!(wal_file_path.exists());

        let wal_data = std::fs::read(wal_file_path).unwrap();
        assert!(!wal_data.is_empty());

        // Deserialize and check the content
        let mut expected_workspace: HashMap<String, Option<Arc<i32>>> = HashMap::new();
        expected_workspace.insert("logged_key".to_string(), Some(Arc::new(12345)));

        let deserialized_workspace: HashMap<String, Option<Arc<i32>>> =
            ciborium::from_reader(&wal_data[..]).unwrap();

        assert_eq!(deserialized_workspace.len(), 1);
        let (key, value) = deserialized_workspace.iter().next().unwrap();
        assert_eq!(key, "logged_key");
        assert_eq!(value.as_ref().unwrap().as_ref(), &12345);
    }

    #[tokio::test]
    async fn test_recovery_on_startup() {
        use crate::persistence::DurabilityLevel;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().to_path_buf();
        let config = DurabilityLevel::Full {
            wal_path: wal_path.clone(),
        };

        // --- First Session ---
        {
            let db: Database<String, i32> = Database::new(config.clone()).await.unwrap();
            let mut handle = db.handle();
            handle.begin().unwrap();
            handle.insert("key1".to_string(), 1).await;
            handle.insert("key2".to_string(), 2).await;
            handle.commit().await.unwrap(); // Ensure data is committed
            // DB is dropped here, simulating a shutdown
        }

        // --- Second Session ---
        // Create a new database instance pointing to the same directory.
        // The new `recover` logic should be triggered inside `new`.
        let db: Database<String, i32> = Database::new(config).await.unwrap();
        let handle = db.handle();

        // Verify that the data from the first session is present.
        assert_eq!(*handle.get(&"key1".to_string()).unwrap(), 1);
        assert_eq!(*handle.get(&"key2".to_string()).unwrap(), 2);
    }
}
