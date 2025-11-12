# FluxMap

[![Crates.io](https://img.shields.io/crates/v/fluxmap.svg)](https://crates.io/crates/fluxmap)
[![Docs.rs](https://docs.rs/fluxmap/badge.svg)](https://docs.rs/fluxmap)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**A high-performance, thread-safe, transactional, and durable in-memory key-value store for modern async Rust.**

FluxMap provides a pure Rust, in-memory database solution that combines the speed of a skiplist data structure with the safety of Multi-Version Concurrency Control (MVCC). It offers ACID-compliant transactions with Serializable Snapshot Isolation (SSI), the highest level of isolation, preventing subtle concurrency bugs like write skew.

It is designed for ease of use, high performance, and seamless integration into `tokio`-based applications.

## Features

-   **ACID Transactions:** Guarantees atomicity, consistency, isolation, and durability.
-   **Serializable Snapshot Isolation (SSI):** The strongest MVCC isolation level, protecting against phantom reads, write skew, and other subtle anomalies.
-   **Concurrent & Thread-Safe:** Built for modern `async` Rust. The `Database` can be safely shared across threads using `Arc`.
-   **Configurable Durability:**
    -   **In-Memory:** For maximum performance when data persistence is not required.
    -   **Durable (WAL):** Use a Write-Ahead Log for durability. Choose between:
        -   `Relaxed`: (Group Commit) Commits are buffered and flushed periodically for high throughput.
        -   `Full`: (fsync-per-transaction) Commits are flushed to disk before acknowledging, ensuring maximum safety.
-   **Configurable Maintenance:**
    -   **Automatic Vacuuming:** Optional background thread to automatically clean up old data versions and reclaim memory.
    -   **Memory Limiting:** Optional memory limit with LRU-based eviction to keep memory usage in check.
-   **High Performance:** A lock-free skiplist implementation provides excellent performance for reads and low-contention writes.
-   **Ergonomic API:** A clean and simple API for `get`, `insert`, and `remove` operations. Supports both simple autocommit operations and explicit, multi-statement transactions.
-   **Range & Prefix Scans:** Efficiently query ranges of keys or keys with a specific prefix, with both `Vec` and `Stream`-based APIs.

## Quick Start

First, add FluxMap to your `Cargo.toml`:

```toml
[dependencies]
fluxmap = "0.3.2"
tokio = { version = "1", features = ["full"] }
```

### Example 1: In-Memory Autocommit

For simple use cases, each operation runs in its own small transaction.

```rust
use fluxmap::db::Database;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Create a new in-memory database using the builder.
    let db: Arc<Database<String, String>> = Arc::new(Database::builder().build().await.unwrap());

    // Create a handle to interact with the database.
    let handle = db.handle();

    // Insert a key-value pair. This is an autocommit operation.
    handle.insert("hello".to_string(), "world".to_string()).await.unwrap();

    // Retrieve the value.
    let value = handle.get(&"hello".to_string()).unwrap();
    println!("Value: {}", *value);

    assert_eq!(*value, "world");
}
```

### Example 2: Explicit Transactions

For atomic, multi-statement operations, use the `transaction` helper. It automatically handles beginning, committing, and rolling back the transaction.

```rust
use fluxmap::db::Database;
use fluxmap::error::FluxError;
use std::sync::Arc;

// It's good practice to define a custom error type for your application.
#[derive(Debug)]
enum AppError {
    InsufficientFunds,
    DbError(FluxError),
}

// Implement From<FluxError> to allow the `?` operator to work inside the transaction.
impl From<FluxError> for AppError {
    fn from(e: FluxError) -> Self {
        AppError::DbError(e)
    }
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await?);
    let mut handle = db.handle();

    // Account balances
    handle.insert("alice".to_string(), 100).await?;
    handle.insert("bob".to_string(), 100).await?;

    // Atomically transfer 20 from Alice to Bob
    let result = handle.transaction(|h| Box::pin(async move {
        let alice_balance = h.get(&"alice".to_string()).unwrap();
        let bob_balance = h.get(&"bob".to_string()).unwrap();

        if *alice_balance >= 20 {
            h.insert("alice".to_string(), *alice_balance - 20).await?;
            h.insert("bob".to_string(), *bob_balance + 20).await?;
            Ok("Transfer successful!")
        } else {
            Err(AppError::InsufficientFunds) // Return our custom error
        }
    })).await;

    match result {
        Ok(msg) => println!("{}", msg),
        Err(e) => match e {
            AppError::InsufficientFunds => println!("Transfer failed: Not enough money!"),
            AppError::DbError(db_err) => println!("Transfer failed due to a database error: {}", db_err),
        }
    }

    // Verify the final state
    let final_alice = handle.get(&"alice".to_string()).unwrap();
    let final_bob = handle.get(&"bob".to_string()).unwrap();

    println!("Final balances: Alice = {}, Bob = {}", *final_alice, *final_bob);
    assert_eq!(*final_alice, 80);
    assert_eq!(*final_bob, 120);

    Ok(())
}
```

### Example 3: Durable Database with WAL

To persist data to disk, configure a durability level using the builder.

```rust
use fluxmap::{db::Database, persistence::PersistenceOptions};
use std::sync::Arc;
use tempfile::tempdir; // For a temporary directory in this example

#[tokio::main]
async fn main() {
    // Create a temporary directory for the WAL files.
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();

    // Configure the database for full durability.
    let db: Arc<Database<String, i32>> = Arc::new(
        Database::builder()
            .durability_full(PersistenceOptions::new(wal_path.clone()))
            .build()
            .await
            .unwrap(),
    );

    // Insert data
    let mut handle = db.handle();
    handle.insert("persistent_key".to_string(), 123).await.unwrap();
    drop(handle);
    drop(db); // Simulate a shutdown

    // --- Restart the application ---

    // Create a new database instance pointing to the same directory.
    // It will automatically recover the data from the WAL.
    let recovered_db: Arc<Database<String, i32>> = Arc::new(
        Database::builder()
            .durability_full(PersistenceOptions::new(wal_path))
            .build()
            .await
            .unwrap(),
    );
    let recovered_handle = recovered_db.handle();

    // The data is still there!
    let value = recovered_handle.get(&"persistent_key".to_string()).unwrap();
    println!("Recovered value: {}", *value);
    assert_eq!(*value, 123);
}
```

### Example 4: Prefix Scans

Efficiently find all keys that start with a given prefix.

```rust
use fluxmap::db::Database;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());
    let handle = db.handle();

    handle.insert("user:alice".to_string(), 100).await.unwrap();
    handle.insert("user:bob".to_string(), 200).await.unwrap();
    handle.insert("item:a".to_string(), 99).await.unwrap();

    // Find all keys starting with "user:"
    let user_keys = handle.prefix_scan("user:");
    assert_eq!(user_keys.len(), 2);
    println!("Found users: {:?}", user_keys);
}
```

### Example 5: Memory Limiting and Eviction

Set a memory limit to automatically evict the least recently used (LRU) items when the database exceeds its capacity.

```rust
use fluxmap::db::Database;
use fluxmap::mem::MemSize;
use std::sync::Arc;

// Your key and value types must implement `MemSize` for memory limiting to work.
// For this example, we'll use simple types that already have it implemented.
// For custom structs, you would implement it like this:
//
// #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
// struct MyValue {
//     data: String,
//     num: u64,
// }
//
// impl MemSize for MyValue {
//     fn mem_size(&self) -> usize {
//         std::mem::size_of::<Self>() + self.data.capacity()
//     }
// }

#[tokio::main]
async fn main() {
    // Set a small memory limit (e.g., 500 bytes) to demonstrate eviction.
    let db: Arc<Database<String, String>> = Arc::new(
        Database::builder()
            .max_memory(500)
            .build()
            .await
            .unwrap(),
    );
    let handle = db.handle();

    // Insert keys. The total memory size of a key-value pair will be estimated.
    handle.insert("key1".to_string(), "a long value to take up space".to_string()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await; // Ensure distinct access times
    handle.insert("key2".to_string(), "another long value to take up space".to_string()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    // Access key1 to make it the most recently used
    let _ = handle.get(&"key1".to_string());

    // Insert another key, which should push memory usage over the limit.
    // This will trigger an eviction of the least recently used key ("key2").
    handle.insert("key3".to_string(), "a final long value".to_string()).await.unwrap();

    // "key2" should now be gone.
    assert!(handle.get(&"key2".to_string()).is_none(), "key2 should be evicted");
    assert!(handle.get(&"key1".to_string()).is_some(), "key1 should still exist");
    println!("Eviction successful!");
}
```

## Core Concepts

### Configuration

The `Database` is configured using the builder pattern, which provides a flexible way to set durability and maintenance options.

```rust
use fluxmap::{
    db::{Database, VacuumOptions},
    persistence::PersistenceOptions,
};
use std::time::Duration;
# use tempfile::tempdir;

# #[tokio::main]
# async fn main() {
# let temp_dir = tempdir().unwrap();
# let wal_path = temp_dir.path().to_path_buf();
// A durable database with relaxed durability, auto-vacuuming, and a memory limit.
let db = Database::<String, String>::builder()
    .durability_relaxed(PersistenceOptions {
        wal_path,
        wal_pool_size: 4,
        wal_segment_size_bytes: 16 * 1024 * 1024, // 16MB per segment
    })
    // Flush when the first of these conditions is met:
    .flush_interval(Duration::from_secs(1))         // 1. After 1 second has passed
    .flush_after_commits(1000)                      // 2. After 1000 commits
    .flush_after_bytes(8 * 1024 * 1024)             // 3. After 8MB of data is written
    .auto_vacuum(VacuumOptions {
        interval: Duration::from_secs(30), // Run vacuum every 30 seconds
    })
    .max_memory(512 * 1024 * 1024) // Set a 512MB memory limit
    .build()
    .await
    .unwrap();
# }
```

### Database and Handle

-   **`Database<K, V>`**: The central object that owns all data. It's thread-safe and should be wrapped in an `Arc` to be shared across tasks.
-   **`Handle<'db, K, V>`**: A lightweight session handle for interacting with the database. You create handles from the `Database` instance. Handles are **not** `Send` or `Sync` and should be created per-task.

### Transactions

FluxMap supports two modes of operation:

1.  **Autocommit (Default):** When you call `get`, `insert`, or `remove` directly on a `Handle`, the operation is wrapped in its own transaction. This is simple and safe but can be less efficient for multiple dependent operations.
2.  **Explicit Transactions:** For grouping multiple operations into a single atomic unit, you have two options:
    -   **`handle.transaction(|h| ...)`:** This is the recommended, high-level approach. It provides a closure with a mutable handle and automatically manages the transaction's lifecycle. If the closure returns `Ok`, it commits. If it returns `Err`, it rolls back.
    -   **`handle.begin()`, `handle.commit()`, `handle.rollback()`:** These low-level methods give you manual control over the transaction boundaries.

### Durability Levels

You can control the trade-off between performance and safety by configuring durability via the `DatabaseBuilder`.

-   **`InMemory`**: The default. No data is written to disk.
-   **`Relaxed`**: (Group Commit) Commits are written to the OS buffer and a background thread flushes them to disk when the first of several conditions is met (e.g., time elapsed, number of commits, or bytes written). This offers high performance and durability against process crashes, but recent commits may be lost in case of an OS crash or power failure.
-   **`Full`**: (fsync-per-transaction) Each transaction is fully synced to the disk before the `commit` call returns. This provides the strongest durability guarantee but has a higher performance overhead.

### Automatic Vacuuming

When configured via `auto_vacuum` on the builder, the database will spawn a background thread to periodically run the vacuum process. This reclaims memory from old, dead data versions. If not enabled, you can still call `db.vacuum()` manually.

### Memory Limiting and Eviction

You can configure a memory limit to prevent the database from growing indefinitely. When the estimated memory usage exceeds this limit, FluxMap will automatically evict the least recently used (LRU) keys to stay under the limit.

To use this feature, your key and value types must implement the `fluxmap::mem::MemSize` trait, which helps the database estimate how much memory each entry consumes.

```rust
use fluxmap::db::Database;
use fluxmap::mem::MemSize; // Don't forget to import the trait

// For custom types, you need to implement MemSize.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash, Ord, PartialOrd)]
struct MyKey(String);

impl std::borrow::Borrow<str> for MyKey {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl MemSize for MyKey {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.0.capacity()
    }
}

# #[tokio::main]
# async fn main() {
// Configure a 256MB limit.
let db = Database::<MyKey, String>::builder()
    .max_memory(256 * 1024 * 1024)
    .build()
    .await
    .unwrap();
# }
```

## Under the Hood: MVCC and SSI

FluxMap is built on a Multi-Version Concurrency Control (MVCC) model. Instead of locking data, every modification creates a new version of the value, tagged with the transaction ID. When you read a key, the database finds the correct version that is visible to your transaction's "snapshot" of the data.

This approach allows for non-blocking reads—readers never have to wait for writers.

To provide true serializability, FluxMap implements **Serializable Snapshot Isolation (SSI)**. It tracks read/write dependencies between concurrent transactions. If it detects a "write skew" or other anomaly that would violate serializability, it will automatically abort one of the conflicting transactions, forcing it to be retried. This ensures that your transactions behave as if they were run one after another, eliminating a whole class of subtle concurrency bugs.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](https://github.com/AbiruEkanayaka/FluxMap/blob/main/LICENSE).