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
-   **Optional Durability:**
    -   **In-Memory:** For maximum performance when data persistence is not required.
    -   **Durable (WAL):** Use a Write-Ahead Log for durability. Choose between:
        -   `Relaxed`: (Group Commit) Commits are buffered and flushed periodically for high throughput.
        -   `Full`: (fsync-per-transaction) Commits are flushed to disk before acknowledging, ensuring maximum safety.
-   **High Performance:** A lock-free skiplist implementation provides excellent performance for reads and low-contention writes.
-   **Ergonomic API:** A clean and simple API for `get`, `insert`, and `remove` operations. Supports both simple autocommit operations and explicit, multi-statement transactions.
-   **Range & Prefix Scans:** Efficiently query ranges of keys or keys with a specific prefix, with both `Vec` and `Stream`-based APIs.

## Quick Start

First, add FluxMap to your `Cargo.toml`:

```toml
[dependencies]
fluxmap = "0.1.0" # Replace with the latest version
tokio = { version = "1", features = ["full"] }
```

### Example 1: In-Memory Autocommit

For simple use cases, each operation runs in its own small transaction.

```rust
use fluxmap::db::Database;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Create a new in-memory database.
    let db: Arc<Database<String, String>> = Arc::new(Database::new_in_memory());

    // Create a handle to interact with the database.
    let handle = db.handle();

    // Insert a key-value pair. This is an autocommit operation.
    handle.insert("hello".to_string(), "world".to_string()).await;

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

#[tokio::main]
async fn main() -> Result<(), FluxError> {
    let db: Arc<Database<String, i32>> = Arc::new(Database::new_in_memory());
    let mut handle = db.handle();

    // Account balances
    handle.insert("alice".to_string(), 100).await;
    handle.insert("bob".to_string(), 100).await;

    // Atomically transfer 20 from Alice to Bob
    let result = handle.transaction(|h| Box::pin(async move {
        let alice_balance = h.get(&"alice".to_string()).unwrap_or(Arc::new(0));
        let bob_balance = h.get(&"bob".to_string()).unwrap_or(Arc::new(0));

        if *alice_balance >= 20 {
            h.insert("alice".to_string(), *alice_balance - 20).await;
            h.insert("bob".to_string(), *bob_balance + 20).await;
            Ok("Transfer successful!")
        } else {
            Err("Insufficient funds!") // Returning an Err will automatically roll back
        }
    })).await;

    match result {
        Ok(msg) => println!("{}", msg),
        Err(e) => println!("Transfer failed: {}", e),
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

To persist data to disk, configure a `DurabilityLevel`.

```rust
use fluxmap::db::Database;
use fluxmap::persistence::DurabilityLevel;
use std::sync::Arc;
use tempfile::tempdir; // For a temporary directory in this example

#[tokio::main]
async fn main() {
    // Create a temporary directory for the WAL files.
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();

    // Configure the database for full durability.
    let config = DurabilityLevel::Full { wal_path: wal_path.clone() };
    let db: Arc<Database<String, i32>> = Arc::new(Database::new(config).await.unwrap());

    // Insert data
    let mut handle = db.handle();
    handle.insert("persistent_key".to_string(), 123).await;
    drop(handle);
    drop(db); // Simulate a shutdown

    // --- Restart the application ---

    // Create a new database instance pointing to the same directory.
    // It will automatically recover the data from the WAL.
    let new_config = DurabilityLevel::Full { wal_path };
    let recovered_db: Arc<Database<String, i32>> = Arc::new(Database::new(new_config).await.unwrap());
    let recovered_handle = recovered_db.handle();

    // The data is still there!
    let value = recovered_handle.get(&"persistent_key".to_string()).unwrap();
    println!("Recovered value: {}", *value);
    assert_eq!(*value, 123);
}
```

## Core Concepts

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

You can control the trade-off between performance and safety using `DurabilityLevel`:

-   **`DurabilityLevel::InMemory`**: The default. No data is written to disk.
-   **`DurabilityLevel::Relaxed { wal_path, flush_interval }`**: (Group Commit) Commits are written to the OS buffer and a background thread flushes them to disk periodically. This offers good performance and durability against process crashes, but recent commits may be lost in case of an OS crash or power failure.
-   **`DurabilityLevel::Full { wal_path }`**: (fsync-per-transaction) Each transaction is fully synced to the disk before the `commit` call returns. This provides the strongest durability guarantee but has a higher performance overhead.

## Under the Hood: MVCC and SSI

FluxMap is built on a Multi-Version Concurrency Control (MVCC) model. Instead of locking data, every modification creates a new version of the value, tagged with the transaction ID. When you read a key, the database finds the correct version that is visible to your transaction's "snapshot" of the data.

This approach allows for non-blocking reads—readers never have to wait for writers.

To provide true serializability, FluxMap implements **Serializable Snapshot Isolation (SSI)**. It tracks read/write dependencies between concurrent transactions. If it detects a "write skew" or other anomaly that would violate serializability, it will automatically abort one of the conflicting transactions, forcing it to be retried. This ensures that your transactions behave as if they were run one after another, eliminating a whole class of subtle concurrency bugs.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](https://github.com/AbiruEkanayaka/FluxMap/blob/main/LICENSE).