use fluxmap::db::Database;
use fluxmap::error::FluxError;
use fluxmap::persistence::PersistenceOptions;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_autocommit_insert_and_get() {
    let db: Database<String, String> = Database::builder().build().await.unwrap();
    let handle = db.handle();

    handle
        .insert("key1".to_string(), "value1".to_string())
        .await
        .unwrap();

    let val = handle.get(&"key1".to_string());
    assert_eq!(val.as_deref().map(|s| s.as_str()), Some("value1"));
}

#[tokio::test]
async fn test_autocommit_remove() {
    let db: Database<String, String> = Database::builder().build().await.unwrap();
    let handle = db.handle();

    handle
        .insert("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let val = handle.get(&"key1".to_string());
    assert!(val.is_some());

    let removed_val = handle.remove(&"key1".to_string()).await.unwrap();
    assert_eq!(
        removed_val.as_deref().map(|s| s.as_str()),
        Some("value1")
    );

    let val_after_remove = handle.get(&"key1".to_string());
    assert!(val_after_remove.is_none());
}

#[tokio::test]
async fn test_ryow_insert_get() {
    let db: Arc<Database<String, String>> = Arc::new(Database::builder().build().await.unwrap());
    let mut handle = db.handle();

    handle.begin().unwrap();

    // Insert a value into the workspace
    handle
        .insert("ryow_key".to_string(), "ryow_value".to_string())
        .await
        .unwrap();

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

    handle.rollback().unwrap();
}

#[tokio::test]
async fn test_ryow_insert_remove_get() {
    let db: Database<String, String> = Database::builder().build().await.unwrap();
    let mut handle = db.handle();

    handle.begin().unwrap();

    // Insert a value
    handle
        .insert("ryow_key_del".to_string(), "ryow_value_del".to_string())
        .await
        .unwrap();
    let val_inserted = handle.get(&"ryow_key_del".to_string());
    assert_eq!(
        val_inserted.as_deref().map(|s| s.as_str()),
        Some("ryow_value_del")
    );

    // Remove the value within the same transaction
    let removed_val = handle.remove(&"ryow_key_del".to_string()).await.unwrap();
    assert_eq!(
        removed_val.as_deref().map(|s| s.as_str()),
        Some("ryow_value_del")
    );

    // Get the value within the same transaction - should see None
    let val_after_remove = handle.get(&"ryow_key_del".to_string());
    assert!(val_after_remove.is_none());

    handle.rollback().unwrap();
}

#[tokio::test]
async fn test_explicit_commit() {
    let db: Database<String, String> = Database::builder().build().await.unwrap();
    let mut handle = db.handle();

    handle.begin().unwrap();
    handle
        .insert("key1".to_string(), "value1".to_string())
        .await
        .unwrap();

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
    let db: Database<String, String> = Database::builder().build().await.unwrap();
    let mut handle = db.handle();

    handle.begin().unwrap();
    handle
        .insert("key1".to_string(), "value1".to_string())
        .await
        .unwrap();

    // RYOW should work
    let val = handle.get(&"key1".to_string());
    assert_eq!(val.as_deref().map(|s| s.as_str()), Some("value1"));

    handle.rollback().unwrap();

    // Value should not be visible after rollback
    let other_handle = db.handle();
    assert!(other_handle.get(&"key1".to_string()).is_none());

    // Trying to get it from the same handle should now use autocommit path and find nothing
    assert!(handle.get(&"key1".to_string()).is_none());
}

#[tokio::test]
async fn test_begin_twice_fails() {
    let db: Database<String, String> = Database::builder().build().await.unwrap();
    let mut handle = db.handle();
    handle.begin().unwrap();
    let res = handle.begin();
    assert!(res.is_err());
    assert_eq!(res.unwrap_err(), FluxError::TransactionAlreadyActive);
}

#[tokio::test]
async fn test_commit_without_begin_fails() {
    let db: Database<String, String> = Database::builder().build().await.unwrap();
    let mut handle = db.handle();
    let res = handle.commit().await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err(), FluxError::NoActiveTransaction);
}

#[tokio::test]
async fn test_rollback_without_begin_fails() {
    let db: Database<String, String> = Database::builder().build().await.unwrap();
    let mut handle = db.handle();
    let res = handle.rollback();
    assert!(res.is_err());
    assert_eq!(res.unwrap_err(), FluxError::NoActiveTransaction);
}

#[tokio::test]
async fn test_serialization_conflict_aborts() {
    let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());

    // Setup: insert x=10, y=20
    let setup_handle = db.handle();
    setup_handle.insert("x".to_string(), 10).await.unwrap();
    setup_handle.insert("y".to_string(), 20).await.unwrap();

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
    h1.insert("y".to_string(), *x1 + *y1).await.unwrap(); // y = 10 + 20 = 30

    // Tx2 writes to x based on y
    h2.insert("x".to_string(), *x2 + *y2).await.unwrap(); // x = 10 + 20 = 30

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
    let db: Arc<Database<String, String>> = Arc::new(Database::builder().build().await.unwrap());
    let mut handle = db.handle();

    let result = handle
        .transaction(|h| {
            Box::pin(async move {
                h.insert("key".to_string(), "value".to_string())
                    .await
                    .unwrap();
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
    let db: Arc<Database<String, String>> = Arc::new(Database::builder().build().await.unwrap());
    let mut handle = db.handle();

    let result: Result<String, FluxError> = handle
        .transaction(|h| {
            Box::pin(async move {
                h.insert("key".to_string(), "value".to_string())
                    .await
                    .unwrap();
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
    let db: Arc<Database<String, String>> = Arc::new(Database::builder().build().await.unwrap());

    {
        let mut handle = db.handle();
        handle.begin().unwrap();
        handle
            .insert("key".to_string(), "value".to_string())
            .await
            .unwrap();
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
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();
    let options = PersistenceOptions::new(wal_path.clone());

    // Create a durable database
    let db: Database<String, i32> = Database::builder()
        .durability_full(options)
        .build()
        .await
        .unwrap();
    let mut handle = db.handle();

    // Begin, insert, and commit
    handle.begin().unwrap();
    handle
        .insert("logged_key".to_string(), 12345)
        .await
        .unwrap();
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
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().to_path_buf();
    let options = PersistenceOptions::new(wal_path.clone());

    // --- First Session ---
    {
        let db: Database<String, i32> = Database::builder()
            .durability_full(options.clone())
            .build()
            .await
            .unwrap();
        let mut handle = db.handle();
        handle.begin().unwrap();
        handle.insert("key1".to_string(), 1).await.unwrap();
        handle.insert("key2".to_string(), 2).await.unwrap();
        handle.commit().await.unwrap(); // Ensure data is committed
                                        // DB is dropped here, simulating a shutdown
    }

    // --- Second Session ---
    // Create a new database instance pointing to the same directory.
    // The new `recover` logic should be triggered inside `new`.
    let db: Database<String, i32> = Database::builder()
        .durability_full(options)
        .build()
        .await
        .unwrap();
    let handle = db.handle();

    // Verify that the data from the first session is present.
    assert_eq!(*handle.get(&"key1".to_string()).unwrap(), 1);
    assert_eq!(*handle.get(&"key2".to_string()).unwrap(), 2);
}

#[tokio::test]
async fn test_autocommit_range_scan() {
    let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());
    let handle = db.handle();
    handle.insert("a".to_string(), 1).await.unwrap();
    handle.insert("b".to_string(), 2).await.unwrap();
    handle.insert("c".to_string(), 3).await.unwrap();

    let results = handle.range(&"a".to_string(), &"b".to_string());
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, "a");
    assert_eq!(*results[1].1, 2);
}

#[tokio::test]
async fn test_transactional_prefix_scan() {
    let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());
    let mut handle = db.handle();
    handle.insert("user:1".to_string(), 100).await.unwrap();
    handle.insert("user:2".to_string(), 200).await.unwrap();
    handle.insert("item:1".to_string(), 999).await.unwrap();

    handle.begin().unwrap();
    // In transaction, update a value that will be part of the scan
    handle.insert("user:3".to_string(), 300).await.unwrap();

    let results = handle.prefix_scan("user:");
    assert_eq!(results.len(), 3);
    assert!(results.iter().any(|(k, _)| k == "user:1"));
    assert!(results.iter().any(|(k, _)| k == "user:2"));
    assert!(results.iter().any(|(k, v)| k == "user:3" && **v == 300));

    handle.commit().await.unwrap();

    // Verify after commit
    let final_results = db.handle().prefix_scan("user:");
    assert_eq!(final_results.len(), 3);
}

#[tokio::test]
async fn test_scan_induces_serialization_conflict() {
    let db: Arc<Database<String, i32>> = Arc::new(Database::builder().build().await.unwrap());

    // Setup: insert a key in the range
    let setup_handle = db.handle();
    setup_handle.insert("b".to_string(), 10).await.unwrap();

    let mut h1 = db.handle();
    let mut h2 = db.handle();

    // Tx1 starts and scans a range including "b"
    h1.begin().unwrap();
    let range = h1.range(&"a".to_string(), &"c".to_string());
    assert_eq!(range.len(), 1);
    assert_eq!(range[0].0, "b");

    // Tx2 starts, writes to "b" (which Tx1 has read via the scan), and commits.
    h2.begin().unwrap();
    h2.insert("b".to_string(), 20).await.unwrap();
    let res2 = h2.commit().await;
    assert!(res2.is_ok(), "Tx2 should commit successfully");

    // Now, when Tx1 tries to commit, it should fail because its read set (containing "b")
    // conflicts with Tx2's write.
    let res1 = h1.commit().await;
    assert!(res1.is_err());
    assert_eq!(res1.unwrap_err(), FluxError::SerializationConflict);

    // Check final state
    let final_handle = db.handle();
    let final_b = final_handle.get(&"b".to_string()).unwrap();
    assert_eq!(*final_b, 20); // from tx2
}
