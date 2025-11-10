use fluxmap::db::Database;
use fluxmap::mem::MemSize;
use std::sync::Arc;

// A simple struct to test MemSize implementation.
#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
struct TestVal(String, u64);

impl MemSize for TestVal {
    fn mem_size(&self) -> usize {
        self.0.mem_size() + std::mem::size_of::<u64>() + std::mem::size_of::<Self>()
    }
}

#[tokio::test]
async fn test_eviction_on_memory_limit() {
    // Set a very small memory limit to trigger eviction easily.
    // Let's estimate the size of one entry:
    // Node<String, i32>: ~64 bytes (depends on arch)
    // VersionNode<Arc<i32>>: ~24 bytes
    // String key ("keyX"): 24 (String struct) + 4 (capacity) = 28 bytes
    // i32 value: 4 bytes
    // Total is roughly 120 bytes.
    // Let's set a limit of 300 bytes, which should allow 2 keys but not 3.
    let db: Arc<Database<String, i32>> = Arc::new(
        Database::builder()
            .max_memory(300)
            .build()
            .await
            .unwrap(),
    );

    let handle = db.handle();

    // Insert two keys. This should be under the limit.
    handle.insert("key1".to_string(), 1).await.unwrap();
    // Sleep to ensure access timestamps are distinct.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    handle.insert("key2".to_string(), 2).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert!(handle.get(&"key1".to_string()).is_some());
    assert!(handle.get(&"key2".to_string()).is_some());

    // This insert should push memory over the limit and trigger an eviction.
    // "key1" is the least recently used and should be evicted.
    handle.insert("key3".to_string(), 3).await.unwrap();

    // Verify that the LRU key ("key1") was evicted.
    assert!(
        handle.get(&"key1".to_string()).is_none(),
        "key1 should have been evicted"
    );
    assert!(
        handle.get(&"key2".to_string()).is_some(),
        "key2 should still exist"
    );
    assert!(
        handle.get(&"key3".to_string()).is_some(),
        "key3 should exist"
    );

    // Access key2 to make it the most recently used.
    let _ = handle.get(&"key2".to_string());
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Insert another key. This should evict key3, which is now the LRU.
    handle.insert("key4".to_string(), 4).await.unwrap();

    assert!(
        handle.get(&"key3".to_string()).is_none(),
        "key3 should have been evicted"
    );
    assert!(
        handle.get(&"key2".to_string()).is_some(),
        "key2 should still exist because it was accessed"
    );
    assert!(
        handle.get(&"key4".to_string()).is_some(),
        "key4 should exist"
    );
}
