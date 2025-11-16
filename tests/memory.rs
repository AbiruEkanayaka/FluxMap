use fluxmap::db::Database;
use fluxmap::error::FluxError;
use fluxmap::mem::{EvictionPolicy, MemSize};
use futures::StreamExt;
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
    fastrand::seed(0);
    // Set a very small memory limit to trigger eviction easily.
    // Let's estimate the size of one entry:
    // Node<String, i32>: ~64 bytes (depends on arch)
    // VersionNode<Arc<i32>>: ~24 bytes
    // String key ("keyX"): 24 (String struct) + 4 (capacity) = 28 bytes
    // i32 value: 4 bytes
    // Total is roughly 120 bytes.
    // Let's set a limit of 300 bytes, which should allow 2 keys but not 3.
    let db: Arc<Database<String, i32>> =
        Arc::new(Database::builder().max_memory(450).build().await.unwrap());

    let handle = db.handle();

    // Insert two keys. This should be under the limit.
    handle.insert("key1".to_string(), 1).await.unwrap();
    // Sleep to ensure access timestamps are distinct.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    handle.insert("key2".to_string(), 2).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert!(handle.get(&"key1".to_string()).unwrap().is_some());
    assert!(handle.get(&"key2".to_string()).unwrap().is_some());

    // This insert should push memory over the limit and trigger an eviction.
    // "key1" is the least recently used and should be evicted.
    handle.insert("key3".to_string(), 3).await.unwrap();

    // Verify that the LRU key ("key1") was evicted.
    assert!(
        handle.get(&"key1".to_string()).unwrap().is_none(),
        "key1 should have been evicted"
    );
    assert!(
        handle.get(&"key2".to_string()).unwrap().is_some(),
        "key2 should still exist"
    );
    assert!(
        handle.get(&"key3".to_string()).unwrap().is_some(),
        "key3 should exist"
    );

    // Access key2 to make it the most recently used.
    let _ = handle.get(&"key2".to_string()).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Insert another key. This should evict key3, which is now the LRU.
    handle.insert("key4".to_string(), 4).await.unwrap();

    assert!(
        handle.get(&"key3".to_string()).unwrap().is_none(),
        "key3 should have been evicted"
    );
    assert!(
        handle.get(&"key2".to_string()).unwrap().is_some(),
        "key2 should still exist because it was accessed"
    );
    assert!(
        handle.get(&"key4".to_string()).unwrap().is_some(),
        "key4 should exist"
    );
}

#[tokio::test]
async fn test_eviction_on_memory_limit_lfu() {
    fastrand::seed(0);
    // Set a very small memory limit to trigger eviction easily.
    let db: Arc<Database<String, i32>> = Arc::new(
        Database::builder()
            .max_memory(450) // Allows ~2 keys
            .eviction_policy(EvictionPolicy::Lfu)
            .build()
            .await
            .unwrap(),
    );
    let handle = db.handle();

    handle.insert("key_frequent".to_string(), 1).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    handle
        .insert("key_infrequent".to_string(), 2)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    // Make key_frequent very frequent
    for _ in 0..5 {
        let _ = handle.get(&"key_frequent".to_string()).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    // At this point, access counts should be:
    // - key_frequent: 6 (1 from insert, 5 from gets)
    // - key_infrequent: 1 (from insert)

    // This insert will trigger eviction.
    handle.insert("key_new".to_string(), 3).await.unwrap();

    // A new key "key_new" is now present with count 1.
    // The eviction logic should run and choose a victim from the three keys.
    // The LFU victim is "key_infrequent" because it has the lowest access count (1)
    // and is the oldest among the keys with that count.

    assert!(
        handle.get(&"key_infrequent".to_string()).unwrap().is_none(),
        "key_infrequent should have been evicted"
    );
    assert!(
        handle.get(&"key_frequent".to_string()).unwrap().is_some(),
        "key_frequent should still exist"
    );
    assert!(
        handle.get(&"key_new".to_string()).unwrap().is_some(),
        "key_new should exist"
    );
}

#[tokio::test]
async fn test_eviction_on_memory_limit_random() {
    fastrand::seed(0);
    // Set a very small memory limit to trigger eviction easily.
    let db: Arc<Database<String, i32>> = Arc::new(
        Database::builder()
            .max_memory(450) // Allows ~2 keys
            .eviction_policy(EvictionPolicy::Random)
            .build()
            .await
            .unwrap(),
    );
    let handle = db.handle();

    handle.insert("key1".to_string(), 1).await.unwrap();
    handle.insert("key2".to_string(), 2).await.unwrap();

    assert!(handle.get(&"key1".to_string()).unwrap().is_some());
    assert!(handle.get(&"key2".to_string()).unwrap().is_some());

    // This insert should push memory over the limit and trigger a random eviction.
    handle.insert("key3".to_string(), 3).await.unwrap();

    let key1_exists = handle.get(&"key1".to_string()).unwrap().is_some();
    let key2_exists = handle.get(&"key2".to_string()).unwrap().is_some();
    let key3_exists = handle.get(&"key3".to_string()).unwrap().is_some();

    let keys_present = [key1_exists, key2_exists, key3_exists]
        .iter()
        .filter(|&&x| x)
        .count();

    assert_eq!(
        keys_present, 2,
        "Exactly one key should have been evicted"
    );
    assert!(key3_exists, "The most recently inserted key should always exist post-eviction");
}

#[tokio::test]
async fn test_eviction_on_memory_limit_arc() {
    fastrand::seed(0);
    // Set a memory limit that can hold roughly 2-3 items.
    let db: Arc<Database<String, i32>> = Arc::new(
        Database::builder()
            .max_memory(450)
            .eviction_policy(EvictionPolicy::Arc)
            .build()
            .await
            .unwrap(),
    );
    let handle = db.handle();

    // 1. Insert key1, key2. They should land in T1.
    handle.insert("key1".to_string(), 1).await.unwrap();
    handle.insert("key2".to_string(), 2).await.unwrap();

    // 2. Access key1 again. This should promote it to T2.
    assert!(handle.get(&"key1".to_string()).unwrap().is_some());

    // At this point:
    // T1 should contain: key2
    // T2 should contain: key1

    // 3. Insert key3. This will likely fill the cache and trigger an eviction.
    // The victim should be the LRU item from T1, which is key2.
    handle.insert("key3".to_string(), 3).await.unwrap();

    // 4. Verify the state.
    // key1 should still be present because it was in T2 (frequent).
    // key2 should have been evicted.
    // key3 should be present as it was just inserted.
    assert!(
        handle.get(&"key1".to_string()).unwrap().is_some(),
        "key1 should exist as it was promoted to T2"
    );
    assert!(
        handle.get(&"key2".to_string()).unwrap().is_none(),
        "key2 should have been evicted from T1"
    );
    assert!(
        handle.get(&"key3".to_string()).unwrap().is_some(),
        "key3 should exist as it was just inserted"
    );
}

#[tokio::test]
async fn test_manual_eviction_flow() {
    let db: Arc<Database<String, i32>> = Arc::new(
        Database::builder()
            .max_memory(450) // Allows ~2 keys
            .eviction_policy(EvictionPolicy::Manual)
            .build()
            .await
            .unwrap(),
    );
    let handle = db.handle();

    // Insert two keys, should be fine.
    handle.insert("key1".to_string(), 1).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    handle.insert("key2".to_string(), 2).await.unwrap();

    // This insert should fail, asking for manual eviction.
    let result = handle.insert("key3".to_string(), 3).await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), FluxError::MemoryLimitExceeded);

    // Now, manually handle the eviction.
    // We'll implement a simple LRU-style eviction manually.
    let mut victims: Vec<fluxmap::db::KeyMetadata<String>> =
        handle.scan_metadata().collect().await;
    victims.sort_by_key(|v| v.last_accessed);

    assert!(!victims.is_empty());
    let victim_key = victims.first().unwrap().key.clone();

    // Evict the victim.
    let evicted = handle.evict(&victim_key).await.unwrap();
    assert!(evicted);
    assert_eq!(victim_key, "key1"); // "key1" should be the LRU

    // Retry the insert. It should now succeed.
    handle.insert("key3".to_string(), 3).await.unwrap();

    // Verify final state
    assert!(
        handle.get(&"key1".to_string()).unwrap().is_none(),
        "key1 should be evicted"
    );
    assert!(
        handle.get(&"key2".to_string()).unwrap().is_some(),
        "key2 should exist"
    );
    assert!(
        handle.get(&"key3".to_string()).unwrap().is_some(),
        "key3 should now exist"
    );

    // Check memory info
    let info = db.memory_info();
    assert_eq!(info.max_bytes, Some(450));
    assert!(info.current_bytes <= info.max_bytes.unwrap());
}
