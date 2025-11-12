use fluxmap::{error, SkipList};
use futures::pin_mut;
use futures::StreamExt;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

#[tokio::test]
async fn test_new_skip_list() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    assert_eq!(skip_list.len(), 0);
    assert!(skip_list.is_empty());
    // assert_eq!(skip_list.level.load(Ordering::Relaxed), 0); // level is private
}

#[tokio::test]
async fn test_default() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    assert!(skip_list.is_empty());
}

#[tokio::test]
async fn test_insert_and_get() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    let tx_manager = skip_list.transaction_manager();

    // Writer Transaction
    let writer_tx = tx_manager.begin();
    skip_list
        .insert("c".to_string(), Arc::new("three".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("a".to_string(), Arc::new("one".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("b".to_string(), Arc::new("two".to_string()), &writer_tx)
        .await;
    tx_manager.commit(&writer_tx, || Ok(())).unwrap();

    // Reader Transaction
    let reader_tx = tx_manager.begin();
    assert_eq!(skip_list.len(), 3);
    assert_eq!(
        *skip_list.get(&"a".to_string(), &reader_tx).unwrap(),
        "one".to_string()
    );
    assert_eq!(
        *skip_list.get(&"b".to_string(), &reader_tx).unwrap(),
        "two".to_string()
    );
    assert_eq!(
        *skip_list.get(&"c".to_string(), &reader_tx).unwrap(),
        "three".to_string()
    );
    assert!(skip_list.get(&"f".to_string(), &reader_tx).is_none());
}

#[tokio::test]
async fn test_insert_duplicate_key() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    let tx_manager = skip_list.transaction_manager();

    let writer_tx = tx_manager.begin();
    skip_list
        .insert("a".to_string(), Arc::new("one".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("a".to_string(), Arc::new("one_new".to_string()), &writer_tx)
        .await; // Prepend new version
    tx_manager.commit(&writer_tx, || Ok(())).unwrap();

    let reader_tx = tx_manager.begin();
    assert_eq!(skip_list.len(), 1);
    assert_eq!(
        *skip_list.get(&"a".to_string(), &reader_tx).unwrap(),
        "one_new".to_string()
    );
}

#[tokio::test]
async fn test_update_value() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    let tx_manager = skip_list.transaction_manager();

    let writer_tx_1 = tx_manager.begin();
    skip_list
        .insert("a".to_string(), Arc::new("one".to_string()), &writer_tx_1)
        .await;
    tx_manager.commit(&writer_tx_1, || Ok(())).unwrap();

    let reader_tx_1 = tx_manager.begin();
    assert_eq!(
        *skip_list.get(&"a".to_string(), &reader_tx_1).unwrap(),
        "one".to_string()
    );
    tx_manager.commit(&reader_tx_1, || Ok(())).unwrap(); // Commit the reader to release dependencies

    let writer_tx_2 = tx_manager.begin();
    skip_list
        .insert(
            "a".to_string(),
            Arc::new("one_updated".to_string()),
            &writer_tx_2,
        )
        .await;
    tx_manager.commit(&writer_tx_2, || Ok(())).unwrap();

    let reader_tx_2 = tx_manager.begin();
    assert_eq!(skip_list.len(), 1);
    assert_eq!(
        *skip_list.get(&"a".to_string(), &reader_tx_2).unwrap(),
        "one_updated".to_string()
    );
}

#[tokio::test]
async fn test_get_empty() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    let tx = skip_list.transaction_manager().begin();
    assert!(skip_list.get(&"a".to_string(), &tx).is_none());
}

#[tokio::test]
async fn test_remove() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    let tx_manager = skip_list.transaction_manager();

    let writer_tx = tx_manager.begin();
    skip_list
        .insert("a".to_string(), Arc::new("one".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("b".to_string(), Arc::new("two".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("c".to_string(), Arc::new("three".to_string()), &writer_tx)
        .await;
    tx_manager.commit(&writer_tx, || Ok(())).unwrap();

    let reader_tx_1 = tx_manager.begin();
    assert_eq!(skip_list.len(), 3);

    let remover_tx = tx_manager.begin();
    assert_eq!(
        *skip_list
            .remove(&"b".to_string(), &remover_tx)
            .await
            .unwrap(),
        "two".to_string()
    );
    tx_manager.commit(&remover_tx, || Ok(())).unwrap();

    let reader_tx_2 = tx_manager.begin();
    assert_eq!(skip_list.len(), 3);
    assert!(skip_list.get(&"b".to_string(), &reader_tx_2).is_none());

    // Verify that the old transaction still sees the old data
    assert!(skip_list.get(&"b".to_string(), &reader_tx_1).is_some());

    let remover_tx_2 = tx_manager.begin();
    assert!(
        skip_list
            .remove(&"d".to_string(), &remover_tx_2)
            .await
            .is_none()
    );
    assert_eq!(skip_list.len(), 3);
}

#[tokio::test]
async fn test_contains_key() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    let tx_manager = skip_list.transaction_manager();

    let writer_tx = tx_manager.begin();
    skip_list
        .insert("a".to_string(), Arc::new("one".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("c".to_string(), Arc::new("three".to_string()), &writer_tx)
        .await;
    tx_manager.commit(&writer_tx, || Ok(())).unwrap();

    let reader_tx = tx_manager.begin();
    assert!(skip_list.contains_key(&"a".to_string(), &reader_tx));
    assert!(skip_list.contains_key(&"c".to_string(), &reader_tx));
    assert!(!skip_list.contains_key(&"b".to_string(), &reader_tx));
    assert!(!skip_list.contains_key(&"d".to_string(), &reader_tx));
}

#[tokio::test]
async fn test_range() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    let tx_manager = skip_list.transaction_manager();

    let writer_tx = tx_manager.begin();
    skip_list
        .insert("a".to_string(), Arc::new("1".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("b".to_string(), Arc::new("2".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("c".to_string(), Arc::new("3".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("d".to_string(), Arc::new("4".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("e".to_string(), Arc::new("5".to_string()), &writer_tx)
        .await;
    tx_manager.commit(&writer_tx, || Ok(())).unwrap();

    let reader_tx = tx_manager.begin();
    let range = skip_list.range(&"b".to_string(), &"d".to_string(), &reader_tx);
    assert_eq!(range.len(), 3);
    assert_eq!(range[0].0, "b".to_string());
    assert_eq!(range[1].0, "c".to_string());
    assert_eq!(range[2].0, "d".to_string());

    let range_all = skip_list.range(&"a".to_string(), &"z".to_string(), &reader_tx);
    assert_eq!(range_all.len(), 5);
}

#[tokio::test]
async fn test_prefix_scan() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    let tx_manager = skip_list.transaction_manager();

    let writer_tx = tx_manager.begin();
    skip_list
        .insert("apple".to_string(), Arc::new("1".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("apply".to_string(), Arc::new("2".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("banana".to_string(), Arc::new("3".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("bandana".to_string(), Arc::new("4".to_string()), &writer_tx)
        .await;
    tx_manager.commit(&writer_tx, || Ok(())).unwrap();

    let reader_tx = tx_manager.begin();
    let scan = skip_list.prefix_scan("app", &reader_tx);
    assert_eq!(scan.len(), 2);
    assert_eq!(scan[0].0, "apple".to_string());
    assert_eq!(scan[1].0, "apply".to_string());

    let scan2 = skip_list.prefix_scan("ban", &reader_tx);
    assert_eq!(scan2.len(), 2);
}

#[tokio::test]
async fn test_range_stream() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    let tx_manager = skip_list.transaction_manager();

    let writer_tx = tx_manager.begin();
    skip_list
        .insert("a".to_string(), Arc::new("1".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("b".to_string(), Arc::new("2".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("c".to_string(), Arc::new("3".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("d".to_string(), Arc::new("4".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("e".to_string(), Arc::new("5".to_string()), &writer_tx)
        .await;
    tx_manager.commit(&writer_tx, || Ok(())).unwrap();

    let reader_tx = tx_manager.begin();
    let start_key = "b".to_string();
    let end_key = "d".to_string();
    let range_stream = skip_list.range_stream(&start_key, &end_key, &reader_tx);
    pin_mut!(range_stream);
    assert_eq!(range_stream.next().await.unwrap().0, "b".to_string());
    assert_eq!(range_stream.next().await.unwrap().0, "c".to_string());
    assert_eq!(range_stream.next().await.unwrap().0, "d".to_string());
    assert!(range_stream.next().await.is_none());
}

#[tokio::test]
async fn test_prefix_scan_stream() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list: SkipList<String, String> = SkipList::new(mem, clock);
    let tx_manager = skip_list.transaction_manager();

    let writer_tx = tx_manager.begin();
    skip_list
        .insert("apple".to_string(), Arc::new("1".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("apply".to_string(), Arc::new("2".to_string()), &writer_tx)
        .await;
    skip_list
        .insert("banana".to_string(), Arc::new("3".to_string()), &writer_tx)
        .await;
    tx_manager.commit(&writer_tx, || Ok(())).unwrap();

    let reader_tx = tx_manager.begin();
    let prefix_key = "app".to_string();
    let scan_stream = skip_list.prefix_scan_stream(&prefix_key, &reader_tx);
    pin_mut!(scan_stream);
    assert_eq!(scan_stream.next().await.unwrap().0, "apple".to_string());
    assert_eq!(scan_stream.next().await.unwrap().0, "apply".to_string());
    assert!(scan_stream.next().await.is_none());
}

#[tokio::test]
async fn test_concurrent_insert() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::new(mem, clock));
    let tx_manager = skip_list.transaction_manager().clone();
    let mut tasks = vec![];

    for i in 0..1000 {
        let skip_list = skip_list.clone();
        let tx_manager = tx_manager.clone();
        tasks.push(tokio::spawn(async move {
            let tx = tx_manager.begin();
            skip_list
                .insert(i.to_string(), Arc::new(i.to_string()), &tx)
                .await;
            tx_manager.commit(&tx, || Ok(())).unwrap();
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(skip_list.len(), 1000);
}

#[tokio::test]
async fn test_concurrent_insert_and_remove() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::new(mem, clock));
    let tx_manager = skip_list.transaction_manager().clone();

    // Insert 1000 items.
    let mut tasks = vec![];
    for i in 0..1000 {
        let skip_list = skip_list.clone();
        let tx_manager = tx_manager.clone();
        tasks.push(tokio::spawn(async move {
            let tx = tx_manager.begin();
            skip_list
                .insert(i.to_string(), Arc::new(i.to_string()), &tx)
                .await;
            tx_manager.commit(&tx, || Ok(())).unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let _reader_tx = tx_manager.begin();
    assert_eq!(skip_list.len(), 1000);

    // Concurrently remove half of the items.
    let mut tasks = vec![];
    for i in 0..500 {
        let skip_list = skip_list.clone();
        let tx_manager = tx_manager.clone();
        tasks.push(tokio::spawn(async move {
            let tx = tx_manager.begin();
            skip_list.remove(&i.to_string(), &tx).await;
            tx_manager.commit(&tx, || Ok(())).unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let final_tx = tx_manager.begin();
    for i in 0..500 {
        assert!(!skip_list.contains_key(&i.to_string(), &final_tx));
    }
    for i in 500..1000 {
        assert!(skip_list.contains_key(&i.to_string(), &final_tx));
    }
}

#[tokio::test]
async fn test_stress_concurrent_operations() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::<String, String>::new(mem, clock));
    let tx_manager = skip_list.transaction_manager().clone();
    let num_tasks = 100;
    let ops_per_task = 100;
    let key_range = 500;

    let mut tasks = vec![];
    for i in 0..num_tasks {
        let skip_list = skip_list.clone();
        let tx_manager = tx_manager.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = rand::rngs::StdRng::seed_from_u64(i as u64);
            let tx = tx_manager.begin();
            for _ in 0..ops_per_task {
                let key = rng.random_range(0..key_range);
                match rng.random_range(0..5) {
                    0 => {
                        skip_list
                            .insert(key.to_string(), Arc::new(key.to_string()), &tx)
                            .await;
                    }
                    1 => {
                        skip_list.get(&key.to_string(), &tx);
                    }
                    2 => {
                        skip_list.remove(&key.to_string(), &tx).await;
                    }
                    3 => {
                        let start = rng.random_range(0..key_range).to_string();
                        let end = rng
                            .random_range(start.parse::<i32>().unwrap()..key_range)
                            .to_string();
                        let range = skip_list.range(&start, &end, &tx);
                        // Check that the snapshot is consistent.
                        for i in 0..range.len().saturating_sub(1) {
                            assert!(range[i].0 <= range[i + 1].0);
                        }
                    }
                    4 => {
                        let prefix = rng.random_range(0..key_range).to_string();
                        let scan = skip_list.prefix_scan(&prefix, &tx);
                        // Check that the snapshot is consistent.
                        for (key, _) in scan {
                            assert!(key.starts_with(&prefix));
                        }
                    }
                    _ => unreachable!(),
                }
            }
            tx_manager.commit(&tx, || Ok(())).unwrap();
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_range_stream_modifications() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::<String, String>::new(mem, clock));
    let tx_manager = skip_list.transaction_manager().clone();
    let num_modifiers = 10;
    let ops_per_modifier = 100;
    let key_range = 100;

    // Spawn tasks that continuously modify the skip list
    let mut modifier_tasks = vec![];
    for i in 0..num_modifiers {
        let skip_list_clone = skip_list.clone();
        let tx_manager = tx_manager.clone();
        modifier_tasks.push(tokio::spawn(async move {
            let mut rng = rand::rngs::StdRng::seed_from_u64(i as u64);
            let tx = tx_manager.begin();
            for _ in 0..ops_per_modifier {
                let key = rng.random_range(0..key_range).to_string();
                match rng.random_range(0..2) {
                    0 => {
                        skip_list_clone
                            .insert(key.clone(), Arc::new(key.clone()), &tx)
                            .await;
                    }
                    1 => {
                        skip_list_clone.remove(&key, &tx).await;
                    }
                    _ => unreachable!(),
                }
            }
            tx_manager.commit(&tx, || Ok(())).unwrap();
        }));
    }

    // Run range stream on the main thread while modifications are happening
    let start_key = "10".to_string();
    let end_key = "50".to_string();
    let tx = tx_manager.begin();
    let stream = skip_list.range_stream(&start_key, &end_key, &tx);
    pin_mut!(stream);

    let mut prev_key = None;
    while let Some((key, _)) = stream.next().await {
        // Assert that keys are within the range and sorted
        assert!(key >= start_key && key <= end_key);
        if let Some(pk) = prev_key {
            assert!(key >= pk);
        }
        prev_key = Some(key);
    }

    // Wait for all modifier tasks to complete
    for task in modifier_tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_prefix_scan_modifications() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::<String, String>::new(mem, clock));
    let tx_manager = skip_list.transaction_manager().clone();
    let num_modifiers = 10;
    let ops_per_modifier = 100;
    let key_range = 100;

    // Spawn tasks that continuously modify the skip list
    let mut modifier_tasks = vec![];
    for i in 0..num_modifiers {
        let skip_list_clone = skip_list.clone();
        let tx_manager = tx_manager.clone();
        modifier_tasks.push(tokio::spawn(async move {
            let mut rng = rand::rngs::StdRng::seed_from_u64(i as u64);
            let tx = tx_manager.begin();
            for _ in 0..ops_per_modifier {
                let key = rng.random_range(0..key_range).to_string();
                match rng.random_range(0..2) {
                    0 => {
                        skip_list_clone
                            .insert(key.clone(), Arc::new(key.clone()), &tx)
                            .await;
                    }
                    1 => {
                        skip_list_clone.remove(&key, &tx).await;
                    }
                    _ => unreachable!(),
                }
            }
            tx_manager.commit(&tx, || Ok(())).unwrap();
        }));
    }

    // Run prefix scan stream on the main thread while modifications are happening
    let prefix = "1".to_string(); // Scan for keys starting with '1'
    let tx = tx_manager.begin();
    let stream = skip_list.prefix_scan_stream(&prefix, &tx);
    pin_mut!(stream);

    let mut prev_key = None;
    while let Some((key, _)) = stream.next().await {
        // Assert that keys start with the prefix and are sorted
        assert!(key.starts_with(&prefix));
        if let Some(pk) = prev_key {
            assert!(key >= pk);
        }
        prev_key = Some(key);
    }

    // Wait for all modifier tasks to complete
    for task in modifier_tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn test_write_skew_prevention() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::<String, i32>::new(mem, clock));
    let tx_manager = skip_list.transaction_manager();

    // Initialize two keys, x and y, with a sum of 100.
    let setup_tx = tx_manager.begin();
    skip_list
        .insert("x".to_string(), Arc::new(60), &setup_tx)
        .await;
    skip_list
        .insert("y".to_string(), Arc::new(40), &setup_tx)
        .await;
    tx_manager.commit(&setup_tx, || Ok(())).unwrap();

    // Transaction 1: Reads x and y, then writes to x.
    let tx1 = tx_manager.begin();
    let x1 = skip_list.get(&"x".to_string(), &tx1).unwrap();
    let y1 = skip_list.get(&"y".to_string(), &tx1).unwrap();
    assert_eq!(*x1 + *y1, 100);

    // Transaction 2: Reads x and y, then writes to y.
    let tx2 = tx_manager.begin();
    let x2 = skip_list.get(&"x".to_string(), &tx2).unwrap();
    let y2 = skip_list.get(&"y".to_string(), &tx2).unwrap();
    assert_eq!(*x2 + *y2, 100);

    // In a simple SI implementation, both transactions would be able to commit,
    // leading to a final state where x=10 and y=10, and the sum is 20, violating the invariant.

    // Tx1 writes based on its read.
    skip_list.insert("x".to_string(), Arc::new(10), &tx1).await;

    // Tx2 writes based on its read.
    skip_list.insert("y".to_string(), Arc::new(10), &tx2).await;

    // Now, attempt to commit both. One must fail.
    let commit1_result = tx_manager.commit(&tx1, || Ok(()));
    let commit2_result = tx_manager.commit(&tx2, || Ok(()));

    let success = commit1_result.is_ok() || commit2_result.is_ok();
    let failure = commit1_result.is_err() || commit2_result.is_err();

    assert!(success, "At least one transaction should succeed");
    assert!(
        failure,
        "At least one transaction should fail due to write skew"
    );

    // Further, check that the error is the one we expect.
    if let Err(e) = &commit1_result {
        assert_eq!(*e, error::FluxError::SerializationConflict);
    }
    if let Err(e) = &commit2_result {
        assert_eq!(*e, error::FluxError::SerializationConflict);
    }

    // Verify the final state is consistent.
    let final_tx = tx_manager.begin();
    let final_x = skip_list
        .get(&"x".to_string(), &final_tx)
        .unwrap_or(Arc::new(0));
    let final_y = skip_list
        .get(&"y".to_string(), &final_tx)
        .unwrap_or(Arc::new(0));

    if commit1_result.is_ok() {
        // Tx1 succeeded, Tx2 failed.
        assert_eq!(*final_x, 10);
        assert_eq!(*final_y, 40); // y should be unchanged
    } else {
        // Tx2 succeeded, Tx1 failed.
        assert_eq!(*final_x, 60); // x should be unchanged
        assert_eq!(*final_y, 10);
    }
    assert_ne!(*final_x + *final_y, 20);
}

#[tokio::test]
async fn test_vacuum() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::<String, i32>::new(mem, clock));
    let tx_manager = skip_list.transaction_manager();

    // Tx1: Insert x=10, y=20
    let tx1 = tx_manager.begin();
    skip_list.insert("x".to_string(), Arc::new(10), &tx1).await;
    skip_list.insert("y".to_string(), Arc::new(20), &tx1).await;
    tx_manager.commit(&tx1, || Ok(())).unwrap();

    // Tx2: Delete x, Update y to 30
    let tx2 = tx_manager.begin();
    skip_list.remove(&"x".to_string(), &tx2).await;
    skip_list.insert("y".to_string(), Arc::new(30), &tx2).await; // This prepends a new version, does not expire the old one.
    tx_manager.commit(&tx2, || Ok(())).unwrap();

    // Run vacuum. Only the version of `x` was explicitly removed.
    let (versions_removed, _keys_removed) = skip_list.vacuum().await.unwrap();

    // Only the explicitly `remove`d version of 'x' should be cleaned up.
    assert_eq!(
        versions_removed, 1,
        "Vacuum should have removed one dead version."
    );

    // A final check to ensure the correct data is still visible.
    let final_tx = tx_manager.begin();
    assert!(skip_list.get(&"x".to_string(), &final_tx).is_none());
    assert_eq!(*skip_list.get(&"y".to_string(), &final_tx).unwrap(), 30);
    tx_manager.commit(&final_tx, || Ok(())).unwrap();

    // --- Test multiple dead versions for the same key ---
    // Create a situation with multiple expired versions for a single key 'z'
    let tx3 = tx_manager.begin();
    skip_list.insert("z".to_string(), Arc::new(1), &tx3).await;
    tx_manager.commit(&tx3, || Ok(())).unwrap();

    let tx4 = tx_manager.begin();
    skip_list.remove(&"z".to_string(), &tx4).await; // Expires z=1
    tx_manager.commit(&tx4, || Ok(())).unwrap();

    let tx5 = tx_manager.begin();
    skip_list.insert("z".to_string(), Arc::new(2), &tx5).await;
    tx_manager.commit(&tx5, || Ok(())).unwrap();

    let tx6 = tx_manager.begin();
    skip_list.remove(&"z".to_string(), &tx6).await; // Expires z=2
    tx_manager.commit(&tx6, || Ok(())).unwrap();

    let tx7 = tx_manager.begin();
    skip_list.insert("z".to_string(), Arc::new(3), &tx7).await; // Current visible version
    tx_manager.commit(&tx7, || Ok(())).unwrap();

    // The version chain for 'z' is now 3 -> 2(expired) -> 1(expired)
    // The vacuum should remove the two expired versions.
    let (versions_removed_z, _) = skip_list.vacuum().await.unwrap();
    assert_eq!(versions_removed_z, 2);

    // Check that the correct version of 'z' is visible.
    let final_tx_z = tx_manager.begin();
    assert_eq!(*skip_list.get(&"z".to_string(), &final_tx_z).unwrap(), 3);
}

#[tokio::test]
async fn test_vacuum_removes_node() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::<String, i32>::new(mem, clock));
    let tx_manager = skip_list.transaction_manager();

    // Tx1: Insert "a"
    let tx1 = tx_manager.begin();
    skip_list.insert("a".to_string(), Arc::new(1), &tx1).await;
    tx_manager.commit(&tx1, || Ok(())).unwrap();

    // Tx2: Remove "a"
    let tx2 = tx_manager.begin();
    assert_eq!(
        skip_list.remove(&"a".to_string(), &tx2).await.unwrap(),
        Arc::new(1)
    );
    tx_manager.commit(&tx2, || Ok(())).unwrap();

    // Before vacuum, the node for "a" still exists but has no visible versions.
    let tx3 = tx_manager.begin();
    assert!(skip_list.get(&"a".to_string(), &tx3).is_none());
    tx_manager.commit(&tx3, || Ok(())).unwrap();

    // Run vacuum. This should remove the dead version of "a" and then
    // mark the now-empty node as "deleted".
    let (versions_removed, keys_removed) = skip_list.vacuum().await.unwrap();
    assert_eq!(versions_removed, 1);
    assert_eq!(keys_removed, 1);

    // After vacuum, the key should still be gone. Traversal should physically
    // remove the node, but we can't easily verify that here. We just
    // confirm the key remains inaccessible.
    let tx4 = tx_manager.begin();
    assert!(skip_list.get(&"a".to_string(), &tx4).is_none());
    tx_manager.commit(&tx4, || Ok(())).unwrap();
}

#[tokio::test]
async fn test_remove_respects_snapshot() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::<String, i32>::new(mem, clock));
    let tx_manager = skip_list.transaction_manager();

    // Tx1: Insert "a" with value 1
    let tx1 = tx_manager.begin();
    skip_list.insert("a".to_string(), Arc::new(1), &tx1).await;
    tx_manager.commit(&tx1, || Ok(())).unwrap();

    // Tx2 (Updater): Start a transaction to update "a" to 2, but DON'T commit yet.
    // This creates a new version that is not visible to other transactions.
    let tx2_updater = tx_manager.begin();
    skip_list
        .insert("a".to_string(), Arc::new(2), &tx2_updater)
        .await;

    // Tx3 (Remover): Start a new transaction. Its snapshot should only see "a" = 1.
    let tx3_remover = tx_manager.begin();

    // The `get` should see the old value.
    assert_eq!(
        *skip_list.get(&"a".to_string(), &tx3_remover).unwrap(),
        1,
        "Remover should see the initial value"
    );

    // Now, `remove` should find the visible version (value 1) and expire it.
    // The old, incorrect implementation would have tried to expire version 2.
    let removed_value = skip_list.remove(&"a".to_string(), &tx3_remover).await;
    assert_eq!(
        *removed_value.unwrap(),
        1,
        "Remove should act on the visible version"
    );

    // Commit the removal.
    tx_manager.commit(&tx3_remover, || Ok(())).unwrap();

    // Tx4 (Final Reader): A new transaction should see the key as removed.
    let tx4_reader = tx_manager.begin();
    assert!(
        skip_list.get(&"a".to_string(), &tx4_reader).is_none(),
        "A new transaction should not see the key"
    );

    // The uncommitted updater transaction should now fail if it tries to commit.
    let commit_result = tx_manager.commit(&tx2_updater, || Ok(()));
    assert!(
        commit_result.is_err(),
        "Updater transaction should fail to commit due to conflict"
    );
    assert_eq!(
        commit_result.unwrap_err(),
        error::FluxError::SerializationConflict
    );
}

#[tokio::test]
async fn test_vacuum_handles_uncommitted_expirer() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::<String, i32>::new(mem, clock));
    let tx_manager = skip_list.transaction_manager();

    // Tx1: Insert "a" with value 1 and commit.
    let tx1 = tx_manager.begin();
    skip_list.insert("a".to_string(), Arc::new(1), &tx1).await;
    tx_manager.commit(&tx1, || Ok(())).unwrap();

    // Tx2: Remove "a" but DO NOT commit yet.
    // This creates a version (value 1) expired by an uncommitted transaction (Tx2).
    let tx2_expirer = tx_manager.begin();
    let removed_val = skip_list.remove(&"a".to_string(), &tx2_expirer).await;
    assert_eq!(*removed_val.unwrap(), 1);

    // Run vacuum. It should NOT remove the version expired by tx2_expirer
    // because tx2_expirer is still active.
    let (versions_removed_before_commit, keys_removed_before_commit) =
        skip_list.vacuum().await.unwrap();
    assert_eq!(
        versions_removed_before_commit, 0,
        "No versions should be removed by vacuum before expirer commits"
    );
    assert_eq!(
        keys_removed_before_commit, 0,
        "No keys should be removed by vacuum before expirer commits"
    );

    // Verify that "a" is still visible to an old snapshot (if any) or not visible to new ones.
    let reader_tx_before_commit = tx_manager.begin();
    assert_eq!(
        *skip_list
            .get(&"a".to_string(), &reader_tx_before_commit)
            .unwrap(),
        1,
        "Key 'a' should still be visible with its original value to a new reader before expirer commits."
    );

    // Commit tx2_expirer. Now the version expired by tx2 is truly dead.
    tx_manager.commit(&tx2_expirer, || Ok(())).unwrap();

    // Run vacuum again. Now it SHOULD remove the version.
    let (versions_removed_after_commit, keys_removed_after_commit) =
        skip_list.vacuum().await.unwrap();
    assert_eq!(
        versions_removed_after_commit, 1,
        "One version should be removed by vacuum after expirer commits"
    );
    assert_eq!(
        keys_removed_after_commit, 1,
        "One key should be removed by vacuum after expirer commits"
    );

    // Verify "a" is still gone.
    let final_reader_tx = tx_manager.begin();
    assert!(skip_list.get(&"a".to_string(), &final_reader_tx).is_none());
}

#[tokio::test]
async fn test_transaction_status_pruning() {
    let mem = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AtomicU64::new(0));
    let skip_list = Arc::new(SkipList::<String, i32>::new(mem, clock));
    let tx_manager = skip_list.transaction_manager();

    let num_transactions = 100;
    let mut committed_tx_ids = Vec::new();

    // Create and commit a number of transactions
    for i in 0..num_transactions {
        let tx = tx_manager.begin();
        skip_list
            .insert(format!("key{}", i), Arc::new(i), &tx)
            .await;
        tx_manager.commit(&tx, || Ok(())).unwrap();
        committed_tx_ids.push(tx.id);
    }

    // At this point, the `statuses` map should contain entries for all committed transactions.
    // The exact count might be `num_transactions` + 1 (for the initial txid 0 or 1).
    let initial_status_count = tx_manager.statuses_len();
    assert!(initial_status_count >= num_transactions as usize);

    let (_versions_removed, _keys_removed) = skip_list.vacuum().await.unwrap();
    // We expect some versions/keys to be removed if there were any logical deletions.
    // For this test, we just care about status pruning.

    // After vacuum, the `statuses` map should be pruned.
    // The number of statuses should be significantly less than the initial count,
    // ideally close to 0 if no active transactions remain and all versions are gone.
    let final_status_count = tx_manager.statuses_len();
    // The `min_retainable_txid` will be the current `next_txid` if no active transactions.
    // So, only statuses for transactions >= `min_retainable_txid` should remain.
    // In this simple case, it should be very small, possibly 0 or 1 (for the next_txid itself).
    assert!(
        final_status_count < initial_status_count,
        "Transaction statuses should have been pruned."
    );
    assert!(
        final_status_count <= 1,
        "Expected very few statuses remaining after pruning."
    ); // Should be 0 or 1 (for the next_txid)
}
