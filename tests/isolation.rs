use fluxmap::db::{Database, IsolationLevel};
use fluxmap::error::FluxError;
use std::sync::Arc;

/// This test demonstrates a classic write-skew anomaly, which Snapshot Isolation (SI)
/// is vulnerable to, but Serializable Snapshot Isolation (SSI) prevents.
///
/// Scenario:
/// Two bank accounts, 'x' and 'y', must maintain a combined balance of >= 0.
/// T1 tries to withdraw 150 from 'x' after checking the combined balance.
/// T2 tries to withdraw 150 from 'y' after checking the combined balance.
/// Under SI, both will succeed, leading to a negative total balance.
/// Under SSI, one must fail.
#[tokio::test]
async fn test_write_skew_under_snapshot_isolation() {
    // 1. Setup database with Snapshot Isolation
    let db: Arc<Database<String, i64>> = Arc::new(
        Database::builder()
            .isolation_level(IsolationLevel::Snapshot)
            .build()
            .await
            .unwrap(),
    );

    // Initial state: x=100, y=100. Invariant: x + y >= 0
    let handle = db.handle();
    handle.insert("x".to_string(), 100).await.unwrap();
    handle.insert("y".to_string(), 100).await.unwrap();
    drop(handle);

    // 2. Start two concurrent transactions
    let db1 = db.clone();
    let t1 = tokio::spawn(async move {
        let mut handle = db1.handle();
        handle.transaction(|h| Box::pin(async move {
            let x = h.get(&"x".to_string()).unwrap().unwrap();
            let y = h.get(&"y".to_string()).unwrap().unwrap();

            // Check invariant before write
            if (*x + *y) >= 150 {
                // Simulate some work
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                h.insert("x".to_string(), *x - 150).await?; // x becomes -50
                Ok(())
            } else {
                // This branch should not be taken in this test
                panic!("Insufficient funds, test logic error");
            }
        })).await
    });

    let db2 = db.clone();
    let t2 = tokio::spawn(async move {
        let mut handle = db2.handle();
        handle.transaction(|h| Box::pin(async move {
            let x = h.get(&"x".to_string()).unwrap().unwrap();
            let y = h.get(&"y".to_string()).unwrap().unwrap();

            // Check invariant before write
            if (*x + *y) >= 150 {
                // Wait slightly less to try and commit first
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                h.insert("y".to_string(), *y - 150).await?; // y becomes -50
                Ok(())
            } else {
                // This branch should not be taken in this test
                panic!("Insufficient funds, test logic error");
            }
        })).await
    });

    let res1: Result<(), FluxError> = t1.await.unwrap();
    let res2: Result<(), FluxError> = t2.await.unwrap();

    // 3. Assertions for SI
    // Under SI, both transactions should commit successfully because they don't
    // write to the same keys they read from the other transaction.
    assert!(res1.is_ok(), "T1 should commit successfully under SI");
    assert!(res2.is_ok(), "T2 should commit successfully under SI");

    // 4. Verify the broken invariant
    let final_handle = db.handle();
    let final_x = final_handle.get(&"x".to_string()).unwrap().unwrap();
    let final_y = final_handle.get(&"y".to_string()).unwrap().unwrap();

    assert_eq!(*final_x, -50);
    assert_eq!(*final_y, -50);
    assert!(
        (*final_x + *final_y) < 0,
        "Write skew occurred: final balance is negative!"
    );
}

#[tokio::test]
async fn test_write_skew_is_prevented_by_serializable() {
    // 1. Setup database with Serializable Snapshot Isolation (SSI)
    let db: Arc<Database<String, i64>> = Arc::new(
        Database::builder()
            .isolation_level(IsolationLevel::Serializable) // The default, but explicit for clarity
            .build()
            .await
            .unwrap(),
    );

    // Initial state: x=100, y=100. Invariant: x + y >= 0
    let handle = db.handle();
    handle.insert("x".to_string(), 100).await.unwrap();
    handle.insert("y".to_string(), 100).await.unwrap();
    drop(handle);

    // 2. Start two concurrent transactions
    let db1 = db.clone();
    let t1 = tokio::spawn(async move {
        let mut handle = db1.handle();
        handle.transaction(|h| Box::pin(async move {
            let x = h.get(&"x".to_string()).unwrap().unwrap();
            let y = h.get(&"y".to_string()).unwrap().unwrap();

            if (*x + *y) >= 150 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                h.insert("x".to_string(), *x - 150).await?;
                Ok(())
            } else {
                panic!("Insufficient funds, test logic error");
            }
        })).await
    });

    let db2 = db.clone();
    let t2 = tokio::spawn(async move {
        let mut handle = db2.handle();
        handle.transaction(|h| Box::pin(async move {
            let x = h.get(&"x".to_string()).unwrap().unwrap();
            let y = h.get(&"y".to_string()).unwrap().unwrap();

            if (*x + *y) >= 150 {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                h.insert("y".to_string(), *y - 150).await?;
                Ok(())
            } else {
                panic!("Insufficient funds, test logic error");
            }
        })).await
    });

    let res1: Result<(), FluxError> = t1.await.unwrap();
    let res2: Result<(), FluxError> = t2.await.unwrap();

    // 3. Assertions for SSI
    // Under SSI, one transaction must be aborted to prevent write skew.
    let success1 = res1.is_ok();
    let success2 = res2.is_ok();

    assert_ne!(success1, success2, "Exactly one transaction should succeed and one should fail. res1: {:?}, res2: {:?}", res1, res2);

    if success1 {
        // T1 succeeded, T2 must have failed with SerializationConflict
        assert_eq!(res2.unwrap_err(), FluxError::SerializationConflict);
    } else {
        // T2 succeeded, T1 must have failed with SerializationConflict
        assert_eq!(res1.unwrap_err(), FluxError::SerializationConflict);
    }

    // 4. Verify the invariant is maintained
    let final_handle = db.handle();
    let final_x = final_handle.get(&"x".to_string()).unwrap().unwrap();
    let final_y = final_handle.get(&"y".to_string()).unwrap().unwrap();

    assert!(
        (*final_x + *final_y) >= 0,
        "Invariant maintained: final balance is non-negative"
    );

    // Check the final state based on which transaction succeeded
    if success1 { // T1 succeeded, T2 failed
        assert_eq!(*final_x, -50);
        assert_eq!(*final_y, 100);
    } else { // T2 succeeded, T1 failed
        assert_eq!(*final_x, 100);
        assert_eq!(*final_y, -50);
    }
}
