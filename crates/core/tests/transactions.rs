use std::collections::HashMap;

use deltalake_core::{checkpoints::create_checkpoint_for, kernel::{Action, EagerSnapshot, Txn}, logstore::LogStoreRef, operations::transaction::{CommitBuilder, FinalizedCommit, TableReference}, protocol::DeltaOperation, DeltaTableConfig};
use deltalake_test::{utils::{LocalStorageIntegration, TestTables}, IntegrationContext};
use object_store::path::Path;

async fn assert_txn(log_store: LogStoreRef, old_snapshot: &mut EagerSnapshot, commit: &FinalizedCommit, expected: HashMap<String, i64>) {
    // Assert snapshot captured during commit is accurate, this uses snapshot.advance
    let new_snapshot = commit.snapshot();
    let txns = new_snapshot.app_transaction_version();
    assert_eq!(txns, &expected);

    // Assert updating snapshot via update, loads transactions
    old_snapshot.update(log_store.clone(), None).await.unwrap();
    let txns = old_snapshot.transactions();
    assert_eq!(txns, &expected);
    
    // Reload snapshot from store and assert transactions
    let new_snapshot = EagerSnapshot::try_new(&Path::default(), log_store.object_store(), DeltaTableConfig::default(), None).await.unwrap();
    let txns = new_snapshot.transactions();
    assert_eq!(txns, &expected);
}

#[tokio::test]
async fn test_transaction_write_read() {
    let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default()).unwrap();
    context.load_table(TestTables::Simple).await.unwrap();
    let log_store = context.table_builder(TestTables::Simple).build_storage().unwrap();
    let store = log_store.object_store();

    let mut old_snapshot = EagerSnapshot::try_new(&Path::default(), store.clone(), DeltaTableConfig::default(), None).await.unwrap();
    let txns = old_snapshot.transactions();
    assert_eq!(txns.len(), 0);

    let commit = CommitBuilder::default().with_actions(vec![
        Action::Txn(Txn {
            app_id: "test".to_string(),
            version: 11,
            ..Default::default()
        })
    ]).build(Some(&old_snapshot as &dyn TableReference), log_store.clone(), DeltaOperation::Update { predicate: None }).unwrap().await.unwrap();
    
    assert_txn(log_store.clone(), &mut old_snapshot, &commit, [("test".to_string(), 11)].into_iter().collect()).await;
}

#[tokio::test]
async fn test_transaction_multiple() {
    let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default()).unwrap();
    context.load_table(TestTables::Simple).await.unwrap();
    let log_store = context.table_builder(TestTables::Simple).build_storage().unwrap();
    let store = log_store.object_store();

    let mut old_snapshot = EagerSnapshot::try_new(&Path::default(), store.clone(), DeltaTableConfig::default(), None).await.unwrap();
    let txns = old_snapshot.transactions();
    assert_eq!(txns.len(), 0);

    let commit = CommitBuilder::default().with_actions(vec![
        Action::Txn(Txn {
            app_id: "test".to_string(),
            version: 11,
            ..Default::default()
        })
    ]).build(Some(&old_snapshot as &dyn TableReference), log_store.clone(), DeltaOperation::Update { predicate: None }).unwrap().await.unwrap();
    
    assert_txn(log_store.clone(), &mut old_snapshot, &commit, [("test".to_string(), 11)].into_iter().collect()).await;

    let mut old_snapshot = EagerSnapshot::try_new(&Path::default(), store.clone(), DeltaTableConfig::default(), None).await.unwrap();
    let commit = CommitBuilder::default().with_actions(vec![
        Action::Txn(Txn {
            app_id: "test".to_string(),
            version: 10,
            ..Default::default()
        }),
        Action::Txn(Txn {
            app_id: "test1".to_string(),
            version: 10,
            ..Default::default()
        }),
    ]).build(Some(&old_snapshot as &dyn TableReference), log_store.clone(), DeltaOperation::Update { predicate: None }).unwrap().await.unwrap();

    assert_txn(log_store.clone(), &mut old_snapshot, &commit, [("test".to_string(), 10), ("test1".to_string(), 10)].into_iter().collect()).await;
}

#[tokio::test]
async fn test_transaction_from_checkpoint() {
    let context = IntegrationContext::new(Box::<LocalStorageIntegration>::default()).unwrap();
    context.load_table(TestTables::Simple).await.unwrap();
    let log_store = context.table_builder(TestTables::Simple).build_storage().unwrap();
    let store = log_store.object_store();

    let mut old_snapshot = EagerSnapshot::try_new(&Path::default(), store.clone(), DeltaTableConfig::default(), None).await.unwrap();
    let txns = old_snapshot.transactions();
    assert_eq!(txns.len(), 0);

    let commit = CommitBuilder::default().with_actions(vec![
        Action::Txn(Txn {
            app_id: "test".to_string(),
            version: 11,
            ..Default::default()
        })
    ]).build(Some(&old_snapshot as &dyn TableReference), log_store.clone(), DeltaOperation::Update { predicate: None }).unwrap().await.unwrap();
    
    assert_txn(log_store.clone(), &mut old_snapshot, &commit, [("test".to_string(), 11)].into_iter().collect()).await;

    create_checkpoint_for(commit.version, &commit.snapshot, log_store.as_ref()).await.unwrap();
    let new_snapshot = EagerSnapshot::try_new(&Path::default(), store.clone(), DeltaTableConfig::default(), None).await.unwrap();
    let txns = new_snapshot.transactions();
    assert_eq!(txns.len(), 1);
    assert_eq!(txns.get("test"), Some(&11));

    let mut old_snapshot = EagerSnapshot::try_new(&Path::default(), store.clone(), DeltaTableConfig::default(), None).await.unwrap();
    let commit = CommitBuilder::default().with_actions(vec![
        Action::Txn(Txn {
            app_id: "test".to_string(),
            version: 10,
            ..Default::default()
        }),
        Action::Txn(Txn {
            app_id: "test1".to_string(),
            version: 10,
            ..Default::default()
        }),
    ]).build(Some(&old_snapshot as &dyn TableReference), log_store.clone(), DeltaOperation::Update { predicate: None }).unwrap().await.unwrap();

    assert_txn(log_store.clone(), &mut old_snapshot, &commit, [("test".to_string(), 10), ("test1".to_string(), 10)].into_iter().collect()).await;

    create_checkpoint_for(commit.version, &commit.snapshot, log_store.as_ref()).await.unwrap();
    let new_snapshot = EagerSnapshot::try_new(&Path::default(), store.clone(), DeltaTableConfig::default(), None).await.unwrap();
    let txns = new_snapshot.transactions();
    assert_eq!(txns.len(), 2);
    assert_eq!(txns.get("test"), Some(&10));
    assert_eq!(txns.get("test1"), Some(&10));
}