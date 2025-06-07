//! Tests for DUPSORT functionality

use heed_core::{EnvBuilder, Database, DatabaseFlags};
use tempfile::TempDir;
use std::sync::Arc;

#[test]
fn test_dupsort_basic() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())
        .unwrap());
    
    // Create database with DUPSORT flag
    let mut wtxn = env.begin_write_txn().unwrap();
    let db: Database<String, String> = env.create_database_with_flags(
        &mut wtxn,
        Some("dupsort_test"),
        DatabaseFlags::DUP_SORT | DatabaseFlags::CREATE
    ).unwrap();
    
    // Insert multiple values for the same key
    db.put_dup(&mut wtxn, "key1".to_string(), "value1".to_string()).unwrap();
    db.put_dup(&mut wtxn, "key1".to_string(), "value2".to_string()).unwrap();
    db.put_dup(&mut wtxn, "key1".to_string(), "value3".to_string()).unwrap();
    
    // Insert single value for another key
    db.put_dup(&mut wtxn, "key2".to_string(), "single_value".to_string()).unwrap();
    
    wtxn.commit().unwrap();
    
    // Read and verify
    let rtxn = env.begin_txn().unwrap();
    
    // Get all duplicates for key1
    let values = db.get_all(&rtxn, &"key1".to_string()).unwrap();
    println!("Values for key1: {:?}", values);
    assert_eq!(values.len(), 3);
    assert!(values.contains(&"value1".to_string()));
    assert!(values.contains(&"value2".to_string()));
    assert!(values.contains(&"value3".to_string()));
    
    // Check key2
    let values = db.get_all(&rtxn, &"key2".to_string()).unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0], "single_value".to_string());
}

#[test]
fn test_dupsort_sorted_order() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())
        .unwrap());
    
    // Create database with DUPSORT flag
    let mut wtxn = env.begin_write_txn().unwrap();
    let db: Database<String, String> = env.create_database_with_flags(
        &mut wtxn,
        Some("dupsort_sorted"),
        DatabaseFlags::DUP_SORT | DatabaseFlags::CREATE
    ).unwrap();
    
    // Insert values in non-sorted order
    db.put_dup(&mut wtxn, "numbers".to_string(), "300".to_string()).unwrap();
    db.put_dup(&mut wtxn, "numbers".to_string(), "100".to_string()).unwrap();
    db.put_dup(&mut wtxn, "numbers".to_string(), "200".to_string()).unwrap();
    db.put_dup(&mut wtxn, "numbers".to_string(), "400".to_string()).unwrap();
    
    wtxn.commit().unwrap();
    
    // Read and verify they're sorted
    let rtxn = env.begin_txn().unwrap();
    let values = db.get_all(&rtxn, &"numbers".to_string()).unwrap();
    
    println!("Sorted values: {:?}", values);
    
    // Values should be lexicographically sorted
    assert_eq!(values.len(), 4);
    assert_eq!(values[0], "100");
    assert_eq!(values[1], "200");
    assert_eq!(values[2], "300");
    assert_eq!(values[3], "400");
}

#[test]
fn test_dupsort_delete() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())
        .unwrap());
    
    // Create database with DUPSORT flag
    let mut wtxn = env.begin_write_txn().unwrap();
    let db: Database<String, String> = env.create_database_with_flags(
        &mut wtxn,
        Some("dupsort_delete"),
        DatabaseFlags::DUP_SORT | DatabaseFlags::CREATE
    ).unwrap();
    
    // Insert multiple values
    db.put_dup(&mut wtxn, "key".to_string(), "value1".to_string()).unwrap();
    db.put_dup(&mut wtxn, "key".to_string(), "value2".to_string()).unwrap();
    db.put_dup(&mut wtxn, "key".to_string(), "value3".to_string()).unwrap();
    
    wtxn.commit().unwrap();
    
    // Delete specific value
    let mut wtxn = env.begin_write_txn().unwrap();
    let deleted = db.delete_dup(&mut wtxn, &"key".to_string(), &"value2".to_string()).unwrap();
    assert!(deleted);
    
    wtxn.commit().unwrap();
    
    // Verify deletion
    let rtxn = env.begin_txn().unwrap();
    let values = db.get_all(&rtxn, &"key".to_string()).unwrap();
    
    assert_eq!(values.len(), 2);
    assert!(values.contains(&"value1".to_string()));
    assert!(!values.contains(&"value2".to_string()));
    assert!(values.contains(&"value3".to_string()));
}

#[test]
fn test_dupsort_mixed_keys() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())
        .unwrap());
    
    // Create database with DUPSORT flag
    let mut wtxn = env.begin_write_txn().unwrap();
    let db: Database<String, String> = env.create_database_with_flags(
        &mut wtxn,
        Some("dupsort_mixed"),
        DatabaseFlags::DUP_SORT | DatabaseFlags::CREATE
    ).unwrap();
    
    // Insert multiple keys with multiple values each
    db.put_dup(&mut wtxn, "a".to_string(), "1".to_string()).unwrap();
    db.put_dup(&mut wtxn, "a".to_string(), "2".to_string()).unwrap();
    
    db.put_dup(&mut wtxn, "b".to_string(), "x".to_string()).unwrap();
    db.put_dup(&mut wtxn, "b".to_string(), "y".to_string()).unwrap();
    db.put_dup(&mut wtxn, "b".to_string(), "z".to_string()).unwrap();
    
    db.put_dup(&mut wtxn, "c".to_string(), "single".to_string()).unwrap();
    
    wtxn.commit().unwrap();
    
    // Verify each key has correct values
    let rtxn = env.begin_txn().unwrap();
    
    let values_a = db.get_all(&rtxn, &"a".to_string()).unwrap();
    assert_eq!(values_a.len(), 2);
    assert_eq!(values_a[0], "1");
    assert_eq!(values_a[1], "2");
    
    let values_b = db.get_all(&rtxn, &"b".to_string()).unwrap();
    assert_eq!(values_b.len(), 3);
    assert_eq!(values_b[0], "x");
    assert_eq!(values_b[1], "y");
    assert_eq!(values_b[2], "z");
    
    let values_c = db.get_all(&rtxn, &"c".to_string()).unwrap();
    assert_eq!(values_c.len(), 1);
    assert_eq!(values_c[0], "single");
}