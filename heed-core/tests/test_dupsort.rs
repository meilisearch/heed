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
    db.put(&mut wtxn, "key1".to_string(), "value1".to_string()).unwrap();
    db.put(&mut wtxn, "key1".to_string(), "value2".to_string()).unwrap();
    db.put(&mut wtxn, "key1".to_string(), "value3".to_string()).unwrap();
    
    // Insert single value for another key
    db.put(&mut wtxn, "key2".to_string(), "single_value".to_string()).unwrap();
    
    wtxn.commit().unwrap();
    
    // Read and verify with cursor
    let rtxn = env.begin_txn().unwrap();
    let mut cursor = db.cursor(&rtxn).unwrap();
    
    // Navigate to key1
    cursor.seek(&"key1".to_string()).unwrap();
    
    // Get all duplicates for key1
    let mut values = Vec::new();
    println!("Getting first dup for key1...");
    if let Some((key, value)) = cursor.first_dup().unwrap() {
        println!("First dup: key={:?}, value={:?}", key, String::from_utf8_lossy(&value));
        assert_eq!(key, b"key1");
        values.push(String::from_utf8_lossy(&value).to_string());
        
        while let Some((key, value)) = cursor.next_dup().unwrap() {
            println!("Next dup: key={:?}, value={:?}", key, String::from_utf8_lossy(&value));
            assert_eq!(key, b"key1");
            values.push(String::from_utf8_lossy(&value).to_string());
        }
    } else {
        println!("No duplicates found!");
    }
    
    // Should have all three values
    println!("Found {} values: {:?}", values.len(), values);
    assert_eq!(values.len(), 3);
    assert!(values.contains(&"value1".to_string()));
    assert!(values.contains(&"value2".to_string()));
    assert!(values.contains(&"value3".to_string()));
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
    
    // Insert values in random order
    db.put(&mut wtxn, "key".to_string(), "zebra".to_string()).unwrap();
    db.put(&mut wtxn, "key".to_string(), "apple".to_string()).unwrap();
    db.put(&mut wtxn, "key".to_string(), "mango".to_string()).unwrap();
    db.put(&mut wtxn, "key".to_string(), "banana".to_string()).unwrap();
    
    wtxn.commit().unwrap();
    
    // Read and verify sorted order
    let rtxn = env.begin_txn().unwrap();
    let mut cursor = db.cursor(&rtxn).unwrap();
    
    cursor.seek(&"key".to_string()).unwrap();
    
    let mut values = Vec::new();
    if let Some((_, value)) = cursor.first_dup().unwrap() {
        values.push(String::from_utf8_lossy(&value).to_string());
        
        while let Some((_, value)) = cursor.next_dup().unwrap() {
            values.push(String::from_utf8_lossy(&value).to_string());
        }
    }
    
    // Values should be in lexicographic order
    assert_eq!(values, vec!["apple", "banana", "mango", "zebra"]);
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
    db.put(&mut wtxn, "key".to_string(), "value1".to_string()).unwrap();
    db.put(&mut wtxn, "key".to_string(), "value2".to_string()).unwrap();
    db.put(&mut wtxn, "key".to_string(), "value3".to_string()).unwrap();
    
    // Delete the entire key (should remove all duplicates)
    assert!(db.delete(&mut wtxn, &"key".to_string()).unwrap());
    
    wtxn.commit().unwrap();
    
    // Verify key is gone
    let rtxn = env.begin_txn().unwrap();
    assert_eq!(db.get(&rtxn, &"key".to_string()).unwrap(), None);
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
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database_with_flags(
        &mut wtxn,
        Some("dupsort_mixed"),
        DatabaseFlags::DUP_SORT | DatabaseFlags::CREATE
    ).unwrap();
    
    // Insert multiple values for different keys
    db.put(&mut wtxn, b"key1".to_vec(), b"a".to_vec()).unwrap();
    db.put(&mut wtxn, b"key1".to_vec(), b"b".to_vec()).unwrap();
    db.put(&mut wtxn, b"key2".to_vec(), b"x".to_vec()).unwrap();
    db.put(&mut wtxn, b"key2".to_vec(), b"y".to_vec()).unwrap();
    db.put(&mut wtxn, b"key2".to_vec(), b"z".to_vec()).unwrap();
    db.put(&mut wtxn, b"key3".to_vec(), b"single".to_vec()).unwrap();
    
    wtxn.commit().unwrap();
    
    // Verify iteration over all entries
    let rtxn = env.begin_txn().unwrap();
    let mut cursor = db.cursor(&rtxn).unwrap();
    
    let mut all_entries = Vec::new();
    
    // Iterate through all keys and their duplicates
    let mut current_key = None;
    while let Some((key, _)) = cursor.next().unwrap() {
        if current_key.as_ref() != Some(&key.to_vec()) {
            // New key found
            current_key = Some(key.to_vec());
            
            // Get all duplicates for this key
            if let Some((k, v)) = cursor.first_dup().unwrap() {
                all_entries.push((k.to_vec(), v.into_owned()));
                
                while let Some((k, v)) = cursor.next_dup().unwrap() {
                    all_entries.push((k.to_vec(), v.into_owned()));
                }
            }
        }
    }
    
    // Should have all 6 entries
    assert_eq!(all_entries.len(), 6);
    
    // Verify key1 entries
    let key1_entries: Vec<_> = all_entries.iter()
        .filter(|(k, _)| k == b"key1")
        .map(|(_, v)| v.clone())
        .collect();
    assert_eq!(key1_entries.len(), 2);
    assert!(key1_entries.contains(&b"a".to_vec()));
    assert!(key1_entries.contains(&b"b".to_vec()));
    
    // Verify key2 entries
    let key2_entries: Vec<_> = all_entries.iter()
        .filter(|(k, _)| k == b"key2")
        .map(|(_, v)| v.clone())
        .collect();
    assert_eq!(key2_entries.len(), 3);
    assert!(key2_entries.contains(&b"x".to_vec()));
    assert!(key2_entries.contains(&b"y".to_vec()));
    assert!(key2_entries.contains(&b"z".to_vec()));
}