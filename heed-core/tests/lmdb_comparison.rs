//! Correctness tests comparing heed-core behavior with LMDB
//! 
//! These tests ensure that heed-core produces the same results as LMDB
//! for various database operations.

use tempfile::TempDir;
use std::sync::Arc;

// Import both heed (FFI LMDB) and heed-core
use heed::{EnvOpenOptions as LmdbEnvOptions, Database as LmdbDatabase, types::{Str}};
use heed_core::{EnvBuilder as CoreEnvBuilder, Database as CoreDatabase};

#[test]
fn test_basic_operations_match() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    
    // Create two subdirectories for each implementation
    let lmdb_path = path.join("lmdb");
    let core_path = path.join("core");
    std::fs::create_dir_all(&lmdb_path).unwrap();
    std::fs::create_dir_all(&core_path).unwrap();
    
    // Test data
    let test_data = vec![
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
        ("apple", "fruit"),
        ("zebra", "animal"),
    ];
    
    // LMDB operations
    let lmdb_result = {
        let env = unsafe { LmdbEnvOptions::new()
            .map_size(10 * 1024 * 1024)
            .open(&lmdb_path)
            .unwrap()
        };
        
        let mut wtxn = env.write_txn().unwrap();
        let db: LmdbDatabase<Str, Str> = env.create_database(&mut wtxn, None).unwrap();
        
        // Insert data
        for (key, value) in &test_data {
            db.put(&mut wtxn, key, value).unwrap();
        }
        
        wtxn.commit().unwrap();
        
        // Read back
        let rtxn = env.read_txn().unwrap();
        let mut results = Vec::new();
        
        for (key, _) in &test_data {
            if let Some(value) = db.get(&rtxn, key).unwrap() {
                results.push((key.to_string(), value.to_string()));
            }
        }
        
        results.sort();
        results
    };
    
    // heed-core operations
    let core_result = {
        let env = Arc::new(CoreEnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&core_path)
            .unwrap()
        );
        
        let mut wtxn = env.begin_write_txn().unwrap();
        let db: CoreDatabase<String, String> = env.create_database(&mut wtxn, None).unwrap();
        
        // Insert data
        for (key, value) in &test_data {
            db.put(&mut wtxn, key.to_string(), value.to_string()).unwrap();
        }
        
        wtxn.commit().unwrap();
        
        // Read back
        let rtxn = env.begin_txn().unwrap();
        let mut results = Vec::new();
        
        for (key, _) in &test_data {
            if let Some(value) = db.get(&rtxn, &key.to_string()).unwrap() {
                results.push((key.to_string(), value));
            }
        }
        
        results.sort();
        results
    };
    
    // Compare results
    assert_eq!(lmdb_result, core_result, "LMDB and heed-core should produce identical results");
}

#[test]
fn test_cursor_iteration_matches() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    
    let lmdb_path = path.join("lmdb");
    let core_path = path.join("core");
    std::fs::create_dir_all(&lmdb_path).unwrap();
    std::fs::create_dir_all(&core_path).unwrap();
    
    // Test with more data to ensure ordering is consistent
    let test_data: Vec<(String, String)> = (0..50)
        .map(|i| (format!("key_{:03}", i), format!("value_{}", i)))
        .collect();
    
    // LMDB cursor iteration
    let lmdb_result = {
        let env = unsafe { LmdbEnvOptions::new()
            .map_size(10 * 1024 * 1024)
            .open(&lmdb_path)
            .unwrap()
        };
        
        let mut wtxn = env.write_txn().unwrap();
        let db: LmdbDatabase<Str, Str> = env.create_database(&mut wtxn, None).unwrap();
        
        for (key, value) in &test_data {
            db.put(&mut wtxn, key, value).unwrap();
        }
        
        wtxn.commit().unwrap();
        
        // Iterate with cursor
        let rtxn = env.read_txn().unwrap();
        let mut results = Vec::new();
        
        for item in db.iter(&rtxn).unwrap() {
            let (key, value) = item.unwrap();
            results.push((key.to_string(), value.to_string()));
        }
        
        results
    };
    
    // heed-core cursor iteration
    let core_result = {
        let env = Arc::new(CoreEnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&core_path)
            .unwrap()
        );
        
        let mut wtxn = env.begin_write_txn().unwrap();
        let db: CoreDatabase<String, String> = env.create_database(&mut wtxn, None).unwrap();
        
        for (key, value) in &test_data {
            db.put(&mut wtxn, key.clone(), value.clone()).unwrap();
        }
        
        wtxn.commit().unwrap();
        
        // Iterate with cursor
        let rtxn = env.begin_txn().unwrap();
        let mut cursor = db.cursor(&rtxn).unwrap();
        let mut results = Vec::new();
        
        while let Some((key, value)) = cursor.next().unwrap() {
            results.push((String::from_utf8(key).unwrap(), value));
        }
        
        results
    };
    
    // Both should produce the same ordered results
    assert_eq!(lmdb_result.len(), core_result.len(), "Same number of entries");
    assert_eq!(lmdb_result, core_result, "Cursor iteration should produce identical results");
}

#[test]
fn test_delete_operations_match() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    
    let lmdb_path = path.join("lmdb");
    let core_path = path.join("core");
    std::fs::create_dir_all(&lmdb_path).unwrap();
    std::fs::create_dir_all(&core_path).unwrap();
    
    let initial_data = vec![
        ("a", "1"),
        ("b", "2"),
        ("c", "3"),
        ("d", "4"),
        ("e", "5"),
    ];
    
    let to_delete = vec!["b", "d"];
    
    // LMDB delete operations
    let lmdb_result = {
        let env = unsafe { LmdbEnvOptions::new()
            .map_size(10 * 1024 * 1024)
            .open(&lmdb_path)
            .unwrap()
        };
        
        let mut wtxn = env.write_txn().unwrap();
        let db: LmdbDatabase<Str, Str> = env.create_database(&mut wtxn, None).unwrap();
        
        // Insert initial data
        for (key, value) in &initial_data {
            db.put(&mut wtxn, key, value).unwrap();
        }
        
        // Delete some keys
        for key in &to_delete {
            db.delete(&mut wtxn, key).unwrap();
        }
        
        wtxn.commit().unwrap();
        
        // Read remaining data
        let rtxn = env.read_txn().unwrap();
        let mut results = Vec::new();
        
        for item in db.iter(&rtxn).unwrap() {
            let (key, value) = item.unwrap();
            results.push((key.to_string(), value.to_string()));
        }
        
        results
    };
    
    // heed-core delete operations
    let core_result = {
        let env = Arc::new(CoreEnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&core_path)
            .unwrap()
        );
        
        let mut wtxn = env.begin_write_txn().unwrap();
        let db: CoreDatabase<String, String> = env.create_database(&mut wtxn, None).unwrap();
        
        // Insert initial data
        for (key, value) in &initial_data {
            db.put(&mut wtxn, key.to_string(), value.to_string()).unwrap();
        }
        
        // Delete some keys
        for key in &to_delete {
            db.delete(&mut wtxn, &key.to_string()).unwrap();
        }
        
        wtxn.commit().unwrap();
        
        // Read remaining data
        let rtxn = env.begin_txn().unwrap();
        let mut cursor = db.cursor(&rtxn).unwrap();
        let mut results = Vec::new();
        
        while let Some((key, value)) = cursor.next().unwrap() {
            results.push((String::from_utf8(key).unwrap(), value));
        }
        
        results
    };
    
    assert_eq!(lmdb_result, core_result, "Delete operations should produce identical results");
}

#[test]
fn test_multiple_databases_match() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    
    let lmdb_path = path.join("lmdb");
    let core_path = path.join("core");
    std::fs::create_dir_all(&lmdb_path).unwrap();
    std::fs::create_dir_all(&core_path).unwrap();
    
    // LMDB multiple databases
    let lmdb_results = {
        let env = unsafe { LmdbEnvOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(10)
            .open(&lmdb_path)
            .unwrap()
        };
        
        let mut wtxn = env.write_txn().unwrap();
        
        let db1: LmdbDatabase<Str, Str> = env.create_database(&mut wtxn, Some("db1")).unwrap();
        let db2: LmdbDatabase<Str, Str> = env.create_database(&mut wtxn, Some("db2")).unwrap();
        
        db1.put(&mut wtxn, "key1", "db1_value").unwrap();
        db2.put(&mut wtxn, "key1", "db2_value").unwrap();
        
        wtxn.commit().unwrap();
        
        let rtxn = env.read_txn().unwrap();
        let val1 = db1.get(&rtxn, "key1").unwrap().unwrap();
        let val2 = db2.get(&rtxn, "key1").unwrap().unwrap();
        
        (val1.to_string(), val2.to_string())
    };
    
    // heed-core multiple databases
    let core_results = {
        let env = Arc::new(CoreEnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(10)
            .open(&core_path)
            .unwrap()
        );
        
        let mut wtxn = env.begin_write_txn().unwrap();
        
        let db1: CoreDatabase<String, String> = env.create_database(&mut wtxn, Some("db1")).unwrap();
        let db2: CoreDatabase<String, String> = env.create_database(&mut wtxn, Some("db2")).unwrap();
        
        db1.put(&mut wtxn, "key1".to_string(), "db1_value".to_string()).unwrap();
        db2.put(&mut wtxn, "key1".to_string(), "db2_value".to_string()).unwrap();
        
        wtxn.commit().unwrap();
        
        let rtxn = env.begin_txn().unwrap();
        let val1 = db1.get(&rtxn, &"key1".to_string()).unwrap().unwrap();
        let val2 = db2.get(&rtxn, &"key1".to_string()).unwrap().unwrap();
        
        (val1, val2)
    };
    
    assert_eq!(lmdb_results, core_results, "Multiple databases should work identically");
}