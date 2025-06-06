//! QuickCheck tests comparing heed-core with original heed (LMDB)

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use quickcheck_macros::quickcheck;
use std::collections::BTreeMap;
use tempfile::TempDir;

// Operations that we'll test
#[derive(Debug, Clone)]
enum DbOperation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Clear,
}

impl Arbitrary for DbOperation {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => {
                let key = Vec::<u8>::arbitrary(g);
                let value = Vec::<u8>::arbitrary(g);
                DbOperation::Put(key, value)
            }
            1 => DbOperation::Delete(Vec::<u8>::arbitrary(g)),
            _ => DbOperation::Clear,
        }
    }
}

// Test configuration
#[derive(Debug, Clone)]
struct TestConfig {
    operations: Vec<DbOperation>,
    map_size: usize,
}

impl Arbitrary for TestConfig {
    fn arbitrary(g: &mut Gen) -> Self {
        // Limit operations to reasonable numbers for testing
        let num_ops = (u32::arbitrary(g) % 20) as usize;
        let mut operations = Vec::with_capacity(num_ops);
        
        for _ in 0..num_ops {
            operations.push(DbOperation::arbitrary(g));
        }
        
        // Map size between 1MB and 100MB
        let map_size = ((u32::arbitrary(g) % 100) + 1) as usize * 1024 * 1024;
        
        TestConfig {
            operations,
            map_size,
        }
    }
}

/// Execute operations on heed-core
fn execute_heed_core(config: &TestConfig) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, String> {
    use heed_core::{EnvBuilder, Environment};
    use std::sync::Arc;
    
    let dir = TempDir::new().map_err(|e| format!("TempDir error: {}", e))?;
    
    let env = Arc::new(EnvBuilder::new()
        .map_size(config.map_size)
        .open(dir.path())
        .map_err(|e| format!("Env open error: {:?}", e))?);
    
    let db: heed_core::Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.begin_write_txn()
            .map_err(|e| format!("Begin write txn error: {:?}", e))?;
        let db = env.create_database(&mut txn, None)
            .map_err(|e| format!("Create db error: {:?}", e))?;
        txn.commit().map_err(|e| format!("Commit error: {:?}", e))?;
        db
    };
    
    // Execute operations
    for op in &config.operations {
        let mut txn = env.begin_write_txn()
            .map_err(|e| format!("Begin write txn error: {:?}", e))?;
        
        match op {
            DbOperation::Put(key, value) => {
                // Skip empty keys which are invalid
                if key.is_empty() {
                    continue;
                }
                db.put(&mut txn, key.clone(), value.clone())
                    .map_err(|e| format!("Put error: {:?}", e))?;
            }
            DbOperation::Delete(key) => {
                // Skip empty keys which are invalid
                if key.is_empty() {
                    continue;
                }
                let _ = db.delete(&mut txn, key)
                    .map_err(|e| format!("Delete error: {:?}", e))?;
            }
            DbOperation::Clear => {
                db.clear(&mut txn)
                    .map_err(|e| format!("Clear error: {:?}", e))?;
            }
        }
        
        txn.commit().map_err(|e| format!("Commit error: {:?}", e))?;
    }
    
    // Read final state
    let txn = env.begin_txn()
        .map_err(|e| format!("Begin read txn error: {:?}", e))?;
    
    let mut result = BTreeMap::new();
    let cursor = db.cursor(&txn)
        .map_err(|e| format!("Cursor error: {:?}", e))?;
    
    let mut current = cursor;
    if let Ok(Some((key, value))) = current.first() {
        result.insert(key.to_vec(), value.to_vec());
        while let Ok(Some((key, value))) = current.next() {
            result.insert(key.to_vec(), value.to_vec());
        }
    }
    
    Ok(result)
}

/// Execute operations on original heed (LMDB)
fn execute_heed_lmdb(config: &TestConfig) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, String> {
    use heed::{EnvOpenOptions, Database as HeedDatabase};
    use heed::types::Bytes;
    
    let dir = TempDir::new().map_err(|e| format!("TempDir error: {}", e))?;
    
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(config.map_size)
            .open(dir.path())
            .map_err(|e| format!("Env open error: {:?}", e))?
    };
    
    let db: HeedDatabase<Bytes, Bytes> = {
        let mut txn = env.write_txn()
            .map_err(|e| format!("Begin write txn error: {:?}", e))?;
        let db = env.create_database(&mut txn, None)
            .map_err(|e| format!("Create db error: {:?}", e))?;
        txn.commit().map_err(|e| format!("Commit error: {:?}", e))?;
        db
    };
    
    // Execute operations
    for op in &config.operations {
        let mut txn = env.write_txn()
            .map_err(|e| format!("Begin write txn error: {:?}", e))?;
        
        match op {
            DbOperation::Put(key, value) => {
                // Skip empty keys which are invalid
                if key.is_empty() {
                    continue;
                }
                db.put(&mut txn, key.as_slice(), value.as_slice())
                    .map_err(|e| format!("Put error: {:?}", e))?;
            }
            DbOperation::Delete(key) => {
                // Skip empty keys which are invalid
                if key.is_empty() {
                    continue;
                }
                let _ = db.delete(&mut txn, key.as_slice())
                    .map_err(|e| format!("Delete error: {:?}", e))?;
            }
            DbOperation::Clear => {
                db.clear(&mut txn)
                    .map_err(|e| format!("Clear error: {:?}", e))?;
            }
        }
        
        txn.commit().map_err(|e| format!("Commit error: {:?}", e))?;
    }
    
    // Read final state
    let txn = env.read_txn()
        .map_err(|e| format!("Begin read txn error: {:?}", e))?;
    
    let mut result = BTreeMap::new();
    
    for entry in db.iter(&txn).map_err(|e| format!("Iter error: {:?}", e))? {
        let (key, value) = entry.map_err(|e| format!("Entry error: {:?}", e))?;
        result.insert(key.to_vec(), value.to_vec());
    }
    
    Ok(result)
}

fn prop_same_results(config: TestConfig) -> TestResult {
    // Skip if no operations
    if config.operations.is_empty() {
        return TestResult::discard();
    }
    
    // Execute on both implementations
    let heed_core_result = match execute_heed_core(&config) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("heed-core error: {}", e);
            return TestResult::error(format!("heed-core failed: {}", e));
        }
    };
    
    let heed_lmdb_result = match execute_heed_lmdb(&config) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("heed-lmdb error: {}", e);
            return TestResult::error(format!("heed-lmdb failed: {}", e));
        }
    };
    
    // Compare results
    if heed_core_result == heed_lmdb_result {
        TestResult::passed()
    } else {
        eprintln!("Results differ!");
        eprintln!("heed-core: {:?}", heed_core_result);
        eprintln!("heed-lmdb: {:?}", heed_lmdb_result);
        TestResult::failed()
    }
}

// More specific test cases
#[quickcheck]
fn prop_sequential_inserts(keys: Vec<Vec<u8>>, values: Vec<Vec<u8>>) -> TestResult {
    if keys.is_empty() || keys.len() != values.len() {
        return TestResult::discard();
    }
    
    let mut operations = Vec::new();
    for (key, value) in keys.into_iter().zip(values.into_iter()) {
        // Skip empty keys
        if key.is_empty() {
            continue;
        }
        operations.push(DbOperation::Put(key, value));
    }
    
    let config = TestConfig {
        operations,
        map_size: 10 * 1024 * 1024,
    };
    
    prop_same_results(config)
}

#[quickcheck]
fn prop_insert_delete_pattern(keys: Vec<Vec<u8>>) -> TestResult {
    if keys.is_empty() {
        return TestResult::discard();
    }
    
    let mut operations = Vec::new();
    
    // Insert all keys
    for key in &keys {
        if key.is_empty() {
            continue;
        }
        operations.push(DbOperation::Put(key.clone(), key.clone())); // Use key as value
    }
    
    // Delete half of them
    for (i, key) in keys.iter().enumerate() {
        if i % 2 == 0 && !key.is_empty() {
            operations.push(DbOperation::Delete(key.clone()));
        }
    }
    
    let config = TestConfig {
        operations,
        map_size: 10 * 1024 * 1024,
    };
    
    prop_same_results(config)
}

#[test]
fn test_specific_failure_case() {
    // This is where we can add specific failing test cases found by QuickCheck
    let operations = vec![
        DbOperation::Put(vec![1, 2, 3], vec![4, 5, 6]),
        DbOperation::Put(vec![7, 8, 9], vec![10, 11, 12]),
        DbOperation::Delete(vec![1, 2, 3]),
    ];
    
    let config = TestConfig {
        operations,
        map_size: 10 * 1024 * 1024,
    };
    
    let heed_core_result = execute_heed_core(&config).unwrap();
    let heed_lmdb_result = execute_heed_lmdb(&config).unwrap();
    
    assert_eq!(heed_core_result, heed_lmdb_result);
}

#[test]
fn test_clear_operation() {
    let operations = vec![
        DbOperation::Put(vec![1], vec![1]),
        DbOperation::Put(vec![2], vec![2]),
        DbOperation::Put(vec![3], vec![3]),
        DbOperation::Clear,
        DbOperation::Put(vec![4], vec![4]),
    ];
    
    let config = TestConfig {
        operations,
        map_size: 10 * 1024 * 1024,
    };
    
    let heed_core_result = execute_heed_core(&config).unwrap();
    let heed_lmdb_result = execute_heed_lmdb(&config).unwrap();
    
    assert_eq!(heed_core_result, heed_lmdb_result);
    assert_eq!(heed_core_result.len(), 1);
    assert_eq!(heed_core_result.get(&vec![4]), Some(&vec![4]));
}

#[quickcheck]
fn prop_quickcheck_operations(config: TestConfig) -> TestResult {
    prop_same_results(config)
}

#[test]
fn test_large_values() {
    let large_value = vec![42u8; 5000]; // 5KB value to trigger overflow pages
    
    let operations = vec![
        DbOperation::Put(vec![1], large_value.clone()),
        DbOperation::Put(vec![2], vec![1, 2, 3]),
        DbOperation::Put(vec![3], large_value.clone()),
        DbOperation::Delete(vec![1]),
    ];
    
    let config = TestConfig {
        operations,
        map_size: 10 * 1024 * 1024,
    };
    
    let heed_core_result = execute_heed_core(&config).unwrap();
    let heed_lmdb_result = execute_heed_lmdb(&config).unwrap();
    
    assert_eq!(heed_core_result, heed_lmdb_result);
}