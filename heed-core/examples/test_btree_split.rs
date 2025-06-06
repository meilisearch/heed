//! Test B+Tree splitting behavior

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing B+Tree splitting...");
    
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };
    
    // Insert entries until we trigger a split
    println!("\nPhase 1: Inserting entries to trigger split...");
    let num_entries = 60; // Should be enough to trigger at least one split
    
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 0..num_entries {
            let key = format!("key_{:03}", i);
            let value = vec![i as u8; 100];
            
            db.put(&mut txn, key.clone(), value)?;
            
            // Check state periodically
            if i % 10 == 9 {
                let db_info = txn.db_info(Some("test_db"))?;
                println!("  After {} entries: depth={}, root={:?}", i + 1, db_info.depth, db_info.root);
            }
        }
        
        let final_info = txn.db_info(Some("test_db"))?;
        println!("  Final state: depth={}, entries={}, root={:?}", 
                 final_info.depth, final_info.entries, final_info.root);
        
        txn.commit()?;
    }
    
    // Phase 2: Verify all entries are still there
    println!("\nPhase 2: Verifying all entries...");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        let mut found_keys = Vec::new();
        while let Some((key, _value)) = cursor.next()? {
            found_keys.push(String::from_utf8_lossy(&key).to_string());
        }
        
        println!("  Found {} entries", found_keys.len());
        
        // Check if all keys are present
        let mut missing = Vec::new();
        for i in 0..num_entries {
            let expected_key = format!("key_{:03}", i);
            if !found_keys.contains(&expected_key) {
                missing.push(expected_key);
            }
        }
        
        if missing.is_empty() {
            println!("  ✓ All entries found!");
        } else {
            println!("  ✗ Missing {} entries:", missing.len());
            for key in &missing {
                println!("    - {}", key);
            }
        }
        
        // Also check ordering
        let mut sorted = found_keys.clone();
        sorted.sort();
        if found_keys == sorted {
            println!("  ✓ Entries are in correct order");
        } else {
            println!("  ✗ Entries are not in correct order");
        }
    }
    
    // Phase 3: Random access test
    println!("\nPhase 3: Random access test...");
    {
        let txn = env.begin_txn()?;
        
        let test_keys = vec![0, 15, 30, 45, 59];
        for i in test_keys {
            let key = format!("key_{:03}", i);
            match db.get(&txn, &key)? {
                Some(value) => {
                    if value[0] == i as u8 {
                        println!("  ✓ {} = correct value", key);
                    } else {
                        println!("  ✗ {} = wrong value (expected {}, got {})", key, i, value[0]);
                    }
                }
                None => {
                    println!("  ✗ {} = not found", key);
                }
            }
        }
    }
    
    println!("\nB+Tree split test completed");
    Ok(())
}