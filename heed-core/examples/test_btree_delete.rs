//! Test B+Tree deletion behavior

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing B+Tree deletion...");
    
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
    
    // Phase 1: Insert entries
    println!("\nPhase 1: Inserting entries...");
    let num_entries = 50; // Enough to cause splits
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 0..num_entries {
            let key = format!("key_{:03}", i);
            let value = vec![i as u8; 256]; // Same size as freelist test
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
        println!("  Inserted {} entries", num_entries);
    }
    
    // Phase 2: Verify all entries
    println!("\nPhase 2: Before deletion...");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        let mut count = 0;
        
        println!("  All entries:");
        while let Some((key, value)) = cursor.next()? {
            println!("    {} -> {} bytes", String::from_utf8_lossy(&key), value.len());
            count += 1;
        }
        println!("  Total: {} entries", count);
        assert_eq!(count, num_entries);
    }
    
    // Phase 3: Delete even entries
    println!("\nPhase 3: Deleting even entries...");
    {
        let mut txn = env.begin_write_txn()?;
        let mut deleted = 0;
        
        for i in (0..num_entries).step_by(2) {
            let key = format!("key_{:03}", i);
            if db.delete(&mut txn, &key)? {
                println!("  Deleted: {}", key);
                deleted += 1;
            } else {
                println!("  Not found: {}", key);
            }
        }
        
        txn.commit()?;
        println!("  Deleted {} entries", deleted);
    }
    
    // Phase 4: Verify remaining entries
    println!("\nPhase 4: After deletion...");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        let mut remaining = Vec::new();
        
        println!("  Remaining entries:");
        while let Some((key, value)) = cursor.next()? {
            let key_str = String::from_utf8_lossy(&key).to_string();
            println!("    {} -> {} bytes", key_str, value.len());
            remaining.push(key_str);
        }
        println!("  Total: {} entries", remaining.len());
        
        // Check that we have the right entries
        let mut expected = Vec::new();
        for i in (1..num_entries).step_by(2) {
            expected.push(format!("key_{:03}", i));
        }
        
        expected.sort();
        remaining.sort();
        
        if expected == remaining {
            println!("  ✓ Correct entries remain");
        } else {
            println!("  ✗ Wrong entries remain");
            println!("    Expected: {:?}", expected);
            println!("    Got: {:?}", remaining);
        }
    }
    
    // Phase 5: Random access to remaining entries
    println!("\nPhase 5: Random access test...");
    {
        let txn = env.begin_txn()?;
        
        // Test some odd entries (should exist)
        for i in vec![1, 5, 9, 13, 17] {
            if i < num_entries {
                let key = format!("key_{:03}", i);
                match db.get(&txn, &key)? {
                    Some(value) => println!("  ✓ {} = {} bytes", key, value.len()),
                    None => println!("  ✗ {} not found", key),
                }
            }
        }
        
        // Test some even entries (should not exist)
        for i in vec![0, 4, 8, 12, 16] {
            if i < num_entries {
                let key = format!("key_{:03}", i);
                match db.get(&txn, &key)? {
                    Some(value) => println!("  ✗ {} = {} bytes (should be deleted)", key, value.len()),
                    None => println!("  ✓ {} correctly deleted", key),
                }
            }
        }
    }
    
    println!("\nB+Tree delete test completed");
    Ok(())
}