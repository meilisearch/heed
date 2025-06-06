//! Detailed test of deletion to see when entries disappear

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Detailed deletion test...");
    
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
    
    // Insert 30 entries to ensure splits
    println!("\nInserting 30 entries...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 0..30 {
            let key = format!("key_{:03}", i);
            let value = vec![i as u8; 256];
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
    }
    
    // Delete entries one by one, checking after each
    println!("\nDeleting entries one by one...");
    for i in (0..30).step_by(2) {
        println!("\n--- Deleting key_{:03} ---", i);
        
        // Delete the key
        {
            let mut txn = env.begin_write_txn()?;
            let key = format!("key_{:03}", i);
            
            if db.delete(&mut txn, &key)? {
                println!("  Deleted successfully");
            } else {
                println!("  ERROR: Not found");
            }
            
            txn.commit()?;
        }
        
        // Check all remaining entries
        {
            let txn = env.begin_txn()?;
            let mut cursor = db.cursor(&txn)?;
            let mut found_keys = Vec::new();
            
            while let Some((key, _)) = cursor.next()? {
                found_keys.push(String::from_utf8_lossy(&key).to_string());
            }
            
            // Check if any odd keys are missing
            let mut missing = Vec::new();
            for j in 0..30 {
                let expected_key = format!("key_{:03}", j);
                let should_exist = j > i || j % 2 == 1;
                let exists = found_keys.contains(&expected_key);
                
                if should_exist && !exists {
                    missing.push(expected_key);
                }
            }
            
            if !missing.is_empty() {
                println!("  ERROR: Missing keys after deleting key_{:03}: {:?}", i, missing);
                println!("  Found keys: {:?}", found_keys);
                return Ok(());
            } else {
                println!("  All expected keys present ({})", found_keys.len());
            }
        }
    }
    
    println!("\nDeletion test completed successfully!");
    Ok(())
}