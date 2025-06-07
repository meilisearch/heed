//! Test simple deletion

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing simple deletion...");
    
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
    
    // Phase 1: Insert 10 entries
    println!("\nPhase 1: Inserting 10 entries...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            let value = vec![i as u8; 10];
            println!("  Inserting: {}", key);
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
    }
    
    // Phase 2: Verify all entries
    println!("\nPhase 2: Verifying initial entries...");
    {
        let txn = env.begin_txn()?;
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            match db.get(&txn, &key)? {
                Some(value) => println!("  ✓ {} = {} bytes", key, value.len()),
                None => println!("  ✗ {} not found", key),
            }
        }
    }
    
    // Phase 3: Delete some entries
    println!("\nPhase 3: Deleting even entries...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in (0..10).step_by(2) {
            let key = format!("key_{:02}", i);
            if db.delete(&mut txn, &key)? {
                println!("  Deleted: {}", key);
            } else {
                println!("  Not found: {}", key);
            }
        }
        
        txn.commit()?;
    }
    
    // Phase 4: Verify remaining entries
    println!("\nPhase 4: Verifying after deletion...");
    {
        let txn = env.begin_txn()?;
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            match db.get(&txn, &key)? {
                Some(value) => {
                    if i % 2 == 0 {
                        println!("  ✗ {} = {} bytes (should be deleted)", key, value.len());
                    } else {
                        println!("  ✓ {} = {} bytes", key, value.len());
                    }
                }
                None => {
                    if i % 2 == 0 {
                        println!("  ✓ {} correctly deleted", key);
                    } else {
                        println!("  ✗ {} not found (should exist)", key);
                    }
                }
            }
        }
    }
    
    // Phase 5: List all remaining entries
    println!("\nPhase 5: All remaining entries:");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        while let Some((key, value)) = cursor.next()? {
            println!("  {} -> {} bytes", String::from_utf8_lossy(&key), value.len());
        }
    }
    
    Ok(())
}