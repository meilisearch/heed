//! Debug why keys are missing after split

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debugging missing keys after split...");
    
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
    
    // Insert entries until we force a split
    println!("\nInserting entries...");
    let mut last_depth = 0;
    for i in 0..50 {
        let mut txn = env.begin_write_txn()?;
        
        let key = format!("key_{:03}", i);
        let value = vec![i as u8; 256];
        db.put(&mut txn, key.clone(), value)?;
        
        let db_info = txn.db_info(Some("test_db"))?;
        if db_info.depth != last_depth {
            println!("  Split occurred at entry {}: depth {} -> {}", i, last_depth, db_info.depth);
            last_depth = db_info.depth;
        }
        
        txn.commit()?;
        
        // After each insert, verify all keys so far
        let txn = env.begin_txn()?;
        for j in 0..=i {
            let check_key = format!("key_{:03}", j);
            if db.get(&txn, &check_key)?.is_none() {
                println!("  ERROR: After inserting {}, key {} is missing!", key, check_key);
                
                // List all keys we can find
                println!("  Keys found:");
                let mut cursor = db.cursor(&txn)?;
                while let Some((k, _)) = cursor.next()? {
                    println!("    - {}", String::from_utf8_lossy(&k));
                }
                
                return Ok(());
            }
        }
    }
    
    println!("\nAll 50 entries inserted successfully");
    
    // Final verification
    println!("\nFinal verification:");
    let txn = env.begin_txn()?;
    let mut cursor = db.cursor(&txn)?;
    let mut count = 0;
    
    while let Some((key, _)) = cursor.next()? {
        if count < 10 || count >= 40 {
            println!("  {}", String::from_utf8_lossy(&key));
        } else if count == 10 {
            println!("  ...");
        }
        count += 1;
    }
    println!("Total: {} entries", count);
    
    Ok(())
}