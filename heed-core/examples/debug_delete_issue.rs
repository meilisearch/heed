//! Debug deletion issue

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debugging deletion issue...");
    
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
    
    // Insert 20 entries (enough to cause a split)
    println!("\nInserting 20 entries...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 0..20 {
            let key = format!("key_{:03}", i);
            let value = vec![i as u8; 256];
            db.put(&mut txn, key, value)?;
        }
        
        let db_info = txn.db_info(Some("test_db"))?;
        println!("After insert: entries={}, depth={}", db_info.entries, db_info.depth);
        
        txn.commit()?;
    }
    
    // Verify all entries before deletion
    println!("\nBefore deletion:");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        let mut count = 0;
        
        while let Some((key, _)) = cursor.next()? {
            println!("  {}", String::from_utf8_lossy(&key));
            count += 1;
        }
        println!("Total: {} entries", count);
    }
    
    // Delete entries one by one in a single transaction
    println!("\nDeleting even entries in single transaction...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in (0..20).step_by(2) {
            let key = format!("key_{:03}", i);
            
            // First check if we can find it
            match db.get(&txn, &key)? {
                Some(_) => {
                    println!("  Found {} - deleting...", key);
                    if db.delete(&mut txn, &key)? {
                        println!("    Deleted successfully");
                    } else {
                        println!("    ERROR: Delete returned false");
                    }
                }
                None => {
                    println!("  ERROR: {} not found!", key);
                    
                    // List what we can find
                    println!("  Current entries:");
                    let mut cursor = db.cursor(&txn)?;
                    while let Some((k, _)) = cursor.next()? {
                        println!("    - {}", String::from_utf8_lossy(&k));
                    }
                    
                    return Ok(());
                }
            }
        }
        
        txn.commit()?;
    }
    
    // Verify after deletion
    println!("\nAfter deletion:");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        let mut count = 0;
        
        while let Some((key, _)) = cursor.next()? {
            println!("  {}", String::from_utf8_lossy(&key));
            count += 1;
        }
        println!("Total: {} entries", count);
    }
    
    Ok(())
}