//! Debug test to understand what's happening

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debug test for page structures...");
    
    // Create a temporary directory
    let dir = TempDir::new()?;
    println!("Created temp dir at: {:?}", dir.path());
    
    // Create environment
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(5 * 1024 * 1024) // 5MB
            .open(dir.path())?
    );
    println!("Environment created");
    
    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.begin_write_txn()?;
        println!("Write transaction started");
        
        let db = env.create_database(&mut txn, Some("test_db"))?;
        println!("Database created");
        
        // Check database info
        let db_info = txn.db_info(Some("test_db"))?;
        println!("Database info: root={:?}, entries={}, depth={}", 
                 db_info.root, db_info.entries, db_info.depth);
        
        txn.commit()?;
        println!("Transaction committed");
        
        db
    };
    
    // Insert a small number of entries to see what happens
    println!("\nInserting entries...");
    for i in 0..10 {
        let mut txn = env.begin_write_txn()?;
        
        let key = format!("key_{:03}", i);
        let value = vec![i as u8; 64]; // Small values
        
        println!("\nInserting entry {}", i);
        
        // Check database info before insert
        let db_info_before = txn.db_info(Some("test_db"))?;
        println!("  Before: root={:?}, entries={}, depth={}", 
                 db_info_before.root, db_info_before.entries, db_info_before.depth);
        
        db.put(&mut txn, key.clone(), value)?;
        
        // Check database info after insert
        let db_info_after = txn.db_info(Some("test_db"))?;
        println!("  After: root={:?}, entries={}, depth={}", 
                 db_info_after.root, db_info_after.entries, db_info_after.depth);
        
        txn.commit()?;
        println!("  Committed");
        
        // If we see the error, break
        if i > 5 {
            // Try to read all entries
            println!("\n  Reading all entries:");
            let txn = env.begin_txn()?;
            let mut cursor = db.cursor(&txn)?;
            let mut count = 0;
            while let Some((key, _value)) = cursor.next()? {
                let key_str = String::from_utf8_lossy(&key);
                println!("    {}", key_str);
                count += 1;
            }
            println!("  Total: {} entries", count);
        }
    }
    
    println!("\nDebug test completed");
    Ok(())
}