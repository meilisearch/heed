//! Debug B+Tree insert operations

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debugging B+Tree insert...");
    
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
    
    // Insert entries one by one and check count
    let num_entries = 20; // Start with fewer entries
    for i in 0..num_entries {
        println!("\n--- Inserting entry {} ---", i);
        
        // Insert entry
        {
            let mut txn = env.begin_write_txn()?;
            let key = format!("key_{:03}", i);
            let value = vec![i as u8; 256];
            println!("  Inserting: {}", key);
            db.put(&mut txn, key, value)?;
            
            // Get db info
            let db_info = txn.db_info(Some("test_db"))?;
            println!("  DB info: root={:?}, entries={}, depth={}, leaf_pages={}, branch_pages={}", 
                     db_info.root, db_info.entries, db_info.depth, 
                     db_info.leaf_pages, db_info.branch_pages);
            
            txn.commit()?;
        }
        
        // Verify all entries
        {
            let txn = env.begin_txn()?;
            let mut cursor = db.cursor(&txn)?;
            let mut count = 0;
            let mut keys = Vec::new();
            
            while let Some((key, _)) = cursor.next()? {
                keys.push(String::from_utf8_lossy(&key).to_string());
                count += 1;
            }
            
            println!("  Current entries ({}): {:?}", count, keys);
            
            // Check if count matches expected
            if count != i + 1 {
                println!("  ERROR: Expected {} entries, found {}", i + 1, count);
                
                // Check which keys are missing
                let mut missing = Vec::new();
                for j in 0..=i {
                    let expected_key = format!("key_{:03}", j);
                    if !keys.contains(&expected_key) {
                        missing.push(expected_key);
                    }
                }
                if !missing.is_empty() {
                    println!("  Missing keys: {:?}", missing);
                }
                
                return Ok(());
            }
        }
    }
    
    println!("\nAll {} entries inserted successfully!", num_entries);
    Ok(())
}