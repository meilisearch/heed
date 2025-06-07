//! Debug test to understand page splitting

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debug test for page splitting...");
    
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(5 * 1024 * 1024)
            .open(dir.path())?
    );
    
    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };
    
    // Insert entries one by one until we trigger a split
    println!("\nInserting entries one by one...");
    for i in 0..20 {
        println!("\n--- Entry {} ---", i);
        
        // Check state before insert
        {
            let txn = env.begin_txn()?;
            let db_info = txn.db_info(Some("test_db"))?;
            println!("Before insert: root={:?}, entries={}, depth={}, page_info={:?}", 
                     db_info.root, db_info.entries, db_info.depth,
                     if db_info.root.0 > 0 {
                         let page = txn.get_page(db_info.root)?;
                         Some((page.header.flags, page.header.num_keys, page.header.free_space()))
                     } else {
                         None
                     });
        }
        
        // Insert
        {
            let mut txn = env.begin_write_txn()?;
            let key = format!("key_{:03}", i);
            let value = vec![i as u8; 64];
            
            match db.put(&mut txn, key.clone(), value) {
                Ok(()) => {
                    println!("Inserted: {}", key);
                    
                    // Check state after insert but before commit
                    let db_info = txn.db_info(Some("test_db"))?;
                    println!("After insert: root={:?}, entries={}, depth={}, page_info={:?}", 
                             db_info.root, db_info.entries, db_info.depth,
                             if db_info.root.0 > 0 {
                                 let page = txn.get_page(db_info.root)?;
                                 Some(page.header.flags)
                             } else {
                                 None
                             });
                    
                    txn.commit()?;
                }
                Err(e) => {
                    println!("Error on insert {}: {:?}", i, e);
                    return Err(e.into());
                }
            }
        }
        
        // Check state after commit
        {
            let txn = env.begin_txn()?;
            let db_info = txn.db_info(Some("test_db"))?;
            println!("After commit: root={:?}, entries={}, depth={}, page_info={:?}", 
                     db_info.root, db_info.entries, db_info.depth,
                     if db_info.root.0 > 0 {
                         let page = txn.get_page(db_info.root)?;
                         Some((page.header.flags, page.header.num_keys, page.header.free_space()))
                     } else {
                         None
                     });
        }
    }
    
    println!("\nDebug test completed");
    Ok(())
}