//! Debug branch page structure

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use heed_core::branch::BranchHeader;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debugging branch page structure...");
    
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
    
    // Insert entries to force a split
    println!("\nInserting entries to force split...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 0..15 {
            let key = format!("key_{:03}", i);
            let value = vec![i as u8; 256];
            db.put(&mut txn, key, value)?;
        }
        
        let db_info = txn.db_info(Some("test_db"))?;
        println!("DB after split: root={:?}, entries={}, depth={}", 
                 db_info.root, db_info.entries, db_info.depth);
        
        if db_info.depth > 0 {
            // We have a branch page, let's examine it
            println!("\nExamining root branch page...");
            let root_page = txn.get_page(db_info.root)?;
            println!("Root page: id={:?}, flags={:?}, num_keys={}", 
                     db_info.root, root_page.header.flags, root_page.header.num_keys);
            
            // Get branch header
            let header = unsafe {
                &*(root_page.data.as_ptr() as *const BranchHeader)
            };
            println!("Branch header: leftmost_child={:?}", header.leftmost_child);
            
            // Print all keys and children
            for i in 0..root_page.header.num_keys as usize {
                let node = root_page.node(i)?;
                let key = node.key()?;
                let child_id = node.page_number()?;
                println!("  Key[{}]: {} -> child {:?}", i, String::from_utf8_lossy(key), child_id);
            }
        }
        
        txn.commit()?;
    }
    
    // Now try to read entries
    println!("\nReading entries...");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        let mut count = 0;
        
        while let Some((key, _)) = cursor.next()? {
            println!("  Found: {}", String::from_utf8_lossy(&key));
            count += 1;
        }
        
        println!("Total entries found: {}", count);
    }
    
    Ok(())
}