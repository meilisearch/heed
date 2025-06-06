//! Debug environment initialization step by step

use heed_core::{EnvBuilder};
use heed_core::error::Result;
use heed_core::page::PageFlags;
use std::sync::Arc;

fn main() -> Result<()> {
    println!("=== Debug Environment Initialization ===\n");
    
    // Create environment
    let dir = tempfile::tempdir().unwrap();
    println!("Creating environment at: {:?}", dir.path());
    
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    println!("✓ Environment created");
    
    // Get environment stats
    println!("\n--- Environment Stats ---");
    match env.stat() {
        Ok(stats) => {
            println!("✓ Environment stats:");
            println!("  - Page size: {}", stats.psize);
            println!("  - Depth: {}", stats.depth);
            println!("  - Branch pages: {}", stats.branch_pages);
            println!("  - Leaf pages: {}", stats.leaf_pages);
            println!("  - Overflow pages: {}", stats.overflow_pages);
            println!("  - Entries: {}", stats.entries);
        }
        Err(e) => {
            println!("✗ Failed to get stats: {:?}", e);
        }
    }
    
    // Try a transaction
    println!("\n--- Testing Transaction ---");
    {
        let mut txn = env.begin_write_txn()?;
        println!("✓ Write transaction created (ID: {})", txn.id().0);
        
        // Check if we can get db info
        match txn.db_info(None) {
            Ok(info) => {
                println!("✓ Main DB info in txn:");
                println!("  - Root: {}", info.root.0);
                println!("  - Entries: {}", info.entries);
                println!("  - Depth: {}", info.depth);
            }
            Err(e) => {
                println!("✗ Failed to get main DB info: {:?}", e);
            }
        }
        
        // Try to allocate a page
        match txn.alloc_page(PageFlags::LEAF) {
            Ok((page_id, page)) => {
                println!("✓ Allocated page {}", page_id.0);
                println!("  - Flags: {:?}", page.header.flags);
            }
            Err(e) => {
                println!("✗ Failed to allocate page: {:?}", e);
            }
        }
        
        txn.commit()?;
        println!("✓ Transaction committed");
    }
    
    // Now try to use the main database
    println!("\n--- Testing Main Database ---");
    {
        use heed_core::db::{Database, DatabaseFlags};
        
        let main_db: Database<Vec<u8>, Vec<u8>> = Database::open(&env, None, DatabaseFlags::empty())?;
        println!("✓ Opened main database");
        
        // Check the database info
        {
            let txn = env.begin_txn()?;
            match main_db.is_empty(&txn) {
                Ok(empty) => println!("  - Is empty: {}", empty),
                Err(e) => println!("  - Failed to check if empty: {:?}", e),
            }
        }
        
        // Try to get a non-existent key first
        {
            let txn = env.begin_txn()?;
            let key = b"nonexistent".to_vec();
            match main_db.get(&txn, &key) {
                Ok(val) => println!("  - Get nonexistent key: {:?}", val),
                Err(e) => println!("  - Failed to get nonexistent key: {:?}", e),
            }
        }
        
        // Try to store something
        {
            let mut txn = env.begin_write_txn()?;
            println!("\nAttempting to store data...");
            
            // Debug: check the DB info before put
            match txn.db_info(None) {
                Ok(info) => {
                    println!("  - DB info before put: root={}, entries={}", info.root.0, info.entries);
                }
                Err(e) => {
                    println!("  - Failed to get DB info: {:?}", e);
                }
            }
            
            let key = b"test".to_vec();
            let value = b"value".to_vec();
            
            match main_db.put(&mut txn, key, value) {
                Ok(_) => {
                    println!("✓ Data stored successfully");
                    
                    // Check DB info after put
                    match txn.db_info(None) {
                        Ok(info) => {
                            println!("  - DB info after put: root={}, entries={}", info.root.0, info.entries);
                        }
                        Err(e) => {
                            println!("  - Failed to get DB info: {:?}", e);
                        }
                    }
                    
                    txn.commit()?;
                    println!("✓ Transaction committed");
                }
                Err(e) => {
                    println!("✗ Failed to store data: {:?}", e);
                    return Err(e);
                }
            }
        }
        
        // Try to read it back
        {
            let txn = env.begin_txn()?;
            let key = b"test".to_vec();
            match main_db.get(&txn, &key) {
                Ok(Some(val)) => println!("✓ Retrieved value: {:?}", String::from_utf8_lossy(&val)),
                Ok(None) => println!("✓ No value found"),
                Err(e) => println!("✗ Failed to retrieve value: {:?}", e),
            }
        }
    }
    
    println!("\n=== Debug completed successfully ===");
    Ok(())
}