//! Debug cursor navigation

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use heed_core::page::PageFlags;
use heed_core::branch::BranchHeader;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debugging cursor navigation...");
    
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
        
        txn.commit()?;
    }
    
    // Manually navigate the tree to debug
    println!("\nManual tree navigation:");
    {
        let txn = env.begin_txn()?;
        let db_info = txn.db_info(Some("test_db"))?;
        
        // Read root page
        let root_page = txn.get_page(db_info.root)?;
        println!("Root page: id={:?}, flags={:?}, num_keys={}", 
                 db_info.root, root_page.header.flags, root_page.header.num_keys);
        
        if root_page.header.flags.contains(PageFlags::BRANCH) {
            // Get branch header
            let header = unsafe {
                &*(root_page.data.as_ptr() as *const BranchHeader)
            };
            println!("  Leftmost child: {:?}", header.leftmost_child);
            
            // Read leftmost child
            let left_page = txn.get_page(header.leftmost_child)?;
            println!("\nLeft child page: id={:?}, flags={:?}, num_keys={}", 
                     header.leftmost_child, left_page.header.flags, left_page.header.num_keys);
            
            if left_page.header.flags.contains(PageFlags::LEAF) {
                for i in 0..left_page.header.num_keys as usize {
                    let node = left_page.node(i)?;
                    let key = node.key()?;
                    println!("    Key[{}]: {}", i, String::from_utf8_lossy(key));
                }
            }
            
            // Read keys and right children
            for i in 0..root_page.header.num_keys as usize {
                let node = root_page.node(i)?;
                let key = node.key()?;
                let child_id = node.page_number()?;
                println!("\n  Branch key[{}]: {} -> child {:?}", i, String::from_utf8_lossy(key), child_id);
                
                // Read right child
                let right_page = txn.get_page(child_id)?;
                println!("  Right child page: id={:?}, flags={:?}, num_keys={}", 
                         child_id, right_page.header.flags, right_page.header.num_keys);
                
                if right_page.header.flags.contains(PageFlags::LEAF) {
                    for j in 0..right_page.header.num_keys as usize {
                        let node = right_page.node(j)?;
                        let key = node.key()?;
                        println!("      Key[{}]: {}", j, String::from_utf8_lossy(key));
                    }
                }
            }
        }
    }
    
    // Now test cursor
    println!("\n\nCursor navigation:");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        let mut count = 0;
        
        println!("Starting cursor iteration...");
        while let Some((key, _)) = cursor.next()? {
            println!("  Cursor found: {}", String::from_utf8_lossy(&key));
            count += 1;
        }
        
        println!("Total entries found by cursor: {}", count);
    }
    
    Ok(())
}