//! Debug B+Tree rebalancing

use heed_core::env::EnvBuilder;
use heed_core::btree::BTree;
use heed_core::meta::DbInfo;
use heed_core::error::PageId;
use heed_core::comparator::LexicographicComparator;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing B+Tree rebalancing with debug...\n");
    
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    // Track statistics
    let mut root = PageId(3);
    let mut db_info = DbInfo::default();
    db_info.root = root;
    db_info.leaf_pages = 1;
    
    // Insert entries to create a decent tree
    println!("1. Inserting 50 entries...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            match BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), value.as_bytes()) {
                Ok(_) => {},
                Err(e) => {
                    println!("ERROR inserting key_{:04}: {:?}", i, e);
                    return Err(Box::new(e));
                }
            }
        }
        
        println!("   Entries: {}", db_info.entries);
        println!("   Depth: {}", db_info.depth);
        println!("   Branch pages: {}", db_info.branch_pages);
        println!("   Leaf pages: {}", db_info.leaf_pages);
        
        txn.commit()?;
    }
    
    // Delete some entries to trigger rebalancing
    println!("\n2. Deleting entries 10-15 to trigger rebalancing...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 10..16 {
            let key = format!("key_{:04}", i);
            println!("   Deleting {}", key);
            match BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, key.as_bytes()) {
                Ok(_) => {},
                Err(e) => {
                    println!("   ERROR deleting {}: {:?}", key, e);
                    return Err(Box::new(e));
                }
            }
        }
        
        println!("   Entries after deletion: {}", db_info.entries);
        
        txn.commit()?;
    }
    
    println!("\nDebug test completed!");
    
    Ok(())
}