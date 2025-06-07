//! Test B+Tree rebalancing

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use heed_core::btree::BTree;
use heed_core::meta::DbInfo;
use heed_core::error::PageId;
use heed_core::comparator::LexicographicComparator;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing B+Tree rebalancing...\n");
    
    let dir = TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    // Create a database
    let _db: Database<String, String> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };
    
    // Track statistics
    let mut root = PageId(3);
    let mut db_info = DbInfo::default();
    db_info.root = root;
    db_info.leaf_pages = 1;
    
    // Insert a lot of entries to create a multi-level tree
    println!("1. Inserting 200 entries to create a multi-level B+Tree...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 0..200 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), value.as_bytes())?;
        }
        
        println!("   Entries: {}", db_info.entries);
        println!("   Depth: {}", db_info.depth);
        println!("   Branch pages: {}", db_info.branch_pages);
        println!("   Leaf pages: {}", db_info.leaf_pages);
        
        txn.commit()?;
    }
    
    // Delete every other entry to trigger rebalancing
    println!("\n2. Deleting every other entry (100 total) to trigger rebalancing...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in (0..200).step_by(2) {
            let key = format!("key_{:04}", i);
            BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, key.as_bytes())?;
        }
        
        println!("   Entries after deletion: {}", db_info.entries);
        println!("   Depth: {}", db_info.depth);
        println!("   Branch pages: {}", db_info.branch_pages);
        println!("   Leaf pages: {}", db_info.leaf_pages);
        
        txn.commit()?;
    }
    
    // Verify remaining entries
    println!("\n3. Verifying remaining entries...");
    {
        let txn = env.begin_txn()?;
        let mut found = 0;
        let mut missing = 0;
        
        for i in 0..200 {
            let key = format!("key_{:04}", i);
            match BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes())? {
                Some(_) => {
                    if i % 2 == 0 {
                        println!("   ERROR: Found deleted key: {}", key);
                    } else {
                        found += 1;
                    }
                }
                None => {
                    if i % 2 == 1 {
                        println!("   ERROR: Missing key: {}", key);
                    } else {
                        missing += 1;
                    }
                }
            }
        }
        
        println!("   Found {} keys (expected 100)", found);
        println!("   Missing {} keys (expected 100)", missing);
    }
    
    // Delete more entries to potentially reduce tree depth
    println!("\n4. Deleting more entries to see if tree shrinks...");
    {
        let mut txn = env.begin_write_txn()?;
        
        // Delete entries to leave only 20
        for i in (1..180).step_by(2) {
            let key = format!("key_{:04}", i);
            BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, key.as_bytes())?;
        }
        
        println!("   Entries after more deletions: {}", db_info.entries);
        println!("   Depth: {}", db_info.depth);
        println!("   Branch pages: {}", db_info.branch_pages);
        println!("   Leaf pages: {}", db_info.leaf_pages);
        
        txn.commit()?;
    }
    
    // Insert new entries to test that the tree can grow again
    println!("\n5. Inserting new entries to test tree can grow again...");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 300..350 {
            let key = format!("key_{:04}", i);
            let value = format!("new_value_{:04}", i);
            BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), value.as_bytes())?;
        }
        
        println!("   Entries after new insertions: {}", db_info.entries);
        println!("   Depth: {}", db_info.depth);
        println!("   Branch pages: {}", db_info.branch_pages);
        println!("   Leaf pages: {}", db_info.leaf_pages);
        
        txn.commit()?;
    }
    
    println!("\nRebalancing test completed successfully!");
    
    Ok(())
}