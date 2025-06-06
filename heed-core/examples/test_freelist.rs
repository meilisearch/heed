//! Test the free list implementation

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use heed_core::error::PageId;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory
    let dir = TempDir::new()?;
    println!("Testing free list at: {:?}", dir.path());
    
    // Create environment with limited size to force page reuse
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(5 * 1024 * 1024) // 5MB - small to test free list
            .open(dir.path())?
    );
    
    // Create a database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };
    
    // Phase 1: Allocate many pages by inserting large values
    println!("\nPhase 1: Allocating pages...");
    let mut allocated_pages = Vec::new();
    {
        let mut txn = env.begin_write_txn()?;
        
        // Insert entries with large values to allocate multiple pages
        for i in 0..20 {
            let key = format!("key_{:03}", i);
            let value = vec![i as u8; 2048]; // Large value to use multiple pages
            db.put(&mut txn, key, value)?;
            
            if i % 5 == 0 {
                println!("  Inserted {} entries", i + 1);
            }
        }
        
        // Track which pages are allocated (this is for demonstration)
        if let heed_core::txn::ModeData::Write { ref next_pgno, .. } = txn.mode_data {
            println!("  Total pages allocated: {}", next_pgno.0);
            allocated_pages.push(next_pgno.0);
        }
        
        txn.commit()?;
    }
    
    // Phase 2: Delete some entries to free pages
    println!("\nPhase 2: Freeing pages by deleting entries...");
    {
        let mut txn = env.begin_write_txn()?;
        
        // Delete every other entry
        for i in (0..20).step_by(2) {
            let key = format!("key_{:03}", i);
            let deleted = db.delete(&mut txn, &key)?;
            if deleted {
                println!("  Deleted {}", key);
            }
        }
        
        // Check free list status before commit
        if let heed_core::txn::ModeData::Write { ref freelist, .. } = txn.mode_data {
            println!("  Pages pending free: {}", freelist.pending_len());
        }
        
        txn.commit()?;
    }
    
    // Phase 3: Insert new entries - should reuse freed pages
    println!("\nPhase 3: Inserting new entries (should reuse freed pages)...");
    {
        let mut txn = env.begin_write_txn()?;
        
        // Check free list at start
        if let heed_core::txn::ModeData::Write { ref freelist, ref next_pgno, .. } = txn.mode_data {
            println!("  Free pages available: {}", freelist.len());
            println!("  Next page to allocate: {}", next_pgno.0);
        }
        
        // Insert new entries
        for i in 100..110 {
            let key = format!("new_key_{:03}", i);
            let value = vec![i as u8; 2048];
            db.put(&mut txn, key, value)?;
        }
        
        // Check if pages were reused
        if let heed_core::txn::ModeData::Write { ref next_pgno, .. } = txn.mode_data {
            println!("  Next page after reuse: {}", next_pgno.0);
            if next_pgno.0 < allocated_pages[0] {
                println!("  ✓ Pages were reused!");
            } else {
                println!("  ✗ Pages were not reused (may need reader tracking)");
            }
        }
        
        txn.commit()?;
    }
    
    // Phase 4: Verify data integrity
    println!("\nPhase 4: Verifying data integrity...");
    {
        let txn = env.begin_txn()?;
        
        // Count remaining original entries
        let mut original_count = 0;
        for i in 0..20 {
            if i % 2 != 0 { // We deleted even entries
                let key = format!("key_{:03}", i);
                if db.get(&txn, &key)?.is_some() {
                    original_count += 1;
                }
            }
        }
        println!("  Original entries remaining: {}", original_count);
        
        // Count new entries
        let mut new_count = 0;
        for i in 100..110 {
            let key = format!("new_key_{:03}", i);
            if db.get(&txn, &key)?.is_some() {
                new_count += 1;
            }
        }
        println!("  New entries: {}", new_count);
        
        // List all entries
        println!("\n  All entries in database:");
        let mut cursor = db.cursor(&txn)?;
        let mut total = 0;
        while let Some((key, _value)) = cursor.next()? {
            let key_str = String::from_utf8_lossy(&key);
            println!("    - {}", key_str);
            total += 1;
        }
        println!("  Total entries: {}", total);
    }
    
    // Phase 5: Test free list persistence
    println!("\nPhase 5: Testing free list persistence...");
    
    // Close and reopen environment
    drop(env);
    
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(5 * 1024 * 1024)
            .open(dir.path())?
    );
    
    // Open existing database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.open_database(&mut txn, Some("test_db"))?;
        txn.commit()?;
        db
    };
    
    // Delete more entries
    {
        let mut txn = env.begin_write_txn()?;
        
        // Check if free list was loaded
        if let heed_core::txn::ModeData::Write { ref freelist, .. } = txn.mode_data {
            println!("  Free list loaded, has transaction pages: {}", freelist.has_txn_free_pages());
        }
        
        // Delete remaining original entries
        for i in (1..20).step_by(2) {
            let key = format!("key_{:03}", i);
            db.delete(&mut txn, &key)?;
        }
        
        txn.commit()?;
    }
    
    // Insert more entries to verify free list still works
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 200..205 {
            let key = format!("final_key_{:03}", i);
            let value = vec![i as u8; 1024];
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
    }
    
    // Final verification
    println!("\nFinal verification:");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        let mut count = 0;
        while cursor.next()?.is_some() {
            count += 1;
        }
        println!("  Total entries in database: {}", count);
    }
    
    println!("\nFree list test completed successfully!");
    Ok(())
}