use heed_core::{EnvBuilder, Database};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    println!("=== Testing Free Page Management ===\n");
    
    // Create database
    let db: Database<String, String> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Step 1: Insert some data
    println!("Step 1: Inserting initial data...");
    let mut _page_ids: Vec<u64> = Vec::new();
    {
        let mut txn = env.begin_write_txn()?;
        
        // Insert enough data to allocate multiple pages
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            db.put(&mut txn, key, value)?;
        }
        
        // Get the transaction info to see pages allocated
        if let Ok(info) = txn.db_info(None) {
            println!("  Database has {} entries, {} leaf pages", info.entries, info.leaf_pages);
        }
        
        txn.commit()?;
    }
    
    // Step 2: Delete some entries
    println!("\nStep 2: Deleting half the entries...");
    {
        let mut txn = env.begin_write_txn()?;
        
        // Delete every other entry
        for i in (0..100).step_by(2) {
            let key = format!("key{:04}", i);
            db.delete(&mut txn, &key)?;
        }
        
        if let Ok(info) = txn.db_info(None) {
            println!("  Database now has {} entries", info.entries);
        }
        
        txn.commit()?;
    }
    
    // Step 3: Insert new data
    println!("\nStep 3: Inserting new data...");
    {
        let mut txn = env.begin_write_txn()?;
        
        // Insert new data
        for i in 100..150 {
            let key = format!("key{:04}", i);
            let value = format!("new_value{:04}", i);
            db.put(&mut txn, key, value)?;
        }
        
        if let Ok(info) = txn.db_info(None) {
            println!("  Database now has {} entries, {} leaf pages", info.entries, info.leaf_pages);
        }
        
        txn.commit()?;
    }
    
    // Step 4: Check file size growth
    println!("\nStep 4: Checking file growth...");
    let file_path = dir.path().join("data.mdb");
    if let Ok(metadata) = std::fs::metadata(&file_path) {
        let size_kb = metadata.len() / 1024;
        println!("  Database file size: {} KB", size_kb);
        
        // In an ideal implementation with page recycling, the file size
        // should not grow significantly after deletions
        if size_kb > 100 {
            println!("  WARNING: File is growing without reusing freed pages!");
        }
    }
    
    // Step 5: Test with active reader
    println!("\nStep 5: Testing with active reader...");
    {
        // Start a read transaction that will prevent page reuse
        let read_txn = env.begin_txn()?;
        println!("  Started read transaction");
        
        // Try to delete and insert in a write transaction
        {
            let mut txn = env.begin_write_txn()?;
            
            // Delete some more entries
            for i in 1..50 {
                let key = format!("key{:04}", i * 2 + 1);
                if let Ok(true) = db.delete(&mut txn, &key) {
                    // Deleted successfully
                }
            }
            
            // Insert new entries
            for i in 200..210 {
                let key = format!("key{:04}", i);
                let value = format!("reader_test{:04}", i);
                db.put(&mut txn, key, value)?;
            }
            
            println!("  Modified data while reader is active");
            txn.commit()?;
        }
        
        // Drop reader
        drop(read_txn);
        println!("  Dropped read transaction");
    }
    
    // Step 6: Check if pages are now reusable
    println!("\nStep 6: Testing page reuse after reader is gone...");
    {
        let mut txn = env.begin_write_txn()?;
        
        // Insert more data
        for i in 300..310 {
            let key = format!("key{:04}", i);
            let value = format!("reuse_test{:04}", i);
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
    }
    
    // Final check
    if let Ok(metadata) = std::fs::metadata(&file_path) {
        let final_size_kb = metadata.len() / 1024;
        println!("\n  Final database file size: {} KB", final_size_kb);
    }
    
    println!("\n=== Free Page Management Test Complete ===");
    Ok(())
}