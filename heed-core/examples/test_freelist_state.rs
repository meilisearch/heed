use heed_core::{EnvBuilder, Database};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    println!("=== Testing FreeList State Persistence ===\n");
    
    // Track page allocations
    let mut allocated_pages = Vec::new();
    
    // Step 1: Create database and allocate some pages
    println!("Step 1: Initial allocation");
    {
        let mut txn = env.begin_write_txn()?;
        
        // Create database
        let _db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
        
        // Allocate pages
        use heed_core::page::PageFlags;
        for i in 0..3 {
            let (page_id, _) = txn.alloc_page(PageFlags::LEAF)?;
            println!("  Allocated page {}: {:?}", i, page_id);
            allocated_pages.push(page_id);
        }
        
        txn.commit()?;
    }
    
    // Step 2: Free some pages
    println!("\nStep 2: Freeing pages");
    {
        let mut txn = env.begin_write_txn()?;
        
        // Free the middle page
        println!("  Freeing page {:?}", allocated_pages[1]);
        txn.free_page(allocated_pages[1])?;
        
        txn.commit()?;
        println!("  Transaction committed");
    }
    
    // Step 3: Just note that we've freed a page
    println!("\nStep 3: Page has been freed and should be available for reuse");
    
    // Step 4: Allocate in new transaction - should reuse freed page
    println!("\nStep 4: New allocation (should reuse)");
    {
        let mut txn = env.begin_write_txn()?;
        
        use heed_core::page::PageFlags;
        let (page_id, _) = txn.alloc_page(PageFlags::LEAF)?;
        println!("  Allocated page: {:?}", page_id);
        
        if page_id == allocated_pages[1] {
            println!("  SUCCESS: Reused freed page!");
        } else {
            println!("  FAIL: Did not reuse freed page");
        }
        
        txn.commit()?;
    }
    
    // Step 5: Check persistence after restart
    println!("\nStep 5: Simulating restart by dropping and recreating env");
    drop(env);
    
    let env2 = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    println!("  Environment reopened");
    
    // Try another allocation
    {
        let mut txn = env2.begin_write_txn()?;
        
        use heed_core::page::PageFlags;
        let (page_id, _) = txn.alloc_page(PageFlags::LEAF)?;
        println!("  Allocated page after restart: {:?}", page_id);
        
        txn.commit()?;
    }
    
    println!("\n=== Test Complete ===");
    Ok(())
}