use heed_core::{EnvBuilder, Database};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    println!("=== Direct Free Page Test ===\n");
    
    // Create database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Step 1: Allocate pages and track them
    println!("Step 1: Allocating pages directly...");
    let freed_pages = {
        let mut txn = env.begin_write_txn()?;
        let mut pages = Vec::new();
        
        // Allocate some pages directly
        use heed_core::page::PageFlags;
        for i in 0..5 {
            let (page_id, page) = txn.alloc_page(PageFlags::LEAF)?;
            println!("  Allocated page {}: {:?}", i, page_id);
            pages.push(page_id);
            
            // Write some data to make it valid
            page.header.num_keys = 0;
        }
        
        // Now free some of them
        println!("\n  Freeing pages 1, 2, 3...");
        txn.free_page(pages[1])?;
        txn.free_page(pages[2])?;
        txn.free_page(pages[3])?;
        
        txn.commit()?;
        
        vec![pages[1], pages[2], pages[3]]
    };
    
    // Step 2: Try to allocate again - should reuse freed pages
    println!("\nStep 2: Allocating new pages (should reuse freed ones)...");
    {
        let mut txn = env.begin_write_txn()?;
        
        // We can't directly access the freelist, but we can infer its behavior
        println!("  Checking page allocation behavior...");
        
        // Allocate new pages
        use heed_core::page::PageFlags;
        for i in 0..3 {
            let (page_id, _page) = txn.alloc_page(PageFlags::LEAF)?;
            println!("  Allocated page: {:?}", page_id);
            
            // Check if it's a reused page
            if freed_pages.contains(&page_id) {
                println!("    -> REUSED freed page!");
            } else {
                println!("    -> NEW page allocated");
            }
        }
        
        txn.commit()?;
    }
    
    // Step 3: Test with reader interference
    println!("\nStep 3: Testing with active reader...");
    {
        // Start a reader
        let _reader = env.begin_txn()?;
        println!("  Reader started");
        
        // Free more pages
        let mut txn = env.begin_write_txn()?;
        
        use heed_core::page::PageFlags;
        let (page_to_free, _) = txn.alloc_page(PageFlags::LEAF)?;
        println!("  Allocated page to free: {:?}", page_to_free);
        
        txn.free_page(page_to_free)?;
        println!("  Freed page while reader is active");
        
        txn.commit()?;
        
        // Try to reuse while reader is still active
        let mut txn2 = env.begin_write_txn()?;
        
        // With active reader, pages shouldn't be reused
        println!("\n  With active reader, freed pages shouldn't be reused");
        
        let (new_page, _) = txn2.alloc_page(PageFlags::LEAF)?;
        println!("  Allocated page: {:?} (should be new, not reused)", new_page);
        
        txn2.commit()?;
    }
    
    // Reader is dropped here
    
    println!("\nStep 4: After reader is gone...");
    {
        let mut txn = env.begin_write_txn()?;
        
        // Pages freed while reader was active might now be reusable
        println!("  Now pages should be reusable");
        
        use heed_core::page::PageFlags;
        let (page_id, _) = txn.alloc_page(PageFlags::LEAF)?;
        println!("  Allocated page: {:?}", page_id);
        
        txn.commit()?;
    }
    
    println!("\n=== Test Complete ===");
    Ok(())
}