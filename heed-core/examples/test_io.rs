//! Test the I/O backend implementation

use heed_core::io::{IoBackend, MmapBackend};
use heed_core::page::{Page, PageFlags};
use heed_core::error::PageId;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary file
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("test.db");
    
    println!("Creating I/O backend at: {:?}", path);
    
    // Create the backend
    let backend = MmapBackend::with_options(&path, 1024 * 1024)?;
    
    println!("Backend created, size: {} pages", backend.size_in_pages());
    
    // Create and write some pages
    for i in 0..5 {
        let page = Page::new(PageId(i), PageFlags::LEAF);
        println!("Writing page {}", i);
        backend.write_page(&page)?;
    }
    
    // Sync to disk
    backend.sync()?;
    println!("Synced to disk");
    
    // Read the pages back
    for i in 0..5 {
        let page = backend.read_page(PageId(i))?;
        println!("Read page {}, flags: {:?}", page.header.pgno, page.header.flags);
        assert_eq!(page.header.pgno, i);
        assert_eq!(page.header.flags, PageFlags::LEAF);
    }
    
    println!("All pages verified!");
    
    // Test growing the file
    let initial_size = backend.size_in_pages();
    println!("Growing from {} to {} pages", initial_size, initial_size * 2);
    backend.grow(initial_size * 2)?;
    println!("New size: {} pages", backend.size_in_pages());
    
    // Write a page in the new area
    let new_page_id = PageId(initial_size + 10);
    let page = Page::new(new_page_id, PageFlags::BRANCH);
    backend.write_page(&page)?;
    
    // Read it back
    let read_page = backend.read_page(new_page_id)?;
    assert_eq!(read_page.header.pgno, new_page_id.0);
    assert_eq!(read_page.header.flags, PageFlags::BRANCH);
    
    println!("Growth test passed!");
    
    Ok(())
}