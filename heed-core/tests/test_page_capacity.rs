//! Test how many entries fit in a page

use heed_core::page::{Page, PageFlags, PageHeader, PAGE_SIZE};
use heed_core::error::PageId;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing page capacity...");
    println!("Page size: {} bytes", PAGE_SIZE);
    println!("Page header size: {} bytes", PageHeader::SIZE);
    println!("Initial data space: {} bytes", PAGE_SIZE - PageHeader::SIZE);
    
    // Create a test page
    let mut page = Page::new(PageId(1), PageFlags::LEAF);
    
    println!("\nInitial page state:");
    println!("  lower: {}", page.header.lower);
    println!("  upper: {}", page.header.upper);
    println!("  free_space: {}", page.header.free_space());
    
    // Try to add entries until full
    let mut count = 0;
    for i in 0..1000 {
        let key = format!("key_{:03}", i);
        let value = vec![i as u8; 64]; // 64 byte values
        
        match page.add_node_sorted(key.as_bytes(), &value) {
            Ok(_) => {
                count += 1;
                if count <= 5 || count % 10 == 0 {
                    println!("\nAfter {} entries:", count);
                    println!("  lower: {}", page.header.lower);
                    println!("  upper: {}", page.header.upper);
                    println!("  free_space: {}", page.header.free_space());
                }
            }
            Err(e) => {
                println!("\nPage full after {} entries: {:?}", count, e);
                println!("Final state:");
                println!("  lower: {}", page.header.lower);
                println!("  upper: {}", page.header.upper);
                println!("  free_space: {}", page.header.free_space());
                
                // Calculate space used
                let ptr_space = count * 2; // Each pointer is 2 bytes
                let node_space = page.header.upper as usize - PageHeader::SIZE;
                let total_used = ptr_space + (PAGE_SIZE - node_space);
                
                println!("\nSpace analysis:");
                println!("  Pointer array: {} bytes ({} entries × 2)", ptr_space, count);
                println!("  Node data: {} bytes", PAGE_SIZE - node_space);
                println!("  Total used: {} bytes", total_used);
                println!("  Efficiency: {:.1}%", (total_used as f64 / PAGE_SIZE as f64) * 100.0);
                
                break;
            }
        }
    }
    
    if count == 1000 {
        println!("\nAdded 1000 entries without filling page!");
    }
    
    Ok(())
}