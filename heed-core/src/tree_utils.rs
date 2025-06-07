//! Tree traversal utilities for page management

use crate::error::{Error, Result, PageId};
use crate::page::{PageFlags, Page};
use crate::txn::{Transaction, Write};
use std::collections::VecDeque;

/// Traverse a B+Tree and collect all page IDs
pub fn collect_tree_pages<'txn>(
    txn: &Transaction<'txn, impl crate::txn::mode::Mode>,
    root: PageId,
) -> Result<Vec<PageId>> {
    let mut pages = Vec::new();
    let mut queue = VecDeque::new();
    queue.push_back(root);
    
    while let Some(page_id) = queue.pop_front() {
        // Skip if already processed (shouldn't happen in a tree, but be safe)
        if pages.contains(&page_id) {
            continue;
        }
        
        pages.push(page_id);
        
        let page = txn.get_page(page_id)?;
        
        if page.header.flags.contains(PageFlags::BRANCH) {
            // Branch page - add leftmost child first
            let leftmost = crate::branch::BranchPage::get_leftmost_child(page)?;
            queue.push_back(leftmost);
            
            // Add all other children
            for i in 0..page.header.num_keys as usize {
                let node = page.node(i)?;
                let child = node.page_number()?;
                queue.push_back(child);
            }
        } else if page.header.flags.contains(PageFlags::LEAF) {
            // Check for overflow pages
            for i in 0..page.header.num_keys as usize {
                let node = page.node(i)?;
                if let Some(overflow_id) = node.overflow_page()? {
                    // Collect all overflow pages in the chain
                    let overflow_pages = collect_overflow_chain(txn, overflow_id)?;
                    pages.extend(overflow_pages);
                }
            }
        }
    }
    
    Ok(pages)
}

/// Collect all pages in an overflow chain
fn collect_overflow_chain<'txn>(
    txn: &Transaction<'txn, impl crate::txn::mode::Mode>,
    start: PageId,
) -> Result<Vec<PageId>> {
    let mut pages = Vec::new();
    let mut current = start;
    
    loop {
        pages.push(current);
        
        let page = txn.get_page(current)?;
        if !page.header.flags.contains(PageFlags::OVERFLOW) {
            return Err(Error::Corruption {
                details: "Expected overflow page in chain".into(),
                page_id: Some(current),
            });
        }
        
        // Get next overflow page (stored in the overflow field)
        let next = PageId(page.header.overflow as u64);
        if next.0 == 0 {
            break;
        }
        
        current = next;
    }
    
    Ok(pages)
}

/// Free all pages in a B+Tree
pub fn free_tree(
    txn: &mut Transaction<'_, Write>,
    root: PageId,
) -> Result<()> {
    // Collect all pages first
    let pages = collect_tree_pages(txn, root)?;
    
    // Free them all
    for page_id in pages {
        txn.free_page(page_id)?;
    }
    
    Ok(())
}

/// Traverse a B+Tree and apply a function to each page
pub fn traverse_tree<F>(
    txn: &Transaction<'_, impl crate::txn::mode::Mode>,
    root: PageId,
    mut f: F,
) -> Result<()>
where
    F: FnMut(PageId, &Page) -> Result<()>,
{
    let mut queue = VecDeque::new();
    let mut visited = std::collections::HashSet::new();
    queue.push_back(root);
    
    while let Some(page_id) = queue.pop_front() {
        // Skip if already visited
        if !visited.insert(page_id) {
            continue;
        }
        
        let page = txn.get_page(page_id)?;
        
        // Apply the function
        f(page_id, page)?;
        
        if page.header.flags.contains(PageFlags::BRANCH) {
            // Branch page - add leftmost child first
            let leftmost = crate::branch::BranchPage::get_leftmost_child(page)?;
            queue.push_back(leftmost);
            
            // Add all other children
            for i in 0..page.header.num_keys as usize {
                let node = page.node(i)?;
                let child = node.page_number()?;
                queue.push_back(child);
            }
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvBuilder;
    use crate::btree::BTree;
    use crate::meta::DbInfo;
    use crate::comparator::LexicographicComparator;
    use tempfile::TempDir;
    
    #[test]
    fn test_collect_tree_pages() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())
            .unwrap();
        
        let mut txn = env.begin_write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo::default();
        db_info.root = root;
        db_info.leaf_pages = 1;
        
        // Insert enough entries to create a multi-level tree
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Collect all pages
        let pages = collect_tree_pages(&txn, root).unwrap();
        
        // Should have at least the root page
        assert!(!pages.is_empty());
        assert!(pages.contains(&root));
        
        // Verify page count matches db_info
        let total_pages = db_info.leaf_pages + db_info.branch_pages + db_info.overflow_pages;
        assert_eq!(pages.len() as u64, total_pages);
    }
    
    #[test]
    fn test_free_tree() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())
            .unwrap();
        
        let mut txn = env.begin_write_txn().unwrap();
        
        // Allocate a new page for our test tree instead of using the main DB root
        let (root, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
        let mut db_info = DbInfo::default();
        db_info.root = root;
        db_info.leaf_pages = 1;
        
        // Create a tree with some data
        let mut current_root = root;
        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            BTree::<LexicographicComparator>::insert(&mut txn, &mut current_root, &mut db_info, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Collect pages before freeing
        let pages_before = collect_tree_pages(&txn, current_root).unwrap();
        let page_count = pages_before.len();
        
        // Free the tree
        free_tree(&mut txn, current_root).unwrap();
        
        // Check that pages were marked for freeing
        if let crate::txn::ModeData::Write { ref freelist, .. } = txn.mode_data {
            let pending = freelist.pending_len();
            // We should have freed all pages in the tree
            assert_eq!(pending, page_count, 
                    "Expected {} pending pages, got {}", page_count, pending);
        }
    }
}