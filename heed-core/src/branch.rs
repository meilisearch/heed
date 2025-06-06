//! Branch page operations for B+Tree
//! 
//! In a B+Tree, branch (internal) pages contain:
//! - n keys that act as separators
//! - n+1 child pointers
//! 
//! The structure is:
//! child[0] | key[0] | child[1] | key[1] | ... | key[n-1] | child[n]
//!
//! Where:
//! - child[0] contains all keys < key[0]
//! - child[i] contains all keys >= key[i-1] and < key[i]  
//! - child[n] contains all keys >= key[n-1]

use crate::error::{Error, Result, PageId};
use crate::page::{Page, PageFlags, PageHeader};
use crate::txn::{Transaction, Write};

/// Branch page structure that properly handles n keys and n+1 children
pub struct BranchPage;

impl BranchPage {
    /// Initialize a new branch page with one key and two children
    pub fn init_root(
        page: &mut Page,
        median_key: &[u8],
        left_child: PageId,
        right_child: PageId,
    ) -> Result<()> {
        // Ensure it's a branch page
        page.header.flags = PageFlags::BRANCH;
        page.header.num_keys = 0;
        page.header.lower = PageHeader::SIZE as u16;
        page.header.upper = crate::page::PAGE_SIZE as u16;
        
        // In B+Tree branch pages, we store n keys and n+1 children
        // For a root with 2 children, we need 1 key
        // We'll use a special marker to indicate the leftmost child
        
        // Add a node with empty key pointing to left child
        // Empty key ensures it sorts before any real key
        let empty_key = b"";
        page.add_node(empty_key, &left_child.0.to_le_bytes())?;
        
        // Add the median key pointing to right child
        page.add_node(median_key, &right_child.0.to_le_bytes())?;
        
        // Verify the branch page was initialized correctly
        let test_left = Self::get_leftmost_child(page)?;
        if test_left.0 != left_child.0 {
            return Err(Error::Corruption {
                details: format!("Branch page initialization failed: expected left child {:?}, got {:?}", left_child, test_left).into(),
                page_id: Some(PageId(page.header.pgno)),
            });
        }
        
        Ok(())
    }
    
    /// Get the leftmost child (child[0])
    pub fn get_leftmost_child(page: &Page) -> Result<PageId> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }
        
        // The leftmost child is stored as the first node with empty key
        if page.header.num_keys > 0 {
            let first_node = page.node(0)?;
            let key = first_node.key()?;
            if key.is_empty() {
                // This is our leftmost child marker
                return first_node.page_number();
            }
        }
        
        // If we don't find an empty key node, something is wrong
        Err(Error::Corruption {
            details: "Branch page missing leftmost child".into(),
            page_id: Some(PageId(page.header.pgno)),
        })
    }
    
    /// Find the appropriate child for a given key
    pub fn find_child(page: &Page, search_key: &[u8]) -> Result<PageId> {
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }
        
        // Special case: empty branch page (shouldn't happen)
        if page.header.num_keys == 0 {
            return Err(Error::Corruption {
                details: "Empty branch page".into(),
                page_id: Some(PageId(page.header.pgno)),
            });
        }
        
        // Check if first node is the leftmost child marker (empty key)
        let has_leftmost_marker = {
            let first_node = page.node(0)?;
            first_node.key()?.is_empty()
        };
        
        // Search through the keys to find the right child
        for i in 0..page.header.num_keys as usize {
            let node = page.node(i)?;
            let node_key = node.key()?;
            
            // Skip empty key (leftmost child marker)
            if node_key.is_empty() {
                continue;
            }
            
            // If search key is less than this node's key, we found our position
            if search_key < node_key {
                if i == 0 || (i == 1 && has_leftmost_marker) {
                    // Use leftmost child
                    return Self::get_leftmost_child(page);
                } else {
                    // Use the previous node's child
                    let prev_node = page.node(i - 1)?;
                    return prev_node.page_number();
                }
            }
        }
        
        // Key is greater than all keys, use the last child
        let last_node = page.node(page.header.num_keys as usize - 1)?;
        last_node.page_number()
    }
    
    /// Add a new key and right child after a split
    pub fn add_split_child(
        page: &mut Page,
        key: &[u8],
        right_child: PageId,
    ) -> Result<()> {
        // Simply add as a normal node
        page.add_node_sorted(key, &right_child.0.to_le_bytes())?;
        Ok(())
    }
}