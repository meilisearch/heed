//! B+Tree implementation for database operations

use crate::error::{Error, Result, PageId};
use crate::page::{PageFlags, PageHeader, SearchResult, PAGE_SIZE};
use crate::txn::{Transaction, Write};
use crate::meta::DbInfo;
use crate::comparator::{Comparator, LexicographicComparator};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::marker::PhantomData;

/// Maximum number of keys per page (B+Tree order)
/// This is calculated based on page size and typical key/value sizes
pub const MAX_KEYS_PER_PAGE: usize = (PAGE_SIZE - PageHeader::SIZE) / 16;

/// Minimum number of keys per page (except root)
pub const MIN_KEYS_PER_PAGE: usize = MAX_KEYS_PER_PAGE / 2;

/// B+Tree operations
pub struct BTree<C = LexicographicComparator> {
    _phantom: PhantomData<C>,
}

impl<C: Comparator> BTree<C> {
    /// Create a new BTree instance
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
    /// Search for a key in the B+Tree
    pub fn search<'txn>(
        txn: &'txn Transaction<'txn, impl crate::txn::mode::Mode>,
        root: PageId,
        key: &[u8],
    ) -> Result<Option<Cow<'txn, [u8]>>> {
        let mut current_page_id = root;
        
        loop {
            let page = txn.get_page(current_page_id)?;
            
            // Handle empty pages (newly created)
            if page.header.num_keys == 0 && page.header.flags.contains(PageFlags::LEAF) {
                return Ok(None);
            }
            
            match page.search_key_with_comparator::<C>(key)? {
                SearchResult::Found { index } => {
                    let node = page.node(index)?;
                    
                    if page.header.flags.contains(PageFlags::LEAF) {
                        // Found in leaf page
                        // Check if value is in overflow pages
                        if let Some(overflow_id) = node.overflow_page()? {
                            // Read from overflow pages
                            let value = crate::overflow::read_overflow_value(txn, overflow_id)?;
                            return Ok(Some(Cow::Owned(value)));
                        } else {
                            // Regular value
                            return Ok(Some(node.value()?));
                        }
                    } else {
                        // In branch page, follow the child pointer
                        current_page_id = crate::branch_v2::BranchPageV2::find_child_with_comparator::<C>(&page, key)?;
                    }
                }
                SearchResult::NotFound { insert_pos } => {
                    if page.header.flags.contains(PageFlags::LEAF) {
                        // Not found in leaf
                        return Ok(None);
                    } else {
                        // In branch page, use the branch helper
                        current_page_id = crate::branch_v2::BranchPageV2::find_child_with_comparator::<C>(&page, key)?;
                    }
                }
            }
        }
    }
    
    /// Update value for an existing key
    pub fn update_value(
        txn: &mut Transaction<'_, Write>,
        root: PageId,
        key: &[u8],
        new_value: &[u8],
    ) -> Result<()> {
        let mut current_page_id = root;
        
        loop {
            let page = txn.get_page_mut(current_page_id)?;
            
            match page.search_key_with_comparator::<C>(key)? {
                SearchResult::Found { index } => {
                    if page.header.flags.contains(PageFlags::LEAF) {
                        // Found in leaf page - update the value
                        let mut node_data = page.node_data_mut(index)?;
                        
                        // Check if we need overflow pages for the new value
                        let max_value_size = crate::page::MAX_VALUE_SIZE;
                        if new_value.len() > max_value_size {
                            // Need overflow pages
                            // Drop the mutable borrow of page before calling write_overflow_value
                            drop(node_data);
                            drop(page);
                            
                            let overflow_id = crate::overflow::write_overflow_value(txn, new_value)?;
                            
                            // Re-acquire the page and node
                            let page = txn.get_page_mut(current_page_id)?;
                            let mut node_data = page.node_data_mut(index)?;
                            node_data.set_overflow(overflow_id)?;
                        } else {
                            // Regular value
                            node_data.set_value(new_value)?;
                        }
                        
                        return Ok(());
                    } else {
                        // In branch page, follow the child
                        current_page_id = crate::branch_v2::BranchPageV2::find_child_with_comparator::<C>(&page, key)?;
                    }
                }
                SearchResult::NotFound { insert_pos } => {
                    if page.header.flags.contains(PageFlags::LEAF) {
                        // Key not found in leaf
                        return Err(Error::KeyNotFound);
                    } else {
                        // In branch page, use the branch helper
                        current_page_id = crate::branch_v2::BranchPageV2::find_child_with_comparator::<C>(&page, key)?;
                    }
                }
            }
        }
    }
    
    /// Insert a key-value pair into the B+Tree with Copy-on-Write
    pub fn insert(
        txn: &mut Transaction<'_, Write>,
        root: &mut PageId,
        db_info: &mut DbInfo,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        // Start insertion from root with COW
        let (new_root, result) = Self::insert_cow(txn, *root, key, value)?;
        *root = new_root;
        
        match result {
            InsertResult::Updated(old_value) => Ok(old_value),
            InsertResult::Inserted => {
                db_info.entries += 1;
                Ok(None)
            }
            InsertResult::Split { median_key, right_page } => {
                // Root was split, create new root
                let (new_root_id, new_root) = txn.alloc_page(PageFlags::BRANCH)?;
                
                // Debug: Check the page IDs
                if root.0 == 0 || right_page.0 == 0 {
                    return Err(Error::Corruption {
                        details: format!("Invalid page IDs during split: old_root={:?}, right_page={:?}", root, right_page).into(),
                        page_id: Some(*root),
                    });
                }
                
                // Initialize the new root with the split information
                crate::branch_v2::BranchPageV2::init_root(
                    new_root,
                    &median_key,
                    *root,        // left child (old root)
                    right_page,   // right child (new page)
                )?;
                
                *root = new_root_id;
                db_info.depth += 1;
                db_info.branch_pages += 1;
                db_info.entries += 1;
                
                Ok(None)
            }
        }
    }
    
    /// Insert with Copy-on-Write - returns new page ID and result
    fn insert_cow(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(PageId, InsertResult)> {
        let page = txn.get_page(page_id)?;
        
        if page.header.flags.contains(PageFlags::LEAF) {
            // Insert into leaf page with COW
            Self::insert_into_leaf_cow(txn, page_id, key, value)
        } else {
            // Insert into branch page with COW
            Self::insert_into_branch_cow(txn, page_id, key, value)
        }
    }
    
    /// Insert into a non-full page
    fn insert_non_full(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult> {
        let page = txn.get_page(page_id)?;
        
        if page.header.flags.contains(PageFlags::LEAF) {
            // Insert into leaf page
            Self::insert_into_leaf(txn, page_id, key, value)
        } else {
            // Insert into branch page
            Self::insert_into_branch(txn, page_id, key, value)
        }
    }
    
    /// Insert into a leaf page
    fn insert_into_leaf(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult> {
        // Check if value needs overflow pages
        let needs_overflow = crate::overflow::needs_overflow(key.len(), value.len());
        
        // Check if key already exists
        let search_result = {
            let page = txn.get_page(page_id)?;
            page.search_key_with_comparator::<C>(key)?
        };
        
        match search_result {
            SearchResult::Found { index } => {
                // Key exists, update value
                // Try to get old value (might be in overflow pages)
                let (old_value, overflow_page_to_free) = {
                    let page = txn.get_page(page_id)?;
                    let node = page.node(index)?;
                    
                    if let Some(overflow_id) = node.overflow_page()? {
                        // Read from overflow pages
                        (Some(crate::overflow::read_overflow_value(txn, overflow_id)?), Some(overflow_id))
                    } else {
                        (node.value().ok().map(|v| v.into_owned()), None)
                    }
                };
                
                // For value size changes, we need to delete and re-insert
                // First delete the old entry
                {
                    let page = txn.get_page_mut(page_id)?;
                    page.remove_node(index)?;
                }
                
                // Free overflow pages if any
                if let Some(overflow_id) = overflow_page_to_free {
                    crate::overflow::free_overflow_chain(txn, overflow_id)?;
                }
                
                // Now insert the new value
                return Self::insert_into_leaf(txn, page_id, key, value)
                    .map(|result| match result {
                        InsertResult::Inserted => InsertResult::Updated(old_value),
                        other => other,
                    });
            }
            SearchResult::NotFound { insert_pos: _ } => {
                if needs_overflow {
                    // Write value to overflow pages first
                    let overflow_id = crate::overflow::write_overflow_value(txn, value)?;
                    
                    // Try to add the node with overflow reference
                    let page = txn.get_page_mut(page_id)?;
                    match page.add_node_sorted_overflow(key, overflow_id) {
                        Ok(_) => Ok(InsertResult::Inserted),
                        Err(Error::Custom(msg)) if msg.contains("Page full") => {
                            // Page is full, need to split
                            // For split, we'll handle overflow as a special case
                            Self::split_leaf_page_with_overflow(txn, page_id, key, overflow_id)
                        }
                        Err(e) => {
                            Err(e)
                        },
                    }
                } else {
                    // Try to add the node normally
                    let page = txn.get_page_mut(page_id)?;
                    match page.add_node_sorted(key, value) {
                        Ok(_) => Ok(InsertResult::Inserted),
                        Err(Error::Custom(msg)) if msg.contains("Page full") => {
                            // Page is full, need to split
                            Self::split_leaf_page(txn, page_id, key, value)
                        }
                        Err(e) => Err(e),
                    }
                }
            }
        }
    }
    
    /// Insert into a branch page
    fn insert_into_branch(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult> {
        let page = txn.get_page(page_id)?;
        
        // Ensure this is actually a branch page
        if !page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::Corruption {
                details: format!("Expected branch page but got {:?}", page.header.flags).into(),
                page_id: Some(page_id),
            });
        }
        
        // Find child to insert into using the branch page logic
        let child_page_id = crate::branch_v2::BranchPageV2::find_child_with_comparator::<C>(&page, key)?;
        
        // Sanity check: child page ID should never be 0
        if child_page_id.0 == 0 {
            return Err(Error::Corruption {
                details: format!("Branch page returned invalid child page ID 0").into(),
                page_id: Some(page_id),
            });
        }
        
        // Recursively insert into child
        let child_result = Self::insert_non_full(txn, child_page_id, key, value)?;
        
        match child_result {
            InsertResult::Updated(old) => Ok(InsertResult::Updated(old)),
            InsertResult::Inserted => Ok(InsertResult::Inserted),
            InsertResult::Split { median_key, right_page } => {
                // Child was split, add median key to this branch
                let page = txn.get_page_mut(page_id)?;
                
                // Use branch_v2 to add the split child
                match crate::branch_v2::BranchPageV2::add_split_child(page, &median_key, right_page) {
                    Ok(()) => Ok(InsertResult::Inserted),
                    Err(Error::Custom(msg)) if msg.contains("Page full") => {
                        // This branch is also full, split it
                        Self::split_branch_page(txn, page_id, median_key, right_page)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }
    
    /// Split a leaf page
    fn split_leaf_page(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        new_key: &[u8],
        new_value: &[u8],
    ) -> Result<InsertResult> {
        // Get the page to split
        let page = txn.get_page(page_id)?;
        
        // Get the nodes that will go to the right page
        let (right_nodes, median_key) = page.split()?;
        
        // Allocate new right page
        let (right_page_id, right_page) = txn.alloc_page(PageFlags::LEAF)?;
        
        // Add all nodes to the right page
        for (key, value) in &right_nodes {
            right_page.add_node_sorted(key, value)?;
        }
        
        // Truncate the left page
        let left_page = txn.get_page_mut(page_id)?;
        let mid_idx = left_page.header.num_keys as usize / 2;
        left_page.truncate(mid_idx);
        
        // Determine which page to insert the new key into
        if new_key < median_key.as_slice() {
            // Insert into left page
            left_page.add_node_sorted(new_key, new_value)?;
        } else {
            // Insert into right page
            let right_page = txn.get_page_mut(right_page_id)?;
            right_page.add_node_sorted(new_key, new_value)?;
        }
        
        Ok(InsertResult::Split {
            median_key,
            right_page: right_page_id,
        })
    }
    
    /// Split a branch page
    fn split_branch_page(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        new_key: Vec<u8>,
        new_page: PageId,
    ) -> Result<InsertResult> {
        // Get the page to split using branch_v2
        let page = txn.get_page(page_id)?;
        
        // Use branch_v2 split method
        let (right_entries, median_key, right_leftmost) = crate::branch_v2::BranchPageV2::split(&page)?;
        
        // Allocate new right page
        let (right_page_id, right_page) = txn.alloc_page(PageFlags::BRANCH)?;
        
        // Initialize right page with the split data
        crate::branch_v2::BranchPageV2::init_from_split(
            right_page,
            right_leftmost,
            &right_entries,
        )?;
        
        // Truncate the left page
        let left_page = txn.get_page_mut(page_id)?;
        let mid_idx = left_page.header.num_keys as usize / 2;
        left_page.truncate(mid_idx);
        
        // Determine which page to insert the new key into
        if new_key.as_slice() < median_key.as_slice() {
            // Insert into left page
            crate::branch_v2::BranchPageV2::add_split_child(left_page, &new_key, new_page)?;
        } else {
            // Insert into right page
            let right_page = txn.get_page_mut(right_page_id)?;
            crate::branch_v2::BranchPageV2::add_split_child(right_page, &new_key, new_page)?;
        }
        
        Ok(InsertResult::Split {
            median_key,
            right_page: right_page_id,
        })
    }
    
    /// Split a leaf page with an overflow value
    fn split_leaf_page_with_overflow(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        new_key: &[u8],
        overflow_page_id: PageId,
    ) -> Result<InsertResult> {
        // Get the page to split
        let page = txn.get_page(page_id)?;
        
        // Get the nodes that will go to the right page
        let (right_nodes, median_key) = page.split()?;
        
        // Allocate new right page
        let (right_page_id, right_page) = txn.alloc_page(PageFlags::LEAF)?;
        
        // Add all nodes to the right page
        for (key, value) in &right_nodes {
            right_page.add_node_sorted(key, value)?;
        }
        
        // Truncate the left page
        let left_page = txn.get_page_mut(page_id)?;
        let mid_idx = left_page.header.num_keys as usize / 2;
        left_page.truncate(mid_idx);
        
        // Determine which page to insert the new key into
        if new_key < median_key.as_slice() {
            // Insert into left page
            left_page.add_node_sorted_overflow(new_key, overflow_page_id)?;
        } else {
            // Insert into right page
            let right_page = txn.get_page_mut(right_page_id)?;
            right_page.add_node_sorted_overflow(new_key, overflow_page_id)?;
        }
        
        Ok(InsertResult::Split {
            median_key,
            right_page: right_page_id,
        })
    }
    
    /// Delete a key from the B+Tree
    pub fn delete(
        txn: &mut Transaction<'_, Write>,
        root: &mut PageId,
        db_info: &mut DbInfo,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        // Start deletion from root
        let result = Self::delete_from_node(txn, *root, key)?;
        
        match result {
            DeleteResult::NotFound => Ok(None),
            DeleteResult::Deleted(old_value) => {
                db_info.entries = db_info.entries.saturating_sub(1);
                Ok(Some(old_value))
            }
            DeleteResult::Underflow { old_value } => {
                // Root underflowed, need to handle
                let root_page = txn.get_page(*root)?;
                
                if root_page.header.flags.contains(PageFlags::BRANCH) && root_page.header.num_keys == 0 {
                    // Root is empty branch, make its only child the new root
                    if let Ok(node) = root_page.node(0) {
                        *root = node.page_number()?;
                        db_info.depth = db_info.depth.saturating_sub(1);
                        db_info.branch_pages = db_info.branch_pages.saturating_sub(1);
                        
                        // Free the old root
                        txn.free_page(PageId(root_page.header.pgno))?;
                    }
                }
                
                db_info.entries = db_info.entries.saturating_sub(1);
                Ok(Some(old_value))
            }
        }
    }
    
    /// Delete from a node
    fn delete_from_node(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
    ) -> Result<DeleteResult> {
        let page = txn.get_page(page_id)?;
        
        if page.header.flags.contains(PageFlags::LEAF) {
            // Delete from leaf
            Self::delete_from_leaf(txn, page_id, key)
        } else {
            // Delete from branch
            Self::delete_from_branch(txn, page_id, key)
        }
    }
    
    /// Delete from a leaf page
    fn delete_from_leaf(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
    ) -> Result<DeleteResult> {
        // First, search for the key and get node info
        let (search_result, num_keys, pgno) = {
            let page = txn.get_page(page_id)?;
            (page.search_key_with_comparator::<C>(key)?, page.header.num_keys, page.header.pgno)
        };
        
        match search_result {
            SearchResult::Found { index } => {
                // Get the old value before deletion
                let (old_value, overflow_page_to_free) = {
                    let page = txn.get_page(page_id)?;
                    let node = page.node(index)?;
                    
                    // Handle overflow values
                    if let Some(overflow_id) = node.overflow_page()? {
                        // Read value from overflow pages
                        let value = crate::overflow::read_overflow_value(txn, overflow_id)?;
                        (value, Some(overflow_id))
                    } else {
                        // Regular value
                        (node.value()?.into_owned(), None)
                    }
                };
                
                // Remove the node
                {
                    let page = txn.get_page_mut(page_id)?;
                    page.remove_node(index)?;
                }
                
                // Free overflow pages if any
                if let Some(overflow_id) = overflow_page_to_free {
                    crate::overflow::free_overflow_chain(txn, overflow_id)?;
                }
                
                // Check if underflow occurred
                let final_num_keys = {
                    let page = txn.get_page(page_id)?;
                    page.header.num_keys
                };
                
                // Check for underflow (but not on root page)
                if final_num_keys < MIN_KEYS_PER_PAGE as u16 && pgno != 3 {
                    // Not root and underflowed
                    Ok(DeleteResult::Underflow { old_value })
                } else {
                    Ok(DeleteResult::Deleted(old_value))
                }
            }
            SearchResult::NotFound { .. } => Ok(DeleteResult::NotFound),
        }
    }
    
    /// Delete from a branch page
    fn delete_from_branch(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
    ) -> Result<DeleteResult> {
        let page = txn.get_page(page_id)?;
        
        // Find child to delete from using branch_v2 logic
        let child_page_id = crate::branch_v2::BranchPageV2::find_child_with_comparator::<C>(&page, key)?;
        
        // We need to track which child index we're using for rebalancing
        // This is a bit tricky with branch_v2's structure
        let child_index = match page.search_key_with_comparator::<C>(key)? {
            SearchResult::Found { index } => index,
            SearchResult::NotFound { insert_pos } => {
                // For branch_v2, if insert_pos is 0, we're going to leftmost child
                // We'll use usize::MAX to represent the leftmost child index
                if insert_pos == 0 {
                    usize::MAX
                } else {
                    insert_pos - 1
                }
            }
        };
        
        // Recursively delete from child
        let child_result = Self::delete_from_node(txn, child_page_id, key)?;
        
        match child_result {
            DeleteResult::NotFound => Ok(DeleteResult::NotFound),
            DeleteResult::Deleted(old_value) => Ok(DeleteResult::Deleted(old_value)),
            DeleteResult::Underflow { old_value } => {
                // Child underflowed, need to rebalance
                Self::rebalance_child(txn, page_id, child_index)?;
                
                // Check if this page also underflowed
                let page = txn.get_page(page_id)?;
                if page.header.num_keys < MIN_KEYS_PER_PAGE as u16 && page.header.pgno != 3 {
                    Ok(DeleteResult::Underflow { old_value })
                } else {
                    Ok(DeleteResult::Deleted(old_value))
                }
            }
        }
    }
    
    /// Rebalance a child that has underflowed
    fn rebalance_child(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        child_index: usize,
    ) -> Result<()> {
        // Handle branch_v2 structure
        let parent = txn.get_page(parent_id)?;
        if !parent.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Parent must be a branch page"));
        }
        
        // Get necessary information from parent
        let (child_id, left_sibling_id, right_sibling_id, _parent_num_keys) = if child_index == usize::MAX {
            // Leftmost child
            let child_id = crate::branch_v2::BranchPageV2::get_leftmost_child(&parent)?;
            let right_sibling_id = if parent.header.num_keys > 0 {
                Some(parent.node(0)?.page_number()?)
            } else {
                None
            };
            (child_id, None, right_sibling_id, parent.header.num_keys)
        } else {
            // Regular child
            let child_id = parent.node(child_index)?.page_number()?;
            
            let left_sibling_id = if child_index == 0 {
                // The left sibling of the first key's right child is the leftmost child
                Some(crate::branch_v2::BranchPageV2::get_leftmost_child(&parent)?)
            } else {
                Some(parent.node(child_index - 1)?.page_number()?)
            };
            
            let right_sibling_id = if child_index < parent.header.num_keys as usize - 1 {
                Some(parent.node(child_index + 1)?.page_number()?)
            } else {
                None
            };
            
            (child_id, left_sibling_id, right_sibling_id, parent.header.num_keys)
        };
        
        // Handle leftmost child rebalancing
        if child_index == usize::MAX {
            // Leftmost child can only borrow from or merge with its right sibling
            if let Some(right_sibling_id) = right_sibling_id {
                // Try to borrow from right sibling
                if Self::try_borrow_from_leftmost_to_right(txn, parent_id, child_id, right_sibling_id)? {
                    return Ok(());
                }
                
                // If can't borrow, merge with right sibling
                Self::merge_leftmost_with_right(txn, parent_id, child_id, right_sibling_id)?;
            }
            return Ok(());
        }
        
        // Try to borrow from left sibling
        if let Some(left_sibling_id) = left_sibling_id {
            if Self::try_borrow_from_left(txn, parent_id, child_index, left_sibling_id, child_id)? {
                return Ok(());
            }
        }
        
        // Try to borrow from right sibling
        if let Some(right_sibling_id) = right_sibling_id {
            if Self::try_borrow_from_right(txn, parent_id, child_index, child_id, right_sibling_id)? {
                return Ok(());
            }
        }
        
        // Can't borrow, must merge
        if let Some(left_sibling_id) = left_sibling_id {
            // For child at index 0, merge leftmost child with it
            if child_index == 0 {
                Self::merge_leftmost_with_right(txn, parent_id, left_sibling_id, child_id)?;
                return Ok(());
            }
            // Merge with left sibling
            Self::merge_nodes(txn, parent_id, child_index - 1, left_sibling_id, child_id)?;
        } else if let Some(right_sibling_id) = right_sibling_id {
            // Merge with right sibling
            Self::merge_nodes(txn, parent_id, child_index, child_id, right_sibling_id)?;
        }
        
        Ok(())
    }
    
    /// Try to borrow a key from left sibling
    fn try_borrow_from_left(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        child_index: usize,
        left_sibling_id: PageId,
        child_id: PageId,
    ) -> Result<bool> {
        // Check if left sibling has enough keys to share
        let left_sibling_keys = {
            let left_sibling = txn.get_page(left_sibling_id)?;
            left_sibling.header.num_keys as usize
        };
        
        // Can only borrow if left sibling has more than minimum keys
        if left_sibling_keys <= MIN_KEYS_PER_PAGE {
            return Ok(false);
        }
        
        // Get the separator key from parent
        let separator_key = {
            let parent = txn.get_page(parent_id)?;
            parent.node(child_index - 1)?.key()?.to_vec()
        };
        
        // Get the rightmost node from left sibling
        let (borrowed_key, borrowed_value, borrowed_child, is_leaf) = {
            let left_sibling = txn.get_page(left_sibling_id)?;
            let last_idx = left_sibling.header.num_keys as usize - 1;
            let node = left_sibling.node(last_idx)?;
            let is_leaf = left_sibling.header.flags.contains(PageFlags::LEAF);
            if is_leaf {
                (
                    node.key()?.to_vec(),
                    node.value()?.into_owned(),
                    None,
                    true
                )
            } else {
                (
                    node.key()?.to_vec(),
                    vec![],  // For branch pages, we don't need the value
                    Some(node.page_number()?),
                    false
                )
            }
        };
        
        // Remove the rightmost node from left sibling
        {
            let left_sibling = txn.get_page_mut(left_sibling_id)?;
            left_sibling.remove_node(left_sibling.header.num_keys as usize - 1)?;
        }
        
        // Insert into child
        if is_leaf {
            // For leaf nodes, the borrowed key goes directly to child
            let child = txn.get_page_mut(child_id)?;
            // Insert at the beginning
            child.add_node_sorted(&borrowed_key, &borrowed_value)?;
            
            // Update separator in parent to be the new first key of child
            let parent = txn.get_page_mut(parent_id)?;
            crate::branch_v2::BranchPageV2::replace_key(parent, &separator_key, &borrowed_key)?
        } else {
            // For branch nodes, we need to handle the child pointers carefully
            let child = txn.get_page_mut(child_id)?;
            
            // The borrowed child becomes the new leftmost child of the right node
            let old_leftmost = crate::branch_v2::BranchPageV2::get_leftmost_child(child)?;
            crate::branch_v2::BranchPageV2::update_leftmost_child(child, borrowed_child.unwrap())?;
            
            // Insert separator with the old leftmost as its child
            child.add_node_sorted(&separator_key, &old_leftmost.0.to_le_bytes())?;
            
            // Update separator in parent to be the borrowed key
            let parent = txn.get_page_mut(parent_id)?;
            crate::branch_v2::BranchPageV2::replace_key(parent, &separator_key, &borrowed_key)?
        }
        
        Ok(true)
    }
    
    /// Try to borrow a key from right sibling
    fn try_borrow_from_right(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        child_index: usize,
        child_id: PageId,
        right_sibling_id: PageId,
    ) -> Result<bool> {
        // Check if right sibling has enough keys to share
        let right_sibling_keys = {
            let right_sibling = txn.get_page(right_sibling_id)?;
            right_sibling.header.num_keys as usize
        };
        
        // Can only borrow if right sibling has more than minimum keys
        if right_sibling_keys <= MIN_KEYS_PER_PAGE {
            return Ok(false);
        }
        
        // Get the separator key from parent
        let separator_key = {
            let parent = txn.get_page(parent_id)?;
            parent.node(child_index)?.key()?.to_vec()
        };
        
        // Get the leftmost node from right sibling
        let (borrowed_key, borrowed_value, borrowed_child, right_new_leftmost, is_leaf) = {
            let right_sibling = txn.get_page(right_sibling_id)?;
            let node = right_sibling.node(0)?;
            let is_leaf = right_sibling.header.flags.contains(PageFlags::LEAF);
            if is_leaf {
                (
                    node.key()?.to_vec(),
                    node.value()?.into_owned(),
                    None,
                    None,
                    true
                )
            } else {
                // For branch pages, we need the leftmost child and the first node's child
                let leftmost = crate::branch_v2::BranchPageV2::get_leftmost_child(right_sibling)?;
                let first_child = node.page_number()?;
                (
                    node.key()?.to_vec(),
                    vec![],  // For branch pages, we don't need the value
                    Some(leftmost),
                    Some(first_child),
                    false
                )
            }
        };
        
        // Remove the leftmost node from right sibling and update its leftmost child if branch
        {
            let right_sibling = txn.get_page_mut(right_sibling_id)?;
            right_sibling.remove_node(0)?;
            if let Some(new_leftmost) = right_new_leftmost {
                crate::branch_v2::BranchPageV2::update_leftmost_child(right_sibling, new_leftmost)?;
            }
        }
        
        // Insert into child
        if is_leaf {
            // For leaf nodes, the borrowed key goes directly to child
            let child = txn.get_page_mut(child_id)?;
            // Insert at the end
            child.add_node_sorted(&borrowed_key, &borrowed_value)?;
            
            // Update separator in parent to be the new first key of right sibling
            let new_separator = {
                let right_sibling = txn.get_page(right_sibling_id)?;
                right_sibling.node(0)?.key()?.to_vec()
            };
            
            // Update the separator key in parent
            let parent = txn.get_page_mut(parent_id)?;
            crate::branch_v2::BranchPageV2::replace_key(parent, &separator_key, &new_separator)?
        } else {
            // For branch nodes, separator goes down to child with borrowed leftmost as its child
            let child = txn.get_page_mut(child_id)?;
            // Insert separator with the borrowed leftmost child
            child.add_node_sorted(&separator_key, &borrowed_child.unwrap().0.to_le_bytes())?;
            
            // Update separator in parent to be the borrowed key
            let parent = txn.get_page_mut(parent_id)?;
            crate::branch_v2::BranchPageV2::replace_key(parent, &separator_key, &borrowed_key)?
        }
        
        Ok(true)
    }
    
    /// Merge two nodes
    fn merge_nodes(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        left_index: usize,
        left_id: PageId,
        right_id: PageId,
    ) -> Result<()> {
        // Get separator key from parent
        let separator_key = {
            let parent = txn.get_page(parent_id)?;
            parent.node(left_index)?.key()?.to_vec()
        };
        
        // Check if we're dealing with branch or leaf pages
        let is_branch = {
            let left_page = txn.get_page(left_id)?;
            left_page.header.flags.contains(PageFlags::BRANCH)
        };
        
        if is_branch {
            // For branch pages, we need to handle the leftmost child pointer
            let left_leftmost = crate::branch_v2::BranchPageV2::get_leftmost_child(txn.get_page(left_id)?)?;
            let right_leftmost = crate::branch_v2::BranchPageV2::get_leftmost_child(txn.get_page(right_id)?)?;
            
            // Collect entries from both pages
            let mut all_entries = Vec::new();
            
            // Get entries from left page
            {
                let left_page = txn.get_page(left_id)?;
                for i in 0..left_page.header.num_keys as usize {
                    let node = left_page.node(i)?;
                    all_entries.push((node.key()?.to_vec(), node.page_number()?));
                }
            }
            
            // Add separator with right_leftmost as its child
            all_entries.push((separator_key.clone(), right_leftmost));
            
            // Get entries from right page
            {
                let right_page = txn.get_page(right_id)?;
                for i in 0..right_page.header.num_keys as usize {
                    let node = right_page.node(i)?;
                    all_entries.push((node.key()?.to_vec(), node.page_number()?));
                }
            }
            
            // Clear left page and rebuild as branch
            {
                let left_page = txn.get_page_mut(left_id)?;
                left_page.clear();
                left_page.header.flags = PageFlags::BRANCH;
                
                // Set leftmost child
                crate::branch_v2::BranchPageV2::update_leftmost_child(left_page, left_leftmost)?;
                
                // Add all entries
                for (key, child) in all_entries {
                    left_page.add_node_sorted(&key, &child.0.to_le_bytes())?;
                }
            }
        } else {
            // For leaf pages, just collect and merge all entries
            let mut all_nodes = Vec::new();
            
            // Get nodes from left page
            {
                let left_page = txn.get_page(left_id)?;
                for i in 0..left_page.header.num_keys as usize {
                    let node = left_page.node(i)?;
                    all_nodes.push((node.key()?.to_vec(), node.value()?.into_owned()));
                }
            }
            
            // Get nodes from right page
            {
                let right_page = txn.get_page(right_id)?;
                for i in 0..right_page.header.num_keys as usize {
                    let node = right_page.node(i)?;
                    all_nodes.push((node.key()?.to_vec(), node.value()?.into_owned()));
                }
            }
            
            // Clear left page and add all nodes
            {
                let left_page = txn.get_page_mut(left_id)?;
                left_page.clear();
                for (key, value) in all_nodes {
                    left_page.add_node_sorted(&key, &value)?;
                }
            }
        }
        
        // Remove separator from parent
        {
            let parent = txn.get_page_mut(parent_id)?;
            parent.remove_node(left_index)?;
        }
        
        // Free right page
        txn.free_page(right_id)?;
        
        Ok(())
    }
    
    /// Try to borrow from leftmost child to its right sibling
    fn try_borrow_from_leftmost_to_right(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        leftmost_id: PageId,
        right_sibling_id: PageId,
    ) -> Result<bool> {
        // Check if leftmost child has enough keys to share
        let leftmost_keys = {
            let leftmost = txn.get_page(leftmost_id)?;
            leftmost.header.num_keys as usize
        };
        
        // Can only borrow if leftmost has more than minimum keys
        if leftmost_keys <= MIN_KEYS_PER_PAGE {
            return Ok(false);
        }
        
        // Get the separator key (first key in parent)
        let separator_key = {
            let parent = txn.get_page(parent_id)?;
            parent.node(0)?.key()?.to_vec()
        };
        
        // Get the rightmost node from leftmost child
        let (borrowed_key, borrowed_value, borrowed_child, is_leaf) = {
            let leftmost = txn.get_page(leftmost_id)?;
            let last_idx = leftmost.header.num_keys as usize - 1;
            let node = leftmost.node(last_idx)?;
            let is_leaf = leftmost.header.flags.contains(PageFlags::LEAF);
            if is_leaf {
                (
                    node.key()?.to_vec(),
                    node.value()?.into_owned(),
                    None,
                    true
                )
            } else {
                (
                    node.key()?.to_vec(),
                    vec![],  // For branch pages, we don't need the value
                    Some(node.page_number()?),
                    false
                )
            }
        };
        
        // Remove the rightmost node from leftmost child
        {
            let leftmost = txn.get_page_mut(leftmost_id)?;
            leftmost.remove_node(leftmost.header.num_keys as usize - 1)?;
        }
        
        // Insert into right sibling
        if is_leaf {
            // For leaf nodes, the borrowed key goes directly to right sibling
            let right_sibling = txn.get_page_mut(right_sibling_id)?;
            // Insert at the beginning
            right_sibling.add_node_sorted(&borrowed_key, &borrowed_value)?;
            
            // Update separator in parent to be the new first key of right sibling
            let parent = txn.get_page_mut(parent_id)?;
            crate::branch_v2::BranchPageV2::replace_key(parent, &separator_key, &borrowed_key)?
        } else {
            // For branch nodes, we need to handle the child pointers carefully
            let right_sibling = txn.get_page_mut(right_sibling_id)?;
            
            // The borrowed child becomes the new leftmost child of the right sibling
            let old_leftmost = crate::branch_v2::BranchPageV2::get_leftmost_child(right_sibling)?;
            crate::branch_v2::BranchPageV2::update_leftmost_child(right_sibling, borrowed_child.unwrap())?;
            
            // Insert separator with the old leftmost as its child
            right_sibling.add_node_sorted(&separator_key, &old_leftmost.0.to_le_bytes())?;
            
            // Update separator in parent to be the borrowed key
            let parent = txn.get_page_mut(parent_id)?;
            crate::branch_v2::BranchPageV2::replace_key(parent, &separator_key, &borrowed_key)?
        }
        
        Ok(true)
    }
    
    /// Merge leftmost child with its right sibling
    fn merge_leftmost_with_right(
        txn: &mut Transaction<'_, Write>,
        parent_id: PageId,
        leftmost_id: PageId,
        right_id: PageId,
    ) -> Result<()> {
        // Get separator key from parent (first key)
        let separator_key = {
            let parent = txn.get_page(parent_id)?;
            parent.node(0)?.key()?.to_vec()
        };
        
        // Check if we're dealing with branch or leaf pages
        let is_branch = {
            let leftmost_page = txn.get_page(leftmost_id)?;
            leftmost_page.header.flags.contains(PageFlags::BRANCH)
        };
        
        if is_branch {
            // For branch pages, handle the leftmost child pointers
            let left_leftmost = crate::branch_v2::BranchPageV2::get_leftmost_child(txn.get_page(leftmost_id)?)?;
            let right_leftmost = crate::branch_v2::BranchPageV2::get_leftmost_child(txn.get_page(right_id)?)?;
            
            // Collect all entries
            let mut all_entries = Vec::new();
            
            // Get entries from leftmost page
            {
                let leftmost_page = txn.get_page(leftmost_id)?;
                for i in 0..leftmost_page.header.num_keys as usize {
                    let node = leftmost_page.node(i)?;
                    all_entries.push((node.key()?.to_vec(), node.page_number()?));
                }
            }
            
            // Add separator with right_leftmost as its child
            all_entries.push((separator_key.clone(), right_leftmost));
            
            // Get entries from right page
            {
                let right_page = txn.get_page(right_id)?;
                for i in 0..right_page.header.num_keys as usize {
                    let node = right_page.node(i)?;
                    all_entries.push((node.key()?.to_vec(), node.page_number()?));
                }
            }
            
            // Clear leftmost page and rebuild
            {
                let leftmost_page = txn.get_page_mut(leftmost_id)?;
                leftmost_page.clear();
                leftmost_page.header.flags = PageFlags::BRANCH;
                
                // Set leftmost child
                crate::branch_v2::BranchPageV2::update_leftmost_child(leftmost_page, left_leftmost)?;
                
                // Add all entries
                for (key, child) in all_entries {
                    leftmost_page.add_node_sorted(&key, &child.0.to_le_bytes())?;
                }
            }
        } else {
            // For leaf pages, just merge all entries
            let mut all_nodes = Vec::new();
            
            // Get nodes from leftmost page
            {
                let leftmost_page = txn.get_page(leftmost_id)?;
                for i in 0..leftmost_page.header.num_keys as usize {
                    let node = leftmost_page.node(i)?;
                    all_nodes.push((node.key()?.to_vec(), node.value()?.into_owned()));
                }
            }
            
            // Get nodes from right page
            {
                let right_page = txn.get_page(right_id)?;
                for i in 0..right_page.header.num_keys as usize {
                    let node = right_page.node(i)?;
                    all_nodes.push((node.key()?.to_vec(), node.value()?.into_owned()));
                }
            }
            
            // Clear leftmost page and add all nodes
            {
                let leftmost_page = txn.get_page_mut(leftmost_id)?;
                leftmost_page.clear();
                for (key, value) in all_nodes {
                    leftmost_page.add_node_sorted(&key, &value)?;
                }
            }
        }
        
        // Remove first key from parent and update leftmost child
        {
            let parent = txn.get_page_mut(parent_id)?;
            
            // Get the new leftmost child (what was the first key's right child)
            let new_leftmost = parent.node(0)?.page_number()?;
            
            // Remove the first key
            parent.remove_node(0)?;
            
            // Update leftmost child in parent
            crate::branch_v2::BranchPageV2::update_leftmost_child(parent, leftmost_id)?;
        }
        
        // Free right page
        txn.free_page(right_id)?;
        
        Ok(())
    }
    
    /// Insert into leaf page with Copy-on-Write
    fn insert_into_leaf_cow(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(PageId, InsertResult)> {
        // First check if key exists without modifying the page
        let search_result = {
            let page = txn.get_page(page_id)?;
            page.search_key_with_comparator::<C>(key)?
        };
        
        
        match search_result {
            SearchResult::Found { index } => {
                // Key exists - need to update with COW
                let (old_value, old_overflow) = {
                    let page = txn.get_page(page_id)?;
                    let node = page.node(index)?;
                    
                    if let Some(overflow_id) = node.overflow_page()? {
                        (Some(crate::overflow::read_overflow_value(txn, overflow_id)?), Some(overflow_id))
                    } else {
                        (node.value().ok().map(|v| v.into_owned()), None)
                    }
                };
                
                // Don't free old overflow pages yet - they're still referenced by the old page
                // The freelist will handle this when the old page is freed
                
                // Check if we need overflow for new value (do this before getting COW page)
                let needs_overflow = crate::overflow::needs_overflow(key.len(), value.len());
                let new_overflow_id = if needs_overflow {
                    Some(crate::overflow::write_overflow_value(txn, value)?)
                } else {
                    None
                };
                
                // Now get COW page and perform modifications
                let (new_page_id, page) = txn.get_page_cow(page_id)?;
                
                // Remove old entry
                page.remove_node(index)?;
                
                // Insert the new value
                if let Some(overflow_id) = new_overflow_id {
                    match page.add_node_sorted_overflow(key, overflow_id) {
                        Ok(_) => Ok((new_page_id, InsertResult::Updated(old_value))),
                        Err(e) => Err(e),
                    }
                } else {
                    match page.add_node_sorted(key, value) {
                        Ok(_) => Ok((new_page_id, InsertResult::Updated(old_value))),
                        Err(e) => Err(e),
                    }
                }
            }
            SearchResult::NotFound { insert_pos: _ } => {
                // Key doesn't exist - check if we need to split
                let needs_split = {
                    let page = txn.get_page(page_id)?;
                    // Calculate size needed - for overflow values, we only store 8 bytes + key
                    let needs_overflow = crate::overflow::needs_overflow(key.len(), value.len());
                    let key_value_size = if needs_overflow {
                        key.len() + 8 + 8  // key + overflow page ID + overhead
                    } else {
                        key.len() + value.len() + 8  // key + value + overhead
                    };
                    let free_space = page.header.free_space();
                    page.header.free_space() < key_value_size
                };
                
                if needs_split {
                    // Page will be full, handle split
                    let needs_overflow = crate::overflow::needs_overflow(key.len(), value.len());
                    if needs_overflow {
                        let overflow_id = crate::overflow::write_overflow_value(txn, value)?;
                        Self::split_leaf_page_with_overflow(txn, page_id, key, overflow_id)
                            .map(|result| (page_id, result))
                    } else {
                        Self::split_leaf_page(txn, page_id, key, value)
                            .map(|result| (page_id, result))
                    }
                } else {
                    // Check if we need overflow (do this before getting COW page)
                    let needs_overflow = crate::overflow::needs_overflow(key.len(), value.len());
                    let overflow_id = if needs_overflow {
                        Some(crate::overflow::write_overflow_value(txn, value)?)
                    } else {
                        None
                    };
                    
                    // Get COW page and insert
                    let (new_page_id, page) = txn.get_page_cow(page_id)?;
                    
                    if let Some(overflow_id) = overflow_id {
                        match page.add_node_sorted_overflow(key, overflow_id) {
                            Ok(_) => Ok((new_page_id, InsertResult::Inserted)),
                            Err(e) => Err(e),
                        }
                    } else {
                        match page.add_node_sorted(key, value) {
                            Ok(_) => Ok((new_page_id, InsertResult::Inserted)),
                            Err(e) => Err(e),
                        }
                    }
                }
            }
        }
    }
    
    /// Insert into branch page with Copy-on-Write
    fn insert_into_branch_cow(
        txn: &mut Transaction<'_, Write>,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<(PageId, InsertResult)> {
        let child_page_id = {
            let page = txn.get_page(page_id)?;
            crate::branch_v2::BranchPageV2::find_child_with_comparator::<C>(&page, key)?
        };
        
        // Recursively insert into child with COW
        let (new_child_id, child_result) = Self::insert_cow(txn, child_page_id, key, value)?;
        
        match child_result {
            InsertResult::Updated(old) => {
                if new_child_id != child_page_id {
                    // Child page changed due to COW, update parent
                    let (new_page_id, parent) = txn.get_page_cow(page_id)?;
                    
                    // Find and update the child pointer
                    for i in 0..parent.header.num_keys as usize {
                        // First read the current value
                        let current_child = {
                            let node = parent.node(i)?;
                            node.page_number()?
                        };
                        
                        if current_child == child_page_id {
                            // Now get mutable access and update
                            let mut node = parent.node_data_mut(i)?;
                            node.set_value(&new_child_id.0.to_le_bytes())?;
                            break;
                        }
                    }
                    
                    Ok((new_page_id, InsertResult::Updated(old)))
                } else {
                    Ok((page_id, InsertResult::Updated(old)))
                }
            }
            InsertResult::Inserted => {
                if new_child_id != child_page_id {
                    // Child page changed due to COW, update parent
                    let (new_page_id, parent) = txn.get_page_cow(page_id)?;
                    
                    // Update child pointer
                    crate::branch_v2::BranchPageV2::update_child_pointer(parent, child_page_id, new_child_id)?;
                    
                    Ok((new_page_id, InsertResult::Inserted))
                } else {
                    Ok((page_id, InsertResult::Inserted))
                }
            }
            InsertResult::Split { median_key, right_page } => {
                // Child was split, need to add median key to this branch
                let (new_page_id, parent) = txn.get_page_cow(page_id)?;
                
                // Update the old child pointer if it changed
                if new_child_id != child_page_id {
                    crate::branch_v2::BranchPageV2::update_child_pointer(parent, child_page_id, new_child_id)?;
                }
                
                // Add the split child
                match crate::branch_v2::BranchPageV2::add_split_child(parent, &median_key, right_page) {
                    Ok(()) => Ok((new_page_id, InsertResult::Inserted)),
                    Err(Error::Custom(msg)) if msg.contains("Page full") => {
                        // This branch is also full, split it
                        Self::split_branch_page(txn, new_page_id, median_key, right_page)
                            .map(|result| (new_page_id, result))
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }
}

/// Result of an insert operation
enum InsertResult {
    /// Key was updated, returns old value
    Updated(Option<Vec<u8>>),
    /// Key was inserted
    Inserted,
    /// Page was split, returns median key and new right page
    Split {
        median_key: Vec<u8>,
        right_page: PageId,
    },
}

/// Result of a delete operation
enum DeleteResult {
    /// Key was not found
    NotFound,
    /// Key was deleted, returns old value
    Deleted(Vec<u8>),
    /// Key was deleted but page underflowed
    Underflow {
        old_value: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvBuilder;
    use tempfile::TempDir;
    
    #[test]
    fn test_btree_search_empty() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())
            .unwrap();
        
        let txn = env.begin_txn().unwrap();
        let root = PageId(3); // Main DB root
        
        let result = BTree::<LexicographicComparator>::search(&txn, root, b"key").unwrap();
        assert!(result.is_none());
    }
    
    #[test]
    fn test_page_operations() {
        // Test that page operations work without hanging
        let mut page = crate::page::Page::new(PageId(1), PageFlags::LEAF);
        
        // Add a node
        page.add_node_sorted(b"test", b"value").unwrap();
        assert_eq!(page.header.num_keys, 1);
        
        // Search for it
        match page.search_key_with_comparator::<LexicographicComparator>(b"test").unwrap() {
            SearchResult::Found { index } => {
                assert_eq!(index, 0);
            }
            _ => panic!("Should have found key"),
        }
    }
    
    #[test]
    fn test_btree_insert_simple() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())
            .unwrap();
        
        let mut txn = env.begin_write_txn().unwrap();
        let mut root = PageId(3); // Main DB root
        let mut db_info = DbInfo::default();
        db_info.root = root;
        db_info.leaf_pages = 1;
        
        // Insert a key using B+Tree
        let old = BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, b"key1", b"value1").unwrap();
        assert!(old.is_none());
        assert_eq!(db_info.entries, 1);
        
        // Check the page directly
        let page = txn.get_page(root).unwrap();
        assert_eq!(page.header.num_keys, 1);
        
        txn.commit().unwrap();
        
        // Search for the key
        let txn = env.begin_txn().unwrap();
        let result = BTree::<LexicographicComparator>::search(&txn, root, b"key1").unwrap();
        assert_eq!(result.as_ref().map(|v| v.as_ref()), Some(&b"value1"[..]));
    }
    
    #[test]
    fn test_btree_insert_multiple() {
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
        
        // Insert multiple keys
        let keys = vec![
            (b"key3", b"value3"),
            (b"key1", b"value1"),
            (b"key5", b"value5"),
            (b"key2", b"value2"),
            (b"key4", b"value4"),
        ];
        
        for (key, value) in &keys {
            let old = BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, *key, *value).unwrap();
            assert!(old.is_none());
        }
        
        assert_eq!(db_info.entries, 5);
        
        txn.commit().unwrap();
        
        // Search for all keys
        let txn = env.begin_txn().unwrap();
        for (key, expected_value) in &keys {
            let result = BTree::<LexicographicComparator>::search(&txn, root, *key).unwrap();
            assert_eq!(result.as_ref().map(|v| v.as_ref()), Some(&expected_value[..]));
        }
    }
    
    #[test]
    fn test_btree_delete_simple() {
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
        
        // Insert some keys
        BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, b"key1", b"value1").unwrap();
        BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, b"key2", b"value2").unwrap();
        BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, b"key3", b"value3").unwrap();
        assert_eq!(db_info.entries, 3);
        
        // Delete a key
        let deleted = BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, b"key2").unwrap();
        assert_eq!(deleted, Some(b"value2".to_vec()));
        assert_eq!(db_info.entries, 2);
        
        // Try to delete non-existent key
        let deleted = BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, b"key4").unwrap();
        assert_eq!(deleted, None);
        assert_eq!(db_info.entries, 2);
        
        txn.commit().unwrap();
        
        // Verify remaining keys
        let txn = env.begin_txn().unwrap();
        assert!(BTree::<LexicographicComparator>::search(&txn, root, b"key1").unwrap().is_some());
        assert!(BTree::<LexicographicComparator>::search(&txn, root, b"key2").unwrap().is_none());
        assert!(BTree::<LexicographicComparator>::search(&txn, root, b"key3").unwrap().is_some());
    }
    
    #[test]
    fn test_btree_delete_with_rebalancing() {
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
        
        // Insert many keys to force page splits
        let num_keys = 50;
        for i in 0..num_keys {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), value.as_bytes()).unwrap();
        }
        assert_eq!(db_info.entries, num_keys);
        
        // Delete keys in a pattern that should trigger rebalancing
        for i in (0..num_keys).step_by(3) {
            let key = format!("key{:04}", i);
            let deleted = BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, key.as_bytes()).unwrap();
            assert!(deleted.is_some());
        }
        
        // Verify the tree is still valid
        for i in 0..num_keys {
            let key = format!("key{:04}", i);
            let result = BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes()).unwrap();
            if i % 3 == 0 {
                assert!(result.is_none(), "Key {} should be deleted", key);
            } else {
                assert!(result.is_some(), "Key {} should exist", key);
            }
        }
        
        txn.commit().unwrap();
    }
    
    #[test]
    fn test_btree_delete_all() {
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
        
        // Insert some keys
        let keys = vec![b"key1", b"key2", b"key3", b"key4", b"key5"];
        for key in &keys {
            BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, *key, *key).unwrap();
        }
        assert_eq!(db_info.entries, keys.len() as u64);
        
        // Delete all keys
        for key in &keys {
            let deleted = BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, *key).unwrap();
            assert!(deleted.is_some());
        }
        assert_eq!(db_info.entries, 0);
        
        // Tree should be empty
        for key in &keys {
            let result = BTree::<LexicographicComparator>::search(&txn, root, *key).unwrap();
            assert!(result.is_none());
        }
        
        txn.commit().unwrap();
    }
    
    #[test]
    fn test_btree_overflow_values() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())
            .unwrap();
        
        let mut txn = env.begin_write_txn().unwrap();
        
        // Properly initialize the database
        let (root_id, root_page) = txn.alloc_page(PageFlags::LEAF).unwrap();
        root_page.header.num_keys = 0;
        
        let mut db_info = DbInfo::default();
        db_info.root = root_id;
        db_info.leaf_pages = 1;
        let mut root = root_id;
        
        // Save initial database info
        txn.update_db_info(None, db_info).unwrap();
        
        // Create a large value that needs overflow pages
        let large_value = vec![0xAB; 5000]; // 5KB
        
        // Insert with large value
        let old = BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, b"large_key", &large_value).unwrap();
        assert!(old.is_none());
        assert_eq!(db_info.entries, 1);
        
        // Insert some normal values
        BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, b"small1", b"value1").unwrap();
        BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, b"small2", b"value2").unwrap();
        
        // Update the root in db_info after all inserts
        db_info.root = root;
        // Update transaction's database info
        txn.update_db_info(None, db_info).unwrap();
        
        txn.commit().unwrap();
        
        // Search for large value
        let txn = env.begin_txn().unwrap();
        let db_info = txn.db_info(None).unwrap();
        let result = BTree::<LexicographicComparator>::search(&txn, db_info.root, b"large_key").unwrap();
        assert_eq!(result.as_ref().map(|v| v.as_ref()), Some(&large_value[..]));
        
        // Search for normal values
        let result = BTree::<LexicographicComparator>::search(&txn, db_info.root, b"small1").unwrap();
        assert_eq!(result.as_ref().map(|v| v.as_ref()), Some(&b"value1"[..]));
        
        drop(txn);
        
        // Delete large value
        let mut txn = env.begin_write_txn().unwrap();
        let mut db_info = *txn.db_info(None).unwrap();
        let mut root = db_info.root;
        let deleted = BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, b"large_key").unwrap();
        assert_eq!(deleted, Some(large_value));
        
        // Update db_info with new root
        db_info.root = root;
        txn.update_db_info(None, db_info).unwrap();
        
        // Verify it's deleted
        let result = BTree::<LexicographicComparator>::search(&txn, db_info.root, b"large_key").unwrap();
        assert!(result.is_none());
        
        txn.commit().unwrap();
    }
}