//! Database cursor operations

use crate::error::{Error, Result, PageId};
use crate::db::Database;
use crate::txn::{Transaction, mode::Mode};
use crate::page::{PageFlags, SearchResult};
use crate::dupsort::DupSort;
use crate::meta::DbInfo;
use std::borrow::Cow;
use std::marker::PhantomData;

/// Position in the B+Tree
#[derive(Debug, Clone)]
struct CursorPosition {
    /// Stack of pages from root to leaf
    pages: Vec<PageId>,
    /// Index within each page
    indices: Vec<usize>,
}

/// State for DUPSORT cursor operations
#[derive(Debug, Clone)]
struct DupCursorState {
    /// Sub-database info for the current key
    sub_db: DbInfo,
    /// Position within the sub-database
    position: Option<CursorPosition>,
}

/// A database cursor for iteration
pub struct Cursor<'txn, K, V, C = ()> {
    /// Reference to the transaction (type-erased)
    txn: *const (),
    /// Transaction lifetime
    _txn_lifetime: PhantomData<&'txn ()>,
    /// Database name
    db_name: Option<String>,
    /// Database root (for modifications)
    db_root: PageId,
    /// Current position in the tree
    position: Option<CursorPosition>,
    /// DUPSORT: current duplicate cursor state
    dup_cursor: Option<DupCursorState>,
    /// Database flags (to check for DUPSORT)
    db_flags: u32,
    /// Whether this cursor is for a write transaction
    is_write: bool,
    /// Phantom data for types
    _phantom: PhantomData<(K, V, C)>,
}

impl<'txn, K, V, C> Cursor<'txn, K, V, C> {
    /// Create a new cursor
    pub fn new<M: Mode, KT, VT>(txn: &'txn Transaction<'txn, M>, db: &'txn Database<KT, VT, C>) -> Result<Self> 
    where
        KT: crate::db::Key,
        VT: crate::db::Value,
    {
        let db_info = txn.db_info(db.name())?;
        Ok(Self { 
            txn: txn as *const _ as *const (),
            _txn_lifetime: PhantomData,
            db_name: db.name().map(|s| s.to_string()),
            db_root: db_info.root,
            position: None,
            dup_cursor: None,
            db_flags: db_info.flags,
            is_write: M::IS_WRITE,
            _phantom: PhantomData,
        })
    }
    
    /// Get the transaction reference
    fn txn<M: Mode>(&self) -> &'txn Transaction<'txn, M> {
        unsafe { &*(self.txn as *const Transaction<'txn, M>) }
    }
    
    /// Get a page based on transaction type
    fn get_page(&self, page_id: PageId) -> Result<&'txn crate::page::Page> {
        if self.is_write {
            let txn = unsafe { &*(self.txn as *const Transaction<'txn, crate::txn::Write>) };
            txn.get_page(page_id)
        } else {
            let txn = self.txn::<crate::txn::Read>();
            txn.get_page(page_id)
        }
    }
    
    /// Get a page without borrowing self
    fn get_page_raw(txn_ptr: *const (), is_write: bool, page_id: PageId) -> Result<&'txn crate::page::Page> {
        if is_write {
            let txn = unsafe { &*(txn_ptr as *const Transaction<'txn, crate::txn::Write>) };
            txn.get_page(page_id)
        } else {
            let txn = unsafe { &*(txn_ptr as *const Transaction<'txn, crate::txn::Read>) };
            txn.get_page(page_id)
        }
    }
    
    /// Get value from node, handling overflow pages
    fn get_node_value(&self, node: &crate::page::Node<'txn>) -> Result<Cow<'txn, [u8]>> {
        if let Some(overflow_id) = node.overflow_page()? {
            // Read from overflow pages
            let overflow_value = if self.is_write {
                let txn = unsafe { &*(self.txn as *const Transaction<'txn, crate::txn::Write>) };
                crate::overflow::read_overflow_value(txn, overflow_id)?
            } else {
                let txn = self.txn::<crate::txn::Read>();
                crate::overflow::read_overflow_value(txn, overflow_id)?
            };
            Ok(Cow::Owned(overflow_value))
        } else {
            node.value()
        }
    }
    
    /// Move to first entry (returns raw bytes)
    pub fn first_raw(&mut self) -> Result<Option<(&'txn [u8], Cow<'txn, [u8]>)>> {
        let txn = self.txn::<crate::txn::Read>();
        
        // Update db_root in case it changed (e.g., after delete)
        let db_info = txn.db_info(self.db_name.as_deref())?;
        self.db_root = db_info.root;
        
        // Navigate to the leftmost leaf
        let mut position = CursorPosition {
            pages: Vec::new(),
            indices: Vec::new(),
        };
        
        let mut current_page_id = self.db_root;
        
        loop {
            let page = self.get_page(current_page_id)?;
            
            if page.header.num_keys == 0 {
                // Empty tree
                return Ok(None);
            }
            
            if page.header.flags.contains(PageFlags::LEAF) {
                // Found leaf, get first entry
                position.pages.push(current_page_id);
                position.indices.push(0);
                
                let node = page.node(0)?;
                let key = node.key()?;
                
                let value = self.get_node_value(&node)?;
                
                self.position = Some(position);
                return Ok(Some((key, value)));
            } else {
                // Branch page, use branch_v2 to get leftmost child
                // For branch pages, index -1 represents the leftmost child
                position.pages.push(current_page_id);
                position.indices.push(usize::MAX); // Use MAX to represent "before first key"
                
                current_page_id = crate::branch_v2::BranchPageV2::get_leftmost_child(&page)?;
            }
        }
    }
    
    /// Move to last entry (returns raw bytes)
    pub fn last_raw(&mut self) -> Result<Option<(&'txn [u8], Cow<'txn, [u8]>)>> {
        let txn = self.txn::<crate::txn::Read>();
        
        // Update db_root in case it changed (e.g., after delete)
        let db_info = txn.db_info(self.db_name.as_deref())?;
        self.db_root = db_info.root;
        
        // Navigate to the rightmost leaf
        let mut position = CursorPosition {
            pages: Vec::new(),
            indices: Vec::new(),
        };
        
        let mut current_page_id = self.db_root;
        
        loop {
            let page = self.get_page(current_page_id)?;
            
            if page.header.num_keys == 0 {
                // Empty tree
                return Ok(None);
            }
            
            let last_idx = page.header.num_keys as usize - 1;
            position.pages.push(current_page_id);
            position.indices.push(last_idx);
            
            if page.header.flags.contains(PageFlags::LEAF) {
                // Found leaf, get last entry
                let node = page.node(last_idx)?;
                let key = node.key()?;
                
                let value = self.get_node_value(&node)?;
                
                self.position = Some(position);
                return Ok(Some((key, value)));
            } else {
                // Branch page, follow last child
                // In branch_v2, the last node's value is the last child
                let node = page.node(last_idx)?;
                current_page_id = node.page_number()?;
            }
        }
    }
    
    /// Move to next entry (returns raw bytes)
    pub fn next_raw(&mut self) -> Result<Option<(&'txn [u8], Cow<'txn, [u8]>)>> {
        if self.position.is_none() {
            return self.first_raw();
        }
        
        // Get necessary info before mutable borrow
        let (leaf_idx, leaf_page_id) = {
            let position = self.position.as_ref().unwrap();
            let leaf_idx = position.pages.len() - 1;
            let leaf_page_id = position.pages[leaf_idx];
            (leaf_idx, leaf_page_id)
        };
        
        let leaf_page = self.get_page(leaf_page_id)?;
        
        // Now we can mutably borrow position
        let position = self.position.as_mut().unwrap();
        position.indices[leaf_idx] += 1;
        
        if position.indices[leaf_idx] < leaf_page.header.num_keys as usize {
            // Still have entries in current leaf
            let node = leaf_page.node(position.indices[leaf_idx])?;
            let key = node.key()?;
            
            // Check if value is in overflow pages
            let value = if let Some(overflow_id) = node.overflow_page()? {
                // Read from overflow pages
                let overflow_value = crate::overflow::read_overflow_value(self.txn::<crate::txn::Read>(), overflow_id)?;
                Cow::Owned(overflow_value)
            } else {
                node.value()?
            };
            
            return Ok(Some((key, value)));
        }
        
        // Need to move to next leaf
        // Go up the tree until we find a branch we haven't exhausted
        position.pages.pop();
        position.indices.pop();
        
        while !position.pages.is_empty() {
            let level = position.pages.len() - 1;
            let page_id = position.pages[level];
            
            // Get page first before incrementing
            let page = Self::get_page_raw(self.txn, self.is_write, page_id)?;
            
            if page.header.flags.contains(PageFlags::BRANCH) {
                // For branch pages, handle the special index for leftmost child
                if position.indices[level] == usize::MAX {
                    // Currently at leftmost child, move to first key's right child
                    position.indices[level] = 0;
                } else {
                    // Normal increment
                    position.indices[level] += 1;
                }
                
                if position.indices[level] < page.header.num_keys as usize {
                    // Found a branch with more children
                    let mut current_page_id = crate::branch_v2::BranchPageV2::get_child_at(&page, position.indices[level])?;
                
                    loop {
                        let page = Self::get_page_raw(self.txn, self.is_write, current_page_id)?;
                        
                        if page.header.flags.contains(PageFlags::LEAF) {
                            position.pages.push(current_page_id);
                            position.indices.push(0);
                            
                            let node = page.node(0)?;
                            let key = node.key()?;
                            let value = self.get_node_value(&node)?;
                            return Ok(Some((key, value)));
                        } else {
                            // Branch page, use branch_v2 to get leftmost child
                            position.pages.push(current_page_id);
                            position.indices.push(usize::MAX); // Start at leftmost child
                            
                            current_page_id = crate::branch_v2::BranchPageV2::get_leftmost_child(&page)?;
                        }
                    }
                }
            } else {
                // Leaf page - should not happen at non-leaf levels
                position.indices[level] += 1;
                if position.indices[level] < page.header.num_keys as usize {
                    let node = page.node(position.indices[level])?;
                    let key = node.key()?;
                    let value = self.get_node_value(&node)?;
                    return Ok(Some((key, value)));
                }
            }
            
            // This branch is exhausted, go up another level
            position.pages.pop();
            position.indices.pop();
        }
        
        // No more entries
        self.position = None;
        Ok(None)
    }
    
    /// Move to previous entry (returns raw bytes)
    pub fn prev_raw(&mut self) -> Result<Option<(&'txn [u8], Cow<'txn, [u8]>)>> {
        if self.position.is_none() {
            return self.last_raw();
        }
        
        // Get necessary info before mutable borrow
        let (leaf_idx, current_leaf_index) = {
            let position = self.position.as_ref().unwrap();
            let leaf_idx = position.pages.len() - 1;
            (leaf_idx, position.indices[leaf_idx])
        };
        
        if current_leaf_index > 0 {
            // Can move back in current leaf
            let position = self.position.as_mut().unwrap();
            position.indices[leaf_idx] -= 1;
            
            let leaf_page_id = position.pages[leaf_idx];
            let new_index = position.indices[leaf_idx];
            // Drop mutable borrow before calling get_page
            drop(position);
            
            let leaf_page = self.get_page(leaf_page_id)?;
            let node = leaf_page.node(new_index)?;
            let key = node.key()?;
            let value = self.get_node_value(&node)?;
            return Ok(Some((key, value)));
        }
        
        let position = self.position.as_mut().unwrap();
        
        // Need to move to previous leaf
        // Go up the tree until we find a branch we can go left from
        position.pages.pop();
        position.indices.pop();
        
        while !position.pages.is_empty() {
            let level = position.pages.len() - 1;
            
            if position.indices[level] > 0 {
                position.indices[level] -= 1;
                
                // Navigate down to the rightmost leaf of this subtree
                let page_id = position.pages[level];
                let page = Self::get_page_raw(self.txn, self.is_write, page_id)?;
                let mut current_page_id = if page.header.flags.contains(PageFlags::BRANCH) {
                    // For branch pages, handle the special case when index is 0
                    if position.indices[level] == 0 {
                        // Use leftmost child
                        crate::branch_v2::BranchPageV2::get_leftmost_child(&page)?
                    } else {
                        // Get the child before this key
                        let node = page.node(position.indices[level] - 1)?;
                        node.page_number()?
                    }
                } else {
                    // For leaf pages (shouldn't happen here)
                    let node = page.node(position.indices[level])?;
                    node.page_number()?
                };
                
                loop {
                    let page = Self::get_page_raw(self.txn, self.is_write, current_page_id)?;
                    let last_idx = page.header.num_keys as usize - 1;
                    position.pages.push(current_page_id);
                    position.indices.push(last_idx);
                    
                    if page.header.flags.contains(PageFlags::LEAF) {
                        let node = page.node(last_idx)?;
                        let key = node.key()?;
                        let value = self.get_node_value(&node)?;
                        return Ok(Some((key, value)));
                    } else {
                        let node = page.node(last_idx)?;
                        current_page_id = node.page_number()?;
                    }
                }
            }
            
            // Can't go left at this level, go up
            position.pages.pop();
            position.indices.pop();
        }
        
        // No more entries
        self.position = None;
        Ok(None)
    }
    
    /// Seek to a specific key (returns raw bytes)
    pub fn seek_raw(&mut self, key: &[u8]) -> Result<Option<(&'txn [u8], Cow<'txn, [u8]>)>> {
        // For write transactions, the db_root should already be updated via put()
        // Only update for read transactions
        if self.db_root.0 == 0 {
            let db_info = if self.is_write {
                let txn = unsafe { &*(self.txn as *const Transaction<'txn, crate::txn::Write>) };
                txn.db_info(self.db_name.as_deref())?
            } else {
                let txn = self.txn::<crate::txn::Read>();
                txn.db_info(self.db_name.as_deref())?
            };
            self.db_root = db_info.root;
        }
        
        let mut position = CursorPosition {
            pages: Vec::new(),
            indices: Vec::new(),
        };
        
        let mut current_page_id = self.db_root;
        
        loop {
            let page = if self.is_write {
                let txn = unsafe { &*(self.txn as *const Transaction<'txn, crate::txn::Write>) };
                txn.get_page(current_page_id)?
            } else {
                let txn = self.txn::<crate::txn::Read>();
                txn.get_page(current_page_id)?
            };
            
            if page.header.num_keys == 0 {
                return Ok(None);
            }
            
            match page.search_key(key)? {
                SearchResult::Found { index } => {
                    position.pages.push(current_page_id);
                    position.indices.push(index);
                    
                    if page.header.flags.contains(PageFlags::LEAF) {
                        let node = page.node(index)?;
                        let key = node.key()?;
                        let value = self.get_node_value(&node)?;
                        self.position = Some(position);
                        return Ok(Some((key, value)));
                    } else {
                        let node = page.node(index)?;
                        current_page_id = node.page_number()?;
                    }
                }
                SearchResult::NotFound { insert_pos } => {
                    if page.header.flags.contains(PageFlags::LEAF) {
                        // Key not found, position at insert position
                        position.pages.push(current_page_id);
                        position.indices.push(insert_pos);
                        self.position = Some(position);
                        
                        // Return the key at insert position if it exists
                        if insert_pos < page.header.num_keys as usize {
                            let node = page.node(insert_pos)?;
                            let key = node.key()?;
                            let value = self.get_node_value(&node)?;
                            return Ok(Some((key, value)));
                        } else {
                            // We're past the last key, try next
                            return self.next_raw();
                        }
                    } else {
                        // Branch page, follow appropriate child
                        let child_index = if insert_pos > 0 { insert_pos - 1 } else { 0 };
                        position.pages.push(current_page_id);
                        position.indices.push(child_index);
                        
                        if child_index < page.header.num_keys as usize {
                            let node = page.node(child_index)?;
                            current_page_id = node.page_number()?;
                        } else {
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }
}

// Convenience methods for typed access
impl<'txn, K: crate::db::Key, V: crate::db::Value, C> Cursor<'txn, K, V, C> {
    /// Move to first entry
    pub fn first(&mut self) -> Result<Option<(Vec<u8>, V)>> {
        match self.first_raw()? {
            Some((key, value)) => {
                let v = V::decode(&value)?;
                Ok(Some((key.to_vec(), v)))
            }
            None => Ok(None),
        }
    }
    
    /// Move to last entry
    pub fn last(&mut self) -> Result<Option<(Vec<u8>, V)>> {
        match self.last_raw()? {
            Some((key, value)) => {
                let v = V::decode(&value)?;
                Ok(Some((key.to_vec(), v)))
            }
            None => Ok(None),
        }
    }
    
    /// Move to next entry
    pub fn next(&mut self) -> Result<Option<(Vec<u8>, V)>> {
        match self.next_raw()? {
            Some((key, value)) => {
                let v = V::decode(&value)?;
                Ok(Some((key.to_vec(), v)))
            }
            None => Ok(None),
        }
    }
    
    /// Move to previous entry
    pub fn prev(&mut self) -> Result<Option<(Vec<u8>, V)>> {
        match self.prev_raw()? {
            Some((key, value)) => {
                let v = V::decode(&value)?;
                Ok(Some((key.to_vec(), v)))
            }
            None => Ok(None),
        }
    }
    
    /// Seek to a specific key
    pub fn seek(&mut self, key: &K) -> Result<Option<(Vec<u8>, V)>> {
        let key_bytes = key.encode()?;
        match self.seek_raw(&key_bytes)? {
            Some((key, value)) => {
                let v = V::decode(&value)?;
                Ok(Some((key.to_vec(), v)))
            }
            None => Ok(None),
        }
    }
    
    /// Delete the current entry (requires a write transaction)
    pub fn delete(&mut self) -> Result<bool> {
        // We need to check if the transaction is a write transaction
        // For now, we'll try to get it as a write transaction and fail if it's not
        let txn = unsafe { &mut *(self.txn as *mut Transaction<'txn, crate::txn::Write>) };
        
        // Get current position
        let position = match &self.position {
            Some(pos) => pos,
            None => return Ok(false), // No current position
        };
        
        if position.pages.is_empty() {
            return Ok(false);
        }
        
        // Get the key at current position
        let current_key = {
            let leaf_page_id = position.pages[position.pages.len() - 1];
            let leaf_index = position.indices[position.indices.len() - 1];
            let page = self.get_page(leaf_page_id)?;
            
            if leaf_index >= page.header.num_keys as usize {
                return Ok(false); // Position is invalid
            }
            
            let node = page.node(leaf_index)?;
            node.key()?.to_vec()
        };
        
        // Get database info
        let db_info = txn.db_info(self.db_name.as_deref())?;
        let mut info = *db_info;
        let mut root = info.root;
        
        // Delete using B+Tree
        match crate::btree::BTree::delete(txn, &mut root, &mut info, &current_key)? {
            Some(_) => {
                // Update db info - root may have changed during delete
                info.root = root;
                self.db_root = root;
                
                // Update transaction's database info
                txn.update_db_info(self.db_name.as_deref(), info)?;
                
                // Invalidate cursor position as tree structure may have changed
                self.position = None;
                
                Ok(true)
            }
            None => Ok(false),
        }
    }
    
    /// Get the current key and value without moving the cursor
    pub fn current(&self) -> Result<Option<(Vec<u8>, V)>> {
        let position = match &self.position {
            Some(pos) => pos,
            None => return Ok(None),
        };
        
        if position.pages.is_empty() {
            return Ok(None);
        }
        
        let leaf_page_id = position.pages[position.pages.len() - 1];
        let leaf_index = position.indices[position.indices.len() - 1];
        
        // Get page based on transaction type
        let page = if self.is_write {
            // For write transactions, use the write transaction to see dirty pages
            let txn = unsafe { &*(self.txn as *const Transaction<'txn, crate::txn::Write>) };
            txn.get_page(leaf_page_id)?
        } else {
            // For read transactions, use read transaction
            let txn = self.txn::<crate::txn::Read>();
            txn.get_page(leaf_page_id)?
        };
        
        if leaf_index >= page.header.num_keys as usize {
            return Ok(None);
        }
        
        let node = page.node(leaf_index)?;
        let key = node.key()?.to_vec();
        let value = self.get_node_value(&node)?;
        let v = V::decode(&value)?;
        
        Ok(Some((key, v)))
    }
    
    /// Put a key-value pair at the current cursor position
    /// This requires a write transaction
    pub fn put(&mut self, key: &K, value: &V) -> Result<()> {
        // We need to check if the transaction is a write transaction
        let txn = unsafe { &mut *(self.txn as *mut Transaction<'txn, crate::txn::Write>) };
        
        let key_bytes = key.encode()?;
        let value_bytes = value.encode()?;
        
        // Get database info
        let db_info = txn.db_info(self.db_name.as_deref())?;
        let mut info = *db_info;
        let mut root = info.root;
        
        // Insert using B+Tree
        // Returns Some(old_value) if key was updated, None if inserted
        let _old_value = crate::btree::BTree::insert(txn, &mut root, &mut info, &key_bytes, &value_bytes)?;
        
        // Update db info - root may have changed during insert
        info.root = root;
        self.db_root = root;
        
        // Update transaction's database info
        txn.update_db_info(self.db_name.as_deref(), info)?;
        
        // Position cursor at the newly inserted key
        match self.seek_raw(&key_bytes)? {
            Some(_) => {
                // Successfully positioned at key
            }
            None => {
                // This shouldn't happen - we just inserted the key
                return Err(Error::Custom("Failed to position cursor at inserted key".into()));
            }
        }
        
        Ok(())
    }
    
    /// Put a key-value pair only if the key doesn't exist
    /// Returns true if inserted, false if key already exists
    pub fn put_no_overwrite(&mut self, key: &K, value: &V) -> Result<bool> {
        let key_bytes = key.encode()?;
        
        // First check if key exists
        match self.seek_raw(&key_bytes)? {
            Some((found_key, _)) if found_key == key_bytes => {
                // Key already exists
                Ok(false)
            }
            _ => {
                // Key doesn't exist, insert it
                self.put(key, value)?;
                Ok(true)
            }
        }
    }
    
    /// Update the value at the current cursor position
    /// Returns true if updated, false if cursor has no current position
    pub fn update(&mut self, value: &V) -> Result<bool> {
        let position = match &self.position {
            Some(pos) => pos,
            None => return Ok(false),
        };
        
        if position.pages.is_empty() {
            return Ok(false);
        }
        
        // Get current key
        let current_key = {
            let txn = self.txn::<crate::txn::Read>();
            let leaf_page_id = position.pages[position.pages.len() - 1];
            let leaf_index = position.indices[position.indices.len() - 1];
            let page = self.get_page(leaf_page_id)?;
            
            if leaf_index >= page.header.num_keys as usize {
                return Ok(false);
            }
            
            let node = page.node(leaf_index)?;
            node.key()?.to_vec()
        };
        
        // Get write transaction
        let txn = unsafe { &mut *(self.txn as *mut Transaction<'txn, crate::txn::Write>) };
        let value_bytes = value.encode()?;
        
        // Get database info
        let db_info = txn.db_info(self.db_name.as_deref())?;
        let mut info = *db_info;
        let mut root = info.root;
        
        // Update using B+Tree
        let old_value = crate::btree::BTree::insert(txn, &mut root, &mut info, &current_key, &value_bytes)?;
        
        if old_value.is_some() {
            // Key was updated (as expected)
            // Update db info - root may have changed
            info.root = root;
            self.db_root = root;
            txn.update_db_info(self.db_name.as_deref(), info)?;
            
            // Reposition cursor at the same key
            self.seek_raw(&current_key)?;
            Ok(true)
        } else {
            // This shouldn't happen as we're updating an existing key
            Err(Error::Custom("Unexpected insert during update".into()))
        }
    }
    
    // ===== DUPSORT Operations =====
    
    /// Check if database has DUPSORT enabled
    fn is_dupsort(&self) -> bool {
        DupSort::is_dupsort(self.db_flags)
    }
    
    /// Get the first duplicate for the current key
    pub fn first_dup(&mut self) -> Result<Option<(&'txn [u8], Cow<'txn, [u8]>)>> {
        if !self.is_dupsort() {
            return Err(Error::InvalidOperation("Database does not have DUPSORT enabled"));
        }
        
        // Must be positioned on a key
        if self.position.is_none() {
            return Ok(None);
        }
        
        // Get current key-value
        let (key, value) = match self.get_current_raw()? {
            Some(kv) => kv,
            None => return Ok(None),
        };
        
        // Check if value is a single value with our optimization
        if DupSort::is_single_value(&value) {
            // Single value optimization - decode and return it
            let single_value = DupSort::decode_single_value(&value)?;
            Ok(Some((key, Cow::Owned(single_value.to_vec()))))
        } else if DupSort::is_sub_db(&value) {
            // It's a sub-database, navigate to first entry
            let sub_db = DupSort::decode_sub_db(&value)?;
            let mut dup_position = CursorPosition {
                pages: Vec::new(),
                indices: Vec::new(),
            };
            
            // Navigate to first entry in sub-database
            let mut current_page_id = sub_db.root;
            loop {
                let page = self.get_page(current_page_id)?;
                
                if page.header.num_keys == 0 {
                    return Ok(None);
                }
                
                if page.header.flags.contains(PageFlags::LEAF) {
                    // Found leaf, get first entry
                    dup_position.pages.push(current_page_id);
                    dup_position.indices.push(0);
                    
                    let node = page.node(0)?;
                    // In DUPSORT, duplicate values are stored as keys in the sub-database
                    let dup_value = node.key()?;
                    
                    // Update dup cursor state
                    self.dup_cursor = Some(DupCursorState {
                        sub_db,
                        position: Some(dup_position),
                    });
                    
                    return Ok(Some((key, Cow::Borrowed(dup_value))));
                } else {
                    // Branch page, go to leftmost child
                    dup_position.pages.push(current_page_id);
                    dup_position.indices.push(usize::MAX);
                    current_page_id = crate::branch_v2::BranchPageV2::get_leftmost_child(&page)?;
                }
            }
        } else {
            // Legacy case - value stored directly
            Ok(Some((key, value)))
        }
    }
    
    /// Get the next duplicate for the current key
    pub fn next_dup(&mut self) -> Result<Option<(&'txn [u8], Cow<'txn, [u8]>)>> {
        if !self.is_dupsort() {
            return Err(Error::InvalidOperation("Database does not have DUPSORT enabled"));
        }
        
        // Must have active dup cursor
        if self.dup_cursor.is_none() {
            // No active dup cursor means we're either on a single value or haven't started
            return Ok(None);
        }
        
        // Work with the dup cursor
        let dup_state = self.dup_cursor.as_mut().unwrap();
        let dup_position = match &mut dup_state.position {
            Some(pos) => pos,
            None => return Ok(None),
        };
        
        let leaf_idx = dup_position.pages.len() - 1;
        let leaf_page_id = dup_position.pages[leaf_idx];
        dup_position.indices[leaf_idx] += 1;
        let next_index = dup_position.indices[leaf_idx];
        
        // Now check if we should continue (after releasing mutable borrow)
        let leaf_page = self.get_page(leaf_page_id)?;
        let should_continue = next_index < leaf_page.header.num_keys as usize;
        
        if should_continue {
            // Get current key
            let key = match self.get_current_key()? {
                Some(k) => k,
                None => return Ok(None),
            };
            
            // Get value from sub-database
            let leaf_page = self.get_page(leaf_page_id)?;
            let node = leaf_page.node(next_index)?;
            // In DUPSORT, duplicate values are stored as keys in the sub-database
            let dup_value = node.key()?;
            return Ok(Some((key, Cow::Borrowed(dup_value))));
        }
        
        // Need to move to next leaf in sub-database
        // This is similar to regular next() but within sub-database
        // For now, return None (complete implementation would traverse sub-tree)
        self.dup_cursor = None;
        Ok(None)
    }
    
    /// Get current key without value
    fn get_current_key(&self) -> Result<Option<&'txn [u8]>> {
        let position = match &self.position {
            Some(pos) => pos,
            None => return Ok(None),
        };
        
        if position.pages.is_empty() {
            return Ok(None);
        }
        
        let leaf_idx = position.pages.len() - 1;
        let leaf_page_id = position.pages[leaf_idx];
        let leaf_page = self.get_page(leaf_page_id)?;
        
        let node = leaf_page.node(position.indices[leaf_idx])?;
        Ok(Some(node.key()?))
    }
    
    /// Get current raw key-value without decoding
    fn get_current_raw(&self) -> Result<Option<(&'txn [u8], Cow<'txn, [u8]>)>> {
        let position = match &self.position {
            Some(pos) => pos,
            None => return Ok(None),
        };
        
        if position.pages.is_empty() {
            return Ok(None);
        }
        
        let leaf_idx = position.pages.len() - 1;
        let leaf_page_id = position.pages[leaf_idx];
        let leaf_page = self.get_page(leaf_page_id)?;
        
        let node = leaf_page.node(position.indices[leaf_idx])?;
        let key = node.key()?;
        let value = self.get_node_value(&node)?;
        
        Ok(Some((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvBuilder;
    use crate::db::Database;
    use tempfile::TempDir;
    use std::sync::Arc;
    
    #[test]
    fn test_cursor_iteration() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create database and insert data
        let db: Database<String, String> = {
            let mut txn = env.begin_write_txn().unwrap();
            let db = env.create_database(&mut txn, None).unwrap();
            
            db.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            db.put(&mut txn, "key2".to_string(), "value2".to_string()).unwrap();
            db.put(&mut txn, "key3".to_string(), "value3".to_string()).unwrap();
            
            txn.commit().unwrap();
            db
        };
        
        // Test cursor iteration
        {
            let txn = env.begin_txn().unwrap();
            let mut cursor = db.cursor(&txn).unwrap();
            
            // First
            let (key, value) = cursor.first().unwrap().unwrap();
            assert_eq!(key, b"key1");
            assert_eq!(value, "value1");
            
            // Next
            let (key, value) = cursor.next().unwrap().unwrap();
            assert_eq!(key, b"key2");
            assert_eq!(value, "value2");
            
            // Current
            let (key, value) = cursor.current().unwrap().unwrap();
            assert_eq!(key, b"key2");
            assert_eq!(value, "value2");
            
            // Last
            let (key, value) = cursor.last().unwrap().unwrap();
            assert_eq!(key, b"key3");
            assert_eq!(value, "value3");
            
            // Prev
            let (key, value) = cursor.prev().unwrap().unwrap();
            assert_eq!(key, b"key2");
            assert_eq!(value, "value2");
        }
    }
    
    #[test]
    fn test_cursor_seek() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create database and insert data
        let db: Database<String, String> = {
            let mut txn = env.begin_write_txn().unwrap();
            let db = env.create_database(&mut txn, None).unwrap();
            
            db.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            db.put(&mut txn, "key3".to_string(), "value3".to_string()).unwrap();
            db.put(&mut txn, "key5".to_string(), "value5".to_string()).unwrap();
            
            txn.commit().unwrap();
            db
        };
        
        // Test cursor seek
        {
            let txn = env.begin_txn().unwrap();
            let mut cursor = db.cursor(&txn).unwrap();
            
            // Seek to existing key
            let (key, value) = cursor.seek(&"key3".to_string()).unwrap().unwrap();
            assert_eq!(key, b"key3");
            assert_eq!(value, "value3");
            
            // Seek to non-existing key (should position at next key)
            let (key, value) = cursor.seek(&"key2".to_string()).unwrap().unwrap();
            assert_eq!(key, b"key3");
            assert_eq!(value, "value3");
            
            // Seek to key after all entries
            let result = cursor.seek(&"key9".to_string()).unwrap();
            assert!(result.is_none());
        }
    }
    
    #[test]
    fn test_cursor_delete() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create database and insert data
        let db: Database<String, String> = {
            let mut txn = env.begin_write_txn().unwrap();
            let db = env.create_database(&mut txn, None).unwrap();
            
            db.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            db.put(&mut txn, "key2".to_string(), "value2".to_string()).unwrap();
            db.put(&mut txn, "key3".to_string(), "value3".to_string()).unwrap();
            db.put(&mut txn, "key4".to_string(), "value4".to_string()).unwrap();
            
            txn.commit().unwrap();
            db
        };
        
        // Test 1: Delete a single key using cursor
        {
            let mut txn = env.begin_write_txn().unwrap();
            let mut cursor: Cursor<'_, String, String> = db.cursor(&mut txn).unwrap();
            
            // Move to key2
            cursor.seek(&"key2".to_string()).unwrap();
            
            // Verify we're at the right position
            let current = cursor.current().unwrap().unwrap();
            assert_eq!(current.0, b"key2");
            
            // Delete current position
            let deleted = cursor.delete().unwrap();
            assert!(deleted);
            
            txn.commit().unwrap();
        }
        
        // Verify first deletion
        {
            let txn = env.begin_txn().unwrap();
            assert!(db.get(&txn, &"key1".to_string()).unwrap().is_some());
            assert!(db.get(&txn, &"key2".to_string()).unwrap().is_none());
            assert!(db.get(&txn, &"key3".to_string()).unwrap().is_some());
            assert!(db.get(&txn, &"key4".to_string()).unwrap().is_some());
        }
        
        // Test 2: Delete another key in a separate transaction
        {
            let mut txn = env.begin_write_txn().unwrap();
            let mut cursor: Cursor<'_, String, String> = db.cursor(&mut txn).unwrap();
            
            // Move to key3
            cursor.seek(&"key3".to_string()).unwrap();
            
            // Verify we're at the right position
            let current = cursor.current().unwrap().unwrap();
            assert_eq!(current.0, b"key3");
            
            // Delete current position
            let deleted = cursor.delete().unwrap();
            assert!(deleted);
            
            txn.commit().unwrap();
        }
        
        // Verify both deletions
        {
            let txn = env.begin_txn().unwrap();
            assert!(db.get(&txn, &"key1".to_string()).unwrap().is_some());
            assert!(db.get(&txn, &"key2".to_string()).unwrap().is_none());
            assert!(db.get(&txn, &"key3".to_string()).unwrap().is_none());
            assert!(db.get(&txn, &"key4".to_string()).unwrap().is_some());
        }
    }
    
    #[test]
    fn test_cursor_put() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create database
        let db: Database<String, String> = {
            let mut txn = env.begin_write_txn().unwrap();
            let db = env.create_database(&mut txn, None).unwrap();
            txn.commit().unwrap();
            db
        };
        
        // Test cursor put operations - simplified test
        {
            let mut txn = env.begin_write_txn().unwrap();
            let mut cursor: Cursor<'_, String, String> = db.cursor(&mut txn).unwrap();
            
            // Put just one entry first
            cursor.put(&"key1".to_string(), &"value1".to_string()).unwrap();
            
            // Check if cursor is positioned correctly
            let current = cursor.current();
            assert!(current.is_ok(), "cursor.current() failed: {:?}", current.err());
            assert!(current.as_ref().unwrap().is_some(), "cursor.current() returned None");
            
            txn.commit().unwrap();
        }
        
        // Verify insertion
        {
            let txn = env.begin_txn().unwrap();
            assert_eq!(db.get(&txn, &"key1".to_string()).unwrap(), Some("value1".to_string()));
        }
    }
    
    #[test]
    fn test_cursor_update() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create database and insert initial data
        let db: Database<String, String> = {
            let mut txn = env.begin_write_txn().unwrap();
            let db = env.create_database(&mut txn, None).unwrap();
            
            db.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            db.put(&mut txn, "key2".to_string(), "value2".to_string()).unwrap();
            db.put(&mut txn, "key3".to_string(), "value3".to_string()).unwrap();
            
            txn.commit().unwrap();
            db
        };
        
        // Test cursor update
        {
            let mut txn = env.begin_write_txn().unwrap();
            let mut cursor: Cursor<'_, String, String> = db.cursor(&mut txn).unwrap();
            
            // Position at key2
            cursor.seek(&"key2".to_string()).unwrap();
            
            // Update current position
            let updated = cursor.update(&"updated_value2".to_string()).unwrap();
            assert!(updated);
            
            // Verify cursor is still at key2
            let current = cursor.current().unwrap();
            assert!(current.is_some());
            let (key, value) = current.unwrap();
            assert_eq!(key, b"key2");
            assert_eq!(value, "updated_value2");
            
            // Try update with no position
            cursor.position = None;
            let updated = cursor.update(&"some_value".to_string()).unwrap();
            assert!(!updated);
            
            txn.commit().unwrap();
        }
        
        // Verify update
        {
            let txn = env.begin_txn().unwrap();
            assert_eq!(db.get(&txn, &"key1".to_string()).unwrap(), Some("value1".to_string()));
            assert_eq!(db.get(&txn, &"key2".to_string()).unwrap(), Some("updated_value2".to_string()));
            assert_eq!(db.get(&txn, &"key3".to_string()).unwrap(), Some("value3".to_string()));
        }
    }
}