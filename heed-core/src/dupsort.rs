//! Duplicate key support (DUPSORT) implementation
//! 
//! This module provides support for storing multiple values per key in a sorted manner.
//! When DUPSORT is enabled, each key can have multiple values stored as a sub-database.

use crate::error::{Error, Result, PageId};
use crate::page::PageFlags;
use crate::txn::{Transaction, Write};
use crate::meta::DbInfo;
use crate::btree::BTree;

/// Duplicate sort node - stores multiple values for a single key
#[derive(Debug)]
pub struct DupNode {
    /// The key
    pub key: Vec<u8>,
    /// Database info for the sub-database containing values
    pub sub_db: DbInfo,
}

/// Duplicate sort handler
pub struct DupSort;

impl DupSort {
    /// Check if a database has duplicate sort enabled
    pub fn is_dupsort(flags: u32) -> bool {
        flags & crate::db::DatabaseFlags::DUP_SORT.bits() != 0
    }
    
    /// Check if a value is a sub-database reference
    pub fn is_sub_db(value: &[u8]) -> bool {
        value.len() == std::mem::size_of::<DbInfo>()
    }
    
    /// Insert a duplicate value
    pub fn insert(
        txn: &mut Transaction<'_, Write>,
        db_info: &mut DbInfo,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool> {
        // First, search for the key in the main database
        let search_result = BTree::search(txn as &Transaction<'_, Write>, db_info.root, key)?;
        match search_result {
            Some(existing_value) => {
                let existing_value = existing_value.into_owned();
                // Key exists - check if it's a sub-database or a single value
                if existing_value.len() == std::mem::size_of::<DbInfo>() {
                    // It's already a sub-database, add to it
                    let mut sub_db = Self::decode_sub_db(&existing_value)?;
                    let mut sub_root = sub_db.root;
                    BTree::insert(txn, &mut sub_root, &mut sub_db, value, &[])?;
                    sub_db.root = sub_root;
                    
                    // Update the sub-database info
                    let encoded = Self::encode_sub_db(&sub_db);
                    BTree::update_value(txn, db_info.root, key, &encoded)?;
                    Ok(false) // Key already existed
                } else {
                    // Convert single value to sub-database
                    let (sub_root, _) = txn.alloc_page(PageFlags::LEAF)?;
                    let mut sub_db = DbInfo {
                        flags: crate::db::DatabaseFlags::DUP_SORT.bits(),
                        depth: 0,
                        branch_pages: 0,
                        leaf_pages: 1,
                        overflow_pages: 0,
                        entries: 0,
                        root: sub_root,
                    };
                    
                    // Insert both the existing value and new value
                    let mut sub_root = sub_db.root;
                    BTree::insert(txn, &mut sub_root, &mut sub_db, &existing_value, &[])?;
                    BTree::insert(txn, &mut sub_root, &mut sub_db, value, &[])?;
                    sub_db.root = sub_root;
                    
                    // Replace the single value with sub-database info
                    let encoded = Self::encode_sub_db(&sub_db);
                    BTree::update_value(txn, db_info.root, key, &encoded)?;
                    Ok(false) // Key already existed
                }
            }
            None => {
                // Key doesn't exist - create new sub-database
                let (sub_root, _) = txn.alloc_page(PageFlags::LEAF)?;
                let mut sub_db = DbInfo {
                    flags: crate::db::DatabaseFlags::DUP_SORT.bits(),
                    depth: 0,
                    branch_pages: 0,
                    leaf_pages: 1,
                    overflow_pages: 0,
                    entries: 0,
                    root: sub_root,
                };
                
                // Insert the value into sub-database
                let mut sub_root = sub_db.root;
                BTree::insert(txn, &mut sub_root, &mut sub_db, value, &[])?;
                sub_db.root = sub_root;
                
                // Insert sub-database info into main database
                let encoded = Self::encode_sub_db(&sub_db);
                let mut root = db_info.root;
                BTree::insert(txn, &mut root, db_info, key, &encoded)?;
                db_info.root = root;
                Ok(true) // New key
            }
        }
    }
    
    /// Get all values for a key
    pub fn get_all<'txn>(
        txn: &Transaction<'txn, impl crate::txn::mode::Mode>,
        root: PageId,
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        match BTree::search(txn, root, key)? {
            Some(value) => {
                if value.len() == std::mem::size_of::<DbInfo>() {
                    // It's a sub-database, iterate through all values
                    let sub_db = Self::decode_sub_db(&value)?;
                    let mut values = Vec::new();
                    
                    // Use cursor to iterate through sub-database
                    let mut stack = vec![sub_db.root];
                    while let Some(page_id) = stack.pop() {
                        let page = txn.get_page(page_id)?;
                        
                        if page.header.flags.contains(PageFlags::LEAF) {
                            for i in 0..page.header.num_keys as usize {
                                let node = page.node(i)?;
                                // In DUPSORT sub-databases, values are stored as keys
                                // The actual value is the key of the sub-database node
                                values.push(node.key()?.to_vec());
                            }
                        } else {
                            // Branch page - add children in reverse order for DFS
                            for i in (0..page.header.num_keys as usize).rev() {
                                let node = page.node(i)?;
                                stack.push(node.page_number()?);
                            }
                        }
                    }
                    
                    Ok(values)
                } else {
                    // Single value
                    Ok(vec![value.into_owned()])
                }
            }
            None => Ok(Vec::new()),
        }
    }
    
    /// Delete a specific value for a key
    pub fn delete(
        txn: &mut Transaction<'_, Write>,
        db_info: &mut DbInfo,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool> {
        match BTree::search(txn, db_info.root, key)? {
            Some(existing_value) => {
                if existing_value.len() == std::mem::size_of::<DbInfo>() {
                    // It's a sub-database
                    let mut sub_db = Self::decode_sub_db(&existing_value)?;
                    
                    // Delete from sub-database
                    let mut sub_root = sub_db.root;
                    match BTree::delete(txn, &mut sub_root, &mut sub_db, value)? {
                        Some(_) => {
                            sub_db.root = sub_root;
                            if sub_db.entries == 0 {
                                // Sub-database is empty, remove the key entirely
                                let mut root = db_info.root;
                                BTree::delete(txn, &mut root, db_info, key)?;
                                db_info.root = root;
                            } else {
                                // Update sub-database info
                                let encoded = Self::encode_sub_db(&sub_db);
                                BTree::update_value(txn, db_info.root, key, &encoded)?;
                            }
                            Ok(true)
                        }
                        None => Ok(false),
                    }
                } else if existing_value.as_ref() == value {
                    // Single value matches, delete it
                    let mut root = db_info.root;
                    BTree::delete(txn, &mut root, db_info, key)?;
                    db_info.root = root;
                    Ok(true)
                } else {
                    // Single value doesn't match
                    Ok(false)
                }
            }
            None => Ok(false),
        }
    }
    
    /// Delete all values for a key
    pub fn delete_all(
        txn: &mut Transaction<'_, Write>,
        db_info: &mut DbInfo,
        key: &[u8],
    ) -> Result<bool> {
        let mut root = db_info.root;
        match BTree::delete(txn, &mut root, db_info, key)? {
            Some(value) => {
                db_info.root = root;
                if value.len() == std::mem::size_of::<DbInfo>() {
                    // It was a sub-database, free all its pages
                    let sub_db = Self::decode_sub_db(&value)?;
                    // TODO: Properly free all pages in the sub-database
                    txn.free_page(sub_db.root)?;
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }
    
    /// Count values for a key
    pub fn count_values<'txn>(
        txn: &Transaction<'txn, impl crate::txn::mode::Mode>,
        root: PageId,
        key: &[u8],
    ) -> Result<usize> {
        match BTree::search(txn, root, key)? {
            Some(value) => {
                if value.len() == std::mem::size_of::<DbInfo>() {
                    let sub_db = Self::decode_sub_db(&value)?;
                    Ok(sub_db.entries as usize)
                } else {
                    Ok(1)
                }
            }
            None => Ok(0),
        }
    }
    
    /// Encode sub-database info
    fn encode_sub_db(db_info: &DbInfo) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<DbInfo>());
        unsafe {
            let ptr = db_info as *const _ as *const u8;
            bytes.extend_from_slice(std::slice::from_raw_parts(ptr, std::mem::size_of::<DbInfo>()));
        }
        bytes
    }
    
    /// Decode sub-database info
    pub fn decode_sub_db(bytes: &[u8]) -> Result<DbInfo> {
        if bytes.len() != std::mem::size_of::<DbInfo>() {
            return Err(Error::Corruption {
                details: "Invalid sub-database info size".into(),
                page_id: None,
            });
        }
        
        let mut db_info = DbInfo::default();
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut db_info as *mut _ as *mut u8,
                std::mem::size_of::<DbInfo>()
            );
        }
        Ok(db_info)
    }
}

/// Cursor for iterating over duplicate values
pub struct DupCursor<'txn, M: crate::txn::mode::Mode> {
    txn: &'txn Transaction<'txn, M>,
    sub_db: Option<DbInfo>,
    current_page: Option<PageId>,
    current_index: usize,
}

impl<'txn, M: crate::txn::mode::Mode> DupCursor<'txn, M> {
    /// Create a new duplicate cursor
    pub fn new(
        txn: &'txn Transaction<'txn, M>,
        root: PageId,
        key: &[u8],
    ) -> Result<Self> {
        match BTree::search(txn, root, key)? {
            Some(value) => {
                if value.len() == std::mem::size_of::<DbInfo>() {
                    let sub_db = DupSort::decode_sub_db(&value)?;
                    Ok(Self {
                        txn,
                        sub_db: Some(sub_db),
                        current_page: Some(sub_db.root),
                        current_index: 0,
                    })
                } else {
                    // Single value - no sub-database
                    Ok(Self {
                        txn,
                        sub_db: None,
                        current_page: None,
                        current_index: 0,
                    })
                }
            }
            None => Ok(Self {
                txn,
                sub_db: None,
                current_page: None,
                current_index: 0,
            }),
        }
    }
    
    /// Move to first duplicate
    pub fn first(&mut self) -> Result<Option<Vec<u8>>> {
        if let Some(sub_db) = &self.sub_db {
            self.current_page = Some(sub_db.root);
            self.current_index = 0;
            self.next()
        } else {
            Ok(None)
        }
    }
    
    /// Move to next duplicate
    pub fn next(&mut self) -> Result<Option<Vec<u8>>> {
        if let Some(page_id) = self.current_page {
            let page = self.txn.get_page(page_id)?;
            
            if self.current_index < page.header.num_keys as usize {
                let node = page.node(self.current_index)?;
                self.current_index += 1;
                Ok(Some(node.key()?.to_vec()))
            } else {
                // TODO: Move to next page in B+Tree
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvBuilder;
    use crate::db::{Database, DatabaseFlags};
    use tempfile::TempDir;
    use std::sync::Arc;
    
    #[test]
    fn test_dupsort_basic() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create database with DUPSORT
        let mut txn = env.begin_write_txn().unwrap();
        let mut db_info = DbInfo {
            flags: DatabaseFlags::DUP_SORT.bits(),
            depth: 0,
            branch_pages: 0,
            leaf_pages: 1,
            overflow_pages: 0,
            entries: 0,
            root: PageId(3), // Assuming main db root
        };
        
        // Insert multiple values for same key
        assert!(DupSort::insert(&mut txn, &mut db_info, b"key1", b"value1").unwrap());
        assert!(!DupSort::insert(&mut txn, &mut db_info, b"key1", b"value2").unwrap());
        assert!(!DupSort::insert(&mut txn, &mut db_info, b"key1", b"value3").unwrap());
        
        txn.commit().unwrap();
        
        // Read all values
        let txn = env.begin_txn().unwrap();
        let values = DupSort::get_all(&txn, db_info.root, b"key1").unwrap();
        assert_eq!(values.len(), 3);
        assert!(values.contains(&b"value1".to_vec()));
        assert!(values.contains(&b"value2".to_vec()));
        assert!(values.contains(&b"value3".to_vec()));
        
        // Count values
        let count = DupSort::count_values(&txn, db_info.root, b"key1").unwrap();
        assert_eq!(count, 3);
    }
    
    #[test]
    fn test_dupsort_delete() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let mut txn = env.begin_write_txn().unwrap();
        let mut db_info = DbInfo {
            flags: DatabaseFlags::DUP_SORT.bits(),
            depth: 0,
            branch_pages: 0,
            leaf_pages: 1,
            overflow_pages: 0,
            entries: 0,
            root: PageId(3),
        };
        
        // Insert values
        DupSort::insert(&mut txn, &mut db_info, b"key1", b"value1").unwrap();
        DupSort::insert(&mut txn, &mut db_info, b"key1", b"value2").unwrap();
        DupSort::insert(&mut txn, &mut db_info, b"key1", b"value3").unwrap();
        
        // Delete specific value
        assert!(DupSort::delete(&mut txn, &mut db_info, b"key1", b"value2").unwrap());
        
        // Check remaining values
        let values = DupSort::get_all(&txn, db_info.root, b"key1").unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&b"value1".to_vec()));
        assert!(!values.contains(&b"value2".to_vec()));
        assert!(values.contains(&b"value3".to_vec()));
        
        txn.commit().unwrap();
    }
}