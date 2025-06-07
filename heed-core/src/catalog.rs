//! Database catalog management
//!
//! The catalog stores information about all named databases in the main database.
//! Database names are stored as keys, and DbInfo structures as values.

use crate::error::{Error, Result, PageId};
use crate::meta::DbInfo;
use crate::btree::BTree;
use crate::txn::{Transaction, Write};
use crate::comparator::LexicographicComparator;

/// Database catalog stored in the main database
pub struct Catalog;

impl Catalog {
    /// Store a database in the catalog
    pub fn put_database(
        txn: &mut Transaction<'_, Write>,
        name: &str,
        info: &DbInfo,
    ) -> Result<()> {
        // Get the main database info and clone it
        let main_db = *txn.db_info(None)?;
        
        // Serialize the database name and info
        let key = name.as_bytes();
        let value = Self::serialize_db_info(info);
        
        // Insert into the main database B+Tree
        let mut root = main_db.root;
        let mut updated_info = main_db;
        BTree::<LexicographicComparator>::insert(txn, &mut root, &mut updated_info, key, &value)?;
        
        // Update the main database info if changed
        if root != main_db.root || updated_info.entries != main_db.entries {
            updated_info.root = root;
            txn.update_db_info(None, updated_info)?;
        }
        
        Ok(())
    }
    
    /// Get a database from the catalog
    pub fn get_database<M: crate::txn::mode::Mode>(
        txn: &Transaction<'_, M>,
        name: &str,
    ) -> Result<Option<DbInfo>> {
        // Get the main database info
        let main_db = txn.db_info(None)?;
        
        if main_db.root == PageId(0) {
            // Empty catalog
            return Ok(None);
        }
        
        // Search in the main database B+Tree
        let key = name.as_bytes();
        match BTree::<LexicographicComparator>::search(txn, main_db.root, key)? {
            Some(value) => Ok(Some(Self::deserialize_db_info(&value)?)),
            None => Ok(None),
        }
    }
    
    /// List all databases in the catalog
    pub fn list_databases<M: crate::txn::mode::Mode>(
        txn: &Transaction<'_, M>,
    ) -> Result<Vec<(String, DbInfo)>> {
        let main_db = txn.db_info(None)?;
        
        if main_db.root == PageId(0) {
            return Ok(Vec::new());
        }
        
        let mut databases = Vec::new();
        
        // Iterate through the main database
        let mut stack = vec![(main_db.root, 0)];
        
        while let Some((page_id, index)) = stack.pop() {
            let page = txn.get_page(page_id)?;
            
            if page.header.flags.contains(crate::page::PageFlags::LEAF) {
                // Process all entries in this leaf page
                for i in index..page.header.num_keys as usize {
                    let node = page.node(i)?;
                    let key = node.key()?;
                    let value = node.value()?;
                    
                    // Decode database name and info
                    if let Ok(name) = String::from_utf8(key.to_vec()) {
                        if let Ok(info) = Self::deserialize_db_info(&value) {
                            databases.push((name, info));
                        }
                    }
                }
            } else {
                // Branch page - add children to stack in reverse order for in-order traversal
                for i in (index..page.header.num_keys as usize).rev() {
                    let node = page.node(i)?;
                    let child_id = node.page_number()?;
                    stack.push((child_id, 0));
                }
            }
        }
        
        Ok(databases)
    }
    
    /// Remove a database from the catalog
    pub fn remove_database(
        txn: &mut Transaction<'_, Write>,
        name: &str,
    ) -> Result<bool> {
        // Get the main database info and clone it
        let main_db = *txn.db_info(None)?;
        
        if main_db.root == PageId(0) {
            return Ok(false);
        }
        
        // Delete from the main database B+Tree
        let key = name.as_bytes();
        let mut root = main_db.root;
        let mut updated_info = main_db;
        let result = BTree::<LexicographicComparator>::delete(txn, &mut root, &mut updated_info, key)?;
        let deleted = result.is_some();
        
        // Update the main database info if changed
        if root != main_db.root || deleted {
            updated_info.root = root;
            txn.update_db_info(None, updated_info)?;
        }
        
        Ok(deleted)
    }
    
    /// Serialize a DbInfo structure
    pub fn serialize_db_info(info: &DbInfo) -> Vec<u8> {
        let mut buf = Vec::with_capacity(std::mem::size_of::<DbInfo>());
        
        // Serialize fields in order
        buf.extend_from_slice(&info.flags.to_le_bytes());
        buf.extend_from_slice(&info.depth.to_le_bytes());
        buf.extend_from_slice(&info.branch_pages.to_le_bytes());
        buf.extend_from_slice(&info.leaf_pages.to_le_bytes());
        buf.extend_from_slice(&info.overflow_pages.to_le_bytes());
        buf.extend_from_slice(&info.entries.to_le_bytes());
        buf.extend_from_slice(&info.root.0.to_le_bytes());
        
        buf
    }
    
    /// Deserialize a DbInfo structure
    pub fn deserialize_db_info(data: &[u8]) -> Result<DbInfo> {
        if data.len() < 48 {  // 4 + 4 + 8 + 8 + 8 + 8 + 8
            return Err(Error::Decoding("Invalid DbInfo data".into()));
        }
        
        let mut offset = 0;
        
        let flags = u32::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
        ]);
        offset += 4;
        
        let depth = u32::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
        ]);
        offset += 4;
        
        let branch_pages = u64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
        ]);
        offset += 8;
        
        let leaf_pages = u64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
        ]);
        offset += 8;
        
        let overflow_pages = u64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
        ]);
        offset += 8;
        
        let entries = u64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
        ]);
        offset += 8;
        
        let root = PageId(u64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
        ]));
        
        Ok(DbInfo {
            flags,
            depth,
            branch_pages,
            leaf_pages,
            overflow_pages,
            entries,
            root,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_serialize_deserialize_db_info() {
        let info = DbInfo {
            flags: 0x42,
            depth: 3,
            branch_pages: 100,
            leaf_pages: 500,
            overflow_pages: 10,
            entries: 1000,
            root: PageId(42),
        };
        
        let serialized = Catalog::serialize_db_info(&info);
        let deserialized = Catalog::deserialize_db_info(&serialized).unwrap();
        
        assert_eq!(info.flags, deserialized.flags);
        assert_eq!(info.depth, deserialized.depth);
        assert_eq!(info.branch_pages, deserialized.branch_pages);
        assert_eq!(info.leaf_pages, deserialized.leaf_pages);
        assert_eq!(info.overflow_pages, deserialized.overflow_pages);
        assert_eq!(info.entries, deserialized.entries);
        assert_eq!(info.root, deserialized.root);
    }
}