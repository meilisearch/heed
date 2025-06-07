//! Database operations and management

use bitflags::bitflags;
use std::marker::PhantomData;
use std::sync::Arc;
use crate::error::{Error, Result};
use crate::txn::{Transaction, mode::Mode};
use crate::env::{Environment, state};
use crate::btree::BTree;
use crate::meta::DbInfo;
use crate::comparator::{Comparator, LexicographicComparator};

bitflags! {
    /// Database flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DatabaseFlags: u32 {
        /// Use reverse string comparison
        const REVERSE_KEY = 0x02;
        /// Use sorted duplicates
        const DUP_SORT = 0x04;
        /// Numeric keys in native byte order
        const INTEGER_KEY = 0x08;
        /// With DUP_SORT, sorted dup items have fixed size
        const DUP_FIXED = 0x10;
        /// With DUP_SORT, dups are numeric in native byte order
        const INTEGER_DUP = 0x20;
        /// With DUP_SORT, use reverse string dups
        const REVERSE_DUP = 0x40;
        /// Create DB if not already existing
        const CREATE = 0x40000;
    }
}

/// Key encoding trait
pub trait Key: Send + Sync {
    /// Encode the key to bytes
    fn encode(&self) -> Result<Vec<u8>>;
}

/// Value encoding trait
pub trait Value: Send + Sync {
    /// Encode the value to bytes
    fn encode(&self) -> Result<Vec<u8>>;
    /// Decode the value from bytes
    fn decode(bytes: &[u8]) -> Result<Self> where Self: Sized;
}

// Implement Key for common types
impl Key for &[u8] {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.to_vec())
    }
}

impl Key for Vec<u8> {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.clone())
    }
}

impl Key for &str {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }
}

impl Key for String {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }
}

// Implement Value for common types
impl Value for Vec<u8> {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.clone())
    }
    
    fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
    }
}

impl Value for &[u8] {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.to_vec())
    }
    
    fn decode(_bytes: &[u8]) -> Result<Self> {
        Err(Error::Custom("Cannot decode into borrowed slice".into()))
    }
}

impl Value for String {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }
    
    fn decode(bytes: &[u8]) -> Result<Self> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| Error::Decoding(format!("Invalid UTF-8: {}", e).into()))
    }
}

/// Database handle
pub struct Database<K = Vec<u8>, V = Vec<u8>, C = LexicographicComparator> {
    env_inner: Arc<crate::env::EnvInner>,
    name: Option<String>,
    info: DbInfo,
    _phantom: PhantomData<(K, V, C)>,
}

impl<K: Key, V: Value, C: Comparator> Database<K, V, C> {
    /// Create a new database handle
    pub(crate) fn new(
        env_inner: Arc<crate::env::EnvInner>,
        name: Option<String>,
        info: DbInfo,
    ) -> Self {
        Self {
            env_inner,
            name,
            info,
            _phantom: PhantomData,
        }
    }
    
    /// Open a database in the environment
    pub fn open(env: &Environment<state::Open>, name: Option<&str>, flags: DatabaseFlags) -> Result<Self> {
        let inner = env.inner();
        
        // Check if it's the main database (no name)
        if name.is_none() {
            let info = inner.databases.read().unwrap()
                .get(&None)
                .copied()
                .ok_or(Error::InvalidDatabase)?;
                
            return Ok(Self {
                env_inner: inner.clone(),
                name: None,
                info,
                _phantom: PhantomData,
            });
        }
        
        // For named databases, we need to check the catalog
        // First check the cache
        if let Some(info) = inner.databases.read().unwrap().get(&name.map(|s| s.to_string())) {
            return Ok(Self {
                env_inner: inner.clone(),
                name: name.map(|s| s.to_string()),
                info: *info,
                _phantom: PhantomData,
            });
        }
        
        // Not in cache, need to look in the catalog or create
        if flags.contains(DatabaseFlags::CREATE) {
            // Create a new database
            let mut txn = env.begin_write_txn()?;
            
            // Check catalog first
            if let Some(info) = crate::catalog::Catalog::get_database(&txn, name.unwrap())? {
                // Already exists in catalog
                txn.abort();
                
                // Cache it
                inner.databases.write().unwrap().insert(name.map(|s| s.to_string()), info);
                
                Ok(Self {
                    env_inner: inner.clone(),
                    name: name.map(|s| s.to_string()),
                    info,
                    _phantom: PhantomData,
                })
            } else {
                // Create new database
                let (root_id, root_page) = txn.alloc_page(crate::page::PageFlags::LEAF)?;
                root_page.header.num_keys = 0;
                
                let info = DbInfo {
                    flags: flags.bits(),
                    depth: 1,
                    branch_pages: 0,
                    leaf_pages: 1,
                    overflow_pages: 0,
                    entries: 0,
                    root: root_id,
                };
                
                // Store in catalog
                crate::catalog::Catalog::put_database(&mut txn, name.unwrap(), &info)?;
                
                // Update transaction's database list
                txn.update_db_info(name, info)?;
                
                // Commit the transaction
                txn.commit()?;
                
                // Cache it
                inner.databases.write().unwrap().insert(name.map(|s| s.to_string()), info);
                
                Ok(Self {
                    env_inner: inner.clone(),
                    name: name.map(|s| s.to_string()),
                    info,
                    _phantom: PhantomData,
                })
            }
        } else {
            // Must exist - check catalog
            let txn = env.begin_txn()?;
            
            if let Some(info) = crate::catalog::Catalog::get_database(&txn, name.unwrap())? {
                // Cache it
                inner.databases.write().unwrap().insert(name.map(|s| s.to_string()), info);
                
                Ok(Self {
                    env_inner: inner.clone(),
                    name: name.map(|s| s.to_string()),
                    info,
                    _phantom: PhantomData,
                })
            } else {
                Err(Error::InvalidDatabase)
            }
        }
    }
    
    /// Get a value from the database
    pub fn get<'txn, M: Mode>(&self, txn: &Transaction<'txn, M>, key: &K) -> Result<Option<V>> {
        let key_bytes = key.encode()?;
        
        // Try to get the latest database info from the transaction first
        // This is important for consistency within a transaction
        let db_info = match txn.db_info(self.name.as_deref()) {
            Ok(info) => info,
            Err(_) => {
                // Not in transaction cache, use our cached info
                // Note: For read transactions from a reopened environment,
                // the entries count may be stale, but the root page is correct
                &self.info
            }
        };
        
        // Don't check entries count for read transactions as it may be stale
        // Just check if we have a valid root page
        if db_info.root == crate::error::PageId(0) {
            return Ok(None);
        }
        
        match BTree::<C>::search(txn, db_info.root, &key_bytes)? {
            Some(value_bytes) => {
                let value = V::decode(&value_bytes)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
    
    /// Put a value into the database
    pub fn put(&self, txn: &mut Transaction<'_, crate::txn::Write>, key: K, value: V) -> Result<()> {
        let key_bytes = key.encode()?;
        let value_bytes = value.encode()?;
        
        // Get mutable db info
        let db_info = txn.db_info(self.name.as_deref())?;
        let mut info = *db_info;
        let mut root = info.root;
        
        // Check if DUPSORT is enabled
        if crate::dupsort::DupSort::is_dupsort(info.flags) {
            // Use DUPSORT insert
            crate::dupsort::DupSort::insert(txn, &mut info, &key_bytes, &value_bytes)?;
            root = info.root;
        } else {
            // Regular insert
            BTree::<C>::insert(txn, &mut root, &mut info, &key_bytes, &value_bytes)?;
        }
        
        // Update db info if root changed
        if root != info.root {
            info.root = root;
        }
        
        // Update transaction's database info
        txn.update_db_info(self.name.as_deref(), info)?;
        
        Ok(())
    }
    
    /// Delete a value from the database
    pub fn delete(&self, txn: &mut Transaction<'_, crate::txn::Write>, key: &K) -> Result<bool> {
        let key_bytes = key.encode()?;
        
        // Get mutable db info
        let db_info = txn.db_info(self.name.as_deref())?;
        let mut info = *db_info;
        let mut root = info.root;
        
        let result = BTree::<C>::delete(txn, &mut root, &mut info, &key_bytes)?;
        if result.is_some() {
            // Update db info if needed
            if root != info.root {
                info.root = root;
            }
            txn.update_db_info(self.name.as_deref(), info)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    /// Get the database name
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
    
    /// Clear all entries from the database
    pub fn clear(&self, txn: &mut Transaction<'_, crate::txn::Write>) -> Result<()> {
        // Allocate a new empty root page
        let (new_root_id, new_root) = txn.alloc_page(crate::page::PageFlags::LEAF)?;
        
        // Initialize the new root page as an empty leaf
        new_root.header.num_keys = 0;
        
        // Update database info
        let db_info = txn.db_info(self.name.as_deref())?;
        let mut info = *db_info;
        
        // Free all pages in the old tree
        if info.root.0 != 0 && info.root != new_root_id {
            crate::tree_utils::free_tree(txn, info.root)?;
        }
        
        info.root = new_root_id;
        info.entries = 0;
        info.depth = 0;
        info.leaf_pages = 1;
        info.branch_pages = 0;
        info.overflow_pages = 0;
        
        txn.update_db_info(self.name.as_deref(), info)?;
        
        Ok(())
    }
    
    /// Create a cursor for iterating over the database
    pub fn cursor<'txn, M: crate::txn::mode::Mode>(&'txn self, txn: &'txn Transaction<'txn, M>) -> Result<crate::cursor::Cursor<'txn, K, V, C>> {
        crate::cursor::Cursor::new(txn, self)
    }
    
    /// Get the number of entries in the database
    pub fn len<M: Mode>(&self, txn: &Transaction<'_, M>) -> Result<u64> {
        let db_info = txn.db_info(self.name.as_deref())?;
        Ok(db_info.entries)
    }
    
    /// Check if the database is empty
    pub fn is_empty<M: Mode>(&self, txn: &Transaction<'_, M>) -> Result<bool> {
        Ok(self.len(txn)? == 0)
    }
    
    /// Put a value into the database with duplicate support
    /// If DUPSORT is enabled, this allows multiple values per key
    pub fn put_dup(&self, txn: &mut Transaction<'_, crate::txn::Write>, key: K, value: V) -> Result<()> {
        let key_bytes = key.encode()?;
        let value_bytes = value.encode()?;
        
        // Get mutable db info
        let db_info = txn.db_info(self.name.as_deref())?;
        let mut info = *db_info;
        
        if crate::dupsort::DupSort::is_dupsort(info.flags) {
            // Use dupsort insertion
            crate::dupsort::DupSort::insert(txn, &mut info, &key_bytes, &value_bytes)?;
        } else {
            // Regular insertion
            let mut root = info.root;
            BTree::<C>::insert(txn, &mut root, &mut info, &key_bytes, &value_bytes)?;
            if root != info.root {
                info.root = root;
            }
        }
        
        // Update transaction's database info
        txn.update_db_info(self.name.as_deref(), info)?;
        
        Ok(())
    }
    
    /// Get all values for a key (for DUPSORT databases)
    pub fn get_all<'txn, M: Mode>(&self, txn: &Transaction<'txn, M>, key: &K) -> Result<Vec<V>> {
        let key_bytes = key.encode()?;
        
        // Get current database info from transaction
        let db_info = txn.db_info(self.name.as_deref())?;
        
        if crate::dupsort::DupSort::is_dupsort(db_info.flags) {
            let values = crate::dupsort::DupSort::get_all(txn, db_info.root, &key_bytes)?;
            let mut decoded_values = Vec::with_capacity(values.len());
            for value_bytes in values {
                decoded_values.push(V::decode(&value_bytes)?);
            }
            Ok(decoded_values)
        } else {
            // Regular get - return single value as array
            match BTree::<C>::search(txn, db_info.root, &key_bytes)? {
                Some(value_bytes) => {
                    let value = V::decode(&value_bytes)?;
                    Ok(vec![value])
                }
                None => Ok(Vec::new()),
            }
        }
    }
    
    /// Delete a specific value for a key (for DUPSORT databases)
    pub fn delete_dup(&self, txn: &mut Transaction<'_, crate::txn::Write>, key: &K, value: &V) -> Result<bool> {
        let key_bytes = key.encode()?;
        let value_bytes = value.encode()?;
        
        // Get mutable db info
        let db_info = txn.db_info(self.name.as_deref())?;
        let mut info = *db_info;
        
        let deleted = if crate::dupsort::DupSort::is_dupsort(info.flags) {
            crate::dupsort::DupSort::delete(txn, &mut info, &key_bytes, &value_bytes)?
        } else {
            // For non-DUPSORT, delete only if value matches
            match BTree::<C>::search(txn, info.root, &key_bytes)? {
                Some(existing_value) => {
                    if existing_value == value_bytes {
                        let mut root = info.root;
                        BTree::<C>::delete(txn, &mut root, &mut info, &key_bytes)?;
                        if root != info.root {
                            info.root = root;
                        }
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        };
        
        // Update transaction's database info
        txn.update_db_info(self.name.as_deref(), info)?;
        
        Ok(deleted)
    }
}

impl<K, V, C> Clone for Database<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            env_inner: self.env_inner.clone(),
            name: self.name.clone(),
            info: self.info,
            _phantom: PhantomData,
        }
    }
}

// Environment extension to create databases
impl Environment<state::Open> {
    /// Open or create a database
    pub fn create_database<K: Key, V: Value>(
        self: &Arc<Self>,
        txn: &mut Transaction<'_, crate::txn::Write>,
        name: Option<&str>,
    ) -> Result<Database<K, V>> {
        self.create_database_with_flags(txn, name, DatabaseFlags::empty())
    }
    
    /// Open or create a database with specific flags
    pub fn create_database_with_flags<K: Key, V: Value>(
        self: &Arc<Self>,
        txn: &mut Transaction<'_, crate::txn::Write>,
        name: Option<&str>,
        flags: DatabaseFlags,
    ) -> Result<Database<K, V>> {
        // Main database (unnamed) is special - it already exists
        if name.is_none() {
            let info = txn.db_info(None)?;
            return Ok(Database::new(self.inner().clone(), None, *info));
        }
        
        // For named databases, check if it already exists in the main database
        let db_name = name.unwrap();
        
        // Check if database already exists in transaction cache
        if let Ok(info) = txn.db_info(Some(db_name)) {
            return Ok(Database::new(self.inner().clone(), Some(db_name.to_string()), *info));
        }
        
        // Check if database exists in the main database catalog
        let main_db_info = *txn.db_info(None)?;
        match BTree::<LexicographicComparator>::search(txn, main_db_info.root, db_name.as_bytes())? {
            Some(value) => {
                // Database exists, decode the DbInfo using Catalog deserialization
                if let Ok(info) = crate::catalog::Catalog::deserialize_db_info(&value) {
                    // Cache in transaction
                    txn.update_db_info(Some(db_name), info)?;
                    
                    return Ok(Database::new(self.inner().clone(), Some(db_name.to_string()), info));
                }
            }
            None => {
                // Database doesn't exist, create it
                let (root_id, _) = txn.alloc_page(crate::page::PageFlags::LEAF)?;
                
                let info = DbInfo {
                    flags: flags.bits(),
                    depth: 0,
                    branch_pages: 0,
                    leaf_pages: 1,
                    overflow_pages: 0,
                    entries: 0,
                    root: root_id,
                };
                
                // Store in the main database catalog
                let mut main_db_info = main_db_info;
                let mut main_root = main_db_info.root;
                
                // Encode DbInfo using Catalog serialization
                let info_bytes = crate::catalog::Catalog::serialize_db_info(&info);
                
                // Insert into main database
                BTree::<LexicographicComparator>::insert(txn, &mut main_root, &mut main_db_info, db_name.as_bytes(), &info_bytes)?;
                
                // Update main database info
                main_db_info.root = main_root;
                txn.update_db_info(None, main_db_info)?;
                
                // Cache in transaction
                txn.update_db_info(Some(db_name), info)?;
                
                return Ok(Database::new(self.inner().clone(), Some(db_name.to_string()), info));
            }
        }
        
        Err(Error::InvalidDatabase)
    }
    
    /// Open an existing database
    pub fn open_database<K: Key, V: Value>(
        self: &Arc<Self>,
        txn: &Transaction<'_, impl Mode>,
        name: Option<&str>,
    ) -> Result<Database<K, V>> {
        // Try transaction cache first
        if let Ok(info) = txn.db_info(name) {
            return Ok(Database::new(self.inner().clone(), name.map(String::from), *info));
        }
        
        // Check environment cache
        if let Some(info) = self.inner().databases.read().unwrap().get(&name.map(String::from)) {
            return Ok(Database::new(self.inner().clone(), name.map(String::from), *info));
        }
        
        // For named databases, look in the main database
        if let Some(db_name) = name {
            let main_db_info = txn.db_info(None)?;
            match BTree::<LexicographicComparator>::search(txn, main_db_info.root, db_name.as_bytes())? {
                Some(value) => {
                    // Try to deserialize using Catalog format
                    if let Ok(info) = crate::catalog::Catalog::deserialize_db_info(&value) {
                        // Cache in the environment
                        self.inner().databases.write().unwrap().insert(Some(db_name.to_string()), info);
                        return Ok(Database::new(self.inner().clone(), Some(db_name.to_string()), info));
                    }
                }
                None => {
                }
            }
        }
        
        Err(Error::InvalidDatabase)
    }
    
    /// List all named databases
    pub fn list_databases(
        self: &Arc<Self>,
        txn: &Transaction<'_, impl Mode>,
    ) -> Result<Vec<String>> {
        let main_db_info = txn.db_info(None)?;
        let mut databases = Vec::new();
        
        // Create a cursor to iterate through the main database
        let db: Database<Vec<u8>, Vec<u8>> = Database::new(
            self.inner().clone(),
            None,
            *main_db_info
        );
        let mut cursor = db.cursor(txn)?;
        
        // Iterate through all entries
        while let Some((key, _value)) = cursor.next()? {
            if let Ok(name) = String::from_utf8(key) {
                databases.push(name);
            }
        }
        
        Ok(databases)
    }
    
    /// Drop a database
    pub fn drop_database(
        self: &Arc<Self>,
        txn: &mut Transaction<'_, crate::txn::Write>,
        name: &str,
    ) -> Result<()> {
        // Cannot drop the main database
        if name.is_empty() {
            return Err(Error::InvalidOperation("Cannot drop the main database"));
        }
        
        // Check if database exists
        let (db_info_to_drop, main_db_info_root) = {
            let main_db_info = txn.db_info(None)?;
            match BTree::<LexicographicComparator>::search(txn, main_db_info.root, name.as_bytes())? {
                Some(value) => {
                    if value.len() == std::mem::size_of::<DbInfo>() {
                        let mut db_info = DbInfo::default();
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                value.as_ptr(),
                                &mut db_info as *mut _ as *mut u8,
                                std::mem::size_of::<DbInfo>()
                            );
                        }
                        Some((db_info, main_db_info.root))
                    } else {
                        None
                    }
                }
                None => None,
            }
        }.ok_or(Error::InvalidDatabase)?;
        
        // TODO: Free all pages used by this database
        // For now, just mark the root page as free
        txn.free_page(db_info_to_drop.root)?;
        
        // Remove from main database
        let main_db_info = *txn.db_info(None)?;
        let mut main_db_info_mut = main_db_info;
        let mut main_root = main_db_info_root;
        
        BTree::<LexicographicComparator>::delete(txn, &mut main_root, &mut main_db_info_mut, name.as_bytes())?;
        
        // Update main database info
        main_db_info_mut.root = main_root;
        txn.update_db_info(None, main_db_info_mut)?;
        
        // Remove from transaction cache
        txn.data.databases.remove(&Some(name.to_string()));
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvBuilder;
    use tempfile::TempDir;
    
    #[test]
    fn test_database_operations() {
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
        
        // Insert data
        {
            let mut txn = env.begin_write_txn().unwrap();
            db.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            db.put(&mut txn, "key2".to_string(), "value2".to_string()).unwrap();
            txn.commit().unwrap();
        }
        
        // Read data
        {
            let txn = env.begin_txn().unwrap();
            
            let val1 = db.get(&txn, &"key1".to_string()).unwrap();
            assert_eq!(val1, Some("value1".to_string()));
            
            let val2 = db.get(&txn, &"key2".to_string()).unwrap();
            assert_eq!(val2, Some("value2".to_string()));
            
            let val3 = db.get(&txn, &"key3".to_string()).unwrap();
            assert_eq!(val3, None);
        }
    }
    
    #[test]
    fn test_database_delete() {
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
        
        // Insert data
        {
            let mut txn = env.begin_write_txn().unwrap();
            db.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            db.put(&mut txn, "key2".to_string(), "value2".to_string()).unwrap();
            db.put(&mut txn, "key3".to_string(), "value3".to_string()).unwrap();
            txn.commit().unwrap();
        }
        
        // Delete data
        {
            let mut txn = env.begin_write_txn().unwrap();
            
            // Delete existing key
            let deleted = db.delete(&mut txn, &"key2".to_string()).unwrap();
            assert!(deleted);
            
            // Try to delete non-existent key
            let deleted = db.delete(&mut txn, &"key4".to_string()).unwrap();
            assert!(!deleted);
            
            txn.commit().unwrap();
        }
        
        // Verify deletion
        {
            let txn = env.begin_txn().unwrap();
            
            let val1 = db.get(&txn, &"key1".to_string()).unwrap();
            assert_eq!(val1, Some("value1".to_string()));
            
            let val2 = db.get(&txn, &"key2".to_string()).unwrap();
            assert_eq!(val2, None);
            
            let val3 = db.get(&txn, &"key3".to_string()).unwrap();
            assert_eq!(val3, Some("value3".to_string()));
        }
    }
    
    #[test]
    fn test_database_clear() {
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
        
        // Insert data
        {
            let mut txn = env.begin_write_txn().unwrap();
            db.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            db.put(&mut txn, "key2".to_string(), "value2".to_string()).unwrap();
            db.put(&mut txn, "key3".to_string(), "value3".to_string()).unwrap();
            txn.commit().unwrap();
        }
        
        // Verify data exists
        {
            let txn = env.begin_txn().unwrap();
            assert_eq!(db.len(&txn).unwrap(), 3);
            assert!(!db.is_empty(&txn).unwrap());
        }
        
        // Clear the database
        {
            let mut txn = env.begin_write_txn().unwrap();
            db.clear(&mut txn).unwrap();
            txn.commit().unwrap();
        }
        
        // Verify database is empty
        {
            let txn = env.begin_txn().unwrap();
            assert_eq!(db.len(&txn).unwrap(), 0);
            assert!(db.is_empty(&txn).unwrap());
            
            // All keys should be gone
            assert_eq!(db.get(&txn, &"key1".to_string()).unwrap(), None);
            assert_eq!(db.get(&txn, &"key2".to_string()).unwrap(), None);
            assert_eq!(db.get(&txn, &"key3".to_string()).unwrap(), None);
        }
        
        // Can insert new data after clear
        {
            let mut txn = env.begin_write_txn().unwrap();
            db.put(&mut txn, "new_key".to_string(), "new_value".to_string()).unwrap();
            txn.commit().unwrap();
        }
        
        {
            let txn = env.begin_txn().unwrap();
            assert_eq!(db.get(&txn, &"new_key".to_string()).unwrap(), Some("new_value".to_string()));
            assert_eq!(db.len(&txn).unwrap(), 1);
        }
    }
    
    #[test]
    fn test_named_databases() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create multiple named databases
        let (db1, db2, db3) = {
            let mut txn = env.begin_write_txn().unwrap();
            let db1: Database<String, String> = env.create_database(&mut txn, Some("users")).unwrap();
            let db2: Database<String, String> = env.create_database(&mut txn, Some("products")).unwrap();
            let db3: Database<String, String> = env.create_database(&mut txn, Some("orders")).unwrap();
            txn.commit().unwrap();
            (db1, db2, db3)
        };
        
        // Verify database names
        assert_eq!(db1.name(), Some("users"));
        assert_eq!(db2.name(), Some("products"));
        assert_eq!(db3.name(), Some("orders"));
        
        // Insert data into each database
        {
            let mut txn = env.begin_write_txn().unwrap();
            db1.put(&mut txn, "user1".to_string(), "Alice".to_string()).unwrap();
            db2.put(&mut txn, "prod1".to_string(), "Widget".to_string()).unwrap();
            db3.put(&mut txn, "order1".to_string(), "Pending".to_string()).unwrap();
            txn.commit().unwrap();
        }
        
        // Verify data isolation between databases
        {
            let txn = env.begin_txn().unwrap();
            
            // Each database has its own data
            assert_eq!(db1.get(&txn, &"user1".to_string()).unwrap(), Some("Alice".to_string()));
            assert_eq!(db1.get(&txn, &"prod1".to_string()).unwrap(), None);
            assert_eq!(db1.get(&txn, &"order1".to_string()).unwrap(), None);
            
            assert_eq!(db2.get(&txn, &"prod1".to_string()).unwrap(), Some("Widget".to_string()));
            assert_eq!(db2.get(&txn, &"user1".to_string()).unwrap(), None);
            assert_eq!(db2.get(&txn, &"order1".to_string()).unwrap(), None);
            
            assert_eq!(db3.get(&txn, &"order1".to_string()).unwrap(), Some("Pending".to_string()));
            assert_eq!(db3.get(&txn, &"user1".to_string()).unwrap(), None);
            assert_eq!(db3.get(&txn, &"prod1".to_string()).unwrap(), None);
        }
        
        // Test reopening named database
        {
            let txn = env.begin_txn().unwrap();
            let db1_reopened: Database<String, String> = env.open_database(&txn, Some("users")).unwrap();
            assert_eq!(db1_reopened.get(&txn, &"user1".to_string()).unwrap(), Some("Alice".to_string()));
        }
        
        // Test listing databases
        {
            let txn = env.begin_txn().unwrap();
            let mut dbs = env.list_databases(&txn).unwrap();
            dbs.sort();
            assert_eq!(dbs, vec!["orders", "products", "users"]);
        }
    }
    
    #[test]
    fn test_database_drop() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create named databases
        {
            let mut txn = env.begin_write_txn().unwrap();
            let db1: Database<String, String> = env.create_database(&mut txn, Some("temp_db")).unwrap();
            let db2: Database<String, String> = env.create_database(&mut txn, Some("keep_db")).unwrap();
            
            // Add data
            db1.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            db2.put(&mut txn, "key2".to_string(), "value2".to_string()).unwrap();
            
            txn.commit().unwrap();
        }
        
        // Verify both databases exist
        {
            let txn = env.begin_txn().unwrap();
            let dbs = env.list_databases(&txn).unwrap();
            assert!(dbs.contains(&"temp_db".to_string()));
            assert!(dbs.contains(&"keep_db".to_string()));
        }
        
        // Drop one database
        {
            let mut txn = env.begin_write_txn().unwrap();
            env.drop_database(&mut txn, "temp_db").unwrap();
            txn.commit().unwrap();
        }
        
        // Verify database is dropped
        {
            let txn = env.begin_txn().unwrap();
            let dbs = env.list_databases(&txn).unwrap();
            assert!(!dbs.contains(&"temp_db".to_string()));
            assert!(dbs.contains(&"keep_db".to_string()));
            
            // Try to open dropped database
            let result: Result<Database<String, String>> = env.open_database(&txn, Some("temp_db"));
            assert!(result.is_err());
            
            // Other database still works
            let db2: Database<String, String> = env.open_database(&txn, Some("keep_db")).unwrap();
            assert_eq!(db2.get(&txn, &"key2".to_string()).unwrap(), Some("value2".to_string()));
        }
        
        // Cannot drop main database
        {
            let mut txn = env.begin_write_txn().unwrap();
            let result = env.drop_database(&mut txn, "");
            assert!(result.is_err());
        }
    }
    
    #[test]
    fn test_database_persistence() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().to_path_buf();
        
        // Create environment and databases
        {
            let env = Arc::new(
                EnvBuilder::new()
                    .map_size(10 * 1024 * 1024)
                    .open(&db_path)
                    .unwrap()
            );
            
            let mut txn = env.begin_write_txn().unwrap();
            let db1: Database<String, String> = env.create_database(&mut txn, Some("persistent_db")).unwrap();
            db1.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            
            let db2: Database<String, String> = env.create_database(&mut txn, Some("another_db")).unwrap();
            db2.put(&mut txn, "key2".to_string(), "value2".to_string()).unwrap();
            
            txn.commit().unwrap();
        }
        
        // Reopen environment and verify databases persist
        {
            let env = Arc::new(
                EnvBuilder::new()
                    .map_size(10 * 1024 * 1024)
                    .open(&db_path)
                    .unwrap()
            );
            
            let txn = env.begin_txn().unwrap();
            
            // List databases
            let mut dbs = env.list_databases(&txn).unwrap();
            dbs.sort();
            assert_eq!(dbs, vec!["another_db", "persistent_db"]);
            
            // Open and verify data
            let db1: Database<String, String> = env.open_database(&txn, Some("persistent_db")).unwrap();
            assert_eq!(db1.get(&txn, &"key1".to_string()).unwrap(), Some("value1".to_string()));
            
            let db2: Database<String, String> = env.open_database(&txn, Some("another_db")).unwrap();
            assert_eq!(db2.get(&txn, &"key2".to_string()).unwrap(), Some("value2".to_string()));
        }
    }
    
    #[test]
    fn test_create_existing_database() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create a database
        let db1 = {
            let mut txn = env.begin_write_txn().unwrap();
            let db: Database<String, String> = env.create_database(&mut txn, Some("test_db")).unwrap();
            db.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            txn.commit().unwrap();
            db
        };
        
        // Try to create the same database again
        {
            let mut txn = env.begin_write_txn().unwrap();
            let db2: Database<String, String> = env.create_database(&mut txn, Some("test_db")).unwrap();
            
            // Should return the existing database
            let val = db2.get(&txn, &"key1".to_string()).unwrap();
            assert_eq!(val, Some("value1".to_string()));
            
            // Both handles should work
            assert_eq!(db1.name(), db2.name());
        }
    }
    
    #[test]
    fn test_database_dupsort() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create database with DUPSORT
        let db: Database<String, String> = {
            let mut txn = env.begin_write_txn().unwrap();
            let db = env.create_database_with_flags(&mut txn, Some("dupsort_db"), DatabaseFlags::DUP_SORT).unwrap();
            txn.commit().unwrap();
            db
        };
        
        // Insert multiple values for the same key
        {
            let mut txn = env.begin_write_txn().unwrap();
            db.put_dup(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
            db.put_dup(&mut txn, "key1".to_string(), "value2".to_string()).unwrap();
            db.put_dup(&mut txn, "key1".to_string(), "value3".to_string()).unwrap();
            
            // Different key
            db.put_dup(&mut txn, "key2".to_string(), "valueA".to_string()).unwrap();
            db.put_dup(&mut txn, "key2".to_string(), "valueB".to_string()).unwrap();
            
            txn.commit().unwrap();
        }
        
        // Read all values for a key
        {
            let txn = env.begin_txn().unwrap();
            
            let values = db.get_all(&txn, &"key1".to_string()).unwrap();
            assert_eq!(values.len(), 3);
            assert!(values.contains(&"value1".to_string()));
            assert!(values.contains(&"value2".to_string()));
            assert!(values.contains(&"value3".to_string()));
            
            let values2 = db.get_all(&txn, &"key2".to_string()).unwrap();
            assert_eq!(values2.len(), 2);
            assert!(values2.contains(&"valueA".to_string()));
            assert!(values2.contains(&"valueB".to_string()));
            
            // Non-existent key
            let values3 = db.get_all(&txn, &"key3".to_string()).unwrap();
            assert_eq!(values3.len(), 0);
        }
        
        // Delete specific value
        {
            let mut txn = env.begin_write_txn().unwrap();
            
            // Delete one value
            let deleted = db.delete_dup(&mut txn, &"key1".to_string(), &"value2".to_string()).unwrap();
            assert!(deleted);
            
            // Try to delete non-existent value
            let deleted = db.delete_dup(&mut txn, &"key1".to_string(), &"value4".to_string()).unwrap();
            assert!(!deleted);
            
            txn.commit().unwrap();
        }
        
        // Verify deletion
        {
            let txn = env.begin_txn().unwrap();
            
            let values = db.get_all(&txn, &"key1".to_string()).unwrap();
            assert_eq!(values.len(), 2);
            assert!(values.contains(&"value1".to_string()));
            assert!(!values.contains(&"value2".to_string()));
            assert!(values.contains(&"value3".to_string()));
        }
    }
}