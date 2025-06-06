//! Environment management with type-state pattern

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::Mutex;

use crate::error::{Error, Result, TransactionId, PageId};
use crate::meta::{MetaPage, DbInfo, META_PAGE_1, META_PAGE_2};
use crate::page::Page;
use crate::txn::{Transaction, Read, Write};
use crate::reader::ReaderTable;
use crate::io::{IoBackend, MmapBackend};

/// Environment state marker traits
pub mod state {
    /// Sealed trait for environment states
    mod sealed {
        pub trait Sealed {}
    }
    
    /// Environment state trait
    pub trait State: sealed::Sealed {}
    
    /// Closed environment state
    #[derive(Debug)]
    pub struct Closed;
    impl sealed::Sealed for Closed {}
    impl State for Closed {}
    
    /// Open environment state
    #[derive(Debug)]
    pub struct Open;
    impl sealed::Sealed for Open {}
    impl State for Open {}
    
    /// Read-only environment state
    #[derive(Debug)]
    pub struct ReadOnly;
    impl sealed::Sealed for ReadOnly {}
    impl State for ReadOnly {}
}

use state::*;

/// Maximum number of named databases
pub const MAX_DBS: u32 = 128;

/// Default map size (1GB)
pub const DEFAULT_MAP_SIZE: usize = 1 << 30;

/// Durability modes for write transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// No sync - fastest but no durability guarantees
    NoSync,
    /// Async sync - data is flushed asynchronously (default)
    AsyncFlush,
    /// Sync data pages only - metadata might be lost
    SyncData,
    /// Full sync - both data and metadata are synced (safest)
    FullSync,
}

/// Shared environment data
pub(crate) struct EnvInner {
    /// Path to the database
    path: PathBuf,
    /// I/O backend
    pub(crate) io: Box<dyn IoBackend>,
    /// Current map size
    map_size: usize,
    /// Current transaction ID
    pub(crate) txn_id: AtomicU64,
    /// Write lock
    pub(crate) write_lock: Mutex<()>,
    /// Reader table
    pub(crate) readers: ReaderTable,
    /// Named databases
    pub(crate) databases: RwLock<HashMap<Option<String>, DbInfo>>,
    /// Free pages tracking
    pub(crate) free_pages: RwLock<Vec<PageId>>,
    /// Durability mode
    pub(crate) durability: DurabilityMode,
    /// Checksum mode
    pub(crate) checksum_mode: crate::checksum::ChecksumMode,
    /// Next page ID to allocate
    pub(crate) next_page_id: AtomicU64,
}

// Safety: EnvInner is Send/Sync because IoBackend is Send/Sync
unsafe impl Send for EnvInner {}
unsafe impl Sync for EnvInner {}

impl EnvInner {
    /// Get the current meta page
    pub(crate) fn meta(&self) -> Result<MetaPage> {
        // Read both meta pages
        let meta0 = self.io.read_page(META_PAGE_1)?;
        let meta1 = self.io.read_page(META_PAGE_2)?;
        
        // Cast data area to MetaPage
        let meta0 = unsafe { &*(meta0.data.as_ptr() as *const MetaPage) };
        let meta1 = unsafe { &*(meta1.data.as_ptr() as *const MetaPage) };
        
        // Validate and return the most recent valid one
        let meta0_valid = meta0.validate().is_ok();
        let meta1_valid = meta1.validate().is_ok();
        
        match (meta0_valid, meta1_valid) {
            (true, true) => {
                // Both valid, use the one with higher transaction ID
                if meta0.last_txnid.0 >= meta1.last_txnid.0 {
                    Ok(*meta0)
                } else {
                    Ok(*meta1)
                }
            }
            (true, false) => Ok(*meta0),
            (false, true) => Ok(*meta1),
            (false, false) => Err(Error::Corrupted),
        }
    }
    
    /// Get the non-current meta page ID (for writing)
    pub(crate) fn next_meta_page_id(&self) -> Result<PageId> {
        let meta = self.meta()?;
        // If current is page 0, next is page 1
        if meta.last_txnid.0 % 2 == 0 {
            Ok(META_PAGE_2)
        } else {
            Ok(META_PAGE_1)
        }
    }
    
}

/// Database environment
pub struct Environment<S: State = Closed> {
    inner: Option<Arc<EnvInner>>,
    _state: PhantomData<S>,
}

/// Builder for creating environments
pub struct EnvBuilder {
    map_size: usize,
    max_readers: u32,
    max_dbs: u32,
    flags: u32,
    durability: DurabilityMode,
    checksum_mode: crate::checksum::ChecksumMode,
}

impl EnvBuilder {
    /// Create a new environment builder
    pub fn new() -> Self {
        Self {
            map_size: DEFAULT_MAP_SIZE,
            max_readers: 126,
            max_dbs: MAX_DBS,
            flags: 0,
            durability: DurabilityMode::AsyncFlush,
            checksum_mode: crate::checksum::ChecksumMode::None,
        }
    }
    
    /// Set the map size
    pub fn map_size(mut self, size: usize) -> Self {
        self.map_size = size;
        self
    }
    
    /// Set the maximum number of readers
    pub fn max_readers(mut self, readers: u32) -> Self {
        self.max_readers = readers;
        self
    }
    
    /// Set the maximum number of named databases
    pub fn max_dbs(mut self, dbs: u32) -> Self {
        self.max_dbs = dbs.min(MAX_DBS);
        self
    }
    
    /// Set the durability mode
    pub fn durability(mut self, mode: DurabilityMode) -> Self {
        self.durability = mode;
        self
    }
    
    /// Set the checksum mode
    pub fn checksum_mode(mut self, mode: crate::checksum::ChecksumMode) -> Self {
        self.checksum_mode = mode;
        self
    }
    
    /// Build and open the environment
    pub fn open(self, path: impl AsRef<Path>) -> Result<Environment<Open>> {
        let path = path.as_ref();
        
        // Create directory if it doesn't exist
        std::fs::create_dir_all(path)?;
        
        let data_path = path.join("data.mdb");
        let _lock_path = path.join("lock.mdb");
        
        // Create I/O backend
        let mut io: Box<dyn IoBackend> = Box::new(MmapBackend::with_options(&data_path, self.map_size as u64)?);
        
        // Check if this is a new database by trying to read meta pages
        let is_new_db = match io.read_page(META_PAGE_1) {
            Ok(page) => {
                let meta = unsafe { &*(page.data.as_ptr() as *const MetaPage) };
                meta.magic != crate::meta::MAGIC
            }
            Err(_) => true,
        };
        
        let mut last_txn_id = 0;
        let mut last_page_id = 3; // After two meta pages and two root pages
        let mut meta_info = MetaPage::new();
        
        if is_new_db {
            // Initialize new database
            let mut meta = MetaPage::new();
            meta.mapsize = self.map_size as u64;
            meta.maxreaders = self.max_readers;
            meta.dbs = self.max_dbs;
            meta.last_txnid = TransactionId(0);
            meta.free_db.root = PageId(2);
            meta.main_db.root = PageId(3);
            meta.last_pg = PageId(3);
            
            // Write meta page 0
            let meta_page0 = Page::from_meta(&meta, META_PAGE_1);
            io.write_page(&meta_page0)?;
            
            // Write meta page 1
            meta.last_txnid = TransactionId(1);
            let meta_page1 = Page::from_meta(&meta, META_PAGE_2);
            io.write_page(&meta_page1)?;
            
            // Initialize free DB root page (page 2)
            let free_page = Page::new(PageId(2), crate::page::PageFlags::LEAF);
            io.write_page(&free_page)?;
            
            // Initialize main DB root page (page 3)
            let main_page = Page::new(PageId(3), crate::page::PageFlags::LEAF);
            io.write_page(&main_page)?;
            
            // Sync to disk
            io.sync()?;
            
            last_txn_id = 1;
            meta_info = meta;
        } else {
            // Load existing meta info
            let inner = Arc::new(EnvInner {
                path: path.to_path_buf(),
                io,
                map_size: self.map_size,
                txn_id: AtomicU64::new(0),
                write_lock: Mutex::new(()),
                readers: ReaderTable::new(self.max_readers as usize),
                databases: RwLock::new(HashMap::new()),
                free_pages: RwLock::new(Vec::new()),
                durability: self.durability,
                checksum_mode: self.checksum_mode,
                next_page_id: AtomicU64::new(0),
            });
            
            meta_info = inner.meta()?;
            last_txn_id = meta_info.last_txnid.0;
            last_page_id = meta_info.last_pg.0;
            
            // Recreate with correct values
            drop(inner);
            io = Box::new(MmapBackend::with_options(&data_path, self.map_size as u64)?);
        }
        
        // Initialize reader table
        let readers = ReaderTable::new(self.max_readers as usize);
        
        let inner = Arc::new(EnvInner {
            path: path.to_path_buf(),
            io,
            map_size: self.map_size,
            txn_id: AtomicU64::new(last_txn_id),
            write_lock: Mutex::new(()),
            readers,
            databases: RwLock::new(HashMap::new()),
            free_pages: RwLock::new(Vec::new()),
            durability: self.durability,
            checksum_mode: self.checksum_mode,
            next_page_id: AtomicU64::new(last_page_id + 1),
        });
        
        // Initialize main database entry
        {
            let mut dbs = inner.databases.write().unwrap();
            dbs.insert(None, meta_info.main_db);
        }
        
        // Note: Named databases will be loaded on-demand from the catalog
        // We can't load them here because we'd need a transaction, but the 
        // environment isn't fully constructed yet
        
        Ok(Environment {
            inner: Some(inner),
            _state: PhantomData,
        })
    }
}

impl Default for EnvBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Environment<Closed> {
    /// Create a new closed environment
    pub fn new() -> Self {
        Self {
            inner: None,
            _state: PhantomData,
        }
    }
    
    /// Open the environment
    pub fn open(self, path: impl AsRef<Path>) -> Result<Environment<Open>> {
        EnvBuilder::new().open(path)
    }
}

impl Environment<Open> {
    /// Begin a read transaction
    pub fn begin_txn(&self) -> Result<Transaction<'_, Read>> {
        Transaction::new_read(self)
    }
    
    /// Begin a write transaction
    pub fn begin_write_txn(&self) -> Result<Transaction<'_, Write>> {
        Transaction::new_write(self)
    }
    
    /// Get inner reference (for internal use)
    pub(crate) fn inner(&self) -> &Arc<EnvInner> {
        self.inner.as_ref().unwrap()
    }
    
    /// Get inner reference (for testing)
    #[cfg(test)]
    pub fn inner_test(&self) -> &Arc<EnvInner> {
        self.inner.as_ref().unwrap()
    }
    
    /// Sync data to disk
    pub fn sync(&self) -> Result<()> {
        let inner = self.inner.as_ref().unwrap();
        match inner.durability {
            DurabilityMode::NoSync => {
                // No sync requested
                Ok(())
            }
            DurabilityMode::AsyncFlush | DurabilityMode::SyncData | DurabilityMode::FullSync => {
                inner.io.sync()?;
                Ok(())
            }
        }
    }
    
    /// Force a full synchronous sync regardless of durability mode
    pub fn force_sync(&self) -> Result<()> {
        let inner = self.inner.as_ref().unwrap();
        inner.io.sync()?;
        Ok(())
    }
    
    /// Get environment statistics
    pub fn stat(&self) -> Result<crate::meta::DbStats> {
        let inner = self.inner.as_ref().unwrap();
        let meta = inner.meta()?;
        
        Ok(crate::meta::DbStats {
            psize: meta.psize,
            depth: meta.main_db.depth,
            branch_pages: meta.main_db.branch_pages,
            leaf_pages: meta.main_db.leaf_pages,
            overflow_pages: meta.main_db.overflow_pages,
            entries: meta.main_db.entries,
        })
    }
}

impl<S: State> Drop for Environment<S> {
    fn drop(&mut self) {
        if let Some(_inner) = self.inner.take() {
            // Meta page alternation handled in commit
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::page::PAGE_SIZE;
    use super::*;
    use tempfile::TempDir;
    use std::sync::Arc;
    
    #[test]
    fn test_env_creation() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .open(dir.path())
            .unwrap();
            
        let stats = env.stat().unwrap();
        assert_eq!(stats.psize, PAGE_SIZE as u32);
    }
    
    #[test]
    fn test_env_reopen() {
        let dir = TempDir::new().unwrap();
        
        // Create and close environment
        {
            let env = EnvBuilder::new().open(dir.path()).unwrap();
            let _txn = env.begin_write_txn().unwrap();
            // Transaction commits on drop
        }
        
        // Reopen and verify
        {
            let env = EnvBuilder::new().open(dir.path()).unwrap();
            let stats = env.stat().unwrap();
            assert_eq!(stats.psize, PAGE_SIZE as u32);
        }
    }
    
    #[test]
    fn test_durability_modes() {
        use crate::db::Database;
        let dir = TempDir::new().unwrap();
        
        // Test with FullSync mode
        {
            let env = Arc::new(
                EnvBuilder::new()
                    .map_size(10 * 1024 * 1024)
                    .durability(DurabilityMode::FullSync)
                    .open(dir.path())
                    .unwrap()
            );
            
            // Create database and insert data
            let db: Database<String, String> = {
                let mut txn = env.begin_write_txn().unwrap();
                let db = env.create_database(&mut txn, None).unwrap();
                
                db.put(&mut txn, "key1".to_string(), "value1".to_string()).unwrap();
                db.put(&mut txn, "key2".to_string(), "value2".to_string()).unwrap();
                
                // Commit with full sync
                txn.commit().unwrap();
                db
            };
            
            // Force drop to close mmap
            drop(db);
            drop(env);
        }
        
        // Reopen and verify data persisted
        {
            let env = Arc::new(
                EnvBuilder::new()
                    .map_size(10 * 1024 * 1024)
                    .open(dir.path())
                    .unwrap()
            );
            
            let txn = env.begin_txn().unwrap();
            let db: Database<String, String> = env.open_database(&txn, None).unwrap();
            
            assert_eq!(db.get(&txn, &"key1".to_string()).unwrap(), Some("value1".to_string()));
            assert_eq!(db.get(&txn, &"key2".to_string()).unwrap(), Some("value2".to_string()));
        }
    }
    
    #[test]
    fn test_no_sync_mode() {
        let dir = TempDir::new().unwrap();
        
        // Test with NoSync mode - should be fastest
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .durability(DurabilityMode::NoSync)
                .open(dir.path())
                .unwrap()
        );
        
        let start = std::time::Instant::now();
        
        // Perform many small transactions
        for _ in 0..100 {
            let mut txn = env.begin_write_txn().unwrap();
            // Just allocate a page
            let _ = txn.alloc_page(crate::page::PageFlags::LEAF).unwrap();
            txn.commit().unwrap();
        }
        
        let no_sync_duration = start.elapsed();
        
        // Now test with FullSync mode
        let dir2 = TempDir::new().unwrap();
        let env2 = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .durability(DurabilityMode::FullSync)
                .open(dir2.path())
                .unwrap()
        );
        
        let start = std::time::Instant::now();
        
        // Perform same transactions
        for _ in 0..100 {
            let mut txn = env2.begin_write_txn().unwrap();
            let _ = txn.alloc_page(crate::page::PageFlags::LEAF).unwrap();
            txn.commit().unwrap();
        }
        
        let full_sync_duration = start.elapsed();
        
        // NoSync should be significantly faster
        println!("NoSync: {:?}, FullSync: {:?}", no_sync_duration, full_sync_duration);
        assert!(no_sync_duration < full_sync_duration);
    }
}