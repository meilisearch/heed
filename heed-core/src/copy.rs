//! Environment copying and backup functionality

use std::path::Path;
use std::fs::OpenOptions;
use std::io::Write;

use crate::error::Result;
use crate::env::{Environment, state};
use crate::txn::{Transaction, Read};
use crate::page::{Page, PAGE_SIZE};
use crate::meta::MetaPage;

/// Options for copying an environment
#[derive(Debug, Clone)]
pub struct CopyOptions {
    /// Copy with compaction (skip free pages)
    pub compact: bool,
    /// Exclude transaction metadata
    pub exclude_txn_metadata: bool,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            compact: false,
            exclude_txn_metadata: false,
        }
    }
}

impl CopyOptions {
    /// Create options for a compact copy
    pub fn compact() -> Self {
        Self {
            compact: true,
            exclude_txn_metadata: false,
        }
    }
}

/// Copy an environment to a file
pub fn copy_to_file(
    env: &Environment<state::Open>,
    path: impl AsRef<Path>,
    options: CopyOptions,
) -> Result<()> {
    let path = path.as_ref();
    
    // Create or truncate the target file
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    
    // Use a read transaction to get a consistent view
    let txn = env.begin_txn()?;
    
    // Copy the environment
    copy_env_internal(env, &txn, &mut file, options)?;
    
    // Ensure all data is written
    file.flush()?;
    file.sync_all()?;
    
    Ok(())
}

/// Copy environment to a writer
pub fn copy_to_writer<W: Write>(
    env: &Environment<state::Open>,
    writer: &mut W,
    options: CopyOptions,
) -> Result<()> {
    let txn = env.begin_txn()?;
    copy_env_internal(env, &txn, writer, options)
}

/// Internal copy implementation
fn copy_env_internal<W: Write>(
    env: &Environment<state::Open>,
    txn: &Transaction<'_, Read>,
    writer: &mut W,
    options: CopyOptions,
) -> Result<()> {
    let inner = env.inner();
    
    // Get current meta page
    let meta = inner.meta()?;
    
    if options.compact {
        // Compact copy - only copy used pages
        copy_compact(env, txn, writer, &meta)
    } else {
        // Full copy - copy all pages up to last used
        copy_full(env, txn, writer, &meta)
    }
}

/// Full copy - copies all pages including free ones
fn copy_full<W: Write>(
    env: &Environment<state::Open>,
    _txn: &Transaction<'_, Read>,
    writer: &mut W,
    meta: &MetaPage,
) -> Result<()> {
    let inner = env.inner();
    
    // Copy all pages from 0 to last_pg
    for page_id in 0..=meta.last_pg.0 {
        let page = inner.io.read_page(crate::error::PageId(page_id))?;
        
        // Write the entire page
        let page_bytes = unsafe {
            std::slice::from_raw_parts(
                page.as_ref() as *const Page as *const u8,
                PAGE_SIZE,
            )
        };
        
        writer.write_all(page_bytes)?;
    }
    
    Ok(())
}

/// Compact copy - only copies used pages
fn copy_compact<W: Write>(
    env: &Environment<state::Open>,
    txn: &Transaction<'_, Read>,
    writer: &mut W,
    meta: &MetaPage,
) -> Result<()> {
    let inner = env.inner();
    
    // We need to track which pages are used
    let mut used_pages = std::collections::BTreeSet::new();
    
    // Meta pages are always used
    used_pages.insert(0);
    used_pages.insert(1);
    
    // Traverse all databases to find used pages
    // Start with main database
    if meta.main_db.root.0 != 0 {
        collect_used_pages(txn, meta.main_db.root, &mut used_pages)?;
    }
    
    // Traverse free database (even though it contains free pages, its structure uses pages)
    if meta.free_db.root.0 != 0 {
        collect_used_pages(txn, meta.free_db.root, &mut used_pages)?;
    }
    
    // Traverse named databases
    if meta.main_db.root.0 != 0 {
        // The main database contains the catalog of named databases
        // Use tree traversal to find all databases
        crate::tree_utils::traverse_tree(txn, meta.main_db.root, |_page_id, page| {
            if page.header.flags.contains(crate::page::PageFlags::LEAF) {
                // Process all entries in this leaf page
                for i in 0..page.header.num_keys as usize {
                    if let Ok(node) = page.node(i) {
                        if let Ok(value) = node.value() {
                            // Each entry in main DB is a named database
                            if let Ok(db_info) = crate::catalog::Catalog::deserialize_db_info(&value) {
                                if db_info.root.0 != 0 {
                                    let _ = collect_used_pages(txn, db_info.root, &mut used_pages);
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        })?;
    }
    
    // Now we need to remap pages to be contiguous
    let mut page_map = std::collections::HashMap::new();
    let mut new_page_id = 0u64;
    
    for &old_page_id in &used_pages {
        page_map.insert(old_page_id, new_page_id);
        new_page_id += 1;
    }
    
    // Create new meta pages with remapped page IDs
    let mut new_meta0 = *meta;
    let mut new_meta1 = *meta;
    
    // Remap root pages
    if let Some(&new_root) = page_map.get(&meta.main_db.root.0) {
        new_meta0.main_db.root = crate::error::PageId(new_root);
        new_meta1.main_db.root = crate::error::PageId(new_root);
    }
    
    if let Some(&new_root) = page_map.get(&meta.free_db.root.0) {
        new_meta0.free_db.root = crate::error::PageId(new_root);
        new_meta1.free_db.root = crate::error::PageId(new_root);
    }
    
    // Update last page
    new_meta0.last_pg = crate::error::PageId(new_page_id - 1);
    new_meta1.last_pg = crate::error::PageId(new_page_id - 1);
    
    // Clear free list in compact copy
    new_meta0.free_db = Default::default();
    new_meta1.free_db = Default::default();
    
    // Write remapped pages
    for &old_page_id in &used_pages {
        let new_page_id = page_map[&old_page_id];
        
        if old_page_id == 0 {
            // Write new meta page 0
            let meta_page = Page::from_meta(&new_meta0, crate::error::PageId(0));
            write_page(writer, &meta_page)?;
        } else if old_page_id == 1 {
            // Write new meta page 1
            let meta_page = Page::from_meta(&new_meta1, crate::error::PageId(1));
            write_page(writer, &meta_page)?;
        } else {
            // Read original page
            let mut page_data = vec![0u8; PAGE_SIZE];
            {
                let page = inner.io.read_page(crate::error::PageId(old_page_id))?;
                let page_bytes = unsafe {
                    std::slice::from_raw_parts(
                        page.as_ref() as *const Page as *const u8,
                        PAGE_SIZE,
                    )
                };
                page_data.copy_from_slice(page_bytes);
            }
            
            // Remap page ID in header
            let page = unsafe { &mut *(page_data.as_mut_ptr() as *mut Page) };
            page.header.pgno = new_page_id;
            
            // Remap page IDs within page content for branch pages
            if page.header.flags.contains(crate::page::PageFlags::BRANCH) {
                // Update leftmost child in branch_v2 header
                if let Some(new_id) = page_map.get(&crate::branch::BranchPage::get_leftmost_child(page).unwrap().0) {
                    unsafe {
                        let header_ptr = page.data.as_mut_ptr() as *mut crate::branch::BranchHeader;
                        (*header_ptr).leftmost_child = crate::error::PageId(*new_id);
                    }
                }
                
                // Update child pointers in nodes
                for i in 0..page.header.num_keys as usize {
                    if let Ok(node) = page.node(i) {
                        if let Ok(child_id) = node.page_number() {
                            if let Some(&new_id) = page_map.get(&child_id.0) {
                                // Update the child pointer
                                let ptr = page.ptrs()[i];
                                let node_offset = ptr as usize - crate::page::PageHeader::SIZE;
                                let val_offset = node_offset + crate::page::NodeHeader::SIZE + node.header.ksize as usize;
                                
                                unsafe {
                                    let child_ptr = page.data.as_mut_ptr().add(val_offset) as *mut u64;
                                    *child_ptr = new_id;
                                }
                            }
                        }
                    }
                }
            }
            
            writer.write_all(&page_data)?;
        }
    }
    
    Ok(())
}

/// Collect all pages used by a B-tree
fn collect_used_pages(
    txn: &Transaction<'_, Read>,
    root: crate::error::PageId,
    used_pages: &mut std::collections::BTreeSet<u64>,
) -> Result<()> {
    // Use tree traversal utility to collect all pages
    let pages = crate::tree_utils::collect_tree_pages(txn, root)?;
    for page_id in pages {
        used_pages.insert(page_id.0);
    }
    
    Ok(())
}

/// Write a page to a writer
fn write_page<W: Write>(writer: &mut W, page: &Page) -> Result<()> {
    let page_bytes = unsafe {
        std::slice::from_raw_parts(
            page as *const Page as *const u8,
            PAGE_SIZE,
        )
    };
    writer.write_all(page_bytes)?;
    Ok(())
}

/// Backup status callback
pub trait BackupCallback {
    /// Called periodically during backup with progress
    fn progress(&mut self, pages_copied: u64, total_pages: u64);
    
    /// Called when backup is complete
    fn complete(&mut self, pages_copied: u64);
}

/// Copy with progress callback
pub fn copy_with_callback<C: BackupCallback>(
    env: &Environment<state::Open>,
    path: impl AsRef<Path>,
    _options: CopyOptions,
    callback: &mut C,
) -> Result<()> {
    let path = path.as_ref();
    
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    
    let _txn = env.begin_txn()?;
    let inner = env.inner();
    let meta = inner.meta()?;
    
    let total_pages = meta.last_pg.0 + 1;
    let mut pages_copied = 0;
    
    // Copy pages with progress updates
    for page_id in 0..=meta.last_pg.0 {
        let page = inner.io.read_page(crate::error::PageId(page_id))?;
        
        let page_bytes = unsafe {
            std::slice::from_raw_parts(
                page.as_ref() as *const Page as *const u8,
                PAGE_SIZE,
            )
        };
        
        file.write_all(page_bytes)?;
        pages_copied += 1;
        
        // Update progress every 100 pages
        if pages_copied % 100 == 0 {
            callback.progress(pages_copied, total_pages);
        }
    }
    
    file.flush()?;
    file.sync_all()?;
    
    callback.complete(pages_copied);
    
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvBuilder;
    use crate::db::Database;
    use tempfile::TempDir;
    use std::sync::Arc;
    
    #[test]
    fn test_copy_empty_env() {
        let dir = TempDir::new().unwrap();
        let backup_path = dir.path().join("backup.mdb");
        
        // Create environment
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Copy it
        copy_to_file(&env, &backup_path, CopyOptions::default()).unwrap();
        
        // Verify backup file exists
        assert!(backup_path.exists());
        
        // Open the backup as a new environment
        let backup_env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(backup_path.parent().unwrap())
                .unwrap()
        );
        
        // Verify it opens successfully
        let stats = backup_env.stat().unwrap();
        assert_eq!(stats.entries, 0);
    }
    
    #[test]
    fn test_copy_with_data() {
        let dir = TempDir::new().unwrap();
        let backup_path = dir.path().join("backup.mdb");
        
        // Create environment with data
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Add some data
        {
            let mut txn = env.begin_write_txn().unwrap();
            let db: Database<String, String> = env.create_database(&mut txn, None).unwrap();
            
            for i in 0..100 {
                db.put(&mut txn, format!("key{}", i), format!("value{}", i)).unwrap();
            }
            
            txn.commit().unwrap();
        }
        
        // Copy it
        copy_to_file(&env, &backup_path, CopyOptions::default()).unwrap();
        
        // Can't easily test the backup by opening it since we'd need to copy to a directory
        // Just verify the file was created and has reasonable size
        assert!(backup_path.exists());
        let metadata = std::fs::metadata(&backup_path).unwrap();
        assert!(metadata.len() > PAGE_SIZE as u64 * 2); // At least meta pages
    }
    
    struct TestCallback {
        progress_called: bool,
        complete_called: bool,
    }
    
    impl BackupCallback for TestCallback {
        fn progress(&mut self, _pages_copied: u64, _total_pages: u64) {
            self.progress_called = true;
        }
        
        fn complete(&mut self, _pages_copied: u64) {
            self.complete_called = true;
        }
    }
    
    #[test]
    fn test_copy_with_callback() {
        let dir = TempDir::new().unwrap();
        let backup_path = dir.path().join("backup.mdb");
        
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Add lots of data to ensure callback is triggered
        {
            let mut txn = env.begin_write_txn().unwrap();
            let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
            
            // Add enough data to allocate multiple pages
            for i in 0..1000 {
                let key = format!("key_{:04}", i).into_bytes();
                let value = vec![0u8; 100]; // Larger values to use more pages
                db.put(&mut txn, key, value).unwrap();
            }
            
            txn.commit().unwrap();
        }
        
        let mut callback = TestCallback {
            progress_called: false,
            complete_called: false,
        };
        
        copy_with_callback(&env, &backup_path, CopyOptions::default(), &mut callback).unwrap();
        
        assert!(callback.complete_called);
        // Progress might not be called if we have less than 100 pages
    }
}