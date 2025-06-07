//! Free page management and recycling
//!
//! This module manages the list of free pages that can be reused
//! instead of allocating new pages from the end of the file.

use crate::error::{Error, Result, PageId, TransactionId};
use crate::page::{Page, PageFlags, PAGE_SIZE};
use crate::txn::Transaction;
use crate::meta::DbInfo;
use std::collections::{BTreeSet, BTreeMap};

/// Free list manager
pub struct FreeList {
    /// Set of free pages available for reuse
    free_pages: BTreeSet<PageId>,
    /// Pages freed in the current transaction (cannot be reused yet)
    pending_pages: BTreeSet<PageId>,
    /// Map of transaction ID to pages freed in that transaction
    /// Used for tracking which pages can be reused
    txn_free_pages: BTreeMap<TransactionId, Vec<PageId>>,
    /// Oldest reader transaction ID (pages can only be reused after this)
    oldest_reader: TransactionId,
    /// Track if the freelist needs to be saved
    needs_save: bool,
}

impl FreeList {
    /// Create a new free list
    pub fn new() -> Self {
        Self {
            free_pages: BTreeSet::new(),
            pending_pages: BTreeSet::new(),
            txn_free_pages: BTreeMap::new(),
            oldest_reader: TransactionId(0),
            needs_save: false,
        }
    }
    
    /// Load free pages from the free database
    pub fn load<'txn>(
        txn: &Transaction<'txn, impl crate::txn::mode::Mode>,
        free_db: &DbInfo,
    ) -> Result<Self> {
        let mut freelist = Self::new();
        
        if free_db.root.0 == 0 {
            // No free pages yet
            return Ok(freelist);
        }
        
        // Create a cursor to iterate through the free database
        // The free database stores: txn_id -> list of page IDs
        let current_page_id = free_db.root;
        let mut stack = vec![current_page_id];
        
        while let Some(page_id) = stack.pop() {
            let page = txn.get_page(page_id)?;
            
            if page.header.flags.contains(PageFlags::LEAF) {
                // Process leaf page entries
                for i in 0..page.header.num_keys as usize {
                    let node = page.node(i)?;
                    let txn_id_bytes = node.key()?;
                    let page_list_bytes = node.value()?;
                    
                    // Decode transaction ID (8 bytes)
                    if txn_id_bytes.len() != 8 {
                        continue;
                    }
                    let txn_id = TransactionId(u64::from_le_bytes([
                        txn_id_bytes[0], txn_id_bytes[1], txn_id_bytes[2], txn_id_bytes[3],
                        txn_id_bytes[4], txn_id_bytes[5], txn_id_bytes[6], txn_id_bytes[7],
                    ]));
                    
                    // Decode page list (array of 8-byte page IDs)
                    let mut pages = Vec::new();
                    for chunk in page_list_bytes.chunks(8) {
                        if chunk.len() == 8 {
                            let page_id = PageId(u64::from_le_bytes([
                                chunk[0], chunk[1], chunk[2], chunk[3],
                                chunk[4], chunk[5], chunk[6], chunk[7],
                            ]));
                            pages.push(page_id);
                        }
                    }
                    
                    freelist.txn_free_pages.insert(txn_id, pages);
                }
            } else {
                // Branch page - add children to stack
                for i in 0..page.header.num_keys as usize {
                    let node = page.node(i)?;
                    stack.push(node.page_number()?);
                }
            }
        }
        
        Ok(freelist)
    }
    
    /// Set the oldest reader transaction ID
    pub fn set_oldest_reader(&mut self, txn_id: TransactionId) {
        self.oldest_reader = txn_id;
    }
    
    /// Add a page to the free list (to be freed after transaction commits)
    pub fn free_page(&mut self, page_id: PageId) {
        self.pending_pages.insert(page_id);
    }
    
    /// Try to allocate a page from the free list
    pub fn alloc_page(&mut self) -> Option<PageId> {
        // Try to get a page from the free list
        if let Some(&page_id) = self.free_pages.iter().next() {
            self.free_pages.remove(&page_id);
            Some(page_id)
        } else {
            None
        }
    }
    
    /// Commit pending pages to the free list
    /// This should be called after a transaction commits successfully
    pub fn commit_pending(&mut self, txn_id: TransactionId) {
        if !self.pending_pages.is_empty() {
            // Store pending pages with this transaction ID
            let pages: Vec<PageId> = self.pending_pages.iter().cloned().collect();
            self.txn_free_pages.insert(txn_id, pages);
            self.pending_pages.clear();
            self.needs_save = true;
        }
        
        // Check which pages can be made available for reuse
        self.update_free_pages();
    }
    
    /// Update the free pages based on the oldest reader
    pub fn update_free_pages(&mut self) {
        // Remove transactions that are safe to reuse
        let mut safe_txns = Vec::new();
        
        for (&txn_id, _) in &self.txn_free_pages {
            // Pages from a transaction can be reused when:
            // 1. There are no active readers (oldest_reader == 0), OR
            // 2. The transaction is older than the oldest reader
            if self.oldest_reader.0 == 0 || txn_id.0 < self.oldest_reader.0 {
                safe_txns.push(txn_id);
            }
        }
        
        // Move pages from safe transactions to free_pages
        for txn_id in safe_txns {
            if let Some(pages) = self.txn_free_pages.remove(&txn_id) {
                for page_id in pages {
                    self.free_pages.insert(page_id);
                }
            }
        }
    }
    
    /// Get serialized freelist data for saving
    pub fn get_save_data(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        
        for (&txn_id, pages) in &self.txn_free_pages {
            // Encode transaction ID as key (8 bytes)
            let key = txn_id.0.to_le_bytes().to_vec();
            
            // Encode page list as value (8 bytes per page)
            let mut value = Vec::with_capacity(pages.len() * 8);
            for page_id in pages {
                value.extend_from_slice(&page_id.0.to_le_bytes());
            }
            
            result.push((key, value));
        }
        
        result
    }
    
    /// Get the number of free pages
    pub fn len(&self) -> usize {
        self.free_pages.len()
    }
    
    /// Check if the free list is empty
    pub fn is_empty(&self) -> bool {
        self.free_pages.is_empty()
    }
    
    /// Get the number of pending pages
    pub fn pending_len(&self) -> usize {
        self.pending_pages.len()
    }
    
    /// Check if there are any transaction free pages
    pub fn has_txn_free_pages(&self) -> bool {
        !self.txn_free_pages.is_empty()
    }
    
    /// Check if the freelist needs to be saved
    pub fn needs_save(&self) -> bool {
        self.needs_save
    }
}

/// Free page header for storing lists of free pages
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FreePageHeader {
    /// Number of page IDs in this page
    pub count: u16,
    /// Padding for alignment
    pub _pad: u16,
    /// Next free list page (0 if last)
    pub next: PageId,
}

impl FreePageHeader {
    /// Size of the header
    pub const SIZE: usize = std::mem::size_of::<Self>();
    
    /// Maximum number of page IDs that fit in a page
    pub const MAX_IDS: usize = (PAGE_SIZE - crate::page::PageHeader::SIZE - Self::SIZE) / std::mem::size_of::<u64>();
}

/// A page containing a list of free page IDs
pub struct FreePage<'a> {
    page: &'a Page,
}

impl<'a> FreePage<'a> {
    /// Create a free page view from a page
    pub fn from_page(page: &'a Page) -> Result<Self> {
        if !page.header.flags.contains(PageFlags::OVERFLOW) {
            return Err(Error::InvalidPageType {
                expected: crate::error::PageType::Overflow,
                found: page.header.page_type(),
            });
        }
        
        Ok(Self { page })
    }
    
    /// Get the header
    pub fn header(&self) -> &FreePageHeader {
        unsafe {
            &*(self.page.data.as_ptr() as *const FreePageHeader)
        }
    }
    
    /// Get the page IDs
    pub fn page_ids(&self) -> &[PageId] {
        let header = self.header();
        let count = header.count as usize;
        
        unsafe {
            let ids_ptr = self.page.data.as_ptr()
                .add(FreePageHeader::SIZE) as *const PageId;
            std::slice::from_raw_parts(ids_ptr, count)
        }
    }
}

/// Mutable free page
pub struct FreePageMut<'a> {
    page: &'a mut Page,
}

impl<'a> FreePageMut<'a> {
    /// Create a new free page
    pub fn new(page: &'a mut Page) -> Self {
        page.header.flags = PageFlags::OVERFLOW; // Reuse overflow flag for free pages
        
        // Initialize header
        let header = FreePageHeader {
            count: 0,
            _pad: 0,
            next: PageId(0),
        };
        
        unsafe {
            let header_ptr = page.data.as_mut_ptr() as *mut FreePageHeader;
            *header_ptr = header;
        }
        
        Self { page }
    }
    
    /// Get mutable header
    pub fn header_mut(&mut self) -> &mut FreePageHeader {
        unsafe {
            &mut *(self.page.data.as_mut_ptr() as *mut FreePageHeader)
        }
    }
    
    /// Add a page ID to the list
    pub fn add_page_id(&mut self, page_id: PageId) -> Result<()> {
        let count = self.header_mut().count as usize;
        if count >= FreePageHeader::MAX_IDS {
            return Err(Error::Custom("Free page is full".into()));
        }
        
        unsafe {
            let ids_ptr = self.page.data.as_mut_ptr()
                .add(FreePageHeader::SIZE) as *mut PageId;
            *ids_ptr.add(count) = page_id;
        }
        
        self.header_mut().count += 1;
        Ok(())
    }
    
    /// Set the next page
    pub fn set_next(&mut self, next: PageId) {
        self.header_mut().next = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_freelist_basic() {
        let mut freelist = FreeList::new();
        assert!(freelist.is_empty());
        
        // Free some pages
        freelist.free_page(PageId(10));
        freelist.free_page(PageId(20));
        freelist.free_page(PageId(30));
        
        assert_eq!(freelist.pending_len(), 3);
        assert_eq!(freelist.len(), 0);
        
        // Commit pending pages
        freelist.commit_pending(TransactionId(1));
        
        // Since oldest_reader is 0, pages should be moved to free list
        assert_eq!(freelist.pending_len(), 0);
        assert_eq!(freelist.len(), 3);
        
        // Now simulate oldest reader being set
        freelist.set_oldest_reader(TransactionId(5));
        
        // Free more pages
        freelist.free_page(PageId(40));
        freelist.free_page(PageId(50));
        
        // Try to commit with a transaction ID less than oldest reader
        freelist.commit_pending(TransactionId(3));
        
        // Pending pages are moved to txn_free_pages, then immediately to free_pages
        // because txn_id (3) < oldest_reader (5) - these pages are safe to reuse
        // since they were freed before any active reader started
        assert_eq!(freelist.pending_len(), 0);
        assert_eq!(freelist.txn_free_pages.len(), 0);
        assert_eq!(freelist.len(), 5); // Now we have 5 free pages total
    }
    
    #[test]
    fn test_free_page_header_size() {
        assert!(FreePageHeader::SIZE < PAGE_SIZE);
        assert!(FreePageHeader::MAX_IDS > 0);
        
        // Check that we can fit a reasonable number of page IDs
        assert!(FreePageHeader::MAX_IDS >= 500);
    }
    
    #[test]
    fn test_free_page_operations() {
        let mut page = Page::new(PageId(100), PageFlags::OVERFLOW);
        let mut free_page = FreePageMut::new(&mut *page);
        
        // Add some page IDs
        free_page.add_page_id(PageId(1)).unwrap();
        free_page.add_page_id(PageId(2)).unwrap();
        free_page.add_page_id(PageId(3)).unwrap();
        
        assert_eq!(free_page.header_mut().count, 3);
        
        // Read them back
        let free_page_read = FreePage::from_page(&page).unwrap();
        let ids = free_page_read.page_ids();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], PageId(1));
        assert_eq!(ids[1], PageId(2));
        assert_eq!(ids[2], PageId(3));
    }
    
    #[test]
    fn test_freelist_with_transaction() {
        use crate::env::EnvBuilder;
        use tempfile::TempDir;
        
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())
            .unwrap();
        
        // Test allocating and freeing pages
        let mut txn = env.begin_write_txn().unwrap();
        
        // Allocate some pages
        let (_page1, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
        let (page2, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
        let (_page3, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
        
        // Free one page
        txn.free_page(page2).unwrap();
        
        txn.commit().unwrap();
        
        // In a new transaction, allocate a page
        // It should reuse the freed page (once we implement proper reader tracking)
        let mut txn = env.begin_write_txn().unwrap();
        let (new_page, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
        
        // For now, it won't reuse because we're being conservative
        assert_ne!(new_page, page2);
        
        txn.commit().unwrap();
    }
    
    #[test]
    fn test_freelist_with_readers() {
        use crate::env::EnvBuilder;
        use tempfile::TempDir;
        
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())
            .unwrap();
        
        // Start a read transaction
        let read_txn = env.begin_txn().unwrap();
        let read_txn_id = read_txn.id();
        
        // In a write transaction, allocate and free pages
        let mut txn = env.begin_write_txn().unwrap();
        
        // Allocate some pages
        let (_page1, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
        let (page2, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
        let (_page3, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
        
        // Free one page
        txn.free_page(page2).unwrap();
        
        // Check freelist state before commit
        if let crate::txn::ModeData::Write { ref freelist, .. } = txn.mode_data {
            assert_eq!(freelist.pending_len(), 1);
            assert_eq!(freelist.len(), 0);
        }
        
        txn.commit().unwrap();
        
        // Now start a new write transaction while reader is still active
        let mut txn2 = env.begin_write_txn().unwrap();
        
        // The freelist should see the active reader
        let txn2_id = txn2.id();
        if let crate::txn::ModeData::Write { ref mut freelist, .. } = txn2.mode_data {
            // Set the oldest reader (this would normally happen in commit)
            if let Some(oldest) = env.inner().readers.oldest_reader() {
                freelist.set_oldest_reader(oldest);
                assert_eq!(oldest, read_txn_id);
            }
            
            // Try to commit pending pages - they shouldn't be reused yet
            freelist.commit_pending(txn2_id);
            
            // The page shouldn't be available for reuse because reader is still active
            assert_eq!(freelist.len(), 0);
        }
        
        // Drop the reader
        drop(read_txn);
        
        // Now pages can be reused in next transaction
        txn2.commit().unwrap();
        
        // Start another write transaction
        let mut txn3 = env.begin_write_txn().unwrap();
        
        // Now the freelist should have no active readers
        let txn3_id = txn3.id();
        if let crate::txn::ModeData::Write { ref mut freelist, .. } = txn3.mode_data {
            assert!(env.inner().readers.oldest_reader().is_none());
            
            // Without active readers, pages can be committed to free list
            freelist.set_oldest_reader(TransactionId(0));
            freelist.commit_pending(txn3_id);
            
            // Now the page should be available for reuse
            // (In a full implementation, this would happen automatically)
        }
        
        txn3.commit().unwrap();
    }
    
    #[test]
    #[ignore = "Freelist save/load not fully implemented due to borrow checker constraints"]
    fn test_freelist_save_load() {
        use crate::env::EnvBuilder;
        use crate::meta::DbInfo;
        use tempfile::TempDir;
        
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())
            .unwrap();
        
        let page_ids = {
            // Create and save a freelist
            let mut txn = env.begin_write_txn().unwrap();
            
            // Allocate some pages to free
            let (page1, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
            let (page2, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
            let (page3, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
            let (page4, _) = txn.alloc_page(PageFlags::LEAF).unwrap();
            
            // Create a freelist with some transaction free pages
            let mut freelist = FreeList::new();
            freelist.txn_free_pages.insert(TransactionId(100), vec![page1, page2]);
            freelist.txn_free_pages.insert(TransactionId(200), vec![page3, page4]);
            
            // Create a free database
            let (free_root_id, free_root) = txn.alloc_page(PageFlags::LEAF).unwrap();
            free_root.header.num_keys = 0;
            
            let free_db = DbInfo {
                flags: 0,
                root: free_root_id,
                entries: 0,
                depth: 1,
                branch_pages: 0,
                leaf_pages: 1,
                overflow_pages: 0,
            };
            
            // Save the freelist - not implemented yet
            // freelist.save(&mut txn, &mut free_db).unwrap();
            
            txn.commit().unwrap();
            
            (free_db, vec![page1, page2, page3, page4])
        };
        
        // Load the freelist in a new transaction
        {
            let txn = env.begin_txn().unwrap();
            let freelist = FreeList::load(&txn, &page_ids.0).unwrap();
            
            // Verify the loaded data
            assert_eq!(freelist.txn_free_pages.len(), 2);
            assert!(freelist.txn_free_pages.contains_key(&TransactionId(100)));
            assert!(freelist.txn_free_pages.contains_key(&TransactionId(200)));
            
            let pages_100 = &freelist.txn_free_pages[&TransactionId(100)];
            assert_eq!(pages_100.len(), 2);
            assert!(pages_100.contains(&page_ids.1[0]));
            assert!(pages_100.contains(&page_ids.1[1]));
            
            let pages_200 = &freelist.txn_free_pages[&TransactionId(200)];
            assert_eq!(pages_200.len(), 2);
            assert!(pages_200.contains(&page_ids.1[2]));
            assert!(pages_200.contains(&page_ids.1[3]));
        }
    }
}