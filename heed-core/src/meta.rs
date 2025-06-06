//! Meta page and database metadata management

use std::mem::size_of;
use static_assertions::const_assert;
use crate::error::{Error, Result, PageId, TransactionId};
use crate::page::{Page, PageFlags, PAGE_SIZE};

/// Page ID for meta page 1
pub const META_PAGE_1: PageId = PageId(0);

/// Page ID for meta page 2
pub const META_PAGE_2: PageId = PageId(1);

/// Version of the database format
pub const DB_VERSION: u32 = 1;

/// Magic number to identify database files
pub const MAGIC: u32 = 0xBEEFC0DE;

/// Database statistics
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DbStats {
    /// Size of database in pages
    pub psize: u32,
    /// Depth of B-tree
    pub depth: u32,
    /// Number of internal pages
    pub branch_pages: u64,
    /// Number of leaf pages
    pub leaf_pages: u64,
    /// Number of overflow pages
    pub overflow_pages: u64,
    /// Number of entries
    pub entries: u64,
}

/// Database info stored in branch pages
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DbInfo {
    /// Database flags
    pub flags: u32,
    /// Depth of tree
    pub depth: u32,
    /// Number of pages
    pub branch_pages: u64,
    /// Number of leaf pages
    pub leaf_pages: u64,
    /// Number of overflow pages
    pub overflow_pages: u64,
    /// Number of entries
    pub entries: u64,
    /// Root page number
    pub root: PageId,
}

impl Default for DbInfo {
    fn default() -> Self {
        Self {
            flags: 0,
            depth: 0,
            branch_pages: 0,
            leaf_pages: 0,
            overflow_pages: 0,
            entries: 0,
            root: PageId(0),
        }
    }
}

/// Meta page structure - contains database metadata
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MetaPage {
    /// Magic number
    pub magic: u32,
    /// Database format version
    pub version: u32,
    /// Address of this page (for validation)
    pub address: *mut MetaPage,
    /// Database flags
    pub flags: u32,
    /// Database page size
    pub psize: u32,
    /// Max reader slots
    pub maxreaders: u32,
    /// Database ID for lock file
    pub dbs: u32,
    /// Last page number in database
    pub last_pg: PageId,
    /// Last transaction ID
    pub last_txnid: TransactionId,
    /// Total map size
    pub mapsize: u64,
    /// Main database info
    pub main_db: DbInfo,
    /// Free database info
    pub free_db: DbInfo,
}

impl MetaPage {
    /// Create a new meta page
    pub fn new() -> Self {
        Self {
            magic: MAGIC,
            version: DB_VERSION,
            address: std::ptr::null_mut(),
            flags: 0,
            psize: PAGE_SIZE as u32,
            maxreaders: 126,
            dbs: 2,
            last_pg: PageId(1),
            last_txnid: TransactionId(0),
            mapsize: 0,
            main_db: DbInfo::default(),
            free_db: DbInfo::default(),
        }
    }
    
    /// Validate the meta page
    pub fn validate(&self) -> Result<()> {
        if self.magic != MAGIC {
            return Err(Error::Corruption {
                details: format!("Invalid magic number: 0x{:x}", self.magic),
                page_id: None,
            });
        }
        
        if self.version != DB_VERSION {
            return Err(Error::VersionMismatch {
                expected: DB_VERSION,
                found: self.version,
            });
        }
        
        if self.psize as usize != PAGE_SIZE {
            return Err(Error::Corruption {
                details: format!("Invalid page size: {}", self.psize),
                page_id: None,
            });
        }
        
        Ok(())
    }
    
    /// Convert to a page
    pub fn to_page(&self, pgno: u64) -> Box<Page> {
        let mut page = Page::new(PageId(pgno), PageFlags::META);
        
        // Copy meta page data into page
        unsafe {
            let meta_ptr = page.data.as_mut_ptr() as *mut MetaPage;
            *meta_ptr = *self;
            (*meta_ptr).address = meta_ptr;
        }
        
        page
    }
    
    /// Create from a page
    pub fn from_page(page: &Page) -> Result<&Self> {
        if !page.header.flags.contains(PageFlags::META) {
            return Err(Error::InvalidPageType {
                expected: crate::error::PageType::Meta,
                found: page.header.page_type(),
            });
        }
        
        let meta = unsafe { &*(page.data.as_ptr() as *const MetaPage) };
        meta.validate()?;
        Ok(meta)
    }
}

const_assert!(size_of::<MetaPage>() < PAGE_SIZE - size_of::<crate::page::PageHeader>());

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_meta_page_size() {
        assert!(size_of::<MetaPage>() < PAGE_SIZE - size_of::<crate::page::PageHeader>());
    }
    
    #[test]
    fn test_meta_page_validation() {
        let meta = MetaPage::new();
        assert!(meta.validate().is_ok());
        
        let mut bad_meta = meta;
        bad_meta.magic = 0xDEADBEEF;
        assert!(bad_meta.validate().is_err());
    }
    
    #[test]
    fn test_meta_page_conversion() {
        let meta = MetaPage::new();
        let page = meta.to_page(0);
        
        assert!(page.header.flags.contains(PageFlags::META));
        
        let meta2 = MetaPage::from_page(&page).unwrap();
        assert_eq!(meta2.magic, MAGIC);
        assert_eq!(meta2.version, DB_VERSION);
    }
}