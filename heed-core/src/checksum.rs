//! Page checksum validation for data integrity
//!
//! This module provides CRC32 checksums for pages to detect corruption.
//! Each page can have an optional checksum stored in its header that is
//! validated on read and updated on write.

use crc32fast::Hasher;
use crate::error::{Error, Result, PageId};
use crate::page::Page;

/// Checksum type (CRC32)
pub type Checksum = u32;

/// Checksum calculator
pub struct ChecksumCalculator {
    hasher: Hasher,
}

impl ChecksumCalculator {
    /// Create a new checksum calculator
    pub fn new() -> Self {
        Self {
            hasher: Hasher::new(),
        }
    }
    
    /// Calculate checksum for a page
    pub fn calculate_page_checksum(page: &Page) -> Checksum {
        let mut hasher = Hasher::new();
        
        // Hash the header (excluding the checksum field itself)
        hasher.update(&page.header.pgno.to_le_bytes());
        hasher.update(&page.header.flags.bits().to_le_bytes());
        hasher.update(&page.header.lower.to_le_bytes());
        hasher.update(&page.header.upper.to_le_bytes());
        hasher.update(&page.header.num_keys.to_le_bytes());
        
        // Hash the page data
        hasher.update(&page.data);
        
        hasher.finalize()
    }
    
    /// Validate a page's checksum
    pub fn validate_page(page: &Page) -> Result<()> {
        if page.header.checksum == 0 {
            // No checksum stored - skip validation
            return Ok(());
        }
        
        let calculated = Self::calculate_page_checksum(page);
        if calculated != page.header.checksum {
            return Err(Error::Corruption {
                details: format!(
                    "Checksum mismatch: expected 0x{:08x}, got 0x{:08x}",
                    page.header.checksum, calculated
                ),
                page_id: Some(PageId(page.header.pgno)),
            });
        }
        
        Ok(())
    }
    
    /// Update a page's checksum
    pub fn update_page_checksum(page: &mut Page) {
        let checksum = Self::calculate_page_checksum(page);
        page.header.checksum = checksum;
    }
}

/// Environment-level checksum configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumMode {
    /// No checksums (default for compatibility)
    None,
    /// Checksums on meta pages only
    MetaOnly,
    /// Checksums on all pages
    Full,
}

impl Default for ChecksumMode {
    fn default() -> Self {
        ChecksumMode::None
    }
}

/// Trait for checksum-aware page operations
pub trait ChecksummedPage {
    /// Validate this page's checksum
    fn validate_checksum(&self) -> Result<()>;
    
    /// Update this page's checksum
    fn update_checksum(&mut self);
    
    /// Check if this page has a checksum
    fn has_checksum(&self) -> bool;
}

impl ChecksummedPage for Page {
    fn validate_checksum(&self) -> Result<()> {
        ChecksumCalculator::validate_page(self)
    }
    
    fn update_checksum(&mut self) {
        ChecksumCalculator::update_page_checksum(self)
    }
    
    fn has_checksum(&self) -> bool {
        self.header.checksum != 0
    }
}

/// Batch checksum validation for performance
pub struct BatchValidator {
    failed_pages: Vec<(PageId, Checksum, Checksum)>, // (page_id, expected, actual)
}

impl BatchValidator {
    /// Create a new batch validator
    pub fn new() -> Self {
        Self {
            failed_pages: Vec::new(),
        }
    }
    
    /// Validate a batch of pages
    pub fn validate_pages(&mut self, pages: &[(PageId, &Page)]) -> Result<()> {
        self.failed_pages.clear();
        
        for (page_id, page) in pages {
            if page.header.checksum != 0 {
                let calculated = ChecksumCalculator::calculate_page_checksum(page);
                if calculated != page.header.checksum {
                    self.failed_pages.push((*page_id, page.header.checksum, calculated));
                }
            }
        }
        
        if !self.failed_pages.is_empty() {
            let details = self.failed_pages.iter()
                .map(|(pid, expected, actual)| {
                    format!("Page {}: expected 0x{:08x}, got 0x{:08x}", pid, expected, actual)
                })
                .collect::<Vec<_>>()
                .join("; ");
                
            return Err(Error::Corruption {
                details: format!("Multiple checksum failures: {}", details),
                page_id: Some(self.failed_pages[0].0),
            });
        }
        
        Ok(())
    }
    
    /// Get failed pages from last validation
    pub fn failed_pages(&self) -> &[(PageId, Checksum, Checksum)] {
        &self.failed_pages
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageFlags;
    
    #[test]
    fn test_checksum_calculation() {
        let mut page = Page::new(PageId(1), PageFlags::LEAF);
        
        // Initially no checksum
        assert_eq!(page.header.checksum, 0);
        assert!(!page.has_checksum());
        
        // Update checksum
        page.update_checksum();
        assert_ne!(page.header.checksum, 0);
        assert!(page.has_checksum());
        
        // Validation should succeed
        assert!(page.validate_checksum().is_ok());
        
        // Store the checksum
        let original_checksum = page.header.checksum;
        
        // Modify the page
        page.header.num_keys = 5;
        
        // Checksum should now be invalid
        assert!(page.validate_checksum().is_err());
        
        // Update checksum again
        page.update_checksum();
        assert_ne!(page.header.checksum, original_checksum);
        assert!(page.validate_checksum().is_ok());
    }
    
    #[test]
    fn test_checksum_with_data() {
        let mut page1 = Page::new(PageId(1), PageFlags::LEAF);
        let mut page2 = Page::new(PageId(1), PageFlags::LEAF);
        
        // Add some data to page1
        page1.add_node(b"key", b"value").unwrap();
        
        // Update checksums
        page1.update_checksum();
        page2.update_checksum();
        
        // Checksums should be different
        assert_ne!(page1.header.checksum, page2.header.checksum);
        
        // Both should validate
        assert!(page1.validate_checksum().is_ok());
        assert!(page2.validate_checksum().is_ok());
    }
    
    #[test]
    fn test_batch_validation() {
        let mut validator = BatchValidator::new();
        
        // Create some pages
        let mut page1 = Page::new(PageId(1), PageFlags::LEAF);
        let mut page2 = Page::new(PageId(2), PageFlags::BRANCH);
        let mut page3 = Page::new(PageId(3), PageFlags::LEAF);
        
        // Update checksums for page1 and page2
        page1.update_checksum();
        page2.update_checksum();
        // page3 has no checksum
        
        // All should validate
        let pages = vec![
            (PageId(1), page1.as_ref()),
            (PageId(2), page2.as_ref()),
            (PageId(3), page3.as_ref()),
        ];
        assert!(validator.validate_pages(&pages[..]).is_ok());
        assert!(validator.failed_pages().is_empty());
        
        // Corrupt page1
        page1.header.num_keys = 99;
        
        // Validation should fail
        let pages = vec![
            (PageId(1), page1.as_ref()),
            (PageId(2), page2.as_ref()),
            (PageId(3), page3.as_ref()),
        ];
        assert!(validator.validate_pages(&pages[..]).is_err());
        assert_eq!(validator.failed_pages().len(), 1);
        assert_eq!(validator.failed_pages()[0].0, PageId(1));
    }
    
    #[test]
    fn test_no_checksum_validation() {
        let page = Page::new(PageId(1), PageFlags::LEAF);
        
        // Page with no checksum should validate successfully
        assert_eq!(page.header.checksum, 0);
        assert!(page.validate_checksum().is_ok());
    }
}