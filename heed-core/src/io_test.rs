#[cfg(test)]
mod tests {
    use crate::io::{IoBackend, MmapBackend};
    use crate::page::{Page, PageFlags};
    use crate::error::PageId;
    use tempfile::TempDir;
    
    #[test]
    fn test_io_backend_basic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        
        // Create backend
        let backend = MmapBackend::with_options(&path, 1024 * 1024).unwrap();
        
        // Create and write a page
        let page = Page::new(PageId(10), PageFlags::LEAF);
        backend.write_page(&page).unwrap();
        
        // Read it back
        let read_page = backend.read_page(PageId(10)).unwrap();
        assert_eq!(read_page.header.pgno, 10);
        assert_eq!(read_page.header.flags, PageFlags::LEAF);
        
        // Test sync
        backend.sync().unwrap();
        
        // Test size
        assert!(backend.size_in_pages() > 0);
    }
    
    #[test]
    fn test_io_backend_grow() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        
        // Create small backend
        let backend = MmapBackend::with_options(&path, 16 * 1024).unwrap();
        let initial_size = backend.size_in_pages();
        
        // Grow it
        backend.grow(initial_size * 2).unwrap();
        
        // Check new size
        assert_eq!(backend.size_in_pages(), initial_size * 2);
    }
}