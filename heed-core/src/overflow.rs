//! Overflow page management for large values
//! 
//! When a value is too large to fit in a regular page, it's stored
//! in overflow pages. The node in the regular page contains a reference
//! to the first overflow page.

use crate::error::{Error, Result, PageId};
use crate::page::{PageFlags, PAGE_SIZE};
use crate::txn::{Transaction, mode::Mode};

/// Maximum value size that fits in a regular page
/// We need space for: NodeHeader (8 bytes) + key + value + page header + pointers
/// Conservative estimate: PAGE_SIZE / 4
pub const MAX_INLINE_VALUE_SIZE: usize = PAGE_SIZE / 4;

/// Overflow page header
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OverflowHeader {
    /// Next overflow page (0 if this is the last page)
    pub next_page: u64,
    /// Total size of the value (only set in first overflow page)
    pub total_size: u64,
}

impl OverflowHeader {
    /// Size of overflow header
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// Write a large value to overflow pages
pub fn write_overflow_value<'txn>(
    txn: &mut Transaction<'txn, crate::txn::Write>,
    value: &[u8],
) -> Result<PageId> {
    let total_size = value.len();
    // Account for both PageHeader and OverflowHeader
    let data_per_page = PAGE_SIZE - crate::page::PageHeader::SIZE - OverflowHeader::SIZE;
    let num_pages = (total_size + data_per_page - 1) / data_per_page;
    
    if num_pages == 0 {
        return Err(Error::InvalidParameter("Empty value for overflow"));
    }
    
    let mut first_page_id = None;
    let mut prev_page_id = None;
    let mut offset = 0;
    
    // Allocate and write overflow pages
    for i in 0..num_pages {
        let (page_id, page) = txn.alloc_page(PageFlags::OVERFLOW)?;
        
        if i == 0 {
            first_page_id = Some(page_id);
        }
        
        // Calculate how much data goes in this page
        let chunk_size = std::cmp::min(data_per_page, total_size - offset);
        let is_last = i == num_pages - 1;
        
        // Write overflow header
        let header = OverflowHeader {
            next_page: if is_last { 0 } else { 0 }, // Will be updated when we allocate next page
            total_size: if i == 0 { total_size as u64 } else { 0 },
        };
        
        unsafe {
            let header_ptr = page.data.as_mut_ptr() as *mut OverflowHeader;
            *header_ptr = header;
            
            // Write data after header
            let data_ptr = page.data.as_mut_ptr().add(OverflowHeader::SIZE);
            std::ptr::copy_nonoverlapping(
                value.as_ptr().add(offset),
                data_ptr,
                chunk_size
            );
        }
        
        // Update previous page's next pointer
        if let Some(prev_id) = prev_page_id {
            let prev_page = txn.get_page_mut(prev_id)?;
            unsafe {
                let header_ptr = prev_page.data.as_mut_ptr() as *mut OverflowHeader;
                (*header_ptr).next_page = page_id.0;
            }
        }
        
        prev_page_id = Some(page_id);
        offset += chunk_size;
    }
    
    Ok(first_page_id.unwrap())
}

/// Read a large value from overflow pages
pub fn read_overflow_value<'txn, M: Mode>(
    txn: &'txn Transaction<'txn, M>,
    first_page_id: PageId,
) -> Result<Vec<u8>> {
    let first_page = txn.get_page(first_page_id)?;
    
    // Check it's an overflow page
    if !first_page.header.flags.contains(PageFlags::OVERFLOW) {
        return Err(Error::Corruption {
            details: "Expected overflow page".into(),
            page_id: Some(first_page_id),
        });
    }
    
    // Read header to get total size
    let header = unsafe {
        *(first_page.data.as_ptr() as *const OverflowHeader)
    };
    
    let total_size = header.total_size as usize;
    let mut result = Vec::with_capacity(total_size);
    // Account for both PageHeader and OverflowHeader
    let data_per_page = PAGE_SIZE - crate::page::PageHeader::SIZE - OverflowHeader::SIZE;
    
    let mut current_page_id = first_page_id;
    let mut bytes_read = 0;
    
    loop {
        let page = txn.get_page(current_page_id)?;
        let header = unsafe {
            *(page.data.as_ptr() as *const OverflowHeader)
        };
        
        // Calculate how much data is in this page
        let chunk_size = std::cmp::min(data_per_page, total_size - bytes_read);
        
        // Read data
        unsafe {
            let data_ptr = page.data.as_ptr().add(OverflowHeader::SIZE);
            result.extend_from_slice(std::slice::from_raw_parts(data_ptr, chunk_size));
        }
        
        bytes_read += chunk_size;
        
        // Move to next page
        if header.next_page == 0 {
            break;
        }
        current_page_id = PageId(header.next_page);
    }
    
    if bytes_read != total_size {
        return Err(Error::Corruption {
            details: format!("Overflow value size mismatch: expected {}, got {}", total_size, bytes_read).into(),
            page_id: Some(first_page_id),
        });
    }
    
    Ok(result)
}

/// Check if a value should be stored in overflow pages
pub fn needs_overflow(key_size: usize, value_size: usize) -> bool {
    // Conservative check: if key + value + headers would take more than 1/4 of page
    key_size + value_size + 32 > MAX_INLINE_VALUE_SIZE
}

/// Free all overflow pages in a chain
pub fn free_overflow_chain(
    txn: &mut Transaction<'_, crate::txn::Write>,
    first_page_id: PageId,
) -> Result<()> {
    let mut current_page_id = first_page_id;
    
    loop {
        let page = txn.get_page(current_page_id)?;
        
        // Check it's an overflow page
        if !page.header.flags.contains(PageFlags::OVERFLOW) {
            return Err(Error::Corruption {
                details: "Expected overflow page in chain".into(),
                page_id: Some(current_page_id),
            });
        }
        
        // Get next page before freeing this one
        let header = unsafe {
            *(page.data.as_ptr() as *const OverflowHeader)
        };
        let next_page = header.next_page;
        
        // Free current page
        txn.free_page(current_page_id)?;
        
        // Move to next page
        if next_page == 0 {
            break;
        }
        current_page_id = PageId(next_page);
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvBuilder;
    use tempfile::TempDir;
    
    #[test]
    fn test_overflow_write_read() {
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())
            .unwrap();
        
        // Create a large value that requires multiple overflow pages
        let large_value = vec![0xAB; 10 * 1024]; // 10KB
        
        let mut txn = env.begin_write_txn().unwrap();
        
        // Write to overflow pages
        let overflow_id = write_overflow_value(&mut txn, &large_value).unwrap();
        
        // Read back
        let read_value = read_overflow_value(&txn, overflow_id).unwrap();
        assert_eq!(read_value, large_value);
        
        txn.commit().unwrap();
        
        // Read in new transaction
        let txn = env.begin_txn().unwrap();
        let read_value = read_overflow_value(&txn, overflow_id).unwrap();
        assert_eq!(read_value, large_value);
    }
    
    #[test]
    fn test_needs_overflow() {
        assert!(!needs_overflow(10, 100));
        assert!(!needs_overflow(100, 500));
        assert!(needs_overflow(100, 2000));
        assert!(needs_overflow(500, 1500));
    }
}