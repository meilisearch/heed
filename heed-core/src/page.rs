//! Page management and structures
//!
//! Pages are the fundamental unit of storage in the database.
//! Each page is aligned to the page size (typically 4KB) and contains
//! a header followed by data.

use std::mem::{size_of, MaybeUninit};
use std::slice;
use std::borrow::Cow;
use std::ptr;
use bitflags::bitflags;
use static_assertions::const_assert;
use crate::error::{Error, Result, PageId, PageType};
use crate::comparator::{Comparator, LexicographicComparator};

/// Default page size constant
pub const PAGE_SIZE: usize = 4096;

/// Maximum value size that can be stored inline (not in overflow pages)
/// This is roughly 1/4 of a page to allow for reasonable node density
pub const MAX_VALUE_SIZE: usize = PAGE_SIZE / 4;

const_assert!(PAGE_SIZE >= 512);
const_assert!(PAGE_SIZE.is_power_of_two());

bitflags! {
    /// Flags for page types and states
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PageFlags: u16 {
        /// Branch page (internal B+tree node)
        const BRANCH = 0x01;
        /// Leaf page (contains actual data)
        const LEAF = 0x02;
        /// Overflow page (for large values)
        const OVERFLOW = 0x04;
        /// Meta page (database metadata)
        const META = 0x08;
        /// Page is dirty (modified in current transaction)
        const DIRTY = 0x10;
        /// Page has duplicates
        const DUPFIXED = 0x20;
        /// Subtree root page
        const SUBP = 0x40;
        /// Fake leaf page for append mode
        const LOOSE = 0x80;
        /// Persistent flags mask
        const PERSISTENT = Self::BRANCH.bits() | Self::LEAF.bits() | 
                          Self::OVERFLOW.bits() | Self::META.bits();
    }
}

/// Page header structure
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    /// Page number
    pub pgno: u64,
    /// Page flags
    pub flags: PageFlags,
    /// Number of items on page
    pub num_keys: u16,
    /// Lower bound of free space
    pub lower: u16,
    /// Upper bound of free space  
    pub upper: u16,
    /// Overflow page count (or parent page for branch)
    pub overflow: u32,
    /// Page checksum (CRC32) - 0 means no checksum
    pub checksum: u32,
}

impl PageHeader {
    /// Size of the page header
    pub const SIZE: usize = size_of::<Self>();
    
    /// Create a new page header
    pub fn new(pgno: u64, flags: PageFlags) -> Self {
        Self {
            pgno,
            flags,
            num_keys: 0,
            lower: Self::SIZE as u16,
            upper: PAGE_SIZE as u16,
            overflow: 0,
            checksum: 0,
        }
    }
    
    /// Get the page type
    pub fn page_type(&self) -> PageType {
        if self.flags.contains(PageFlags::BRANCH) {
            PageType::Branch
        } else if self.flags.contains(PageFlags::LEAF) {
            PageType::Leaf
        } else if self.flags.contains(PageFlags::OVERFLOW) {
            PageType::Overflow
        } else if self.flags.contains(PageFlags::META) {
            PageType::Meta
        } else {
            PageType::Free
        }
    }
    
    /// Get available space on page
    pub fn free_space(&self) -> usize {
        (self.upper - self.lower) as usize
    }
}

/// A page in the database
#[repr(C, align(4096))]
pub struct Page {
    /// Page header
    pub header: PageHeader,
    /// Page data
    pub data: [u8; PAGE_SIZE - PageHeader::SIZE],
}

// Ensure Page is exactly PAGE_SIZE
const_assert!(size_of::<Page>() == PAGE_SIZE);
const_assert!(std::mem::align_of::<Page>() == PAGE_SIZE);

impl Page {
    /// Create a new empty page
    pub fn new(pgno: PageId, flags: PageFlags) -> Box<Self> {
        let mut page = Box::new(MaybeUninit::<Page>::uninit());
        unsafe {
            let page_ptr = page.as_mut_ptr();
            
            // Initialize header
            (*page_ptr).header = PageHeader::new(pgno.0, flags);
            
            // Zero out data section
            ptr::write_bytes((*page_ptr).data.as_mut_ptr(), 0, PAGE_SIZE - PageHeader::SIZE);
            
            page.assume_init()
        }
    }
    
    /// Create a page from a MetaPage
    pub fn from_meta(meta: &crate::meta::MetaPage, page_id: PageId) -> Box<Self> {
        let mut page = Self::new(page_id, PageFlags::META);
        
        // Copy the meta page data into the page data area (after the header)
        unsafe {
            let meta_ptr = meta as *const crate::meta::MetaPage as *const u8;
            let dst_ptr = page.data.as_mut_ptr();
            ptr::copy_nonoverlapping(meta_ptr, dst_ptr, size_of::<crate::meta::MetaPage>());
        }
        
        page
    }
    
    /// Create a page from raw bytes (zero-copy)
    /// 
    /// # Safety
    /// The caller must ensure the bytes are properly aligned and valid
    pub unsafe fn from_raw(bytes: &[u8]) -> &Self {
        assert_eq!(bytes.len(), PAGE_SIZE);
        assert_eq!(bytes.as_ptr() as usize % PAGE_SIZE, 0, "Page must be aligned");
        unsafe { &*(bytes.as_ptr() as *const Page) }
    }
    
    /// Create a mutable page from raw bytes
    /// 
    /// # Safety
    /// The caller must ensure the bytes are properly aligned and valid
    pub unsafe fn from_raw_mut(bytes: &mut [u8]) -> &mut Self {
        assert_eq!(bytes.len(), PAGE_SIZE);
        assert_eq!(bytes.as_ptr() as usize % PAGE_SIZE, 0, "Page must be aligned");
        unsafe { &mut *(bytes.as_mut_ptr() as *mut Page) }
    }
    
    /// Get page as bytes
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self as *const _ as *const u8, PAGE_SIZE)
        }
    }
    
    /// Get mutable page as bytes
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self as *mut _ as *mut u8, PAGE_SIZE)
        }
    }
    
    /// Get pointer array for keys (for branch/leaf pages)
    pub fn ptrs(&self) -> &[u16] {
        let num_keys = self.header.num_keys as usize;
        
        // For branch pages using branch_v2, we need to skip the branch header
        let offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch_v2::BranchHeader::SIZE
        } else {
            0
        };
        
        unsafe {
            slice::from_raw_parts(
                self.data.as_ptr().add(offset) as *const u16,
                num_keys,
            )
        }
    }
    
    /// Get mutable pointer array
    pub fn ptrs_mut(&mut self) -> &mut [u16] {
        let num_keys = self.header.num_keys as usize;
        
        // For branch pages using branch_v2, we need to skip the branch header
        let offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch_v2::BranchHeader::SIZE
        } else {
            0
        };
        
        unsafe {
            slice::from_raw_parts_mut(
                self.data.as_mut_ptr().add(offset) as *mut u16,
                num_keys,
            )
        }
    }
    
    /// Get node at index
    pub fn node(&self, index: usize) -> Result<Node> {
        if index >= self.header.num_keys as usize {
            return Err(Error::InvalidParameter("node index out of bounds"));
        }
        
        let ptr = self.ptrs()[index];
        
        // Check that the pointer is within the used area of the page
        // Pointers point to absolute offsets in the page, nodes are stored between header.upper and PAGE_SIZE
        if ptr < self.header.upper || ptr >= PAGE_SIZE as u16 {
            return Err(Error::Corruption {
                details: "Node pointer out of bounds".into(),
                page_id: Some(PageId(self.header.pgno)),
            });
        }
        
        let node_ptr = unsafe {
            self.data.as_ptr().add(ptr as usize - PageHeader::SIZE) as *const NodeHeader
        };
        
        Ok(Node {
            header: unsafe { *node_ptr },
            page: self,
            offset: ptr,
        })
    }
    
    /// Get a mutable node data reference by index
    pub fn node_data_mut(&mut self, index: usize) -> Result<NodeDataMut> {
        let num_keys = self.header.num_keys as usize;
        if index >= num_keys {
            return Err(Error::InvalidParameter("Node index out of bounds"));
        }
        
        let ptr = self.ptrs()[index];
        
        // Check that the pointer is within bounds
        if ptr < self.header.upper || ptr >= PAGE_SIZE as u16 {
            return Err(Error::Corruption {
                details: "Node pointer out of bounds".into(),
                page_id: Some(PageId(self.header.pgno)),
            });
        }
        
        Ok(NodeDataMut {
            page: self,
            offset: ptr,
        })
    }
    
    /// Add a node to the page at the correct sorted position
    pub fn add_node_sorted(&mut self, key: &[u8], value: &[u8]) -> Result<usize> {
        self.add_node_sorted_with_comparator::<LexicographicComparator>(key, value)
    }
    
    /// Add a node to the page at the correct sorted position with a custom comparator
    pub fn add_node_sorted_with_comparator<C: Comparator>(&mut self, key: &[u8], value: &[u8]) -> Result<usize> {
        self.add_node_sorted_internal_with_comparator::<C>(key, value, false, 0)
    }
    
    /// Add a node with overflow page reference
    pub fn add_node_sorted_overflow(&mut self, key: &[u8], overflow_page_id: PageId) -> Result<usize> {
        self.add_node_sorted_overflow_with_comparator::<LexicographicComparator>(key, overflow_page_id)
    }
    
    /// Add a node with overflow page reference with a custom comparator
    pub fn add_node_sorted_overflow_with_comparator<C: Comparator>(&mut self, key: &[u8], overflow_page_id: PageId) -> Result<usize> {
        // For overflow nodes, we store the page ID as the "value"
        let page_bytes = overflow_page_id.0.to_le_bytes();
        self.add_node_sorted_internal_with_comparator::<C>(key, &page_bytes, true, std::mem::size_of::<u64>())
    }
    
    /// Internal method to add a node
    fn add_node_sorted_internal(&mut self, key: &[u8], value: &[u8], is_overflow: bool, value_size_override: usize) -> Result<usize> {
        self.add_node_sorted_internal_with_comparator::<LexicographicComparator>(key, value, is_overflow, value_size_override)
    }
    
    /// Internal method to add a node with a custom comparator
    fn add_node_sorted_internal_with_comparator<C: Comparator>(&mut self, key: &[u8], value: &[u8], is_overflow: bool, value_size_override: usize) -> Result<usize> {
        let actual_value_size = if is_overflow { value_size_override } else { value.len() };
        let node_size = NodeHeader::SIZE + key.len() + value.len();
        
        if self.header.free_space() < node_size + size_of::<u16>() {
            return Err(Error::Custom("Page full".into()));
        }
        
        // Find insertion position
        let insert_pos = match self.search_key_with_comparator::<C>(key)? {
            SearchResult::Found { index: _ } => {
                return Err(Error::Custom("Key already exists".into()));
            }
            SearchResult::NotFound { insert_pos } => insert_pos,
        };
        
        // Allocate space from upper bound, ensuring alignment for NodeHeader
        self.header.upper -= node_size as u16;
        // Align to 2-byte boundary for NodeHeader
        if self.header.upper % 2 != 0 {
            self.header.upper -= 1;
        }
        let node_offset = self.header.upper;
        
        // Write node header
        let mut node_header = NodeHeader {
            flags: NodeFlags::empty(),
            ksize: key.len() as u16,
            lo: (actual_value_size & 0xffff) as u16,
            hi: (actual_value_size >> 16) as u16,
        };
        
        if is_overflow {
            node_header.flags.insert(NodeFlags::BIGDATA);
        }
        
        unsafe {
            // node_offset is the absolute offset in the page, we need to subtract PageHeader::SIZE to get offset in data array
            let data_offset = node_offset as usize - PageHeader::SIZE;
            let node_ptr = self.data.as_mut_ptr().add(data_offset) as *mut NodeHeader;
            
            // Verify alignment
            debug_assert_eq!(node_ptr as usize % 2, 0, "NodeHeader must be 2-byte aligned");
            
            *node_ptr = node_header;
            
            // Write key
            let key_ptr = node_ptr.add(1) as *mut u8;
            ptr::copy_nonoverlapping(key.as_ptr(), key_ptr, key.len());
            
            // Write value (or overflow page ID)
            let val_ptr = key_ptr.add(key.len());
            ptr::copy_nonoverlapping(value.as_ptr(), val_ptr, value.len());
        }
        
        // Insert pointer at the correct position
        self.insert_ptr(insert_pos, node_offset);
        self.header.num_keys += 1;
        self.header.lower += size_of::<u16>() as u16;
        
        Ok(insert_pos)
    }
    
    /// Add a node to the page (unsorted, appends at end)
    pub fn add_node(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.add_node_sorted(key, value)?;
        Ok(())
    }
    
    /// Insert pointer at index
    fn insert_ptr(&mut self, index: usize, ptr: u16) {
        // Get current count of pointers
        let current_count = self.header.num_keys as usize;
        
        // Ensure we have space for the new pointer
        assert!(index <= current_count, "Insert index out of bounds");
        
        // For branch pages using branch_v2, we need to skip the branch header
        let offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch_v2::BranchHeader::SIZE
        } else {
            0
        };
        
        // Get pointer to the start of the pointer array
        let ptrs_ptr = unsafe { self.data.as_mut_ptr().add(offset) as *mut u16 };
        
        // Shift existing pointers if needed
        if index < current_count {
            unsafe {
                let src = ptrs_ptr.add(index);
                let dst = ptrs_ptr.add(index + 1);
                ptr::copy(src, dst, current_count - index);
            }
        }
        
        // Insert the new pointer
        unsafe {
            *ptrs_ptr.add(index) = ptr;
        }
    }
    
    /// Search for a key using binary search (assumes sorted nodes)
    pub fn search_key(&self, key: &[u8]) -> Result<SearchResult> {
        self.search_key_with_comparator::<LexicographicComparator>(key)
    }
    
    /// Search for a key using binary search with a custom comparator
    pub fn search_key_with_comparator<C: Comparator>(&self, key: &[u8]) -> Result<SearchResult> {
        if self.header.num_keys == 0 {
            return Ok(SearchResult::NotFound { insert_pos: 0 });
        }
        
        // Binary search through sorted nodes
        let mut left = 0;
        let mut right = self.header.num_keys as usize;
        
        while left < right {
            let mid = left + (right - left) / 2;
            let node = self.node(mid)?;
            let node_key = node.key()?;
            
            match C::compare(key, node_key) {
                std::cmp::Ordering::Less => right = mid,
                std::cmp::Ordering::Greater => left = mid + 1,
                std::cmp::Ordering::Equal => return Ok(SearchResult::Found { index: mid }),
            }
        }
        
        Ok(SearchResult::NotFound { insert_pos: left })
    }
    
    /// Get the middle node for splitting
    pub fn middle_node(&self) -> Result<(Vec<u8>, usize)> {
        let mid_idx = self.header.num_keys as usize / 2;
        let node = self.node(mid_idx)?;
        let key = node.key()?.to_vec();
        Ok((key, mid_idx))
    }
    
    /// Split this page into two pages, returning nodes for the right page
    pub fn split(&self) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>)> {
        let mid_idx = self.header.num_keys as usize / 2;
        let mut right_nodes = Vec::new();
        
        // Collect nodes for the right page
        for i in mid_idx..self.header.num_keys as usize {
            let node = self.node(i)?;
            let key = node.key()?.to_vec();
            let value = node.value()?.into_owned();
            right_nodes.push((key, value));
        }
        
        // Get the median key
        let median_node = self.node(mid_idx)?;
        let median_key = median_node.key()?.to_vec();
        
        Ok((right_nodes, median_key))
    }
    
    /// Remove nodes starting from index
    pub fn truncate(&mut self, from_index: usize) {
        if from_index >= self.header.num_keys as usize {
            return;
        }
        
        // Update header
        self.header.num_keys = from_index as u16;
        
        // Calculate new lower bound, accounting for branch header if present
        let header_offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch_v2::BranchHeader::SIZE
        } else {
            0
        };
        
        self.header.lower = PageHeader::SIZE as u16 + header_offset as u16 + (from_index * size_of::<u16>()) as u16;
        
        // Note: We don't reclaim the space from removed nodes, they'll be
        // overwritten when new nodes are added
    }
    
    /// Remove a node at the specified index
    pub fn remove_node(&mut self, index: usize) -> Result<()> {
        if index >= self.header.num_keys as usize {
            return Err(Error::InvalidParameter("Node index out of bounds"));
        }
        
        // For branch pages using branch_v2, we need to skip the branch header
        let offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch_v2::BranchHeader::SIZE
        } else {
            0
        };
        
        // Get pointer to the start of the pointer array
        let ptrs_ptr = unsafe { self.data.as_mut_ptr().add(offset) as *mut u16 };
        
        // Shift pointers after the removed one
        if index < self.header.num_keys as usize - 1 {
            unsafe {
                let src = ptrs_ptr.add(index + 1);
                let dst = ptrs_ptr.add(index);
                ptr::copy(src, dst, self.header.num_keys as usize - index - 1);
            }
        }
        
        // Update header
        self.header.num_keys -= 1;
        self.header.lower -= size_of::<u16>() as u16;
        
        // Note: We don't reclaim the space from the removed node, it will be
        // overwritten when new nodes are added
        
        Ok(())
    }
    
    /// Clear all nodes from the page
    pub fn clear(&mut self) {
        self.header.num_keys = 0;
        
        // Calculate initial lower bound, accounting for branch header if present
        let header_offset = if self.header.flags.contains(PageFlags::BRANCH) {
            crate::branch_v2::BranchHeader::SIZE
        } else {
            0
        };
        
        self.header.lower = PageHeader::SIZE as u16 + header_offset as u16;
        self.header.upper = PAGE_SIZE as u16;
        // Note: We don't need to clear the data, it will be overwritten
    }
}

/// Result of searching for a key in a page
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchResult {
    /// Key was found at index
    Found { index: usize },
    /// Key was not found, would be inserted at position
    NotFound { insert_pos: usize },
}

bitflags! {
    /// Node flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct NodeFlags: u16 {
        /// Node contains a sub-page
        const BIGDATA = 0x01;
        /// Node contains a sub-database
        const SUBDATA = 0x02;
        /// Node contains duplicate data
        const DUPDATA = 0x04;
        /// Node value is in overflow pages
        const OVERFLOW = 0x08;
    }
}

/// Node header within a page
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NodeHeader {
    /// Node flags
    pub flags: NodeFlags,
    /// Key size
    pub ksize: u16,
    /// Low 16 bits of value size
    pub lo: u16,
    /// High 16 bits of value size
    pub hi: u16,
}

impl NodeHeader {
    /// Size of node header
    pub const SIZE: usize = size_of::<Self>();
    
    /// Get value size
    pub fn value_size(&self) -> usize {
        (self.lo as usize) | ((self.hi as usize) << 16)
    }
    
    /// Set value size
    pub fn set_value_size(&mut self, size: usize) {
        self.lo = (size & 0xffff) as u16;
        self.hi = (size >> 16) as u16;
    }
}

/// A node within a page
pub struct Node<'a> {
    /// Node header
    pub header: NodeHeader,
    /// Reference to containing page
    page: &'a Page,
    /// Offset within page data
    offset: u16,
}

impl<'a> Node<'a> {
    /// Get the key bytes
    pub fn key(&self) -> Result<&'a [u8]> {
        let key_offset = (self.offset as usize - PageHeader::SIZE) + NodeHeader::SIZE;
        let key_len = self.header.ksize as usize;
        
        // The node data starts at self.offset and extends towards PAGE_SIZE
        // We need to ensure key_offset + key_len doesn't exceed the data array bounds
        if key_offset + key_len > self.page.data.len() {
            return Err(Error::Corruption {
                details: "Node key extends beyond page".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }
        
        Ok(unsafe {
            slice::from_raw_parts(
                self.page.data.as_ptr().add(key_offset),
                key_len,
            )
        })
    }
    
    /// Get the value bytes
    pub fn value(&self) -> Result<Cow<'a, [u8]>> {
        let val_offset = (self.offset as usize - PageHeader::SIZE) + NodeHeader::SIZE + self.header.ksize as usize;
        let val_len = self.header.value_size();
        
        if self.header.flags.contains(NodeFlags::BIGDATA) {
            // Value is in overflow pages
            // Read u64 from potentially unaligned location
            let mut pgno_bytes = [0u8; 8];
            unsafe {
                ptr::copy_nonoverlapping(
                    self.page.data.as_ptr().add(val_offset),
                    pgno_bytes.as_mut_ptr(),
                    8
                );
            }
            let pgno = u64::from_le_bytes(pgno_bytes);
            // For now, return an error indicating overflow pages need to be loaded
            // The caller should use a transaction to load the overflow value
            return Err(Error::Custom(format!("Value in overflow page {}", pgno).into()));
        }
        
        // Ensure value doesn't extend beyond the data array
        if val_offset + val_len > self.page.data.len() {
            return Err(Error::Corruption {
                details: "Node value extends beyond page".into(),
                page_id: Some(PageId(self.page.header.pgno)),
            });
        }
        
        Ok(Cow::Borrowed(unsafe {
            slice::from_raw_parts(
                self.page.data.as_ptr().add(val_offset),
                val_len,
            )
        }))
    }
    
    /// Get page number for branch nodes
    pub fn page_number(&self) -> Result<PageId> {
        if !self.page.header.flags.contains(PageFlags::BRANCH) {
            return Err(Error::InvalidOperation("Not a branch page"));
        }
        
        // The offset is an absolute position in the page, we need to convert to data array offset
        let data_offset = self.offset as usize - PageHeader::SIZE;
        let val_offset = data_offset + NodeHeader::SIZE + self.header.ksize as usize;
        
        // Read u64 from potentially unaligned location
        let mut pgno_bytes = [0u8; 8];
        unsafe {
            ptr::copy_nonoverlapping(
                self.page.data.as_ptr().add(val_offset),
                pgno_bytes.as_mut_ptr(),
                8
            );
        }
        let pgno = u64::from_le_bytes(pgno_bytes);
        
        Ok(PageId(pgno))
    }
    
    /// Get overflow page ID if this is an overflow value
    pub fn overflow_page(&self) -> Result<Option<PageId>> {
        if !self.header.flags.contains(NodeFlags::BIGDATA) {
            return Ok(None);
        }
        
        let val_offset = (self.offset as usize - PageHeader::SIZE) + NodeHeader::SIZE + self.header.ksize as usize;
        
        // Read u64 from potentially unaligned location
        let mut pgno_bytes = [0u8; 8];
        unsafe {
            ptr::copy_nonoverlapping(
                self.page.data.as_ptr().add(val_offset),
                pgno_bytes.as_mut_ptr(),
                8
            );
        }
        let pgno = u64::from_le_bytes(pgno_bytes);
        
        Ok(Some(PageId(pgno)))
    }
}

/// Mutable node data accessor
pub struct NodeDataMut<'a> {
    page: &'a mut Page,
    offset: u16,
}

impl<'a> NodeDataMut<'a> {
    /// Set the value of this node
    pub fn set_value(&mut self, new_value: &[u8]) -> Result<()> {
        let node_ptr = unsafe {
            self.page.data.as_ptr().add(self.offset as usize - PageHeader::SIZE) as *const NodeHeader
        };
        let header = unsafe { *node_ptr };
        
        // Check if new value fits
        let old_value_size = header.value_size() as usize;
        if new_value.len() != old_value_size && !header.flags.contains(NodeFlags::BIGDATA) {
            return Err(Error::InvalidParameter("Cannot change value size without reallocation"));
        }
        
        // Copy new value
        let val_offset = self.offset as usize - PageHeader::SIZE + NodeHeader::SIZE + header.ksize as usize;
        unsafe {
            std::ptr::copy_nonoverlapping(
                new_value.as_ptr(),
                self.page.data.as_mut_ptr().add(val_offset),
                new_value.len().min(old_value_size)
            );
        }
        
        Ok(())
    }
    
    /// Set this node to use an overflow page
    pub fn set_overflow(&mut self, overflow_id: PageId) -> Result<()> {
        let node_ptr = unsafe {
            self.page.data.as_mut_ptr().add(self.offset as usize - PageHeader::SIZE) as *mut NodeHeader
        };
        let header = unsafe { &mut *node_ptr };
        
        // Update flags
        header.flags.insert(NodeFlags::BIGDATA);
        
        // Store overflow page ID as value
        let val_offset = self.offset as usize - PageHeader::SIZE + NodeHeader::SIZE + header.ksize as usize;
        let pgno_bytes = overflow_id.0.to_le_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(
                pgno_bytes.as_ptr(),
                self.page.data.as_mut_ptr().add(val_offset),
                8
            );
        }
        
        // Update value size to indicate it's an overflow reference
        let vsize = std::mem::size_of::<u64>() as u32;
        header.lo = vsize as u16;
        header.hi = (vsize >> 16) as u16;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_page_creation() {
        let page = Page::new(PageId(1), PageFlags::LEAF);
        assert_eq!(page.header.pgno, 1);
        assert_eq!(page.header.flags, PageFlags::LEAF);
        assert_eq!(page.header.num_keys, 0);
        assert_eq!(page.header.free_space(), PAGE_SIZE - PageHeader::SIZE);
    }
    
    #[test]
    fn test_add_node() {
        let mut page = Page::new(PageId(1), PageFlags::LEAF);
        
        page.add_node(b"key1", b"value1").unwrap();
        assert_eq!(page.header.num_keys, 1);
        
        let node = page.node(0).unwrap();
        assert_eq!(node.key().unwrap(), b"key1");
        assert_eq!(node.value().unwrap().as_ref(), b"value1");
    }
    
    #[test]
    fn test_search_key() {
        let mut page = Page::new(PageId(1), PageFlags::LEAF);
        
        page.add_node(b"aaa", b"1").unwrap();
        page.add_node(b"ccc", b"3").unwrap();
        page.add_node(b"bbb", b"2").unwrap();
        
        match page.search_key(b"bbb").unwrap() {
            SearchResult::Found { index } => {
                let node = page.node(index).unwrap();
                assert_eq!(node.key().unwrap(), b"bbb");
            }
            _ => panic!("Key should be found"),
        }
        
        match page.search_key(b"ddd").unwrap() {
            SearchResult::NotFound { insert_pos } => {
                assert_eq!(insert_pos, 3);
            }
            _ => panic!("Key should not be found"),
        }
    }
    
    #[test]
    fn test_add_single_node() {
        let mut page = Page::new(PageId(1), PageFlags::LEAF);
        assert_eq!(page.header.num_keys, 0);
        
        page.add_node_sorted(b"key1", b"value1").unwrap();
        assert_eq!(page.header.num_keys, 1);
        
        // Check that node can be retrieved
        let node = page.node(0).unwrap();
        assert_eq!(node.key().unwrap(), b"key1");
        assert_eq!(node.value().unwrap().as_ref(), b"value1");
    }
}