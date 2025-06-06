//! Fixed-size value optimizations
//!
//! This module provides optimizations for databases with fixed-size keys or values.
//! When all keys have the same size, we can use more efficient storage and comparison.

use crate::error::{Error, Result};
use crate::page::{Page, PageFlags};
use std::mem::size_of;

/// Trait for fixed-size types that can be stored directly
pub trait FixedSize: Sized {
    /// The size in bytes of this type
    const SIZE: usize;
    
    /// Convert to bytes
    fn to_bytes(&self) -> Vec<u8>;
    
    /// Convert from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self>;
}

/// Marker trait for types that can use integer key optimization
pub trait IntegerKey: FixedSize + Ord + Copy {}

// Implement FixedSize for common integer types
macro_rules! impl_fixed_size {
    ($t:ty) => {
        impl FixedSize for $t {
            const SIZE: usize = size_of::<$t>();
            
            fn to_bytes(&self) -> Vec<u8> {
                self.to_be_bytes().to_vec()
            }
            
            fn from_bytes(bytes: &[u8]) -> Result<Self> {
                if bytes.len() != Self::SIZE {
                    return Err(Error::Custom(format!(
                        "Invalid byte length for {}: expected {}, got {}",
                        stringify!($t),
                        Self::SIZE,
                        bytes.len()
                    ).into()));
                }
                let mut arr = [0u8; Self::SIZE];
                arr.copy_from_slice(bytes);
                Ok(Self::from_be_bytes(arr))
            }
        }
        
        impl IntegerKey for $t {}
    };
}

impl_fixed_size!(u8);
impl_fixed_size!(u16);
impl_fixed_size!(u32);
impl_fixed_size!(u64);
impl_fixed_size!(u128);
impl_fixed_size!(i8);
impl_fixed_size!(i16);
impl_fixed_size!(i32);
impl_fixed_size!(i64);
impl_fixed_size!(i128);

/// Optimized page layout for fixed-size keys
pub struct FixedSizePage<'a, K: FixedSize + Ord> {
    page: &'a Page,
    _phantom: std::marker::PhantomData<K>,
}

impl<'a, K: FixedSize + Ord> FixedSizePage<'a, K> {
    /// Create a fixed-size page view
    pub fn new(page: &'a Page) -> Self {
        Self {
            page,
            _phantom: std::marker::PhantomData,
        }
    }
    
    /// Get the number of keys that fit in a page
    pub fn capacity() -> usize {
        // Account for page header and some overhead
        let available = crate::page::PAGE_SIZE - crate::page::PageHeader::SIZE - 16;
        available / (K::SIZE + size_of::<u64>()) // key + value pointer/size
    }
    
    /// Binary search for a key (optimized for fixed-size)
    pub fn search_key(&self, key: &K) -> Result<SearchResult> {
        let num_keys = self.page.header.num_keys as usize;
        if num_keys == 0 {
            return Ok(SearchResult::NotFound { insert_pos: 0 });
        }
        
        // Binary search with direct key comparison
        let mut left = 0;
        let mut right = num_keys;
        
        while left < right {
            let mid = left + (right - left) / 2;
            let node = self.page.node(mid)?;
            let node_key_bytes = node.key()?;
            let node_key = K::from_bytes(node_key_bytes)?;
            
            match node_key.cmp(key) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return Ok(SearchResult::Found { index: mid }),
            }
        }
        
        Ok(SearchResult::NotFound { insert_pos: left })
    }
}

/// Search result for fixed-size pages
#[derive(Debug, Clone, Copy)]
pub enum SearchResult {
    /// Key found at index
    Found { index: usize },
    /// Key not found, would be inserted at position
    NotFound { insert_pos: usize },
}

/// Database flags extension for fixed-size optimization
pub mod flags {
    use bitflags::bitflags;
    
    bitflags! {
        /// Extended database flags for fixed-size optimizations
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct ExtendedFlags: u32 {
            /// All keys have the same fixed size
            const FIXED_SIZE_KEY = 0x10000;
            /// All values have the same fixed size
            const FIXED_SIZE_VALUE = 0x20000;
            /// Keys are integers (enables additional optimizations)
            const INTEGER_KEY = 0x40000;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_fixed_size_u32() {
        let val: u32 = 42;
        let bytes = val.to_bytes();
        assert_eq!(bytes.len(), 4);
        
        let restored = u32::from_bytes(&bytes).unwrap();
        assert_eq!(restored, val);
        
        // Test big-endian encoding
        assert_eq!(bytes, [0, 0, 0, 42]);
    }
    
    #[test]
    fn test_fixed_size_i64() {
        let val: i64 = -12345;
        let bytes = val.to_bytes();
        assert_eq!(bytes.len(), 8);
        
        let restored = i64::from_bytes(&bytes).unwrap();
        assert_eq!(restored, val);
    }
    
    #[test]
    fn test_invalid_byte_length() {
        let bytes = [1, 2, 3]; // Wrong length for u32
        let result = u32::from_bytes(&bytes);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_capacity_calculation() {
        let capacity = FixedSizePage::<u32>::capacity();
        // Should fit many u32 keys in a 4KB page
        assert!(capacity > 100);
        assert!(capacity < 1000);
    }
}