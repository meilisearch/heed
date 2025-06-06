//! Node structures and operations for zero-copy access

use std::borrow::Cow;
use crate::error::{PageId, Result};
use crate::page::{NodeFlags, NodeHeader};

/// Value stored in a node
#[derive(Debug, Clone)]
pub enum NodeValue<'a> {
    /// Regular data value
    Data(Cow<'a, [u8]>),
    /// Reference to another page (for branch nodes)
    PageRef(PageId),
    /// Overflow page reference for large values
    Overflow { 
        /// First overflow page
        page: PageId, 
        /// Total size of the value
        size: u64 
    },
    /// Sub-database reference
    SubDb {
        /// Root page of sub-database
        root: PageId,
        /// Number of entries
        entries: u64,
    },
}

impl<'a> NodeValue<'a> {
    /// Get the value as bytes if it's data
    pub fn as_data(&self) -> Option<&[u8]> {
        match self {
            NodeValue::Data(cow) => Some(cow.as_ref()),
            _ => None,
        }
    }
    
    /// Get the page reference if it's a page ref
    pub fn as_page_ref(&self) -> Option<PageId> {
        match self {
            NodeValue::PageRef(id) => Some(*id),
            _ => None,
        }
    }
    
    /// Check if this is an overflow value
    pub fn is_overflow(&self) -> bool {
        matches!(self, NodeValue::Overflow { .. })
    }
    
    /// Convert to owned value
    pub fn into_owned(self) -> NodeValue<'static> {
        match self {
            NodeValue::Data(cow) => NodeValue::Data(Cow::Owned(cow.into_owned())),
            NodeValue::PageRef(id) => NodeValue::PageRef(id),
            NodeValue::Overflow { page, size } => NodeValue::Overflow { page, size },
            NodeValue::SubDb { root, entries } => NodeValue::SubDb { root, entries },
        }
    }
}

/// A key-value node with zero-copy access
#[derive(Debug, Clone)]
pub struct KeyValue<'a> {
    /// The key bytes
    pub key: Cow<'a, [u8]>,
    /// The value
    pub value: NodeValue<'a>,
}

impl<'a> KeyValue<'a> {
    /// Create a new key-value pair
    pub fn new(key: impl Into<Cow<'a, [u8]>>, value: NodeValue<'a>) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
    
    /// Create a data key-value pair
    pub fn data(key: impl Into<Cow<'a, [u8]>>, data: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            key: key.into(),
            value: NodeValue::Data(data.into()),
        }
    }
    
    /// Create a page reference key-value pair
    pub fn page_ref(key: impl Into<Cow<'a, [u8]>>, page: PageId) -> Self {
        Self {
            key: key.into(),
            value: NodeValue::PageRef(page),
        }
    }
    
    /// Convert to owned
    pub fn into_owned(self) -> KeyValue<'static> {
        KeyValue {
            key: Cow::Owned(self.key.into_owned()),
            value: self.value.into_owned(),
        }
    }
    
    /// Borrow the key-value pair
    pub fn as_ref(&self) -> KeyValue<'_> {
        KeyValue {
            key: Cow::Borrowed(self.key.as_ref()),
            value: match &self.value {
                NodeValue::Data(cow) => NodeValue::Data(Cow::Borrowed(cow.as_ref())),
                NodeValue::PageRef(id) => NodeValue::PageRef(*id),
                NodeValue::Overflow { page, size } => NodeValue::Overflow { page: *page, size: *size },
                NodeValue::SubDb { root, entries } => NodeValue::SubDb { root: *root, entries: *entries },
            },
        }
    }
}

/// Builder for creating nodes efficiently
pub struct NodeBuilder {
    flags: NodeFlags,
    key_size: usize,
    value_size: usize,
}

impl NodeBuilder {
    /// Create a new node builder
    pub fn new() -> Self {
        Self {
            flags: NodeFlags::empty(),
            key_size: 0,
            value_size: 0,
        }
    }
    
    /// Set the key size
    pub fn key_size(mut self, size: usize) -> Self {
        self.key_size = size;
        self
    }
    
    /// Set the value size
    pub fn value_size(mut self, size: usize) -> Self {
        self.value_size = size;
        self
    }
    
    /// Mark as containing big data
    pub fn big_data(mut self) -> Self {
        self.flags.insert(NodeFlags::BIGDATA);
        self
    }
    
    /// Mark as containing sub-data
    pub fn sub_data(mut self) -> Self {
        self.flags.insert(NodeFlags::SUBDATA);
        self
    }
    
    /// Build the node header
    pub fn build(self) -> NodeHeader {
        NodeHeader {
            flags: self.flags,
            ksize: self.key_size as u16,
            lo: (self.value_size & 0xffff) as u16,
            hi: (self.value_size >> 16) as u16,
        }
    }
    
    /// Calculate total size needed for this node
    pub fn total_size(&self) -> usize {
        NodeHeader::SIZE + self.key_size + self.value_size
    }
}

impl Default for NodeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over nodes in a page
pub struct NodeIterator<'a> {
    page: &'a crate::page::Page,
    current: usize,
    count: usize,
}

impl<'a> NodeIterator<'a> {
    /// Create a new node iterator
    pub fn new(page: &'a crate::page::Page) -> Self {
        Self {
            page,
            current: 0,
            count: page.header.num_keys as usize,
        }
    }
}

impl<'a> Iterator for NodeIterator<'a> {
    type Item = Result<crate::page::Node<'a>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.count {
            return None;
        }
        
        let result = self.page.node(self.current);
        self.current += 1;
        Some(result)
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.current;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for NodeIterator<'a> {}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_node_value() {
        let data = NodeValue::Data(Cow::Borrowed(b"hello"));
        assert_eq!(data.as_data(), Some(&b"hello"[..]));
        assert_eq!(data.as_page_ref(), None);
        assert!(!data.is_overflow());
        
        let page_ref = NodeValue::PageRef(PageId(42));
        assert_eq!(page_ref.as_page_ref(), Some(PageId(42)));
        assert_eq!(page_ref.as_data(), None);
    }
    
    #[test]
    fn test_key_value() {
        let kv = KeyValue::data(b"key", b"value");
        assert_eq!(kv.key.as_ref(), b"key");
        assert_eq!(kv.value.as_data(), Some(&b"value"[..]));
        
        let owned = kv.into_owned();
        assert!(matches!(owned.key, Cow::Owned(_)));
    }
    
    #[test]
    fn test_node_builder() {
        let header = NodeBuilder::new()
            .key_size(10)
            .value_size(100000)
            .big_data()
            .build();
            
        assert_eq!(header.ksize, 10);
        assert_eq!(header.value_size(), 100000);
        assert!(header.flags.contains(NodeFlags::BIGDATA));
    }
}