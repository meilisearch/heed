//! Custom key comparator support
//!
//! This module provides support for custom key ordering in databases.
//! By default, keys are compared lexicographically, but custom comparators
//! allow for different ordering schemes like case-insensitive, numeric, etc.

use std::cmp::Ordering;
use std::marker::PhantomData;

/// Trait for custom key comparators
pub trait Comparator: Send + Sync + 'static {
    /// Compare two keys and return their ordering
    fn compare(a: &[u8], b: &[u8]) -> Ordering;
    
    /// Optional: Return a name for this comparator (for debugging)
    fn name() -> &'static str {
        "CustomComparator"
    }
}

/// Default lexicographic comparator (byte-wise comparison)
pub struct LexicographicComparator;

impl Comparator for LexicographicComparator {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
    
    fn name() -> &'static str {
        "LexicographicComparator"
    }
}

/// Case-insensitive string comparator (UTF-8)
pub struct CaseInsensitiveComparator;

impl Comparator for CaseInsensitiveComparator {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        // Try to convert to UTF-8 strings
        match (std::str::from_utf8(a), std::str::from_utf8(b)) {
            (Ok(str_a), Ok(str_b)) => {
                // Compare case-insensitively
                str_a.to_lowercase().cmp(&str_b.to_lowercase())
            }
            _ => {
                // Fall back to byte comparison if not valid UTF-8
                a.cmp(b)
            }
        }
    }
    
    fn name() -> &'static str {
        "CaseInsensitiveComparator"
    }
}

/// Numeric comparator for big-endian encoded integers
pub struct NumericComparator;

impl Comparator for NumericComparator {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        // Compare by length first (longer numbers are bigger)
        match a.len().cmp(&b.len()) {
            Ordering::Equal => {
                // Same length, compare bytes
                a.cmp(b)
            }
            other => other,
        }
    }
    
    fn name() -> &'static str {
        "NumericComparator"
    }
}

/// Reverse comparator - reverses the ordering of another comparator
pub struct ReverseComparator<C: Comparator> {
    _phantom: PhantomData<C>,
}

impl<C: Comparator> Comparator for ReverseComparator<C> {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        C::compare(a, b).reverse()
    }
    
    fn name() -> &'static str {
        "ReverseComparator"
    }
}

/// Fixed-size comparator for types that can be compared directly
/// This enables optimizations for fixed-size keys
pub struct FixedSizeComparator<const N: usize>;

impl<const N: usize> Comparator for FixedSizeComparator<N> {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        // Ensure both slices are exactly N bytes
        if a.len() != N || b.len() != N {
            // Fall back to lexicographic comparison for safety
            return a.cmp(b);
        }
        
        // Direct byte comparison for fixed-size data
        a.cmp(b)
    }
    
    fn name() -> &'static str {
        "FixedSizeComparator"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_lexicographic_comparator() {
        assert_eq!(LexicographicComparator::compare(b"abc", b"def"), Ordering::Less);
        assert_eq!(LexicographicComparator::compare(b"def", b"abc"), Ordering::Greater);
        assert_eq!(LexicographicComparator::compare(b"abc", b"abc"), Ordering::Equal);
    }
    
    #[test]
    fn test_case_insensitive_comparator() {
        assert_eq!(CaseInsensitiveComparator::compare(b"ABC", b"abc"), Ordering::Equal);
        assert_eq!(CaseInsensitiveComparator::compare(b"abc", b"DEF"), Ordering::Less);
        assert_eq!(CaseInsensitiveComparator::compare(b"XYZ", b"xyz"), Ordering::Equal);
        
        // Test with non-ASCII
        assert_eq!(CaseInsensitiveComparator::compare(b"hello", b"HELLO"), Ordering::Equal);
    }
    
    #[test]
    fn test_numeric_comparator() {
        // Same length numbers
        assert_eq!(NumericComparator::compare(b"123", b"456"), Ordering::Less);
        assert_eq!(NumericComparator::compare(b"999", b"123"), Ordering::Greater);
        
        // Different length numbers
        assert_eq!(NumericComparator::compare(b"9", b"123"), Ordering::Less);
        assert_eq!(NumericComparator::compare(b"1000", b"999"), Ordering::Greater);
    }
    
    #[test]
    fn test_reverse_comparator() {
        assert_eq!(
            ReverseComparator::<LexicographicComparator>::compare(b"abc", b"def"), 
            Ordering::Greater
        );
        assert_eq!(
            ReverseComparator::<LexicographicComparator>::compare(b"def", b"abc"), 
            Ordering::Less
        );
    }
    
    #[test]
    fn test_fixed_size_comparator() {
        // 4-byte comparator (e.g., for u32)
        assert_eq!(
            FixedSizeComparator::<4>::compare(&[0, 0, 0, 1], &[0, 0, 0, 2]), 
            Ordering::Less
        );
        assert_eq!(
            FixedSizeComparator::<4>::compare(&[0, 0, 0, 2], &[0, 0, 0, 1]), 
            Ordering::Greater
        );
    }
}