//! Example demonstrating custom key comparators for search operations
//!
//! This example shows how different comparators affect key ordering by
//! using BTree operations directly.

use heed_core::{EnvBuilder, Result};
use heed_core::btree::BTree;
use heed_core::comparator::{
    LexicographicComparator, 
    CaseInsensitiveComparator, 
    NumericComparator, 
    ReverseComparator
};
use heed_core::meta::DbInfo;
use heed_core::error::PageId;
use heed_core::page::PageFlags;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<()> {
    // Create temporary directory
    let dir = TempDir::new().unwrap();
    
    // Open environment
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .open(dir.path())?
    );
    
    println!("Testing custom comparators with BTree operations");
    println!("Environment opened at: {:?}", dir.path());
    
    // Test data
    let keys = vec![
        "apple", "Banana", "cherry", "Date", "elderberry"
    ];
    
    // Example 1: Lexicographic Comparator (default)
    println!("\n=== Lexicographic Comparator (default) ===");
    test_comparator::<LexicographicComparator>(&env, &keys, "Lexicographic")?;
    
    // Example 2: Case-Insensitive Comparator 
    println!("\n=== Case-Insensitive Comparator ===");
    test_comparator::<CaseInsensitiveComparator>(&env, &keys, "Case-insensitive")?;
    
    // Example 3: Numeric Comparator
    println!("\n=== Numeric Comparator ===");
    let numeric_keys = vec!["1", "10", "2", "20", "3"];
    test_comparator::<NumericComparator>(&env, &numeric_keys, "Numeric")?;
    
    // Example 4: Reverse Comparator
    println!("\n=== Reverse Lexicographic Comparator ===");
    test_comparator::<ReverseComparator<LexicographicComparator>>(&env, &keys, "Reverse")?;
    
    println!("\nComparator demonstration complete!");
    Ok(())
}

fn test_comparator<C: heed_core::comparator::Comparator>(
    env: &Arc<heed_core::Environment<heed_core::env::state::Open>>,
    keys: &[&str],
    name: &str
) -> Result<()> {
    let mut txn = env.begin_write_txn()?;
    
    // Create a new leaf page for this test
    let (root_id, _root_page) = txn.alloc_page(PageFlags::LEAF)?;
    let mut db_info = DbInfo::default();
    db_info.root = root_id;
    db_info.leaf_pages = 1;
    let mut root = root_id;
    
    // Insert all keys with BTree using the specified comparator
    println!("Inserting keys: {:?}", keys);
    for key in keys {
        let value = format!("value_{}", key);
        BTree::<C>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), value.as_bytes())?;
    }
    
    println!("Keys inserted with {} comparator", name);
    
    // Now search for each key to demonstrate the comparison behavior
    println!("Search results:");
    for key in keys {
        let result = BTree::<C>::search(&txn, root, key.as_bytes())?;
        match result {
            Some(value) => {
                let value_str = String::from_utf8_lossy(&value);
                println!("  '{}' => '{}'", key, value_str);
            }
            None => {
                println!("  '{}' => NOT FOUND", key);
            }
        }
    }
    
    // Test a search for a key that might behave differently with different comparators
    let test_key = "APPLE";
    let result = BTree::<C>::search(&txn, root, test_key.as_bytes())?;
    match result {
        Some(value) => {
            let value_str = String::from_utf8_lossy(&value);
            println!("  Search for '{}' => Found: '{}'", test_key, value_str);
        }
        None => {
            println!("  Search for '{}' => NOT FOUND", test_key);
        }
    }
    
    txn.abort(); // Don't commit, we're just testing
    
    Ok(())
}