//! Example demonstrating custom key comparators
//!
//! This example shows how to use different types of comparators for key ordering:
//! - Default lexicographic comparator
//! - Case-insensitive string comparator
//! - Numeric comparator
//! - Reverse comparator

use heed_core::{EnvBuilder, Result};
use heed_core::db::Database;
use heed_core::comparator::{
    LexicographicComparator, 
    CaseInsensitiveComparator, 
    NumericComparator, 
    ReverseComparator
};
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
    
    println!("Environment opened at: {:?}", dir.path());
    
    // Example 1: Default Lexicographic Comparator
    println!("\n=== Example 1: Default Lexicographic Comparator ===");
    {
        let mut txn = env.begin_write_txn()?;
        let db: Database<String, String, LexicographicComparator> = 
            env.create_database_with_flags(&mut txn, Some("lexicographic"), heed_core::db::DatabaseFlags::empty())?;
        
        // Insert data (will be sorted lexicographically)
        db.put(&mut txn, "banana".to_string(), "yellow".to_string())?;
        db.put(&mut txn, "apple".to_string(), "red".to_string())?;
        db.put(&mut txn, "Cherry".to_string(), "red".to_string())?; // Capital C comes before lowercase
        db.put(&mut txn, "date".to_string(), "brown".to_string())?;
        
        txn.commit()?;
        
        // Read back in order
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        println!("Lexicographic order:");
        cursor.first()?;
        while let Some((key, value)) = cursor.next()? {
            println!("  {} => {}", String::from_utf8_lossy(&key), value);
        }
    }
    
    // Example 2: Case-Insensitive Comparator
    println!("\n=== Example 2: Case-Insensitive Comparator ===");
    {
        let mut txn = env.begin_write_txn()?;
        let db: Database<String, String, CaseInsensitiveComparator> = 
            env.create_database_with_flags(&mut txn, Some("case_insensitive"), heed_core::db::DatabaseFlags::empty())?;
        
        // Insert the same data (will be sorted case-insensitively)
        db.put(&mut txn, "banana".to_string(), "yellow".to_string())?;
        db.put(&mut txn, "apple".to_string(), "red".to_string())?;
        db.put(&mut txn, "Cherry".to_string(), "red".to_string())?; // Will be sorted with other 'c' words
        db.put(&mut txn, "date".to_string(), "brown".to_string())?;
        
        txn.commit()?;
        
        // Read back in order
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        println!("Case-insensitive order:");
        cursor.first()?;
        while let Some((key, value)) = cursor.next()? {
            println!("  {} => {}", String::from_utf8_lossy(&key), value);
        }
    }
    
    // Example 3: Numeric Comparator (for string-encoded numbers)
    println!("\n=== Example 3: Numeric Comparator ===");
    {
        let mut txn = env.begin_write_txn()?;
        let db: Database<String, String, NumericComparator> = 
            env.create_database_with_flags(&mut txn, Some("numeric"), heed_core::db::DatabaseFlags::empty())?;
        
        // Insert numeric strings (will be sorted numerically by length then lexicographically)
        db.put(&mut txn, "100".to_string(), "one hundred".to_string())?;
        db.put(&mut txn, "2".to_string(), "two".to_string())?;
        db.put(&mut txn, "30".to_string(), "thirty".to_string())?;
        db.put(&mut txn, "1000".to_string(), "one thousand".to_string())?;
        
        txn.commit()?;
        
        // Read back in order
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        println!("Numeric order (shorter numbers first, then lexicographic within same length):");
        cursor.first()?;
        while let Some((key, value)) = cursor.next()? {
            println!("  {} => {}", String::from_utf8_lossy(&key), value);
        }
    }
    
    // Example 4: Reverse Comparator
    println!("\n=== Example 4: Reverse Lexicographic Comparator ===");
    {
        let mut txn = env.begin_write_txn()?;
        let db: Database<String, String, ReverseComparator<LexicographicComparator>> = 
            env.create_database_with_flags(&mut txn, Some("reverse"), heed_core::db::DatabaseFlags::empty())?;
        
        // Insert data (will be sorted in reverse lexicographic order)
        db.put(&mut txn, "banana".to_string(), "yellow".to_string())?;
        db.put(&mut txn, "apple".to_string(), "red".to_string())?;
        db.put(&mut txn, "cherry".to_string(), "red".to_string())?;
        db.put(&mut txn, "date".to_string(), "brown".to_string())?;
        
        txn.commit()?;
        
        // Read back in order
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        println!("Reverse lexicographic order:");
        cursor.first()?;
        while let Some((key, value)) = cursor.next()? {
            println!("  {} => {}", String::from_utf8_lossy(&key), value);
        }
    }
    
    println!("\nDemonstration complete!");
    Ok(())
}