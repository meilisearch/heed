//! Demonstration of fixed-size value optimizations

use heed_core::{EnvBuilder, Database, DatabaseFlags};
use heed_core::fixed_size::FixedSize;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())?);
    
    // Example 1: Database with u32 keys
    {
        println!("=== Fixed-Size u32 Keys ===");
        let mut wtxn = env.begin_write_txn()?;
        
        // Create a database optimized for u32 keys
        let db: Database<u32, String> = env.create_database(&mut wtxn, Some("u32_keys"))?;
        
        // Insert some data
        db.put(&mut wtxn, 100u32, "One hundred".to_string())?;
        db.put(&mut wtxn, 50u32, "Fifty".to_string())?;
        db.put(&mut wtxn, 200u32, "Two hundred".to_string())?;
        db.put(&mut wtxn, 25u32, "Twenty-five".to_string())?;
        
        wtxn.commit()?;
        
        // Read in numeric order
        let rtxn = env.begin_txn()?;
        let mut cursor = db.cursor(&rtxn)?;
        
        println!("Keys in numeric order:");
        while let Some((key, value)) = cursor.next()? {
            println!("  {} -> {}", key, value);
        }
    }
    
    // Example 2: Database with fixed-size values
    {
        println!("\n=== Fixed-Size Values (Timestamps) ===");
        let mut wtxn = env.begin_write_txn()?;
        
        // Create a database for storing timestamps
        let db: Database<String, u64> = env.create_database(&mut wtxn, Some("timestamps"))?;
        
        // Store some events with timestamps
        db.put(&mut wtxn, "user_login".to_string(), 1234567890u64)?;
        db.put(&mut wtxn, "data_sync".to_string(), 1234567900u64)?;
        db.put(&mut wtxn, "backup_complete".to_string(), 1234567950u64)?;
        
        wtxn.commit()?;
        
        // Read back
        let rtxn = env.begin_txn()?;
        println!("Event timestamps:");
        for event in &["user_login", "data_sync", "backup_complete"] {
            if let Some(timestamp) = db.get(&rtxn, &event.to_string())? {
                println!("  {} -> {}", event, timestamp);
            }
        }
    }
    
    // Example 3: Demonstrate size efficiency
    {
        println!("\n=== Size Efficiency ===");
        
        // Show how fixed-size types are more efficient
        let u32_size = std::mem::size_of::<u32>();
        let u64_size = std::mem::size_of::<u64>();
        
        println!("Fixed sizes:");
        println!("  u32: {} bytes", u32_size);
        println!("  u64: {} bytes", u64_size);
        println!("  u128: {} bytes", std::mem::size_of::<u128>());
        
        // Compare with variable-length encoding
        let num: u32 = 42;
        let as_string = num.to_string();
        let as_bytes = as_string.as_bytes();
        
        println!("\nVariable encoding of {}: {} bytes", num, as_bytes.len());
        println!("Fixed encoding of {}: {} bytes", num, u32_size);
        
        // For large numbers the difference is more pronounced
        let large: u64 = 12345678901234567890;
        let large_string = large.to_string();
        
        println!("\nLarge number {}:", large);
        println!("  As string: {} bytes", large_string.as_bytes().len());
        println!("  As u64: {} bytes", u64_size);
    }
    
    Ok(())
}