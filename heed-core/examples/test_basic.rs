//! Basic test to verify database operations work

use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing basic database operations...");
    
    // Create a temporary directory
    let dir = TempDir::new()?;
    println!("Created temp dir at: {:?}", dir.path());
    
    // Create environment
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .open(dir.path())?
    );
    println!("Environment created");
    
    // Create a database
    let db: Database<String, String> = {
        let mut txn = env.begin_write_txn()?;
        println!("Write transaction started");
        
        let db = env.create_database(&mut txn, Some("test_db"))?;
        println!("Database created");
        
        txn.commit()?;
        println!("Transaction committed");
        
        db
    };
    
    // Test 1: Insert a single entry
    println!("\nTest 1: Insert single entry");
    {
        let mut txn = env.begin_write_txn()?;
        db.put(&mut txn, "hello".to_string(), "world".to_string())?;
        println!("  Put hello -> world");
        txn.commit()?;
        println!("  Committed");
    }
    
    // Test 2: Read the entry
    println!("\nTest 2: Read entry");
    {
        let txn = env.begin_txn()?;
        let value = db.get(&txn, &"hello".to_string())?;
        println!("  Got value: {:?}", value);
        assert_eq!(value, Some("world".to_string()));
    }
    
    // Test 3: Insert multiple entries
    println!("\nTest 3: Insert multiple entries");
    {
        let mut txn = env.begin_write_txn()?;
        
        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.put(&mut txn, key.clone(), value.clone())?;
            println!("  Put {} -> {}", key, value);
        }
        
        txn.commit()?;
        println!("  Committed");
    }
    
    // Test 4: Read all entries
    println!("\nTest 4: Read all entries");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        println!("  Iterating with cursor:");
        while let Some((key, value)) = cursor.next()? {
            let key_str = String::from_utf8_lossy(&key);
            println!("    {} -> {}", key_str, value);
        }
    }
    
    // Test 5: Delete an entry
    println!("\nTest 5: Delete entry");
    {
        let mut txn = env.begin_write_txn()?;
        let deleted = db.delete(&mut txn, &"key2".to_string())?;
        println!("  Delete key2: {}", deleted);
        txn.commit()?;
    }
    
    // Test 6: Verify deletion
    println!("\nTest 6: Verify deletion");
    {
        let txn = env.begin_txn()?;
        let value = db.get(&txn, &"key2".to_string())?;
        println!("  Get key2: {:?}", value);
        assert_eq!(value, None);
    }
    
    println!("\nAll tests passed!");
    Ok(())
}