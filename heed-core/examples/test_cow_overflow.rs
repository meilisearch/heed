use heed_core::{EnvBuilder, Database};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create temporary directory
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    println!("Testing Copy-on-Write with overflow pages...");
    
    // Create database
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert small value first
    {
        let mut txn = env.begin_write_txn()?;
        println!("Inserting small value...");
        db.put(&mut txn, "small_key".to_string(), vec![1u8; 100])?;
        txn.commit()?;
    }
    
    // Insert large value that needs overflow
    let large_value = vec![0xAB; 5000]; // 5KB
    {
        let mut txn = env.begin_write_txn()?;
        println!("Inserting large value ({} bytes)...", large_value.len());
        db.put(&mut txn, "large_key".to_string(), large_value.clone())?;
        txn.commit()?;
    }
    
    // Read values back
    {
        let txn = env.begin_txn()?;
        
        println!("Reading small value...");
        let small = db.get(&txn, &"small_key".to_string())?;
        println!("Small value exists: {}", small.is_some());
        
        println!("Reading large value...");
        let large = db.get(&txn, &"large_key".to_string())?;
        match large {
            Some(val) => {
                println!("Large value exists: {} bytes", val.len());
                assert_eq!(val, large_value);
            }
            None => println!("Large value NOT FOUND!"),
        }
    }
    
    // Update large value (test COW with overflow)
    let updated_value = vec![0xCD; 6000]; // 6KB
    {
        let mut txn = env.begin_write_txn()?;
        println!("Updating large value ({} bytes)...", updated_value.len());
        
        // First, check what we have before update
        let before = db.get(&txn, &"large_key".to_string())?;
        println!("Before update: {} bytes", before.map(|v| v.len()).unwrap_or(0));
        
        db.put(&mut txn, "large_key".to_string(), updated_value.clone())?;
        
        // Check immediately after update (before commit)
        let after = db.get(&txn, &"large_key".to_string())?;
        println!("After update (before commit): {} bytes", after.map(|v| v.len()).unwrap_or(0));
        
        txn.commit()?;
    }
    
    // Read updated value
    {
        let txn = env.begin_txn()?;
        println!("Reading updated large value...");
        let large = db.get(&txn, &"large_key".to_string())?;
        match large {
            Some(val) => {
                println!("Updated value exists: {} bytes", val.len());
                assert_eq!(val, updated_value);
            }
            None => println!("Updated value NOT FOUND!"),
        }
    }
    
    println!("Test completed successfully!");
    Ok(())
}