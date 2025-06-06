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
    
    println!("=== Testing overflow page COW issue ===\n");
    
    // Create database using the high-level API
    let db: Database<String, Vec<u8>> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Step 1: Insert a large value
    let large_value1 = vec![0xAA; 5000]; // 5KB
    {
        let mut txn = env.begin_write_txn()?;
        println!("Step 1: Inserting large value (5KB)...");
        db.put(&mut txn, "key1".to_string(), large_value1.clone())?;
        txn.commit()?;
    }
    
    // Step 2: Read it back
    {
        let txn = env.begin_txn()?;
        let val = db.get(&txn, &"key1".to_string())?;
        match val {
            Some(v) => {
                println!("Step 2: Read back {} bytes", v.len());
                assert_eq!(v, large_value1, "Value mismatch!");
            }
            None => panic!("Value not found!"),
        }
    }
    
    // Step 3: Start a read transaction BEFORE updating
    let read_txn = env.begin_txn()?;
    println!("\nStep 3: Started read transaction (snapshot)");
    
    // Step 4: Update the value in a new write transaction
    let large_value2 = vec![0xBB; 6000]; // 6KB
    {
        let mut txn = env.begin_write_txn()?;
        println!("Step 4: Updating to new large value (6KB)...");
        
        // Check value before update
        let before = db.get(&txn, &"key1".to_string())?;
        println!("  Before update in write txn: {} bytes", before.map(|v| v.len()).unwrap_or(0));
        
        db.put(&mut txn, "key1".to_string(), large_value2.clone())?;
        
        // Check value after update but before commit
        let after = db.get(&txn, &"key1".to_string())?;
        println!("  After update in write txn: {} bytes", after.as_ref().map(|v| v.len()).unwrap_or(0));
        if let Some(v) = after {
            if v != large_value2 {
                println!("  WARNING: Updated value doesn't match! Got {} bytes, expected {}", v.len(), large_value2.len());
            }
        }
        
        txn.commit()?;
        println!("  Committed update");
    }
    
    // Step 5: Read from the old read transaction (should see old value)
    {
        println!("\nStep 5: Reading from old read transaction...");
        let val = db.get(&read_txn, &"key1".to_string())?;
        match val {
            Some(v) => {
                println!("  Old transaction sees: {} bytes", v.len());
                if v != large_value1 {
                    println!("  ERROR: Old transaction should see old value (5KB) but sees {} bytes!", v.len());
                    println!("  First few bytes: {:?}", &v[..10.min(v.len())]);
                }
            }
            None => panic!("Value not found in old transaction!"),
        }
    }
    
    // Step 6: Read from a new transaction (should see new value)
    {
        let txn = env.begin_txn()?;
        println!("\nStep 6: Reading from new transaction...");
        let val = db.get(&txn, &"key1".to_string())?;
        match val {
            Some(v) => {
                println!("  New transaction sees: {} bytes", v.len());
                if v != large_value2 {
                    println!("  ERROR: New transaction should see new value (6KB) but sees {} bytes!", v.len());
                    println!("  First few bytes: {:?}", &v[..10.min(v.len())]);
                }
            }
            None => panic!("Value not found in new transaction!"),
        }
    }
    
    println!("\nTest completed!");
    Ok(())
}