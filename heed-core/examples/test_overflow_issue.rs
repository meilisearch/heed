use heed_core::{EnvBuilder, Database};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    // Create database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Step 1: Insert large value
    let large_val1 = vec![0xAA; 5000];
    {
        let mut txn = env.begin_write_txn()?;
        println!("1. Inserting large value ({} bytes)", large_val1.len());
        db.put(&mut txn, b"key".to_vec(), large_val1.clone())?;
        txn.commit()?;
    }
    
    // Step 2: Start a read transaction (snapshot)
    let read_txn = env.begin_txn()?;
    println!("2. Started read transaction (snapshot)");
    
    // Verify the value in the snapshot
    {
        let val = db.get(&read_txn, &b"key".to_vec())?;
        match val {
            Some(v) => println!("   Snapshot sees: {} bytes", v.len()),
            None => println!("   ERROR: Value not found in snapshot!"),
        }
    }
    
    // Step 3: Update the value
    let large_val2 = vec![0xBB; 6000];
    {
        let mut txn = env.begin_write_txn()?;
        println!("\n3. Updating to new value ({} bytes)", large_val2.len());
        
        // Check what we see before update
        let before = db.get(&txn, &b"key".to_vec())?;
        println!("   Before update: {} bytes", before.map(|v| v.len()).unwrap_or(0));
        
        // Update
        db.put(&mut txn, b"key".to_vec(), large_val2.clone())?;
        
        // Check what we see after update
        let after = db.get(&txn, &b"key".to_vec())?;
        println!("   After update: {} bytes", after.as_ref().map(|v| v.len()).unwrap_or(0));
        
        txn.commit()?;
    }
    
    // Step 4: Check what the old read transaction sees
    println!("\n4. Old read transaction should still see old value");
    {
        let val = db.get(&read_txn, &b"key".to_vec())?;
        match val {
            Some(v) => {
                println!("   Snapshot sees: {} bytes", v.len());
                if v.len() != large_val1.len() {
                    println!("   ERROR: Expected {} bytes!", large_val1.len());
                    // Check the actual content
                    if v.len() >= 10 {
                        println!("   First 10 bytes: {:?}", &v[..10]);
                        println!("   Expected: {:?}", &large_val1[..10]);
                    }
                }
            }
            None => println!("   ERROR: Value not found in snapshot!"),
        }
    }
    
    // Step 5: New transaction should see new value
    println!("\n5. New transaction should see new value");
    {
        let txn = env.begin_txn()?;
        let val = db.get(&txn, &b"key".to_vec())?;
        match val {
            Some(v) => {
                println!("   New txn sees: {} bytes", v.len());
                if v.len() != large_val2.len() {
                    println!("   ERROR: Expected {} bytes!", large_val2.len());
                    if v.len() >= 10 {
                        println!("   First 10 bytes: {:?}", &v[..10]);
                        println!("   Expected: {:?}", &large_val2[..10]);
                    }
                }
            }
            None => println!("   ERROR: Value not found!"),
        }
    }
    
    println!("\nTest completed!");
    Ok(())
}