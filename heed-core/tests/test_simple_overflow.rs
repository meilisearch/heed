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
    
    // Test 1: Small value
    println!("Test 1: Small value");
    {
        let mut txn = env.begin_write_txn()?;
        db.put(&mut txn, b"key1".to_vec(), b"small value".to_vec())?;
        txn.commit()?;
    }
    
    {
        let txn = env.begin_txn()?;
        let val = db.get(&txn, &b"key1".to_vec())?;
        println!("  Read small value: {:?}", val.map(|v| String::from_utf8_lossy(&v).to_string()));
    }
    
    // Test 2: Large value
    println!("\nTest 2: Large value");
    let large_val = vec![0xAB; 5000];
    {
        let mut txn = env.begin_write_txn()?;
        println!("  Inserting {} bytes...", large_val.len());
        db.put(&mut txn, b"key2".to_vec(), large_val.clone())?;
        txn.commit()?;
    }
    
    {
        let txn = env.begin_txn()?;
        let val = db.get(&txn, &b"key2".to_vec())?;
        match val {
            Some(v) => {
                println!("  Read large value: {} bytes", v.len());
                assert_eq!(v, large_val);
            }
            None => println!("  ERROR: Large value not found!"),
        }
    }
    
    // Test 3: Update large value
    println!("\nTest 3: Update large value");
    let new_large = vec![0xCD; 6000];
    {
        let mut txn = env.begin_write_txn()?;
        println!("  Updating to {} bytes...", new_large.len());
        db.put(&mut txn, b"key2".to_vec(), new_large.clone())?;
        txn.commit()?;
    }
    
    {
        let txn = env.begin_txn()?;
        let val = db.get(&txn, &b"key2".to_vec())?;
        match val {
            Some(v) => {
                println!("  Read updated value: {} bytes", v.len());
                if v != new_large {
                    println!("  ERROR: Expected {} bytes but got {} bytes", new_large.len(), v.len());
                }
            }
            None => println!("  ERROR: Updated value not found!"),
        }
    }
    
    println!("\nAll tests completed!");
    Ok(())
}