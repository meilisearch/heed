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
        println!("Creating database...");
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert large value
    let large1 = vec![0xAA; 5000];
    {
        let mut txn = env.begin_write_txn()?;
        println!("Inserting 5KB value...");
        db.put(&mut txn, b"key".to_vec(), large1.clone())?;
        txn.commit()?;
    }
    
    // Update to new large value
    let large2 = vec![0xBB; 6000];
    {
        let mut txn = env.begin_write_txn()?;
        println!("\nUpdating to 6KB value...");
        
        // Check current value
        let current = db.get(&txn, &b"key".to_vec())?;
        println!("Before update: {:?} bytes", current.as_ref().map(|v| v.len()));
        
        db.put(&mut txn, b"key".to_vec(), large2.clone())?;
        
        // Check after update
        let after = db.get(&txn, &b"key".to_vec())?;
        println!("After update (in same txn): {:?} bytes", after.as_ref().map(|v| v.len()));
        
        txn.commit()?;
    }
    
    // Read updated value
    {
        let txn = env.begin_txn()?;
        println!("\nReading updated value...");
        let val = db.get(&txn, &b"key".to_vec())?;
        match val {
            Some(v) => {
                println!("Got {} bytes", v.len());
                if v != large2 {
                    println!("ERROR: Value mismatch!");
                    println!("  Expected first bytes: {:?}", &large2[..10]);
                    println!("  Got first bytes: {:?}", &v[..10]);
                }
            }
            None => println!("ERROR: Value not found!"),
        }
    }
    
    Ok(())
}