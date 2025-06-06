use heed_core::{EnvBuilder, Database};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    println!("Creating environment at: {:?}", dir.path());
    
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    println!("Environment created");
    
    // Create database
    let db: Database<Vec<u8>, Vec<u8>> = {
        let mut txn = env.begin_write_txn()?;
        println!("Creating database...");
        let db = env.create_database(&mut txn, None)?;
        println!("Database created, committing...");
        txn.commit()?;
        println!("Transaction committed");
        db
    };
    
    // Try a simple put/get
    {
        let mut txn = env.begin_write_txn()?;
        println!("\nInserting simple value...");
        db.put(&mut txn, b"test".to_vec(), b"value".to_vec())?;
        println!("Value inserted, committing...");
        txn.commit()?;
        println!("Committed");
    }
    
    {
        let txn = env.begin_txn()?;
        println!("\nReading value...");
        let val = db.get(&txn, &b"test".to_vec())?;
        println!("Got: {:?}", val.map(|v| String::from_utf8_lossy(&v).to_string()));
    }
    
    // Now try with larger value that needs overflow
    {
        let mut txn = env.begin_write_txn()?;
        println!("\nInserting large value (5KB)...");
        let large = vec![0x42; 5000];
        println!("Created {} byte value", large.len());
        db.put(&mut txn, b"large".to_vec(), large.clone())?;
        println!("Large value inserted, committing...");
        txn.commit()?;
        println!("Committed");
    }
    
    // Read it back
    {
        let txn = env.begin_txn()?;
        println!("\nReading large value...");
        let val = db.get(&txn, &b"large".to_vec())?;
        match val {
            Some(v) => println!("Got {} bytes", v.len()),
            None => println!("ERROR: Large value not found!"),
        }
    }
    
    // Now update the large value
    {
        let mut txn = env.begin_write_txn()?;
        println!("\nUpdating large value to 6KB...");
        let new_large = vec![0x43; 6000];
        db.put(&mut txn, b"large".to_vec(), new_large.clone())?;
        println!("Updated, committing...");
        txn.commit()?;
        println!("Committed");
    }
    
    // Read updated value
    {
        let txn = env.begin_txn()?;
        println!("\nReading updated value...");
        let val = db.get(&txn, &b"large".to_vec())?;
        match val {
            Some(v) => {
                println!("Got {} bytes", v.len());
                if v.len() != 6000 {
                    println!("ERROR: Expected 6000 bytes!");
                }
            }
            None => println!("ERROR: Updated value not found!"),
        }
    }
    
    Ok(())
}