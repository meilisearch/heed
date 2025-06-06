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
    
    println!("Testing Copy-on-Write implementation...");
    
    // Create database
    let db: Database<String, String> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        txn.commit()?;
        db
    };
    
    // Insert data
    {
        let mut txn = env.begin_write_txn()?;
        println!("Inserting key1...");
        db.put(&mut txn, "key1".to_string(), "value1".to_string())?;
        println!("Inserting key2...");
        db.put(&mut txn, "key2".to_string(), "value2".to_string())?;
        println!("Committing...");
        txn.commit()?;
    }
    
    // Read data
    {
        let txn = env.begin_txn()?;
        println!("Reading key1...");
        let val1 = db.get(&txn, &"key1".to_string())?;
        println!("key1 = {:?}", val1);
        
        let val2 = db.get(&txn, &"key2".to_string())?;
        println!("key2 = {:?}", val2);
    }
    
    // Test update with COW
    {
        let mut txn = env.begin_write_txn()?;
        println!("Updating key1...");
        db.put(&mut txn, "key1".to_string(), "updated_value1".to_string())?;
        txn.commit()?;
    }
    
    // Read updated data
    {
        let txn = env.begin_txn()?;
        let val1 = db.get(&txn, &"key1".to_string())?;
        println!("Updated key1 = {:?}", val1);
    }
    
    println!("Test completed successfully!");
    Ok(())
}