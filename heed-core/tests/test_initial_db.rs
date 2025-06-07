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
        println!("Database created");
        txn.commit()?;
        println!("Transaction committed");
        db
    };
    
    // Try simple put/get first
    {
        let mut txn = env.begin_write_txn()?;
        println!("\nInserting simple value...");
        db.put(&mut txn, b"test".to_vec(), b"value".to_vec())?;
        println!("Simple value inserted");
        txn.commit()?;
        println!("Committed");
    }
    
    // Read it back
    {
        let txn = env.begin_txn()?;
        println!("\nReading simple value...");
        let val = db.get(&txn, &b"test".to_vec())?;
        println!("Got: {:?}", val.map(|v| String::from_utf8_lossy(&v).to_string()));
    }
    
    println!("\nSimple test passed! Now trying large value...");
    
    // Now try large value
    {
        let mut txn = env.begin_write_txn()?;
        println!("\nInserting large value (5KB)...");
        let large = vec![0x42; 5000];
        println!("Large value created: {} bytes", large.len());
        
        // Get db info before insert
        let db_info = txn.db_info(None)?;
        println!("DB info before insert: entries={}, root={:?}", db_info.entries, db_info.root);
        
        db.put(&mut txn, b"large".to_vec(), large)?;
        println!("Large value inserted");
        
        // Get db info after insert
        let db_info = txn.db_info(None)?;
        println!("DB info after insert: entries={}, root={:?}", db_info.entries, db_info.root);
        
        txn.commit()?;
        println!("Committed");
    }
    
    Ok(())
}