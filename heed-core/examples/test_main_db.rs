//! Test the main database first

use heed_core::{EnvBuilder};
use heed_core::db::{Database, DatabaseFlags};
use heed_core::error::Result;

fn main() -> Result<()> {
    println!("Testing main database functionality...\n");
    
    // Create environment
    let dir = tempfile::tempdir().unwrap();
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())?;
    
    println!("✓ Environment created");
    
    // Open the main database (no name)
    let main_db: Database<Vec<u8>, Vec<u8>> = Database::open(&env, None, DatabaseFlags::empty())?;
    println!("✓ Opened main database");
    
    // Store some data in main database
    {
        let mut txn = env.begin_write_txn()?;
        
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        
        main_db.put(&mut txn, key.clone(), value)?;
        println!("✓ Stored data in main database");
        
        // Check we can read it back in same transaction
        if let Some(v) = main_db.get(&txn, &key)? {
            println!("✓ Retrieved value in same txn: {:?}", String::from_utf8_lossy(&v));
        }
        
        txn.commit()?;
        println!("✓ Transaction committed");
    }
    
    // Read it back in new transaction
    {
        let txn = env.begin_txn()?;
        
        let key = b"test_key".to_vec();
        if let Some(value) = main_db.get(&txn, &key)? {
            println!("✓ Retrieved value after commit: {:?}", String::from_utf8_lossy(&value));
        } else {
            println!("✗ Failed to retrieve value after commit");
        }
    }
    
    // Now try creating a named database
    println!("\nTesting named database creation...");
    
    let named_db: Database<Vec<u8>, Vec<u8>> = match Database::open(&env, Some("mydb"), DatabaseFlags::CREATE) {
        Ok(db) => {
            println!("✓ Created named database 'mydb'");
            db
        }
        Err(e) => {
            println!("✗ Failed to create named database: {:?}", e);
            return Err(e);
        }
    };
    
    // Try to use the named database
    {
        let mut txn = env.begin_write_txn()?;
        
        let key = b"named_key".to_vec();
        let value = b"named_value".to_vec();
        
        match named_db.put(&mut txn, key.clone(), value) {
            Ok(_) => println!("✓ Stored data in named database"),
            Err(e) => {
                println!("✗ Failed to store in named database: {:?}", e);
                return Err(e);
            }
        }
        
        txn.commit()?;
    }
    
    println!("\nAll tests passed!");
    Ok(())
}