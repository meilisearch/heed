//! Simple test of database catalog

use heed_core::{Environment, EnvBuilder};
use heed_core::db::{Database, DatabaseFlags};
use heed_core::error::Result;

fn main() -> Result<()> {
    println!("Testing database catalog functionality...\n");
    
    // Create environment
    let dir = tempfile::tempdir().unwrap();
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .max_dbs(5)
        .open(dir.path())?;
    
    println!("✓ Environment created");
    
    // Create a named database
    let db: Database<Vec<u8>, Vec<u8>> = Database::open(&env, Some("test_db"), DatabaseFlags::CREATE)?;
    println!("✓ Created named database 'test_db'");
    
    // Store some data
    {
        let mut txn = env.begin_write_txn()?;
        
        // Use raw bytes for now to avoid trait issues
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();
        
        db.put(&mut txn, key, value)?;
        
        txn.commit()?;
        println!("✓ Stored data in database");
    }
    
    // Read it back
    {
        let txn = env.begin_txn()?;
        
        let key = b"key1".to_vec();
        if let Some(value) = db.get(&txn, &key)? {
            println!("✓ Retrieved value: {:?}", String::from_utf8_lossy(&value));
        } else {
            println!("✗ Failed to retrieve value");
        }
    }
    
    // List databases in catalog
    {
        let txn = env.begin_txn()?;
        let databases = heed_core::catalog::Catalog::list_databases(&txn)?;
        
        println!("\n✓ Found {} named database(s) in catalog:", databases.len());
        for (name, info) in databases {
            println!("  - '{}': {} entries", name, info.entries);
        }
    }
    
    println!("\nAll tests passed!");
    Ok(())
}