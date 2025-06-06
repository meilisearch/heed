use heed_core::env::EnvBuilder;
use heed_core::db::{Database, DatabaseFlags};
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Testing catalog persistence issue...");
    
    // Phase 1: Create databases using Database::open (which uses Catalog)
    {
        println!("\nPhase 1: Creating databases with Database::open");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        // Use Database::open which uses the Catalog
        let db1: Database<String, String> = Database::open(&env, Some("catalog_db1"), DatabaseFlags::CREATE)?;
        let db2: Database<String, String> = Database::open(&env, Some("catalog_db2"), DatabaseFlags::CREATE)?;
        
        println!("Created databases: catalog_db1, catalog_db2");
        
        // Add some data
        {
            let mut txn = env.begin_write_txn()?;
            db1.put(&mut txn, "key1".to_string(), "value1".to_string())?;
            db2.put(&mut txn, "key2".to_string(), "value2".to_string())?;
            txn.commit()?;
            println!("Added data to databases");
        }
    }
    
    // Phase 2: Reopen and try to access the databases
    {
        println!("\nPhase 2: Reopening environment");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        // Try to open with Database::open (should work)
        println!("\nTrying Database::open...");
        match Database::<String, String>::open(&env, Some("catalog_db1"), DatabaseFlags::empty()) {
            Ok(db) => {
                let txn = env.begin_txn()?;
                match db.get(&txn, &"key1".to_string())? {
                    Some(val) => println!("✓ Database::open worked! Got value: {}", val),
                    None => println!("✗ Database::open opened but data not found"),
                }
            }
            Err(e) => println!("✗ Database::open failed: {:?}", e),
        }
        
        // Try to open with env.open_database (might fail due to serialization mismatch)
        println!("\nTrying env.open_database...");
        let txn = env.begin_txn()?;
        match env.open_database::<String, String>(&txn, Some("catalog_db1")) {
            Ok(db) => {
                match db.get(&txn, &"key1".to_string())? {
                    Some(val) => println!("✓ env.open_database worked! Got value: {}", val),
                    None => println!("✗ env.open_database opened but data not found"),
                }
            }
            Err(e) => println!("✗ env.open_database failed: {:?}", e),
        }
    }
    
    // Phase 3: Create databases using env.create_database
    {
        println!("\nPhase 3: Creating databases with env.create_database");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        let mut txn = env.begin_write_txn()?;
        let db3: Database<String, String> = env.create_database(&mut txn, Some("env_db1"))?;
        let db4: Database<String, String> = env.create_database(&mut txn, Some("env_db2"))?;
        
        db3.put(&mut txn, "key3".to_string(), "value3".to_string())?;
        db4.put(&mut txn, "key4".to_string(), "value4".to_string())?;
        txn.commit()?;
        
        println!("Created databases: env_db1, env_db2");
    }
    
    // Phase 4: Reopen and check both types
    {
        println!("\nPhase 4: Final check after reopening");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        let txn = env.begin_txn()?;
        
        // List all databases
        println!("\nListing all databases:");
        let dbs = env.list_databases(&txn)?;
        for db_name in &dbs {
            println!("  - {}", db_name);
        }
        
        // Try to open each type
        println!("\nTrying to open catalog_db1 with both methods:");
        match Database::<String, String>::open(&env, Some("catalog_db1"), DatabaseFlags::empty()) {
            Ok(_) => println!("  ✓ Database::open succeeded"),
            Err(e) => println!("  ✗ Database::open failed: {:?}", e),
        }
        
        match env.open_database::<String, String>(&txn, Some("catalog_db1")) {
            Ok(_) => println!("  ✓ env.open_database succeeded"),
            Err(e) => println!("  ✗ env.open_database failed: {:?}", e),
        }
        
        println!("\nTrying to open env_db1 with both methods:");
        match Database::<String, String>::open(&env, Some("env_db1"), DatabaseFlags::empty()) {
            Ok(_) => println!("  ✓ Database::open succeeded"),
            Err(e) => println!("  ✗ Database::open failed: {:?}", e),
        }
        
        match env.open_database::<String, String>(&txn, Some("env_db1")) {
            Ok(_) => println!("  ✓ env.open_database succeeded"),
            Err(e) => println!("  ✗ env.open_database failed: {:?}", e),
        }
    }
    
    Ok(())
}