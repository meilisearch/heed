use heed_core::env::EnvBuilder;
use heed_core::db::{Database, DatabaseFlags};
use heed_core::btree::BTree;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Detailed catalog debugging...\n");
    
    // Phase 1: Create database using Database::open
    {
        println!("Phase 1: Creating database with Database::open");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        // Check initial state
        {
            let txn = env.begin_txn()?;
            let main_db_info = txn.db_info(None)?;
            println!("Initial main DB: root={:?}, entries={}", main_db_info.root, main_db_info.entries);
        }
        
        // Create database using Database::open
        println!("\nCreating database 'test_db'...");
        let db: Database<String, String> = Database::open(&env, Some("test_db"), DatabaseFlags::CREATE)?;
        
        // Check state after creation
        {
            let txn = env.begin_txn()?;
            let main_db_info = txn.db_info(None)?;
            println!("After creation: root={:?}, entries={}", main_db_info.root, main_db_info.entries);
            
            // Check if we can find the database in the main DB
            match BTree::search(&txn, main_db_info.root, b"test_db")? {
                Some(value) => {
                    println!("Found 'test_db' in main DB!");
                    let info = heed_core::catalog::Catalog::deserialize_db_info(&value)?;
                    println!("  Database info: root={:?}, entries={}", info.root, info.entries);
                }
                None => println!("ERROR: 'test_db' not found in main DB!"),
            }
        }
        
        // Add data to the database
        {
            let mut txn = env.begin_write_txn()?;
            db.put(&mut txn, "key1".to_string(), "value1".to_string())?;
            
            // Check transaction state before commit
            println!("\nBefore data commit:");
            let main_db_info = txn.db_info(None)?;
            println!("  Main DB: root={:?}, entries={}", main_db_info.root, main_db_info.entries);
            
            if let Ok(test_db_info) = txn.db_info(Some("test_db")) {
                println!("  Test DB: root={:?}, entries={}", test_db_info.root, test_db_info.entries);
            }
            
            txn.commit()?;
        }
        
        // Final check
        {
            let txn = env.begin_txn()?;
            let main_db_info = txn.db_info(None)?;
            println!("\nAfter data commit:");
            println!("  Main DB: root={:?}, entries={}", main_db_info.root, main_db_info.entries);
        }
    }
    
    // Phase 2: Reopen and verify
    {
        println!("\n\nPhase 2: Reopening environment");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        // Check state after reopen
        {
            let txn = env.begin_txn()?;
            let main_db_info = txn.db_info(None)?;
            println!("After reopen: root={:?}, entries={}", main_db_info.root, main_db_info.entries);
            
            if main_db_info.entries == 0 {
                println!("ERROR: Main DB entries reset to 0!");
            }
            
            // Try to find the database
            if main_db_info.root.0 != 0 {
                match BTree::search(&txn, main_db_info.root, b"test_db")? {
                    Some(value) => {
                        println!("Found 'test_db' in main DB after reopen!");
                        let info = heed_core::catalog::Catalog::deserialize_db_info(&value)?;
                        println!("  Database info: root={:?}, entries={}", info.root, info.entries);
                    }
                    None => println!("ERROR: 'test_db' not found in main DB after reopen!"),
                }
            }
        }
        
        // Try to open the database
        match Database::<String, String>::open(&env, Some("test_db"), DatabaseFlags::empty()) {
            Ok(db) => {
                println!("\nSuccessfully opened 'test_db'");
                let txn = env.begin_txn()?;
                match db.get(&txn, &"key1".to_string())? {
                    Some(val) => println!("  Found data: key1 = {}", val),
                    None => println!("  ERROR: Data not found"),
                }
            }
            Err(e) => println!("\nERROR: Failed to open 'test_db': {:?}", e),
        }
    }
    
    Ok(())
}