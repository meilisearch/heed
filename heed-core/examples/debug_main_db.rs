use heed_core::env::EnvBuilder;
use heed_core::db::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Debugging main database persistence...\n");
    
    // Phase 1: Create a named database and check main DB
    {
        println!("Phase 1: Creating named database");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        // Check initial main DB state
        {
            let txn = env.begin_txn()?;
            let main_db_info = txn.db_info(None)?;
            println!("Initial main DB info:");
            println!("  Root: {:?}", main_db_info.root);
            println!("  Entries: {}", main_db_info.entries);
        }
        
        // Create a named database
        {
            let mut txn = env.begin_write_txn()?;
            let db: Database<String, String> = env.create_database(&mut txn, Some("test_db"))?;
            db.put(&mut txn, "key1".to_string(), "value1".to_string())?;
            
            // Check main DB state before commit
            let main_db_info = txn.db_info(None)?;
            println!("\nMain DB info before commit:");
            println!("  Root: {:?}", main_db_info.root);
            println!("  Entries: {}", main_db_info.entries);
            
            txn.commit()?;
        }
        
        // Check main DB state after commit
        {
            let txn = env.begin_txn()?;
            let main_db_info = txn.db_info(None)?;
            println!("\nMain DB info after commit:");
            println!("  Root: {:?}", main_db_info.root);
            println!("  Entries: {}", main_db_info.entries);
            
            // Try to read directly from main DB
            use heed_core::btree::BTree;
            match BTree::search(&txn, main_db_info.root, b"test_db")? {
                Some(value) => println!("Found 'test_db' in main DB, value len: {}", value.len()),
                None => println!("'test_db' NOT found in main DB!"),
            }
        }
    }
    
    // Phase 2: Reopen and check main DB
    {
        println!("\n\nPhase 2: Reopening environment");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        // Check main DB state after reopen
        {
            let txn = env.begin_txn()?;
            let main_db_info = txn.db_info(None)?;
            println!("\nMain DB info after reopen:");
            println!("  Root: {:?}", main_db_info.root);
            println!("  Entries: {}", main_db_info.entries);
            
            // Try to read directly from main DB
            use heed_core::btree::BTree;
            if main_db_info.root.0 != 0 {
                match BTree::search(&txn, main_db_info.root, b"test_db")? {
                    Some(value) => println!("Found 'test_db' in main DB, value len: {}", value.len()),
                    None => println!("'test_db' NOT found in main DB!"),
                }
            } else {
                println!("Main DB root is 0!");
            }
        }
        
        // Check the meta page directly by getting stats
        {
            let stats = env.stat()?;
            println!("\nEnvironment stats:");
            println!("  Entries: {}", stats.entries);
            println!("  Depth: {}", stats.depth);
        }
    }
    
    Ok(())
}