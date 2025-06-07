use heed_core::env::EnvBuilder;
use heed_core::db::{Database, DatabaseFlags};
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Debugging commit flow...\n");
    
    // Create environment and database
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(&db_path)?);
    
    // Check initial state
    {
        let txn = env.begin_txn()?;
        let main_db_info = txn.db_info(None)?;
        println!("Initial main DB: root={:?}, entries={}", main_db_info.root, main_db_info.entries);
    }
    
    // Create a named database in a single transaction
    {
        println!("\nCreating database in a transaction...");
        let mut txn = env.begin_write_txn()?;
        
        // Check main DB before creating database
        let main_before = *txn.db_info(None)?;
        println!("Main DB before: root={:?}, entries={}", main_before.root, main_before.entries);
        
        // Create the database using the Environment method
        let _db: Database<String, String> = env.create_database(&mut txn, Some("test_db"))?;
        
        // Check main DB after creating database
        let main_after = *txn.db_info(None)?;
        println!("Main DB after: root={:?}, entries={}", main_after.root, main_after.entries);
        
        // Check what's in the transaction's database map
        println!("\nTransaction's database map before commit:");
        // We can't directly access this, but we know it should have both None (main) and Some("test_db")
        
        txn.commit()?;
        println!("Transaction committed");
    }
    
    // Check state after commit
    {
        let txn = env.begin_txn()?;
        let main_db_info = txn.db_info(None)?;
        println!("\nMain DB after commit: root={:?}, entries={}", main_db_info.root, main_db_info.entries);
    }
    
    // Now test with Database::open approach
    println!("\n\nTesting with Database::open...");
    {
        let _db2: Database<String, String> = Database::open(&env, Some("test_db2"), DatabaseFlags::CREATE)?;
        println!("Created test_db2 with Database::open");
        
        // Check state
        let txn = env.begin_txn()?;
        let main_db_info = txn.db_info(None)?;
        println!("Main DB after Database::open: root={:?}, entries={}", main_db_info.root, main_db_info.entries);
    }
    
    Ok(())
}