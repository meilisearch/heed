//! Debug environment initialization

use heed_core::{EnvBuilder};
use heed_core::error::Result;

fn main() -> Result<()> {
    println!("Testing environment initialization...\n");
    
    // Create environment
    let dir = tempfile::tempdir().unwrap();
    println!("Creating environment at: {:?}", dir.path());
    
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())?;
    
    println!("✓ Environment created");
    
    // Get environment info
    let inner = env.inner();
    
    // Check databases
    {
        let dbs = inner.databases.read().unwrap();
        println!("\nDatabases in environment:");
        for (name, info) in dbs.iter() {
            match name {
                None => println!("  - Main DB: root={}, entries={}", info.root.0, info.entries),
                Some(n) => println!("  - Named '{}': root={}, entries={}", n, info.root.0, info.entries),
            }
        }
    }
    
    // Try to read meta info
    match inner.meta() {
        Ok(meta) => {
            println!("\nMeta page info:");
            println!("  - Last txn ID: {}", meta.last_txnid.0);
            println!("  - Last page: {}", meta.last_pg.0);
            println!("  - Main DB root: {}", meta.main_db.root.0);
            println!("  - Free DB root: {}", meta.free_db.root.0);
        }
        Err(e) => {
            println!("\nError reading meta: {:?}", e);
        }
    }
    
    // Check next page ID
    let next_page = inner.next_page_id.load(std::sync::atomic::Ordering::SeqCst);
    println!("\nNext page ID: {}", next_page);
    
    // Try to begin a transaction
    println!("\nTrying to begin write transaction...");
    match env.begin_write_txn() {
        Ok(txn) => {
            println!("✓ Write transaction created");
            println!("  - Transaction ID: {}", txn.id().0);
            
            // Check transaction databases
            if let Some(main_db) = txn.data.databases.get(&None) {
                println!("  - Main DB in txn: root={}", main_db.root.0);
            }
        }
        Err(e) => {
            println!("✗ Failed to create write transaction: {:?}", e);
            return Err(e);
        }
    }
    
    println!("\nEnvironment debug completed!");
    Ok(())
}