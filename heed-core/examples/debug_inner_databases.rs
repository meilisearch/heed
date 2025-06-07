use heed_core::env::EnvBuilder;
use heed_core::db::{Database, DatabaseFlags};
use heed_core::io::{IoBackend, MmapBackend};
use heed_core::meta::{MetaPage, META_PAGE_1, META_PAGE_2};
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Debugging inner.databases initialization...\n");
    
    // Phase 1: Create database
    {
        println!("Phase 1: Creating database");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        let _db: Database<String, String> = Database::open(&env, Some("test_db"), DatabaseFlags::CREATE)?;
        println!("Created database 'test_db'");
    }
    
    // Check meta pages directly
    {
        println!("\nChecking meta pages directly...");
        let data_path = db_path.join("data.mdb");
        let io: Box<dyn IoBackend> = Box::new(MmapBackend::with_options(&data_path, 10 * 1024 * 1024)?);
        
        let meta0_page = io.read_page(META_PAGE_1)?;
        let meta1_page = io.read_page(META_PAGE_2)?;
        
        let meta0 = unsafe { *(meta0_page.data.as_ptr() as *const MetaPage) };
        let meta1 = unsafe { *(meta1_page.data.as_ptr() as *const MetaPage) };
        
        println!("Meta page 0: txn={}, main_db.entries={}", meta0.last_txnid.0, meta0.main_db.entries);
        println!("Meta page 1: txn={}, main_db.entries={}", meta1.last_txnid.0, meta1.main_db.entries);
        
        // Which is current?
        let current_is_0 = meta0.last_txnid.0 >= meta1.last_txnid.0;
        println!("Current meta page: {}", if current_is_0 { "0" } else { "1" });
    }
    
    // Phase 2: Debug environment initialization
    {
        println!("\n\nPhase 2: Tracing environment initialization");
        
        // Manually trace what EnvBuilder::open does
        let data_path = db_path.join("data.mdb");
        let io: Box<dyn IoBackend> = Box::new(MmapBackend::with_options(&data_path, 10 * 1024 * 1024)?);
        
        // Check if it's a new database
        let is_new_db = match io.read_page(META_PAGE_1) {
            Ok(page) => {
                let meta = unsafe { &*(page.as_ref() as *const heed_core::page::Page as *const MetaPage) };
                meta.magic != heed_core::meta::MAGIC
            }
            Err(_) => true,
        };
        
        println!("Is new database: {}", is_new_db);
        
        if !is_new_db {
            // This is the path taken for existing databases
            println!("\nLoading existing database...");
            
            // The code creates a temporary inner to read meta
            // Let's simulate what inner.meta() does
            let meta0_page = io.read_page(META_PAGE_1)?;
            let meta1_page = io.read_page(META_PAGE_2)?;
            
            let meta0 = unsafe { &*(meta0_page.data.as_ptr() as *const MetaPage) };
            let meta1 = unsafe { &*(meta1_page.data.as_ptr() as *const MetaPage) };
            
            let meta0_valid = meta0.validate().is_ok();
            let meta1_valid = meta1.validate().is_ok();
            
            println!("Meta page 0 valid: {}", meta0_valid);
            println!("Meta page 1 valid: {}", meta1_valid);
            
            let meta_info = match (meta0_valid, meta1_valid) {
                (true, true) => {
                    if meta0.last_txnid.0 >= meta1.last_txnid.0 {
                        *meta0
                    } else {
                        *meta1
                    }
                }
                (true, false) => *meta0,
                (false, true) => *meta1,
                (false, false) => panic!("Both meta pages invalid!"),
            };
            
            println!("\nChosen meta page has:");
            println!("  Last txn ID: {}", meta_info.last_txnid.0);
            println!("  Main DB entries: {}", meta_info.main_db.entries);
            println!("  Main DB root: {:?}", meta_info.main_db.root);
            
            // This is what gets stored in inner.databases
            println!("\nThis main_db info would be stored in inner.databases[None]");
        }
    }
    
    // Phase 3: Actually open and check
    {
        println!("\n\nPhase 3: Actually opening environment");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        let txn = env.begin_txn()?;
        let main_db_info = txn.db_info(None)?;
        println!("Transaction's main DB info: entries={}", main_db_info.entries);
        
        // Try to open the database
        match Database::<String, String>::open(&env, Some("test_db"), DatabaseFlags::empty()) {
            Ok(_) => println!("Successfully opened 'test_db'"),
            Err(e) => println!("Failed to open 'test_db': {:?}", e),
        }
    }
    
    Ok(())
}