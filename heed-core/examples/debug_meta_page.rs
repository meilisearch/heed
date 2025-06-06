use heed_core::env::EnvBuilder;
use heed_core::db::{Database, DatabaseFlags};
use heed_core::io::{IoBackend, MmapBackend};
use heed_core::meta::{MetaPage, META_PAGE_1, META_PAGE_2};
use heed_core::page::Page;
use std::sync::Arc;
use tempfile::TempDir;

fn read_meta_pages(path: &std::path::Path) -> Result<(MetaPage, MetaPage), Box<dyn std::error::Error>> {
    let data_path = path.join("data.mdb");
    let io: Box<dyn IoBackend> = Box::new(MmapBackend::with_options(&data_path, 10 * 1024 * 1024)?);
    
    // Read both meta pages
    let meta0_page = io.read_page(META_PAGE_1)?;
    let meta1_page = io.read_page(META_PAGE_2)?;
    
    // Cast to MetaPage
    let meta0 = unsafe { *(meta0_page.data.as_ptr() as *const MetaPage) };
    let meta1 = unsafe { *(meta1_page.data.as_ptr() as *const MetaPage) };
    
    Ok((meta0, meta1))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Debugging meta page persistence...\n");
    
    // Phase 1: Create environment and database
    {
        println!("Phase 1: Creating database");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        // Create a named database
        let _db: Database<String, String> = Database::open(&env, Some("test_db"), DatabaseFlags::CREATE)?;
        
        println!("Created database 'test_db'");
    }
    
    // Read meta pages directly from disk
    println!("\nReading meta pages from disk...");
    let (meta0, meta1) = read_meta_pages(&db_path)?;
    
    println!("Meta page 0:");
    println!("  Last txn ID: {}", meta0.last_txnid.0);
    println!("  Main DB root: {:?}", meta0.main_db.root);
    println!("  Main DB entries: {}", meta0.main_db.entries);
    
    println!("\nMeta page 1:");
    println!("  Last txn ID: {}", meta1.last_txnid.0);
    println!("  Main DB root: {:?}", meta1.main_db.root);
    println!("  Main DB entries: {}", meta1.main_db.entries);
    
    // Determine which is the current meta page
    let current_meta = if meta0.last_txnid.0 >= meta1.last_txnid.0 { &meta0 } else { &meta1 };
    println!("\nCurrent meta page has:");
    println!("  Last txn ID: {}", current_meta.last_txnid.0);
    println!("  Main DB entries: {}", current_meta.main_db.entries);
    
    if current_meta.main_db.entries == 0 {
        println!("\nERROR: Main DB entries not persisted to meta page!");
    }
    
    // Phase 2: Reopen and verify
    {
        println!("\n\nPhase 2: Reopening environment");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        let txn = env.begin_txn()?;
        let main_db_info = txn.db_info(None)?;
        println!("Main DB after reopen: entries={}", main_db_info.entries);
    }
    
    Ok(())
}