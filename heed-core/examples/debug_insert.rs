//! Simple debug example for B+Tree insertion

use heed_core::{Environment, EnvBuilder, Error};
use heed_core::error::PageId;
use heed_core::meta::DbInfo;
use heed_core::btree::BTree;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    println!("Creating environment at: {:?}", dir.path());
    
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .open(dir.path())?;
    
    println!("Environment created");
    
    let mut txn = env.begin_write_txn()?;
    println!("Write transaction started");
    
    let mut root = PageId(3); // Main DB root
    let mut db_info = DbInfo::default();
    db_info.root = root;
    db_info.leaf_pages = 1;
    
    println!("Inserting key...");
    let old = BTree::insert(&mut txn, &mut root, &mut db_info, b"key1", b"value1")?;
    println!("Insert completed, old value: {:?}", old);
    
    // Check the page
    let page = txn.get_page(root)?;
    println!("Page has {} keys", page.header.num_keys);
    
    println!("Committing transaction...");
    txn.commit()?;
    println!("Transaction committed");
    
    // Search for the key
    println!("Starting read transaction...");
    let txn = env.begin_txn()?;
    let result = BTree::search(&txn, root, b"key1")?;
    println!("Search result: {:?}", result.as_ref().map(|v| std::str::from_utf8(v).unwrap()));
    
    Ok(())
}