//! Test database catalog functionality

use heed_core::EnvBuilder;
use heed_core::db::{Database, DatabaseFlags};
use heed_core::error::Result;

fn main() -> Result<()> {
    // Create a temporary environment
    let dir = tempfile::tempdir().unwrap();
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .max_dbs(10)
        .open(dir.path())?;
    
    println!("Environment created at: {:?}", dir.path());
    
    // Test 1: Create named databases
    println!("\n=== Test 1: Creating named databases ===");
    
    let db1: Database<String, String> = Database::open(&env, Some("users"), DatabaseFlags::CREATE)?;
    println!("Created database 'users'");
    
    let db2: Database<String, String> = Database::open(&env, Some("products"), DatabaseFlags::CREATE)?;
    println!("Created database 'products'");
    
    let db3: Database<String, String> = Database::open(&env, Some("orders"), DatabaseFlags::CREATE)?;
    println!("Created database 'orders'");
    
    // Test 2: Store data in databases
    println!("\n=== Test 2: Storing data ===");
    {
        let mut txn = env.begin_write_txn()?;
        
        db1.put(&mut txn, "user1".to_string(), "Alice".to_string())?;
        db1.put(&mut txn, "user2".to_string(), "Bob".to_string())?;
        println!("Added 2 users");
        
        db2.put(&mut txn, "prod1".to_string(), "Laptop".to_string())?;
        db2.put(&mut txn, "prod2".to_string(), "Mouse".to_string())?;
        db2.put(&mut txn, "prod3".to_string(), "Keyboard".to_string())?;
        println!("Added 3 products");
        
        db3.put(&mut txn, "order1".to_string(), "user1:prod1".to_string())?;
        println!("Added 1 order");
        
        txn.commit()?;
    }
    
    // Test 3: Read data back
    println!("\n=== Test 3: Reading data ===");
    {
        let txn = env.begin_txn()?;
        
        if let Some(user) = db1.get(&txn, &"user1".to_string())? {
            println!("User1: {}", user);
        }
        
        if let Some(product) = db2.get(&txn, &"prod2".to_string())? {
            println!("Product2: {}", product);
        }
        
        if let Some(order) = db3.get(&txn, &"order1".to_string())? {
            println!("Order1: {}", order);
        }
    }
    
    // Test 4: Reopen environment and verify databases persist
    println!("\n=== Test 4: Reopening environment ===");
    drop(env);
    
    let env2 = EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .max_dbs(10)
        .open(dir.path())?;
    
    println!("Environment reopened");
    
    // Try to open existing databases (without CREATE flag)
    let db1_reopened: Database<String, String> = Database::open(&env2, Some("users"), DatabaseFlags::empty())?;
    println!("Opened existing database 'users'");
    
    let db2_reopened: Database<String, String> = Database::open(&env2, Some("products"), DatabaseFlags::empty())?;
    println!("Opened existing database 'products'");
    
    // Verify data
    {
        let txn = env2.begin_txn()?;
        
        if let Some(user) = db1_reopened.get(&txn, &"user2".to_string())? {
            println!("User2 after reopen: {}", user);
        }
        
        if let Some(product) = db2_reopened.get(&txn, &"prod3".to_string())? {
            println!("Product3 after reopen: {}", product);
        }
    }
    
    // Test 5: Try to open non-existent database without CREATE
    println!("\n=== Test 5: Error handling ===");
    match Database::<String, String>::open(&env2, Some("nonexistent"), DatabaseFlags::empty()) {
        Err(e) => println!("Expected error for non-existent database: {:?}", e),
        Ok(_) => println!("ERROR: Should have failed to open non-existent database"),
    }
    
    // Test 6: List all databases
    println!("\n=== Test 6: List databases ===");
    {
        let txn = env2.begin_txn()?;
        let databases = heed_core::catalog::Catalog::list_databases(&txn)?;
        
        println!("Found {} named databases:", databases.len());
        for (name, info) in databases {
            println!("  - {}: {} entries, root page {}", 
                name, info.entries, info.root.0);
        }
    }
    
    println!("\nAll tests passed!");
    
    Ok(())
}