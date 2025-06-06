//! Demonstration of reader enumeration and monitoring

use heed_core::{EnvBuilder, Database};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let env = Arc::new(EnvBuilder::new()
        .map_size(10 * 1024 * 1024)
        .max_readers(10)
        .open(dir.path())?);
    
    // Create a database with some data
    {
        let mut wtxn = env.begin_write_txn()?;
        let db: Database<String, String> = env.create_database(&mut wtxn, None)?;
        
        for i in 0..10 {
            db.put(&mut wtxn, format!("key{}", i), format!("value{}", i))?;
        }
        
        wtxn.commit()?;
    }
    
    println!("=== Reader Monitoring Demo ===\n");
    
    // Spawn multiple reader threads
    let mut handles = vec![];
    
    for i in 0..3 {
        let env_clone = Arc::clone(&env);
        let handle = thread::spawn(move || {
            let rtxn = env_clone.begin_txn().unwrap();
            println!("Reader {} started with transaction ID: {:?}", i, rtxn.id());
            
            // Simulate some work
            thread::sleep(Duration::from_secs(2 + i));
            
            println!("Reader {} finished", i);
            // Transaction dropped here
        });
        handles.push(handle);
        
        // Small delay to ensure different timestamps
        thread::sleep(Duration::from_millis(100));
    }
    
    // Monitor readers while they're active
    let monitor_env = Arc::clone(&env);
    let monitor_handle = thread::spawn(move || {
        for _ in 0..5 {
            thread::sleep(Duration::from_secs(1));
            
            let inner = monitor_env.inner();
            let readers = inner.readers.enumerate_readers();
            
            println!("\n--- Active Readers ---");
            println!("Total count: {}", readers.len());
            
            for reader in readers {
                println!("Reader {}:", reader.slot_index);
                println!("  PID: {}", reader.pid);
                println!("  TID: {}", reader.tid);
                println!("  Transaction ID: {:?}", reader.txn_id);
                println!("  Age: {} seconds", reader.age_seconds());
                println!("  Stale: {}", reader.is_stale);
            }
            
            // Also show oldest reader
            if let Some(oldest) = inner.readers.oldest_reader() {
                println!("\nOldest reader transaction: {:?}", oldest);
            }
        }
    });
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    monitor_handle.join().unwrap();
    
    // Final check - should be no readers
    println!("\n--- Final Check ---");
    let inner = env.inner();
    let final_readers = inner.readers.enumerate_readers();
    println!("Active readers after all threads finished: {}", final_readers.len());
    
    // Demonstrate stale reader cleanup
    println!("\n=== Stale Reader Cleanup ===");
    
    // Simulate a stale reader by acquiring a slot without proper cleanup
    // (In real scenarios, this would happen when a process crashes)
    
    let cleaned = inner.readers.cleanup_stale();
    println!("Cleaned {} stale readers", cleaned);
    
    Ok(())
}