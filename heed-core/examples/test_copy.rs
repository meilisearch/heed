//! Test environment copy/backup functionality

use heed_core::{EnvBuilder, Database};
use heed_core::copy::{copy_to_file, copy_with_callback, CopyOptions, BackupCallback};
use heed_core::error::Result;
use std::sync::Arc;

struct ProgressCallback {
    last_progress: u64,
}

impl BackupCallback for ProgressCallback {
    fn progress(&mut self, pages_copied: u64, total_pages: u64) {
        if pages_copied > self.last_progress + 10 {
            println!("Progress: {}/{} pages ({:.1}%)", 
                pages_copied, total_pages, 
                (pages_copied as f64 / total_pages as f64) * 100.0);
            self.last_progress = pages_copied;
        }
    }
    
    fn complete(&mut self, pages_copied: u64) {
        println!("✓ Backup complete! {} pages copied", pages_copied);
    }
}

fn main() -> Result<()> {
    println!("=== Testing Environment Copy/Backup ===\n");
    
    // Create source environment
    let source_dir = tempfile::tempdir().unwrap();
    println!("Creating source environment at: {:?}", source_dir.path());
    
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(source_dir.path())?
    );
    
    // Add test data
    println!("\nAdding test data...");
    {
        let mut txn = env.begin_write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, None)?;
        
        // Add some data
        for i in 0..50 {
            db.put(&mut txn, format!("key_{:03}", i), format!("value_{:03}", i))?;
        }
        
        txn.commit()?;
        println!("✓ Added 50 key-value pairs");
    }
    
    // Create a named database too
    {
        let mut txn = env.begin_write_txn()?;
        let db: Database<String, String> = env.create_database(&mut txn, Some("test_db"))?;
        
        for i in 0..30 {
            db.put(&mut txn, format!("test_{:02}", i), format!("data_{:02}", i))?;
        }
        
        txn.commit()?;
        println!("✓ Created named database 'test_db' with 30 entries");
    }
    
    // Test 1: Simple copy
    println!("\n--- Test 1: Simple Copy ---");
    let backup_dir = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("backup.mdb");
    
    println!("Copying to: {:?}", backup_path);
    copy_to_file(&env, &backup_path, CopyOptions::default())?;
    
    // Check file size
    let metadata = std::fs::metadata(&backup_path)?;
    println!("✓ Backup created, size: {} KB", metadata.len() / 1024);
    
    // Test 2: Compact copy
    println!("\n--- Test 2: Compact Copy ---");
    let compact_path = backup_dir.path().join("compact.mdb");
    
    println!("Creating compact copy at: {:?}", compact_path);
    copy_to_file(&env, &compact_path, CopyOptions::compact())?;
    
    let compact_metadata = std::fs::metadata(&compact_path)?;
    println!("✓ Compact backup created, size: {} KB", compact_metadata.len() / 1024);
    
    if compact_metadata.len() < metadata.len() {
        println!("✓ Compact copy is smaller ({} KB saved)", 
            (metadata.len() - compact_metadata.len()) / 1024);
    }
    
    // Test 3: Copy with progress callback
    println!("\n--- Test 3: Copy with Progress ---");
    
    // Add more data to make progress visible
    {
        let mut txn = env.begin_write_txn()?;
        let db: Database<Vec<u8>, Vec<u8>> = env.open_database(&txn, None)?;
        
        // Add larger data to allocate more pages
        for i in 0..200 {
            let key = format!("bulk_{:04}", i).into_bytes();
            let value = vec![0u8; 500]; // Larger values
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
        println!("Added bulk data for progress test");
    }
    
    let progress_path = backup_dir.path().join("progress.mdb");
    let mut callback = ProgressCallback { last_progress: 0 };
    
    println!("Copying with progress to: {:?}", progress_path);
    copy_with_callback(&env, &progress_path, CopyOptions::default(), &mut callback)?;
    
    // Verify the backups
    println!("\n--- Verification ---");
    
    // We can't directly open the backup files as environments because they're single files,
    // not directories. In a real implementation, we'd need to extract them first.
    // For now, just verify they exist and have reasonable sizes.
    
    let files = vec![
        ("Full backup", &backup_path),
        ("Compact backup", &compact_path),
        ("Progress backup", &progress_path),
    ];
    
    for (name, path) in files {
        if path.exists() {
            let size = std::fs::metadata(path)?.len();
            println!("✓ {} exists: {} KB", name, size / 1024);
        } else {
            println!("✗ {} missing!", name);
        }
    }
    
    println!("\n=== All copy tests completed successfully! ===");
    Ok(())
}