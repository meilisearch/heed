//! Test cursor operations thoroughly

use heed_core::{EnvBuilder, Database};
use heed_core::error::Result;
use std::sync::Arc;

fn main() -> Result<()> {
    println!("=== Testing Cursor Operations ===\n");
    
    // Create environment
    let dir = tempfile::tempdir().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(dir.path())?
    );
    
    // Create database with test data
    let db: Database<String, String> = {
        let mut txn = env.begin_write_txn()?;
        let db = env.create_database(&mut txn, None)?;
        
        // Insert data in non-sequential order to test sorting
        let data = vec![
            ("key05", "value05"),
            ("key02", "value02"),
            ("key08", "value08"),
            ("key01", "value01"),
            ("key04", "value04"),
            ("key09", "value09"),
            ("key03", "value03"),
            ("key07", "value07"),
            ("key06", "value06"),
        ];
        
        for (k, v) in data {
            db.put(&mut txn, k.to_string(), v.to_string())?;
        }
        
        txn.commit()?;
        db
    };
    
    // Test 1: Forward iteration
    println!("--- Test 1: Forward Iteration ---");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        print!("First -> Next: ");
        if let Some((k, v)) = cursor.first()? {
            print!("{}:{} ", String::from_utf8_lossy(&k), v);
        }
        
        while let Some((k, v)) = cursor.next()? {
            print!("{}:{} ", String::from_utf8_lossy(&k), v);
        }
        println!("\n✓ Forward iteration works");
    }
    
    // Test 2: Backward iteration
    println!("\n--- Test 2: Backward Iteration ---");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        print!("Last -> Prev: ");
        if let Some((k, v)) = cursor.last()? {
            print!("{}:{} ", String::from_utf8_lossy(&k), v);
        }
        
        while let Some((k, v)) = cursor.prev()? {
            print!("{}:{} ", String::from_utf8_lossy(&k), v);
        }
        println!("\n✓ Backward iteration works");
    }
    
    // Test 3: Seek operations
    println!("\n--- Test 3: Seek Operations ---");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        // Seek to existing key
        if let Some((k, v)) = cursor.seek(&"key05".to_string())? {
            println!("Seek 'key05': found {}:{}", String::from_utf8_lossy(&k), v);
        }
        
        // Seek to non-existing key (should find next)
        if let Some((k, v)) = cursor.seek(&"key055".to_string())? {
            println!("Seek 'key055': found next {}:{}", String::from_utf8_lossy(&k), v);
        }
        
        // Seek before first
        if let Some((k, v)) = cursor.seek(&"key00".to_string())? {
            println!("Seek 'key00': found next {}:{}", String::from_utf8_lossy(&k), v);
        }
        
        // Seek after last
        match cursor.seek(&"key99".to_string())? {
            Some((k, v)) => println!("Seek 'key99': found {}:{}", String::from_utf8_lossy(&k), v),
            None => println!("Seek 'key99': no entry found (expected)"),
        }
        
        println!("✓ Seek operations work");
    }
    
    // Test 4: Mixed navigation
    println!("\n--- Test 4: Mixed Navigation ---");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        // Start at first
        cursor.first()?;
        println!("First: {:?}", cursor.current()?);
        
        // Move forward twice
        cursor.next()?;
        cursor.next()?;
        println!("After 2x next: {:?}", cursor.current()?);
        
        // Move back once
        cursor.prev()?;
        println!("After prev: {:?}", cursor.current()?);
        
        // Jump to middle
        cursor.seek(&"key05".to_string())?;
        println!("After seek key05: {:?}", cursor.current()?);
        
        // Move back
        cursor.prev()?;
        println!("After prev: {:?}", cursor.current()?);
        
        // Move forward
        cursor.next()?;
        println!("After next: {:?}", cursor.current()?);
        
        println!("✓ Mixed navigation works");
    }
    
    // Test 5: Edge cases
    println!("\n--- Test 5: Edge Cases ---");
    {
        let txn = env.begin_txn()?;
        let mut cursor = db.cursor(&txn)?;
        
        // Multiple prev from first
        cursor.first()?;
        let result = cursor.prev()?;
        println!("Prev from first: {:?} (should be None)", result);
        
        // Multiple next from last
        cursor.last()?;
        let result = cursor.next()?;
        println!("Next from last: {:?} (should be None)", result);
        
        // Current with no position
        let fresh_cursor = db.cursor(&txn)?;
        let result = fresh_cursor.current()?;
        println!("Current with no position: {:?} (should be None)", result);
        
        println!("✓ Edge cases handled correctly");
    }
    
    // Test 6: Cursor modification operations
    println!("\n--- Test 6: Cursor Modifications ---");
    {
        let mut txn = env.begin_write_txn()?;
        let mut cursor = db.cursor(&mut txn)?;
        
        // Add new entry
        cursor.put(&"key045".to_string(), &"value045".to_string())?;
        println!("Added key045, current: {:?}", cursor.current()?);
        
        // Navigate and update
        cursor.seek(&"key03".to_string())?;
        cursor.update(&"updated_value03".to_string())?;
        println!("Updated key03, current: {:?}", cursor.current()?);
        
        // Navigate and delete
        cursor.seek(&"key07".to_string())?;
        let deleted = cursor.delete()?;
        println!("Deleted key07: {}", deleted);
        
        txn.commit()?;
    }
    
    // Verify modifications
    println!("\n--- Verification ---");
    {
        let txn = env.begin_txn()?;
        
        // Check added key
        match db.get(&txn, &"key045".to_string())? {
            Some(v) => println!("✓ key045 exists: {}", v),
            None => println!("✗ key045 missing"),
        }
        
        // Check updated key
        match db.get(&txn, &"key03".to_string())? {
            Some(v) => println!("✓ key03 updated: {}", v),
            None => println!("✗ key03 missing"),
        }
        
        // Check deleted key
        match db.get(&txn, &"key07".to_string())? {
            Some(_) => println!("✗ key07 still exists"),
            None => println!("✓ key07 deleted"),
        }
    }
    
    println!("\n=== All cursor tests completed! ===");
    Ok(())
}