use heed_core::env::EnvBuilder;
use heed_core::db::{Database, DatabaseFlags};
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Testing catalog fix...\n");
    
    // Test 1: Verify the issue - databases created with Database::open don't persist
    {
        println!("Test 1: Creating database with Database::open (uses Catalog)");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        // Create a database using Database::open (which should use Catalog)
        let _db1: Database<String, String> = Database::open(&env, Some("catalog_db"), DatabaseFlags::CREATE)?;
        
        println!("Created database 'catalog_db'");
    }
    
    // Reopen and check
    {
        println!("\nReopening environment...");
        let env = Arc::new(EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .open(&db_path)?);
        
        match Database::<String, String>::open(&env, Some("catalog_db"), DatabaseFlags::empty()) {
            Ok(_) => println!("✓ Database 'catalog_db' found after reopen!"),
            Err(e) => println!("✗ Database 'catalog_db' NOT found after reopen: {:?}", e),
        }
    }
    
    // Test 2: Compare serialization formats
    {
        println!("\n\nTest 2: Comparing serialization formats");
        
        // Create a DbInfo structure
        let test_info = heed_core::meta::DbInfo {
            flags: 0x42,
            depth: 3,
            branch_pages: 100,
            leaf_pages: 500,
            overflow_pages: 10,
            entries: 1000,
            root: heed_core::error::PageId(42),
        };
        
        // Serialize using Catalog method
        let catalog_bytes = heed_core::catalog::Catalog::serialize_db_info(&test_info);
        println!("Catalog serialization: {} bytes", catalog_bytes.len());
        
        // Serialize using raw memory copy (as done in env.create_database)
        let raw_bytes = unsafe {
            std::slice::from_raw_parts(
                &test_info as *const _ as *const u8,
                std::mem::size_of::<heed_core::meta::DbInfo>()
            )
        };
        println!("Raw memory serialization: {} bytes", raw_bytes.len());
        
        // Check if they're the same
        if catalog_bytes.len() != raw_bytes.len() {
            println!("✗ Serialization formats differ in size!");
        } else if catalog_bytes != raw_bytes {
            println!("✗ Serialization formats differ in content!");
            println!("  Catalog bytes: {:?}", &catalog_bytes[..16]);
            println!("  Raw bytes: {:?}", &raw_bytes[..16]);
        } else {
            println!("✓ Serialization formats match");
        }
        
        // Try to deserialize raw bytes with Catalog method
        match heed_core::catalog::Catalog::deserialize_db_info(raw_bytes) {
            Ok(_) => println!("✓ Raw bytes can be deserialized by Catalog"),
            Err(e) => println!("✗ Raw bytes CANNOT be deserialized by Catalog: {:?}", e),
        }
    }
    
    Ok(())
}