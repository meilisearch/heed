//! Simple benchmark comparison between heed-core and LMDB
//! 
//! This provides a quick performance comparison without full criterion setup

use std::time::{Duration, Instant};
use tempfile::TempDir;
use std::sync::Arc;

fn format_duration(d: Duration) -> String {
    if d.as_secs() > 0 {
        format!("{:.2}s", d.as_secs_f64())
    } else if d.as_millis() > 0 {
        format!("{:.2}ms", d.as_millis() as f64)
    } else {
        format!("{:.2}μs", d.as_micros() as f64)
    }
}

fn bench_sequential_writes() {
    println!("\n=== Sequential Write Benchmark ===");
    println!("Writing 100,000 key-value pairs (16 byte keys, 100 byte values)");
    
    let data: Vec<(Vec<u8>, Vec<u8>)> = (0..100_000)
        .map(|i| {
            let key = format!("key_{:08}", i).into_bytes();
            let value = vec![i as u8; 100];
            (key, value)
        })
        .collect();
    
    // Benchmark heed-core
    {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            heed_core::EnvBuilder::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let start = Instant::now();
        let mut txn = env.begin_write_txn().unwrap();
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        
        for (key, value) in &data {
            db.put(&mut txn, key.clone(), value.clone()).unwrap();
        }
        
        txn.commit().unwrap();
        let duration = start.elapsed();
        
        println!("heed-core: {} ({:.0} ops/sec)", 
            format_duration(duration),
            100_000.0 / duration.as_secs_f64()
        );
    }
    
    // Benchmark LMDB (heed FFI)
    {
        let dir = TempDir::new().unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        };
        
        let start = Instant::now();
        let mut txn = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            env.create_database(&mut txn, None).unwrap();
        
        for (key, value) in &data {
            db.put(&mut txn, key, value).unwrap();
        }
        
        txn.commit().unwrap();
        let duration = start.elapsed();
        
        println!("LMDB FFI:  {} ({:.0} ops/sec)", 
            format_duration(duration),
            100_000.0 / duration.as_secs_f64()
        );
    }
}

fn bench_random_reads() {
    println!("\n=== Random Read Benchmark ===");
    println!("Reading 10,000 random keys from 100,000 total");
    
    // Prepare data
    let all_keys: Vec<Vec<u8>> = (0..100_000)
        .map(|i| format!("key_{:08}", i).into_bytes())
        .collect();
    
    let read_indices: Vec<usize> = (0..10_000)
        .map(|i| (i * 7919) % 100_000) // Pseudo-random but deterministic
        .collect();
    
    // Setup heed-core
    let core_dir = TempDir::new().unwrap();
    let core_env = Arc::new(
        heed_core::EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .open(core_dir.path())
            .unwrap()
    );
    
    // Populate heed-core
    {
        let mut txn = core_env.begin_write_txn().unwrap();
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = core_env.create_database(&mut txn, None).unwrap();
        
        for (i, key) in all_keys.iter().enumerate() {
            let value = vec![i as u8; 100];
            db.put(&mut txn, key.clone(), value).unwrap();
        }
        
        txn.commit().unwrap();
    }
    
    // Setup LMDB
    let lmdb_dir = TempDir::new().unwrap();
    let lmdb_env = unsafe {
        heed::EnvOpenOptions::new()
            .map_size(100 * 1024 * 1024)
            .open(lmdb_dir.path())
            .unwrap()
    };
    
    // Populate LMDB
    {
        let mut txn = lmdb_env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            lmdb_env.create_database(&mut txn, None).unwrap();
        
        for (i, key) in all_keys.iter().enumerate() {
            let value = vec![i as u8; 100];
            db.put(&mut txn, key, &value).unwrap();
        }
        
        txn.commit().unwrap();
    }
    
    // Benchmark heed-core reads
    {
        let txn = core_env.begin_txn().unwrap();
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = 
            core_env.open_database(&txn, None).unwrap();
        
        let start = Instant::now();
        let mut found = 0;
        
        for &idx in &read_indices {
            if let Some(_) = db.get(&txn, &all_keys[idx]).unwrap() {
                found += 1;
            }
        }
        
        let duration = start.elapsed();
        println!("heed-core: {} ({:.0} ops/sec, {} found)", 
            format_duration(duration),
            10_000.0 / duration.as_secs_f64(),
            found
        );
    }
    
    // Benchmark LMDB reads
    {
        let txn = lmdb_env.read_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            lmdb_env.open_database(&txn, None).unwrap().unwrap();
        
        let start = Instant::now();
        let mut found = 0;
        
        for &idx in &read_indices {
            if let Some(_) = db.get(&txn, &all_keys[idx]).unwrap() {
                found += 1;
            }
        }
        
        let duration = start.elapsed();
        println!("LMDB FFI:  {} ({:.0} ops/sec, {} found)", 
            format_duration(duration),
            10_000.0 / duration.as_secs_f64(),
            found
        );
    }
}

fn bench_cursor_iteration() {
    println!("\n=== Cursor Iteration Benchmark ===");
    println!("Iterating through 50,000 entries");
    
    let data: Vec<(Vec<u8>, Vec<u8>)> = (0..50_000)
        .map(|i| {
            let key = format!("key_{:08}", i).into_bytes();
            let value = vec![(i % 256) as u8; 50];
            (key, value)
        })
        .collect();
    
    // Setup and benchmark heed-core
    {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            heed_core::EnvBuilder::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Populate
        let mut txn = env.begin_write_txn().unwrap();
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        
        for (key, value) in &data {
            db.put(&mut txn, key.clone(), value.clone()).unwrap();
        }
        txn.commit().unwrap();
        
        // Benchmark iteration
        let txn = env.begin_txn().unwrap();
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = 
            env.open_database(&txn, None).unwrap();
        
        let start = Instant::now();
        let mut cursor = db.cursor(&txn).unwrap();
        let mut count = 0;
        
        while let Some(_) = cursor.next().unwrap() {
            count += 1;
        }
        
        let duration = start.elapsed();
        println!("heed-core: {} ({:.0} entries/sec, {} total)", 
            format_duration(duration),
            count as f64 / duration.as_secs_f64(),
            count
        );
    }
    
    // Setup and benchmark LMDB
    {
        let dir = TempDir::new().unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        };
        
        // Populate
        let mut txn = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            env.create_database(&mut txn, None).unwrap();
        
        for (key, value) in &data {
            db.put(&mut txn, key, value).unwrap();
        }
        txn.commit().unwrap();
        
        // Benchmark iteration
        let txn = env.read_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            env.open_database(&txn, None).unwrap().unwrap();
        
        let start = Instant::now();
        let mut count = 0;
        
        for _ in db.iter(&txn).unwrap() {
            count += 1;
        }
        
        let duration = start.elapsed();
        println!("LMDB FFI:  {} ({:.0} entries/sec, {} total)", 
            format_duration(duration),
            count as f64 / duration.as_secs_f64(),
            count
        );
    }
}

fn bench_mixed_workload() {
    println!("\n=== Mixed Workload Benchmark ===");
    println!("1000 operations: 70% reads, 20% writes, 10% deletes");
    
    // Prepare initial data
    let initial_data: Vec<(Vec<u8>, Vec<u8>)> = (0..10_000)
        .map(|i| {
            let key = format!("key_{:08}", i).into_bytes();
            let value = vec![i as u8; 100];
            (key, value)
        })
        .collect();
    
    // Benchmark heed-core
    {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            heed_core::EnvBuilder::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Populate initial data
        let mut txn = env.begin_write_txn().unwrap();
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        
        for (key, value) in &initial_data {
            db.put(&mut txn, key.clone(), value.clone()).unwrap();
        }
        txn.commit().unwrap();
        
        // Run mixed workload
        let start = Instant::now();
        let mut reads = 0;
        let mut writes = 0;
        let mut deletes = 0;
        
        for i in 0..1000 {
            let op = i % 100;
            
            if op < 70 {
                // Read operation
                let txn = env.begin_txn().unwrap();
                let db: heed_core::Database<Vec<u8>, Vec<u8>> = 
                    env.open_database(&txn, None).unwrap();
                
                let key = &initial_data[i % initial_data.len()].0;
                let _ = db.get(&txn, key).unwrap();
                reads += 1;
            } else if op < 90 {
                // Write operation
                let mut txn = env.begin_write_txn().unwrap();
                let db: heed_core::Database<Vec<u8>, Vec<u8>> = 
                    env.open_database(&txn, None).unwrap();
                
                let key = format!("new_key_{}", i).into_bytes();
                let value = vec![i as u8; 100];
                db.put(&mut txn, key, value).unwrap();
                txn.commit().unwrap();
                writes += 1;
            } else {
                // Delete operation
                let mut txn = env.begin_write_txn().unwrap();
                let db: heed_core::Database<Vec<u8>, Vec<u8>> = 
                    env.open_database(&txn, None).unwrap();
                
                let key = &initial_data[i % initial_data.len()].0;
                let _ = db.delete(&mut txn, key);
                txn.commit().unwrap();
                deletes += 1;
            }
        }
        
        let duration = start.elapsed();
        println!("heed-core: {} ({:.0} ops/sec) - R:{} W:{} D:{}", 
            format_duration(duration),
            1000.0 / duration.as_secs_f64(),
            reads, writes, deletes
        );
    }
    
    // Benchmark LMDB
    {
        let dir = TempDir::new().unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        };
        
        // Populate initial data
        let mut txn = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            env.create_database(&mut txn, None).unwrap();
        
        for (key, value) in &initial_data {
            db.put(&mut txn, key, value).unwrap();
        }
        txn.commit().unwrap();
        
        // Run mixed workload
        let start = Instant::now();
        let mut reads = 0;
        let mut writes = 0;
        let mut deletes = 0;
        
        for i in 0..1000 {
            let op = i % 100;
            
            if op < 70 {
                // Read operation
                let txn = env.read_txn().unwrap();
                let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
                    env.open_database(&txn, None).unwrap().unwrap();
                
                let key = &initial_data[i % initial_data.len()].0;
                let _ = db.get(&txn, key).unwrap();
                reads += 1;
            } else if op < 90 {
                // Write operation
                let mut txn = env.write_txn().unwrap();
                let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
                    env.open_database(&txn, None).unwrap().unwrap();
                
                let key = format!("new_key_{}", i).into_bytes();
                let value = vec![i as u8; 100];
                db.put(&mut txn, &key, &value).unwrap();
                txn.commit().unwrap();
                writes += 1;
            } else {
                // Delete operation
                let mut txn = env.write_txn().unwrap();
                let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
                    env.open_database(&txn, None).unwrap().unwrap();
                
                let key = &initial_data[i % initial_data.len()].0;
                let _ = db.delete(&mut txn, key);
                txn.commit().unwrap();
                deletes += 1;
            }
        }
        
        let duration = start.elapsed();
        println!("LMDB FFI:  {} ({:.0} ops/sec) - R:{} W:{} D:{}", 
            format_duration(duration),
            1000.0 / duration.as_secs_f64(),
            reads, writes, deletes
        );
    }
}

fn main() {
    println!("heed-core vs LMDB Performance Comparison");
    println!("========================================");
    
    bench_sequential_writes();
    bench_random_reads();
    bench_cursor_iteration();
    bench_mixed_workload();
    
    println!("\nNote: These are simple benchmarks. For comprehensive analysis, use 'cargo bench'.");
}