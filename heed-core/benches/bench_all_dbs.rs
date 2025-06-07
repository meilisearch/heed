//! Quick benchmark comparison between heed-core, LMDB FFI, RocksDB, and redb
//! 
//! This provides a performance comparison across all major embedded databases

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
    println!("Writing 10,000 key-value pairs (16 byte keys, 100 byte values)");
    
    let data: Vec<(Vec<u8>, Vec<u8>)> = (0..10_000)
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
        
        println!("heed-core:  {} ({:.0} ops/sec)", 
            format_duration(duration),
            10_000.0 / duration.as_secs_f64()
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
        
        println!("LMDB FFI:   {} ({:.0} ops/sec)", 
            format_duration(duration),
            10_000.0 / duration.as_secs_f64()
        );
    }
    
    // Benchmark RocksDB
    {
        let dir = TempDir::new().unwrap();
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        
        let db = rocksdb::DB::open(&opts, dir.path()).unwrap();
        
        let start = Instant::now();
        let mut batch = rocksdb::WriteBatch::default();
        
        for (key, value) in &data {
            batch.put(key, value);
        }
        
        db.write(batch).unwrap();
        let duration = start.elapsed();
        
        println!("RocksDB:    {} ({:.0} ops/sec)", 
            format_duration(duration),
            10_000.0 / duration.as_secs_f64()
        );
    }
    
    // Benchmark redb
    {
        let dir = TempDir::new().unwrap();
        let db = redb::Database::create(dir.path().join("redb.db")).unwrap();
        
        const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("bench");
        
        let start = Instant::now();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            for (key, value) in &data {
                table.insert(key.as_slice(), value.as_slice()).unwrap();
            }
        }
        write_txn.commit().unwrap();
        let duration = start.elapsed();
        
        println!("redb:       {} ({:.0} ops/sec)", 
            format_duration(duration),
            10_000.0 / duration.as_secs_f64()
        );
    }
}

fn bench_random_reads() {
    println!("\n=== Random Read Benchmark ===");
    println!("Reading 1,000 random keys from 10,000 total");
    
    // Prepare data
    let all_keys: Vec<Vec<u8>> = (0..10_000)
        .map(|i| format!("key_{:08}", i).into_bytes())
        .collect();
    
    let read_indices: Vec<usize> = (0..1_000)
        .map(|i| (i * 7919) % 10_000) // Pseudo-random but deterministic
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
        
        for (i, key) in all_keys.iter().enumerate() {
            let value = vec![i as u8; 100];
            db.put(&mut txn, key.clone(), value).unwrap();
        }
        
        txn.commit().unwrap();
        
        // Benchmark reads
        let txn = env.begin_txn().unwrap();
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = 
            env.open_database(&txn, None).unwrap();
        
        let start = Instant::now();
        let mut found = 0;
        
        for &idx in &read_indices {
            if let Some(_) = db.get(&txn, &all_keys[idx]).unwrap() {
                found += 1;
            }
        }
        
        let duration = start.elapsed();
        println!("heed-core:  {} ({:.0} ops/sec, {} found)", 
            format_duration(duration),
            1_000.0 / duration.as_secs_f64(),
            found
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
        
        // Populate
        let mut txn = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            env.create_database(&mut txn, None).unwrap();
        
        for (i, key) in all_keys.iter().enumerate() {
            let value = vec![i as u8; 100];
            db.put(&mut txn, key, &value).unwrap();
        }
        
        txn.commit().unwrap();
        
        // Benchmark reads
        let txn = env.read_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            env.open_database(&txn, None).unwrap().unwrap();
        
        let start = Instant::now();
        let mut found = 0;
        
        for &idx in &read_indices {
            if let Some(_) = db.get(&txn, &all_keys[idx]).unwrap() {
                found += 1;
            }
        }
        
        let duration = start.elapsed();
        println!("LMDB FFI:   {} ({:.0} ops/sec, {} found)", 
            format_duration(duration),
            1_000.0 / duration.as_secs_f64(),
            found
        );
    }
    
    // Benchmark RocksDB
    {
        let dir = TempDir::new().unwrap();
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        
        let db = rocksdb::DB::open(&opts, dir.path()).unwrap();
        
        // Populate
        let mut batch = rocksdb::WriteBatch::default();
        for (i, key) in all_keys.iter().enumerate() {
            let value = vec![i as u8; 100];
            batch.put(key, value);
        }
        db.write(batch).unwrap();
        
        // Benchmark reads
        let start = Instant::now();
        let mut found = 0;
        
        for &idx in &read_indices {
            if let Ok(Some(_)) = db.get(&all_keys[idx]) {
                found += 1;
            }
        }
        
        let duration = start.elapsed();
        println!("RocksDB:    {} ({:.0} ops/sec, {} found)", 
            format_duration(duration),
            1_000.0 / duration.as_secs_f64(),
            found
        );
    }
    
    // Benchmark redb
    {
        let dir = TempDir::new().unwrap();
        let db = redb::Database::create(dir.path().join("redb.db")).unwrap();
        
        const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("bench");
        
        // Populate
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            for (i, key) in all_keys.iter().enumerate() {
                let value = vec![i as u8; 100];
                table.insert(key.as_slice(), value.as_slice()).unwrap();
            }
        }
        write_txn.commit().unwrap();
        
        // Benchmark reads
        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TABLE).unwrap();
        
        let start = Instant::now();
        let mut found = 0;
        
        for &idx in &read_indices {
            if let Ok(Some(_)) = table.get(all_keys[idx].as_slice()) {
                found += 1;
            }
        }
        
        let duration = start.elapsed();
        println!("redb:       {} ({:.0} ops/sec, {} found)", 
            format_duration(duration),
            1_000.0 / duration.as_secs_f64(),
            found
        );
    }
}

fn main() {
    println!("Embedded Database Performance Comparison");
    println!("========================================");
    println!("Comparing: heed-core, LMDB FFI, RocksDB, redb");
    
    bench_sequential_writes();
    bench_random_reads();
    
    println!("\nNote: These are simple benchmarks. Real-world performance may vary.");
    println!("For comprehensive analysis, use 'cargo bench'.");
}