//! Benchmarks comparing various embedded databases
//!
//! This benchmark suite compares:
//! - heed-core (pure Rust LMDB)
//! - heed (LMDB FFI)
//! - RocksDB
//! - redb
//! - sled (optional)

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use tempfile::TempDir;
use std::sync::Arc;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use redb::ReadableTable;

// Database traits for uniform interface
trait Database: Send + Sync {
    fn write_batch(&self, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()>;
    fn read_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Vec<u8>>>>;
    fn scan_all(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn delete_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<()>;
    fn name(&self) -> &'static str;
}

// heed-core implementation
struct HeedCoreDb {
    env: Arc<heed_core::Environment<heed_core::env::state::Open>>,
}

impl HeedCoreDb {
    fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let env = Arc::new(
            heed_core::EnvBuilder::new()
                .map_size(1024 * 1024 * 1024) // 1GB
                .open(path)?
        );
        Ok(Self { env })
    }
}

impl Database for HeedCoreDb {
    fn write_batch(&self, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
        let mut txn = self.env.begin_write_txn()?;
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = self.env.create_database(&mut txn, None)?;
        
        for (key, value) in data {
            db.put(&mut txn, key.clone(), value.clone())?;
        }
        
        txn.commit()?;
        Ok(())
    }
    
    fn read_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let txn = self.env.begin_txn()?;
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = self.env.open_database(&txn, None)?;
        
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(db.get(&txn, key)?);
        }
        
        Ok(results)
    }
    
    fn scan_all(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let txn = self.env.begin_txn()?;
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = self.env.open_database(&txn, None)?;
        
        let mut cursor = db.cursor(&txn)?;
        let mut results = Vec::new();
        
        while let Some((key, value)) = cursor.next()? {
            results.push((key, value));
        }
        
        Ok(results)
    }
    
    fn delete_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        let mut txn = self.env.begin_write_txn()?;
        let db: heed_core::Database<Vec<u8>, Vec<u8>> = self.env.open_database(&txn, None)?;
        
        for key in keys {
            db.delete(&mut txn, key)?;
        }
        
        txn.commit()?;
        Ok(())
    }
    
    fn name(&self) -> &'static str {
        "heed-core"
    }
}

// LMDB (heed FFI) implementation
struct LmdbDb {
    env: heed::Env,
}

impl LmdbDb {
    fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1GB
                .open(path)?
        };
        Ok(Self { env })
    }
}

impl Database for LmdbDb {
    fn write_batch(&self, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
        let mut txn = self.env.write_txn()?;
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            self.env.create_database(&mut txn, None)?;
        
        for (key, value) in data {
            db.put(&mut txn, key, value)?;
        }
        
        txn.commit()?;
        Ok(())
    }
    
    fn read_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let txn = self.env.read_txn()?;
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            self.env.open_database(&txn, None)?.unwrap();
        
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(db.get(&txn, key)?.map(|v| v.to_vec()));
        }
        
        Ok(results)
    }
    
    fn scan_all(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let txn = self.env.read_txn()?;
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            self.env.open_database(&txn, None)?.unwrap();
        
        let mut results = Vec::new();
        for item in db.iter(&txn)? {
            let (key, value) = item?;
            results.push((key.to_vec(), value.to_vec()));
        }
        
        Ok(results)
    }
    
    fn delete_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        let mut txn = self.env.write_txn()?;
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = 
            self.env.open_database(&txn, None)?.unwrap();
        
        for key in keys {
            db.delete(&mut txn, key)?;
        }
        
        txn.commit()?;
        Ok(())
    }
    
    fn name(&self) -> &'static str {
        "lmdb-ffi"
    }
}

// RocksDB implementation
struct RocksDb {
    db: rocksdb::DB,
}

impl RocksDb {
    fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        
        let db = rocksdb::DB::open(&opts, path)?;
        Ok(Self { db })
    }
}

impl Database for RocksDb {
    fn write_batch(&self, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        
        for (key, value) in data {
            batch.put(key, value);
        }
        
        self.db.write(batch)?;
        Ok(())
    }
    
    fn read_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        
        for key in keys {
            results.push(self.db.get(key)?);
        }
        
        Ok(results)
    }
    
    fn scan_all(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        
        for item in iter {
            let (key, value) = item?;
            results.push((key.to_vec(), value.to_vec()));
        }
        
        Ok(results)
    }
    
    fn delete_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        
        for key in keys {
            batch.delete(key);
        }
        
        self.db.write(batch)?;
        Ok(())
    }
    
    fn name(&self) -> &'static str {
        "rocksdb"
    }
}

// redb implementation
struct RedbDb {
    db: Arc<redb::Database>,
}

impl RedbDb {
    fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let db = Arc::new(redb::Database::create(path)?);
        Ok(Self { db })
    }
}

const REDB_TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("bench");

impl Database for RedbDb {
    fn write_batch(&self, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REDB_TABLE)?;
            for (key, value) in data {
                table.insert(key.as_slice(), value.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }
    
    fn read_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_TABLE)?;
        
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(table.get(key.as_slice())?.map(|v| v.value().to_vec()));
        }
        
        Ok(results)
    }
    
    fn scan_all(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REDB_TABLE)?;
        
        let mut results = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            results.push((key.value().to_vec(), value.value().to_vec()));
        }
        
        Ok(results)
    }
    
    fn delete_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REDB_TABLE)?;
            for key in keys {
                table.remove(key.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }
    
    fn name(&self) -> &'static str {
        "redb"
    }
}

// Benchmark data generation
fn generate_data(count: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut data = Vec::with_capacity(count);
    
    for _ in 0..count {
        let key: Vec<u8> = (0..key_size).map(|_| rng.gen()).collect();
        let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
        data.push((key, value));
    }
    
    data
}

fn bench_sequential_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_writes");
    group.sample_size(10);
    
    let sizes = vec![1000, 10000, 100000];
    
    for size in sizes {
        let data = generate_data(size, 16, 100);
        
        for db_creator in &[
            |path: &std::path::Path| -> Box<dyn Database> { Box::new(HeedCoreDb::new(path).unwrap()) },
            |path: &std::path::Path| -> Box<dyn Database> { Box::new(LmdbDb::new(path).unwrap()) },
            |path: &std::path::Path| -> Box<dyn Database> { Box::new(RocksDb::new(path).unwrap()) },
            |path: &std::path::Path| -> Box<dyn Database> { Box::new(RedbDb::new(path).unwrap()) },
        ] {
            let temp_dir = TempDir::new().unwrap();
            let db = db_creator(temp_dir.path());
            
            group.bench_with_input(
                BenchmarkId::new(db.name(), size),
                &data,
                |b, data| {
                    b.iter(|| {
                        db.write_batch(black_box(data)).unwrap();
                    });
                },
            );
        }
    }
    
    group.finish();
}

fn bench_random_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_reads");
    group.sample_size(10);
    
    let size = 100000;
    let read_count = 1000;
    
    let data = generate_data(size, 16, 100);
    let mut rng = StdRng::seed_from_u64(123);
    let read_keys: Vec<Vec<u8>> = (0..read_count)
        .map(|_| data[rng.gen_range(0..size)].0.clone())
        .collect();
    
    for db_creator in &[
        |path: &std::path::Path| -> Box<dyn Database> { Box::new(HeedCoreDb::new(path).unwrap()) },
        |path: &std::path::Path| -> Box<dyn Database> { Box::new(LmdbDb::new(path).unwrap()) },
        |path: &std::path::Path| -> Box<dyn Database> { Box::new(RocksDb::new(path).unwrap()) },
        |path: &std::path::Path| -> Box<dyn Database> { Box::new(RedbDb::new(path).unwrap()) },
    ] {
        let temp_dir = TempDir::new().unwrap();
        let db = db_creator(temp_dir.path());
        
        // Populate database
        db.write_batch(&data).unwrap();
        
        group.bench_with_input(
            BenchmarkId::new(db.name(), read_count),
            &read_keys,
            |b, keys| {
                b.iter(|| {
                    let results = db.read_batch(black_box(keys)).unwrap();
                    black_box(results);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_scan_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_all");
    group.sample_size(10);
    
    let sizes = vec![1000, 10000, 50000];
    
    for size in sizes {
        let data = generate_data(size, 16, 100);
        
        for db_creator in &[
            |path: &std::path::Path| -> Box<dyn Database> { Box::new(HeedCoreDb::new(path).unwrap()) },
            |path: &std::path::Path| -> Box<dyn Database> { Box::new(LmdbDb::new(path).unwrap()) },
            |path: &std::path::Path| -> Box<dyn Database> { Box::new(RocksDb::new(path).unwrap()) },
            |path: &std::path::Path| -> Box<dyn Database> { Box::new(RedbDb::new(path).unwrap()) },
        ] {
            let temp_dir = TempDir::new().unwrap();
            let db = db_creator(temp_dir.path());
            
            // Populate database
            db.write_batch(&data).unwrap();
            
            group.bench_function(
                BenchmarkId::new(db.name(), size),
                |b| {
                    b.iter(|| {
                        let results = db.scan_all().unwrap();
                        black_box(results.len());
                    });
                },
            );
        }
    }
    
    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.sample_size(10);
    
    let initial_size = 50000;
    let ops_per_iter = 1000;
    
    let initial_data = generate_data(initial_size, 16, 100);
    let mut rng = StdRng::seed_from_u64(456);
    
    for db_creator in &[
        |path: &std::path::Path| -> Box<dyn Database> { Box::new(HeedCoreDb::new(path).unwrap()) },
        |path: &std::path::Path| -> Box<dyn Database> { Box::new(LmdbDb::new(path).unwrap()) },
        |path: &std::path::Path| -> Box<dyn Database> { Box::new(RocksDb::new(path).unwrap()) },
        |path: &std::path::Path| -> Box<dyn Database> { Box::new(RedbDb::new(path).unwrap()) },
    ] {
        let temp_dir = TempDir::new().unwrap();
        let db = db_creator(temp_dir.path());
        
        // Populate database
        db.write_batch(&initial_data).unwrap();
        
        group.bench_function(
            BenchmarkId::new(db.name(), ops_per_iter),
            |b| {
                b.iter(|| {
                    // 70% reads, 20% writes, 10% deletes
                    for _ in 0..ops_per_iter {
                        let op = rng.gen_range(0..100);
                        
                        if op < 70 {
                            // Read
                            let key = initial_data[rng.gen_range(0..initial_size)].0.clone();
                            let _ = db.read_batch(&[key]).unwrap();
                        } else if op < 90 {
                            // Write
                            let data = generate_data(1, 16, 100);
                            db.write_batch(&data).unwrap();
                        } else {
                            // Delete
                            let key = initial_data[rng.gen_range(0..initial_size)].0.clone();
                            let _ = db.delete_batch(&[key]);
                        }
                    }
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches, 
    bench_sequential_writes,
    bench_random_reads,
    bench_scan_all,
    bench_mixed_workload
);
criterion_main!(benches);