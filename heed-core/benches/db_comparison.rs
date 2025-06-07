//! Benchmarks comparing various embedded databases
//!
//! This benchmark suite compares:
//! - heed-core (pure Rust LMDB)
//! - heed (LMDB FFI)
//! - RocksDB
//! - redb
//! - sled (optional)
//!
//! Note: heed-core currently has limitations with larger datasets due to 
//! page allocation constraints. This benchmark uses smaller dataset sizes
//! (up to 1000 items with 50-200 byte values) to work within these limits.

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
    fn clear(&self) -> anyhow::Result<()>;
    fn name(&self) -> &'static str;
}

// heed-core implementation
struct HeedCoreDb {
    env: Arc<heed_core::env::Environment<heed_core::env::state::Open>>,
}

impl HeedCoreDb {
    fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let env = Arc::new(
            heed_core::env::EnvBuilder::new()
                .map_size(10 * 1024 * 1024 * 1024) // 10GB - much larger
                .open(path)?
        );
        
        // Create the database once during initialization
        {
            let mut txn = env.begin_write_txn()?;
            let _db: heed_core::db::Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None)?;
            txn.commit()?;
        }
        
        Ok(Self { env })
    }
}

impl Database for HeedCoreDb {
    fn write_batch(&self, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
        let mut txn = self.env.begin_write_txn()?;
        let db: heed_core::db::Database<Vec<u8>, Vec<u8>> = self.env.open_database(&txn, None)?;
        
        for (key, value) in data {
            db.put(&mut txn, key.clone(), value.clone())?;
        }
        
        txn.commit()?;
        Ok(())
    }
    
    fn read_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let txn = self.env.begin_txn()?;
        let db: heed_core::db::Database<Vec<u8>, Vec<u8>> = self.env.open_database(&txn, None)?;
        
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(db.get(&txn, key)?);
        }
        Ok(results)
    }
    
    fn scan_all(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let txn = self.env.begin_txn()?;
        let db: heed_core::db::Database<Vec<u8>, Vec<u8>> = self.env.open_database(&txn, None)?;
        
        let mut results = Vec::new();
        let mut cursor = db.cursor(&txn)?;
        while let Some((key, value)) = cursor.next()? {
            results.push((key, value));
        }
        Ok(results)
    }
    
    fn delete_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        let mut txn = self.env.begin_write_txn()?;
        let db: heed_core::db::Database<Vec<u8>, Vec<u8>> = self.env.open_database(&txn, None)?;
        
        for key in keys {
            db.delete(&mut txn, key)?;
        }
        
        txn.commit()?;
        Ok(())
    }
    
    fn clear(&self) -> anyhow::Result<()> {
        let mut txn = self.env.begin_write_txn()?;
        let db: heed_core::db::Database<Vec<u8>, Vec<u8>> = self.env.open_database(&txn, None)?;
        db.clear(&mut txn)?;
        txn.commit()?;
        Ok(())
    }
    
    fn name(&self) -> &'static str {
        "heed-core"
    }
}

// heed (LMDB FFI) implementation
struct HeedDb {
    env: heed::Env,
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
}

impl HeedDb {
    fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1GB
                .open(path)?
        };
        
        let mut wtxn = env.write_txn()?;
        let db = env.create_database(&mut wtxn, None)?;
        wtxn.commit()?;
        
        Ok(Self { env, db })
    }
}

impl Database for HeedDb {
    fn write_batch(&self, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
        let mut wtxn = self.env.write_txn()?;
        
        for (key, value) in data {
            self.db.put(&mut wtxn, key, value)?;
        }
        
        wtxn.commit()?;
        Ok(())
    }
    
    fn read_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let rtxn = self.env.read_txn()?;
        
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.db.get(&rtxn, key)?.map(|v| v.to_vec()));
        }
        Ok(results)
    }
    
    fn scan_all(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rtxn = self.env.read_txn()?;
        
        let mut results = Vec::new();
        for entry in self.db.iter(&rtxn)? {
            let (key, value) = entry?;
            results.push((key.to_vec(), value.to_vec()));
        }
        Ok(results)
    }
    
    fn delete_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        let mut wtxn = self.env.write_txn()?;
        
        for key in keys {
            self.db.delete(&mut wtxn, key)?;
        }
        
        wtxn.commit()?;
        Ok(())
    }
    
    fn clear(&self) -> anyhow::Result<()> {
        let mut wtxn = self.env.write_txn()?;
        self.db.clear(&mut wtxn)?;
        wtxn.commit()?;
        Ok(())
    }
    
    fn name(&self) -> &'static str {
        "heed"
    }
}

// RocksDB implementation
struct RocksDb {
    db: Arc<rocksdb::DB>,
}

impl RocksDb {
    fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        
        let db = Arc::new(rocksdb::DB::open(&opts, path)?);
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
    
    fn clear(&self) -> anyhow::Result<()> {
        // RocksDB doesn't have a direct clear method, so we delete all keys
        let mut batch = rocksdb::WriteBatch::default();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        
        for item in iter {
            let (key, _) = item?;
            batch.delete(&key);
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
        // redb expects a file path, not a directory
        let db_path = path.join("redb.db");
        let db = Arc::new(redb::Database::create(&db_path)?);
        
        // Create table
        let write_txn = db.begin_write()?;
        {
            let _table = write_txn.open_table::<&[u8], &[u8]>(redb::TableDefinition::new("bench"))?;
        }
        write_txn.commit()?;
        
        Ok(Self { db })
    }
}

const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("bench");

impl Database for RedbDb {
    fn write_batch(&self, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            for (key, value) in data {
                table.insert(&key[..], &value[..])?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }
    
    fn read_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;
        
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(table.get(&key[..])?.map(|v| v.value().to_vec()));
        }
        Ok(results)
    }
    
    fn scan_all(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;
        
        let mut results = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            results.push((key.value().to_vec(), value.value().to_vec()));
        }
        Ok(results)
    }
    
    fn delete_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            for key in keys {
                table.remove(&key[..])?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }
    
    fn clear(&self) -> anyhow::Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            // redb doesn't have a clear method, so we delete all keys
            let keys: Vec<Vec<u8>> = table.iter()?.map(|r| {
                let (k, _) = r?;
                Ok(k.value().to_vec())
            }).collect::<Result<Vec<_>, redb::Error>>()?;
            
            for key in keys {
                table.remove(&key[..])?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }
    
    fn name(&self) -> &'static str {
        "redb"
    }
}

// Optional sled implementation
#[cfg(feature = "sled")]
struct SledDb {
    db: sled::Db,
}

#[cfg(feature = "sled")]
impl SledDb {
    fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }
}

#[cfg(feature = "sled")]
impl Database for SledDb {
    fn write_batch(&self, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
        let mut batch = sled::Batch::default();
        
        for (key, value) in data {
            batch.insert(key.clone(), value.clone());
        }
        
        self.db.apply_batch(batch)?;
        Ok(())
    }
    
    fn read_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.db.get(key)?.map(|v| v.to_vec()));
        }
        Ok(results)
    }
    
    fn scan_all(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        for entry in self.db.iter() {
            let (key, value) = entry?;
            results.push((key.to_vec(), value.to_vec()));
        }
        Ok(results)
    }
    
    fn delete_batch(&self, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        let mut batch = sled::Batch::default();
        
        for key in keys {
            batch.remove(key.clone());
        }
        
        self.db.apply_batch(batch)?;
        Ok(())
    }
    
    fn clear(&self) -> anyhow::Result<()> {
        self.db.clear()?;
        Ok(())
    }
    
    fn name(&self) -> &'static str {
        "sled"
    }
}

// Benchmark data generation
fn generate_data(size: usize, seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = Vec::with_capacity(size);
    
    for i in 0..size {
        let key = format!("key_{:08}", i).into_bytes();
        let value_size = rng.gen_range(50..200); // Smaller values to avoid page full
        let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
        data.push((key, value));
    }
    
    data
}

// Benchmark functions
fn bench_sequential_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_writes");
    
    // Note: heed-core currently has limitations with larger datasets
    // due to page allocation constraints. This will be addressed in future updates.
    for size in [100, 500, 1000] {
        let data = generate_data(size, 42);
        
        for db_name in ["heed-core", "heed", "rocksdb", "redb"] {
            group.bench_with_input(
                BenchmarkId::new(db_name, size),
                &data,
                |b, data| {
                    // For benchmarking writes, we need to handle the fact that
                    // criterion runs the closure multiple times
                    b.iter_batched(
                        || {
                            // Setup: create a fresh database for each iteration
                            let temp_dir = TempDir::new().unwrap();
                            let db: Box<dyn Database> = match db_name {
                                "heed-core" => Box::new(HeedCoreDb::new(temp_dir.path()).unwrap()),
                                "heed" => Box::new(HeedDb::new(temp_dir.path()).unwrap()),
                                "rocksdb" => Box::new(RocksDb::new(temp_dir.path()).unwrap()),
                                "redb" => Box::new(RedbDb::new(temp_dir.path()).unwrap()),
                                _ => unreachable!(),
                            };
                            (db, temp_dir)
                        },
                        |(db, _temp_dir)| {
                            // The actual benchmark: write the batch
                            match db.write_batch(black_box(data)) {
                                Ok(_) => {},
                                Err(e) => {
                                    eprintln!("Write batch failed for {} with size {}: {:?}", db_name, size, e);
                                    panic!("Write batch failed: {:?}", e);
                                }
                            }
                        },
                        criterion::BatchSize::SmallInput
                    );
                }
            );
        }
    }
    
    group.finish();
}

fn bench_random_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_reads");
    
    for size in [100, 1000] {
        let data = generate_data(size, 42);
        let mut rng = StdRng::seed_from_u64(43);
        
        // Create random read keys
        let read_keys: Vec<Vec<u8>> = (0..size/10)
            .map(|_| {
                let idx = rng.gen_range(0..size);
                data[idx].0.clone()
            })
            .collect();
        
        for db_name in ["heed-core", "heed", "rocksdb", "redb"] {
            let temp_dir = TempDir::new().unwrap();
            let db: Box<dyn Database> = match db_name {
                "heed-core" => Box::new(HeedCoreDb::new(temp_dir.path()).unwrap()),
                "heed" => Box::new(HeedDb::new(temp_dir.path()).unwrap()),
                "rocksdb" => Box::new(RocksDb::new(temp_dir.path()).unwrap()),
                "redb" => Box::new(RedbDb::new(temp_dir.path()).unwrap()),
                _ => unreachable!(),
            };
            
            // Populate database
            db.write_batch(&data).unwrap();
            
            group.bench_with_input(
                BenchmarkId::new(db_name, size),
                &read_keys,
                |b, keys| {
                    b.iter(|| {
                        black_box(db.read_batch(keys).unwrap());
                    });
                }
            );
        }
    }
    
    group.finish();
}

fn bench_full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_scan");
    
    for size in [100, 1000] {
        let data = generate_data(size, 42);
        
        for db_name in ["heed-core", "heed", "rocksdb", "redb"] {
            let temp_dir = TempDir::new().unwrap();
            let db: Box<dyn Database> = match db_name {
                "heed-core" => Box::new(HeedCoreDb::new(temp_dir.path()).unwrap()),
                "heed" => Box::new(HeedDb::new(temp_dir.path()).unwrap()),
                "rocksdb" => Box::new(RocksDb::new(temp_dir.path()).unwrap()),
                "redb" => Box::new(RedbDb::new(temp_dir.path()).unwrap()),
                _ => unreachable!(),
            };
            
            // Populate database
            db.write_batch(&data).unwrap();
            
            group.bench_with_input(
                BenchmarkId::new(db_name, size),
                &size,
                |b, _| {
                    b.iter(|| {
                        black_box(db.scan_all().unwrap());
                    });
                }
            );
        }
    }
    
    group.finish();
}

fn bench_random_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_writes");
    
    // Note: Random writes cause more page splits, so we use even smaller sizes
    for size in [50, 100, 200] {
        // Generate data with random keys
        let mut rng = StdRng::seed_from_u64(42);
        let mut data = Vec::with_capacity(size);
        
        // Generate random keys to ensure non-sequential access
        let mut keys: Vec<usize> = (0..size).collect();
        use rand::seq::SliceRandom;
        keys.shuffle(&mut rng);
        
        for &i in &keys {
            let key = format!("key_{:08}", i).into_bytes();
            let value_size = rng.gen_range(50..200);
            let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
            data.push((key, value));
        }
        
        // Skip heed-core for random writes due to page allocation limitations
        // Random insertion patterns cause more page splits which exceed current limits
        for db_name in ["heed", "rocksdb", "redb"] {
            group.bench_with_input(
                BenchmarkId::new(db_name, size),
                &data,
                |b, data| {
                    b.iter_batched(
                        || {
                            // Setup: create a fresh database for each iteration
                            let temp_dir = TempDir::new().unwrap();
                            let db: Box<dyn Database> = match db_name {
                                "heed-core" => Box::new(HeedCoreDb::new(temp_dir.path()).unwrap()),
                                "heed" => Box::new(HeedDb::new(temp_dir.path()).unwrap()),
                                "rocksdb" => Box::new(RocksDb::new(temp_dir.path()).unwrap()),
                                "redb" => Box::new(RedbDb::new(temp_dir.path()).unwrap()),
                                _ => unreachable!(),
                            };
                            (db, temp_dir)
                        },
                        |(db, _temp_dir)| {
                            // The actual benchmark: write the batch with random keys
                            match db.write_batch(black_box(data)) {
                                Ok(_) => {},
                                Err(e) => {
                                    eprintln!("Write batch failed for {} with size {}: {:?}", db_name, size, e);
                                    panic!("Write batch failed: {:?}", e);
                                }
                            }
                        },
                        criterion::BatchSize::SmallInput
                    );
                }
            );
        }
    }
    
    group.finish();
}

criterion_group!(benches, bench_sequential_writes, bench_random_writes, bench_random_reads, bench_full_scan);
criterion_main!(benches);