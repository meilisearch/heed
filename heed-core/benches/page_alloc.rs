//! Benchmarks for page allocation and memory management
//!
//! This benchmark focuses on profiling page allocation patterns
//! to identify bottlenecks in memory management.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use heed_core::{EnvBuilder, Database};
use tempfile::TempDir;
use std::sync::Arc;
use pprof::criterion::{PProfProfiler, Output};

fn bench_page_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_allocation");
    
    // Test different allocation patterns
    for pattern in ["sequential", "random", "mixed"] {
        group.bench_with_input(
            BenchmarkId::from_parameter(pattern),
            &pattern,
            |b, &pattern| {
                b.iter_batched(
                    || {
                        let dir = TempDir::new().unwrap();
                        let env = Arc::new(
                            EnvBuilder::new()
                                .map_size(100 * 1024 * 1024)
                                .open(dir.path())
                                .unwrap()
                        );
                        let mut txn = env.begin_write_txn().unwrap();
                        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
                        txn.commit().unwrap();
                        (env, db, dir)
                    },
                    |(env, db, _dir)| {
                        let mut txn = env.begin_write_txn().unwrap();
                        
                        match pattern {
                            "sequential" => {
                                // Sequential allocation pattern
                                for i in 0..100 {
                                    let key = format!("key_{:08}", i).into_bytes();
                                    let value = vec![42u8; 1000];
                                    db.put(&mut txn, key, value).unwrap();
                                }
                            }
                            "random" => {
                                // Random allocation pattern (limited to avoid page full)
                                use rand::{seq::SliceRandom, SeedableRng};
                                use rand::rngs::StdRng;
                                
                                let mut rng = StdRng::seed_from_u64(42);
                                let mut keys: Vec<usize> = (0..30).collect();
                                keys.shuffle(&mut rng);
                                
                                for &i in &keys {
                                    let key = format!("key_{:08}", i).into_bytes();
                                    let value = vec![42u8; 100];
                                    db.put(&mut txn, key, value).unwrap();
                                }
                            }
                            "mixed" => {
                                // Mixed pattern: sequential with occasional deletes
                                for i in 0..50 {
                                    let key = format!("key_{:08}", i).into_bytes();
                                    let value = vec![42u8; 500];
                                    db.put(&mut txn, key, value).unwrap();
                                    
                                    // Delete every 5th key
                                    if i > 0 && i % 5 == 0 {
                                        let del_key = format!("key_{:08}", i - 5).into_bytes();
                                        db.delete(&mut txn, &del_key).unwrap();
                                    }
                                }
                            }
                            _ => unreachable!(),
                        }
                        
                        txn.commit().unwrap();
                    },
                    criterion::BatchSize::SmallInput
                );
            }
        );
    }
    
    group.finish();
}

fn bench_overflow_pages(c: &mut Criterion) {
    let mut group = c.benchmark_group("overflow_pages");
    
    // Test allocation of overflow pages with different value sizes
    for value_size in [1000, 2000, 4000, 8000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}bytes", value_size)),
            &value_size,
            |b, &value_size| {
                b.iter_batched(
                    || {
                        let dir = TempDir::new().unwrap();
                        let env = Arc::new(
                            EnvBuilder::new()
                                .map_size(100 * 1024 * 1024)
                                .open(dir.path())
                                .unwrap()
                        );
                        let mut txn = env.begin_write_txn().unwrap();
                        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
                        txn.commit().unwrap();
                        (env, db, dir)
                    },
                    |(env, db, _dir)| {
                        let mut txn = env.begin_write_txn().unwrap();
                        
                        // Insert values that require overflow pages
                        for i in 0..10 {
                            let key = format!("key_{:08}", i).into_bytes();
                            let value = vec![42u8; value_size];
                            db.put(&mut txn, key, value).unwrap();
                        }
                        
                        txn.commit().unwrap();
                    },
                    criterion::BatchSize::SmallInput
                );
            }
        );
    }
    
    group.finish();
}

fn bench_freelist_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("freelist");
    
    group.bench_function("alloc_free_cycle", |b| {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let mut txn = env.begin_write_txn().unwrap();
        let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
        txn.commit().unwrap();
        
        b.iter(|| {
            // Insert and delete to exercise freelist
            let mut txn = env.begin_write_txn().unwrap();
            
            // Insert batch
            for i in 0..20 {
                let key = format!("temp_key_{:08}", i).into_bytes();
                let value = vec![42u8; 500];
                db.put(&mut txn, key, value).unwrap();
            }
            
            // Delete batch to return pages to freelist
            for i in 0..20 {
                let key = format!("temp_key_{:08}", i).into_bytes();
                db.delete(&mut txn, &key).unwrap();
            }
            
            txn.commit().unwrap();
        });
    });
    
    group.finish();
}

fn bench_transaction_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_overhead");
    
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .open(dir.path())
            .unwrap()
    );
    
    let mut txn = env.begin_write_txn().unwrap();
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
    
    // Pre-populate with some data
    for i in 0..100 {
        let key = format!("key_{:08}", i).into_bytes();
        let value = vec![42u8; 100];
        db.put(&mut txn, key, value).unwrap();
    }
    txn.commit().unwrap();
    
    group.bench_function("read_txn_creation", |b| {
        b.iter(|| {
            let txn = env.begin_txn().unwrap();
            black_box(txn);
        });
    });
    
    group.bench_function("write_txn_creation", |b| {
        b.iter(|| {
            let txn = env.begin_write_txn().unwrap();
            txn.abort();
        });
    });
    
    group.bench_function("small_txn_commit", |b| {
        b.iter(|| {
            let mut txn = env.begin_write_txn().unwrap();
            let key = b"bench_key".to_vec();
            let value = vec![42u8; 10];
            db.put(&mut txn, key, value).unwrap();
            txn.commit().unwrap();
        });
    });
    
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_page_allocation, bench_overflow_pages, bench_freelist_operations, bench_transaction_overhead
}
criterion_main!(benches);