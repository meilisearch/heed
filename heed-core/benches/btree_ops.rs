//! Micro-benchmarks for B+tree operations
//! 
//! This benchmark focuses on profiling individual B+tree operations
//! to identify performance bottlenecks.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use heed_core::{EnvBuilder, Database};
use tempfile::TempDir;
use std::sync::Arc;
use pprof::criterion::{PProfProfiler, Output};

fn bench_insert_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_insert_sequential");
    
    for size in [10, 50, 100, 200] {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        // Setup
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
                        // Benchmark
                        let mut txn = env.begin_write_txn().unwrap();
                        for i in 0..size {
                            let key = format!("key_{:08}", i).into_bytes();
                            let value = vec![42u8; 100];
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

fn bench_search_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_search");
    
    // Prepare test data
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .open(dir.path())
            .unwrap()
    );
    
    let mut txn = env.begin_write_txn().unwrap();
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
    
    // Insert test data
    for i in 0..1000 {
        let key = format!("key_{:08}", i).into_bytes();
        let value = vec![42u8; 100];
        db.put(&mut txn, key, value).unwrap();
    }
    txn.commit().unwrap();
    
    // Benchmark different search patterns
    group.bench_function("search_existing", |b| {
        b.iter(|| {
            let txn = env.begin_txn().unwrap();
            let key = format!("key_{:08}", 500).into_bytes();
            let _result = db.get(&txn, &key).unwrap();
            black_box(_result);
        });
    });
    
    group.bench_function("search_non_existing", |b| {
        b.iter(|| {
            let txn = env.begin_txn().unwrap();
            let key = b"non_existing_key".to_vec();
            let _result = db.get(&txn, &key).unwrap();
            black_box(_result);
        });
    });
    
    group.bench_function("search_range", |b| {
        b.iter(|| {
            let txn = env.begin_txn().unwrap();
            let mut cursor = db.cursor(&txn).unwrap();
            let start_key = format!("key_{:08}", 400).into_bytes();
            cursor.set(&start_key).unwrap();
            
            let mut count = 0;
            for _ in 0..100 {
                if cursor.next().unwrap().is_none() {
                    break;
                }
                count += 1;
            }
            black_box(count);
        });
    });
    
    group.finish();
}

fn bench_page_splits(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_page_splits");
    
    // This benchmark specifically targets operations that cause page splits
    group.bench_function("force_splits", |b| {
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
                
                // Insert keys with large values to force page splits
                for i in 0..20 {
                    let key = format!("key_{:08}", i).into_bytes();
                    let value = vec![42u8; 2000]; // Large value to fill pages quickly
                    db.put(&mut txn, key, value).unwrap();
                }
                
                txn.commit().unwrap();
            },
            criterion::BatchSize::SmallInput
        );
    });
    
    group.finish();
}

fn bench_cursor_navigation(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_cursor");
    
    // Prepare test data
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        EnvBuilder::new()
            .map_size(100 * 1024 * 1024)
            .open(dir.path())
            .unwrap()
    );
    
    let mut txn = env.begin_write_txn().unwrap();
    let db: Database<Vec<u8>, Vec<u8>> = env.create_database(&mut txn, None).unwrap();
    
    // Insert test data
    for i in 0..1000 {
        let key = format!("key_{:08}", i).into_bytes();
        let value = vec![42u8; 100];
        db.put(&mut txn, key, value).unwrap();
    }
    txn.commit().unwrap();
    
    group.bench_function("cursor_full_scan", |b| {
        b.iter(|| {
            let txn = env.begin_txn().unwrap();
            let mut cursor = db.cursor(&txn).unwrap();
            
            let mut count = 0;
            if cursor.first().unwrap().is_some() {
                count += 1;
                while cursor.next().unwrap().is_some() {
                    count += 1;
                }
            }
            black_box(count);
        });
    });
    
    group.bench_function("cursor_reverse_scan", |b| {
        b.iter(|| {
            let txn = env.begin_txn().unwrap();
            let mut cursor = db.cursor(&txn).unwrap();
            
            let mut count = 0;
            if cursor.last().unwrap().is_some() {
                count += 1;
                while cursor.prev().unwrap().is_some() {
                    count += 1;
                }
            }
            black_box(count);
        });
    });
    
    group.finish();
}

// Configure criterion to use pprof for profiling
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_insert_sequential, bench_search_operations, bench_page_splits, bench_cursor_navigation
}
criterion_main!(benches);