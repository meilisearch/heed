# File Organization for heed-core Tests and Examples

## Directory Structure

### `/examples/` (43 files)
Examples are standalone programs that demonstrate specific functionality. They are compiled as separate binaries and can be run with `cargo run --example <name>`.

**Categories:**
1. **Simple demonstrations** (`simple*.rs`):
   - `simple.rs` - Basic usage example
   - `simple_catalog.rs` - Catalog operations demo

2. **Debug tools** (`debug_*.rs`):
   - `debug_branch_page.rs` - Debug branch page structure
   - `debug_btree_insert.rs` - Debug B+tree insertions
   - `debug_cursor_nav.rs` - Debug cursor navigation
   - `debug_delete_issue.rs` - Debug deletion problems
   - `debug_init.rs` - Debug environment initialization
   - `debug_missing_keys.rs` - Debug missing key issues
   - etc.

3. **Test-like examples** (`test_*.rs`):
   - These are more complex examples that test specific features
   - `test_basic.rs`, `test_catalog.rs`, `test_cursor.rs`, etc.
   - Should probably be moved to `/tests/` directory

4. **Benchmarking examples** (`bench_*.rs`):
   - `bench_simple.rs` - Simple benchmarking
   - `bench_all_dbs.rs` - Benchmark multiple databases

### `/tests/` (3 files)
Integration tests that run as separate test binaries with `cargo test`.

- `lmdb_comparison.rs` - Compare behavior with original LMDB
- `quickcheck_comparison.rs` - Property-based testing comparing heed-core with heed
- `test_dupsort.rs` - Test DUPSORT functionality

### `/benches/` (1 file)
Criterion benchmarks that run with `cargo bench`.

- `db_comparison.rs` - Performance comparison between heed-core, heed, RocksDB, and redb
  - **Benchmarks included:**
    - `sequential_writes` - Write data in sequential key order
    - `random_writes` - Write data in random key order (heed-core excluded due to page allocation limits)
    - `random_reads` - Read random keys from populated database
    - `full_scan` - Iterate through all key-value pairs

### `/src/` (inline tests)
- `btree_tests.rs` - Unit tests for B+tree operations (included as a module)
- Other modules have `#[cfg(test)]` sections with unit tests

## Current Limitations

- **heed-core page allocation**: Currently limited to smaller datasets (up to 1000 items with 50-200 byte values for sequential writes, less for random writes)
- **Random insertion patterns**: Cause more page splits in heed-core, exceeding current page allocation limits

## Recommendations

1. **Move test-like examples to `/tests/`**: Files like `test_*.rs` in `/examples/` should be moved to `/tests/` as they're really integration tests, not examples.

2. **Consolidate debug tools**: The many `debug_*.rs` files could be consolidated into a single debug tool with subcommands.

3. **Clean up examples**: Keep only true examples that demonstrate API usage, not debugging or testing tools.

4. **Address page allocation**: Improve page allocation and overflow handling in heed-core to support larger datasets and random insertion patterns.