# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Heed is a Rust-centric LMDB wrapper that provides type-safe database operations with minimal overhead. The project maintains two variants:

- **heed**: Standard LMDB wrapper using `mdb.master` branch (stable)
- **heed3**: Enhanced version with encryption-at-rest using `mdb.master3` branch (experimental)

## Development Commands

### Basic Development
```bash
# Clone with submodules (required for LMDB source)
git clone --recursive https://github.com/meilisearch/heed.git
cd heed

# Initialize submodules if already cloned
git submodule update --init

# Build the project
cargo build

# Run tests
cargo test

# Run all examples
cargo run --example 2>&1 | grep -E '^ ' | awk '!/rmp-serde/' | xargs -n1 cargo run --example
cargo run --example rmp-serde --features serde-rmp

# Run specific example
cargo run --example all-types
```

### Code Quality
```bash
# Run clippy with strict warnings
cargo clippy --all-targets -- --deny warnings

# Format code (requires nightly)
cargo +nightly fmt

# Check all features
cargo check --all-features -p heed
```

### Working with heed3
```bash
# Switch to heed3 development (requires clean git state)
./convert-to-heed3.sh

# Test heed3
cargo test

# Run heed3 examples
cargo run --example 2>&1 | grep -E '^ '| xargs -n1 cargo run --example

# Check heed3 with all features
cargo check --all-features -p heed3

# Rollback heed3 changes (find the "remove-me" commit)
git log --oneline | grep "remove-me"
git reset --hard HEAD~1  # or specific commit hash
```

## Architecture Overview

### Crate Structure
The project is organized as a Cargo workspace with these key components:

- **`heed/heed3`**: Main wrapper libraries providing the public API
- **`heed-traits`**: Core traits for encoding/decoding (`BytesEncode`, `BytesDecode`, `Comparator`)
- **`heed-types`**: Type implementations for various data types (integers, strings, serde types)
- **`lmdb-master-sys/lmdb-master3-sys`**: Low-level FFI bindings to LMDB

### Core Concepts

**Type-Safe Operations**: Databases are typed with key-value type parameters:
```rust
let db: Database<Str, U32<byteorder::NativeEndian>> = env.create_database(&mut wtxn, None)?;
```

**Transaction-Based**: All operations require explicit transactions:
- `RoTxn` for read-only operations  
- `RwTxn` for read-write operations
- Transactions must be committed or they're automatically rolled back

**Hierarchy**: `Environment` → `Database` → `Transaction` → `Operations`

### Important Features

**Serialization Support**:
- Default: `serde-bincode`, `serde-json`
- Optional: `serde-rmp` (requires feature flag)

**Performance Tuning**:
- `mdb_idl_logn_*`: Configure memory allocation for page lists
- `longer-keys`: Remove 511-byte key limit
- `posix-sem`: Use POSIX semaphores (required for Apple App Sandbox)

**Debugging**:
- `use-valgrind`: Better Valgrind support (requires valgrind-devel)

## Development Workflow

### Testing Strategy
The CI tests both heed and heed3 variants across Ubuntu, macOS, and Windows. Key test commands:
- Standard tests: `cargo test`
- heed3 tests: `./convert-to-heed3.sh && cargo test`
- Feature testing: `cargo check --all-features`

### heed3 Development
The `convert-to-heed3.sh` script enables heed3 development by:
1. Copying `heed3/Cargo.toml` to `heed/` directory
2. Replacing `heed::` references with `heed3::`
3. Copying heed3-specific examples
4. Creating a "remove-me" commit for easy rollback

**Important**: Always work with a clean git state when using this script.

### Code Patterns

**Database Creation**:
```rust
let env = unsafe { EnvOpenOptions::new().open("path")? };
let mut wtxn = env.write_txn()?;
let db: Database<KeyType, ValueType> = env.create_database(&mut wtxn, None)?;
```

**ACID Operations**:
```rust
// Write
db.put(&mut wtxn, "key", &value)?;
wtxn.commit()?;

// Read  
let rtxn = env.read_txn()?;
let value = db.get(&rtxn, "key")?;
```

**Iteration**:
```rust
// All entries
for result in db.iter(&rtxn)? { /* ... */ }

// Range queries
for result in db.range(&rtxn, &(10..=50))? { /* ... */ }

// Prefix iteration (with LexicographicComparator)
for result in db.prefix_iter(&rtxn, "prefix")? { /* ... */ }
```

## Common Development Tasks

### Adding New Type Support
1. Implement `BytesEncode` and `BytesDecode` traits in `heed-types`
2. Add type alias in `heed-types/src/lib.rs`
3. Add tests for the new type
4. Update examples if needed

### Working with Custom Comparators
Implement the `Comparator` trait from `heed-traits` for custom key ordering. Use `LexicographicComparator` for prefix-based iteration support.

### Performance Optimization
- Use appropriate `mdb_idl_logn_*` features for your memory constraints
- Enable `longer-keys` if you need keys > 511 bytes
- Consider `posix-sem` on macOS/iOS for better performance