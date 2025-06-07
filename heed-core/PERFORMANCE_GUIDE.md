# heed-core Performance Optimization Guide

This guide explains how to use the automated performance profiling and optimization tools included with heed-core.

## Overview

heed-core includes a comprehensive suite of performance tools:

1. **Micro-benchmarks** - Focused benchmarks for specific operations
2. **Profiling tools** - Automated profiling with flamegraph generation
3. **Performance dashboard** - Visual performance metrics and trends
4. **Regression testing** - CI/CD integration for detecting performance regressions
5. **Optimization scripts** - Automated application of performance improvements

## Quick Start

### Running the Full Performance Suite

```bash
./scripts/run-perf-suite.sh
```

This will:
- Run all benchmarks with different optimization levels
- Generate flamegraphs for hot paths
- Create a performance dashboard
- Provide optimization recommendations

### Running Specific Benchmarks

```bash
# Database comparison benchmark
cargo bench --bench db_comparison

# B+tree operations benchmark
cargo bench --bench btree_ops

# Page allocation benchmark  
cargo bench --bench page_alloc
```

### Profiling with Flamegraphs

```bash
# Install flamegraph tool
cargo install flamegraph

# Generate flamegraph for specific operation
cargo flamegraph --bench btree_ops -- btree_search --bench
```

## Automated Profiling Tool

The `heed-profile` binary provides automated performance analysis:

```bash
cargo run --bin heed-profile
```

This tool will:
1. Run all benchmarks with profiling enabled
2. Analyze hot functions
3. Generate optimization suggestions
4. Create an optimization script

## Performance Dashboard

Generate an HTML dashboard with performance metrics:

```bash
python3 scripts/generate-perf-dashboard.py
```

The dashboard includes:
- Performance metrics with baseline comparison
- Trend charts
- Operation breakdown
- Optimization recommendations

## CI/CD Integration

### Performance Regression Testing

Add to your CI workflow:

```yaml
- name: Performance Regression Test
  run: ./scripts/perf-regression-test.sh
```

This will:
- Compare performance against baseline
- Fail if regression exceeds threshold (default 10%)
- Generate a performance report

### GitHub Actions Example

```yaml
name: Performance Tests

on:
  pull_request:
    paths:
      - 'heed-core/src/**'
      - 'heed-core/benches/**'

jobs:
  performance:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
          override: true
          
      - name: Run Performance Tests
        run: |
          cd heed-core
          ./scripts/perf-regression-test.sh
          
      - name: Upload Results
        uses: actions/upload-artifact@v3
        with:
          name: performance-results
          path: heed-core/target/perf-results/
```

## Optimization Configurations

### Cargo Configuration

The `.cargo/config.toml` file includes optimized build profiles:

```toml
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
```

### CPU-Specific Optimizations

Enable native CPU features:

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### Profile-Guided Optimization (PGO)

1. Build with profiling:
   ```bash
   RUSTFLAGS="-Cprofile-generate=target/pgo-data" cargo build --release
   ```

2. Run representative workload:
   ```bash
   ./target/release/your-binary
   ```

3. Build with profile data:
   ```bash
   llvm-profdata merge -o target/pgo-data/merged.profdata target/pgo-data
   RUSTFLAGS="-Cprofile-use=target/pgo-data/merged.profdata" cargo build --release
   ```

## Performance Best Practices

### 1. Inline Hot Functions

Add `#[inline]` or `#[inline(always)]` to frequently called functions:

```rust
#[inline]
pub fn search(&self, key: &[u8]) -> Result<Option<&[u8]>> {
    // ...
}
```

### 2. Reduce Allocations

Use stack allocation or arena allocators for temporary data:

```rust
// Bad
let mut temp = Vec::new();

// Good  
let mut temp = [0u8; 256];
```

### 3. Optimize Page Size

Adjust page size based on workload:

```rust
// For sequential writes
const PAGE_SIZE: usize = 16384; // 16KB

// For random access
const PAGE_SIZE: usize = 4096;  // 4KB
```

### 4. Use SIMD Instructions

Enable SIMD for search operations:

```rust
#[cfg(target_feature = "avx2")]
fn search_simd(data: &[u8], needle: u8) -> Option<usize> {
    // AVX2 implementation
}
```

## Troubleshooting

### Page Full Errors

If benchmarks fail with "Page full":
1. Reduce value sizes in benchmarks
2. Increase map size in environment
3. Implement better page allocation strategy

### Missing Dependencies

Install required tools:

```bash
# Ubuntu/Debian
sudo apt-get install linux-tools-common linux-tools-generic

# macOS
brew install flamegraph

# Python dependencies
pip install matplotlib pandas
```

### Platform-Specific Issues

- **Linux**: Enable perf events: `echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid`
- **macOS**: Grant dtrace permissions in Security & Privacy settings
- **Windows**: Use Windows Performance Toolkit (WPT)

## Continuous Optimization

1. **Establish Baseline**: Run benchmarks on main branch
2. **Monitor Trends**: Track performance over time
3. **Set Thresholds**: Define acceptable regression limits
4. **Automate Testing**: Include in CI/CD pipeline
5. **Regular Profiling**: Profile monthly or after major changes

## Further Resources

- [Criterion.rs Documentation](https://bheisler.github.io/criterion.rs/book/)
- [The Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [Flamegraph Guide](https://www.brendangregg.com/flamegraphs.html)
- [PGO in Rust](https://doc.rust-lang.org/rustc/profile-guided-optimization.html)