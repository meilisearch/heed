#!/bin/bash
# Comprehensive performance testing and optimization suite

set -e

echo "heed-core Performance Testing Suite"
echo "==================================="
echo ""

# Configuration
RESULTS_DIR="target/perf-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_DIR="$RESULTS_DIR/$TIMESTAMP"

# Create directories
mkdir -p "$REPORT_DIR"

# Function to run benchmarks with different configurations
run_benchmark_suite() {
    local config_name=$1
    local rustflags=$2
    
    echo "Running benchmarks with $config_name configuration..."
    
    RUSTFLAGS="$rustflags" cargo bench --bench db_comparison 2>&1 | tee "$REPORT_DIR/bench_${config_name}_db_comparison.log"
    RUSTFLAGS="$rustflags" cargo bench --bench btree_ops 2>&1 | tee "$REPORT_DIR/bench_${config_name}_btree_ops.log"
    RUSTFLAGS="$rustflags" cargo bench --bench page_alloc 2>&1 | tee "$REPORT_DIR/bench_${config_name}_page_alloc.log"
}

# 1. Baseline benchmarks
echo "1. Running baseline benchmarks..."
run_benchmark_suite "baseline" ""

# 2. With native CPU optimizations
echo -e "\n2. Running with native CPU optimizations..."
run_benchmark_suite "native" "-C target-cpu=native"

# 3. With AVX2 if available
if grep -q avx2 /proc/cpuinfo 2>/dev/null || sysctl -n machdep.cpu.features 2>/dev/null | grep -q AVX2; then
    echo -e "\n3. Running with AVX2 optimizations..."
    run_benchmark_suite "avx2" "-C target-cpu=native -C target-feature=+avx2"
fi

# 4. Profile guided optimization preparation
echo -e "\n4. Preparing for Profile-Guided Optimization (PGO)..."
if command -v llvm-profdata &> /dev/null; then
    echo "Building with PGO instrumentation..."
    RUSTFLAGS="-Cprofile-generate=$REPORT_DIR/pgo-data" cargo build --release --bench db_comparison
    
    echo "Running PGO training..."
    ./target/release/deps/db_comparison-* --bench --profile-time 10
    
    echo "Merging PGO data..."
    llvm-profdata merge -o "$REPORT_DIR/pgo-data/merged.profdata" "$REPORT_DIR/pgo-data"
    
    echo "Building with PGO optimization..."
    RUSTFLAGS="-Cprofile-use=$REPORT_DIR/pgo-data/merged.profdata" cargo build --release --bench db_comparison
    
    echo "Running PGO-optimized benchmarks..."
    run_benchmark_suite "pgo" "-Cprofile-use=$REPORT_DIR/pgo-data/merged.profdata"
else
    echo "LLVM profdata not found, skipping PGO"
fi

# 5. Memory profiling
echo -e "\n5. Running memory profiling..."
if command -v heaptrack &> /dev/null; then
    echo "Running with heaptrack..."
    heaptrack cargo test --release -- --test test_basic 2>&1 | tee "$REPORT_DIR/heaptrack.log"
elif command -v valgrind &> /dev/null; then
    echo "Running with valgrind..."
    valgrind --tool=massif --massif-out-file="$REPORT_DIR/massif.out" \
        cargo test --release -- --test test_basic 2>&1 | tee "$REPORT_DIR/valgrind.log"
else
    echo "No memory profiler found (install heaptrack or valgrind)"
fi

# 6. Flamegraph generation
echo -e "\n6. Generating flamegraphs..."
if command -v cargo-flamegraph &> /dev/null || cargo install flamegraph; then
    echo "Generating flamegraph for sequential writes..."
    cargo flamegraph --bench db_comparison -- sequential_writes --bench
    mv flamegraph.svg "$REPORT_DIR/flamegraph_sequential_writes.svg"
    
    echo "Generating flamegraph for B+tree operations..."
    cargo flamegraph --bench btree_ops -- btree_search --bench
    mv flamegraph.svg "$REPORT_DIR/flamegraph_btree_search.svg"
else
    echo "cargo-flamegraph not available"
fi

# 7. Generate comparison report
echo -e "\n7. Generating performance comparison report..."
python3 - << EOF
import os
import re
from pathlib import Path

report_dir = "$REPORT_DIR"
configs = ["baseline", "native", "avx2", "pgo"]

print("Performance Comparison Report")
print("=" * 50)
print()

# Parse benchmark results
results = {}
for config in configs:
    log_file = Path(report_dir) / f"bench_{config}_db_comparison.log"
    if not log_file.exists():
        continue
    
    with open(log_file) as f:
        content = f.read()
        
    # Extract benchmark times (simplified regex)
    times = re.findall(r'time:\s+\[(\d+\.?\d*)\s+(\w+)', content)
    if times:
        results[config] = float(times[0][0])

# Calculate improvements
if "baseline" in results:
    baseline = results["baseline"]
    print(f"Baseline: {baseline:.2f} µs")
    print()
    
    for config, time in results.items():
        if config != "baseline":
            improvement = ((baseline - time) / baseline) * 100
            symbol = "✅" if improvement > 0 else "❌"
            print(f"{symbol} {config}: {time:.2f} µs ({improvement:+.1f}%)")

print()
print(f"Full results saved to: {report_dir}")
EOF

# 8. Generate HTML dashboard
echo -e "\n8. Generating performance dashboard..."
cd scripts && python3 generate-perf-dashboard.py
mv ../target/performance-dashboard.html "$REPORT_DIR/"

# 9. Create summary
echo -e "\n9. Creating performance summary..."
cat > "$REPORT_DIR/SUMMARY.md" << EOF
# heed-core Performance Test Results

**Date**: $(date)
**Commit**: $(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

## Configuration Tested

1. **Baseline**: Default release build
2. **Native**: With \`-C target-cpu=native\`
3. **AVX2**: With AVX2 instructions (if available)
4. **PGO**: Profile-guided optimization (if available)

## Key Findings

- See performance-dashboard.html for detailed results
- Flamegraphs available for hot path analysis
- Memory profile data in heaptrack/valgrind logs

## Recommendations

Based on the profiling results:

1. Enable native CPU optimizations in production builds
2. Consider PGO for deployment-specific optimization
3. Review flamegraphs to identify optimization opportunities

## Files Generated

- Benchmark logs: bench_*.log
- Flamegraphs: flamegraph_*.svg
- Performance dashboard: performance-dashboard.html
- Memory profiles: heaptrack.log / massif.out
EOF

echo -e "\nPerformance testing complete!"
echo "Results saved to: $REPORT_DIR"
echo ""
echo "Key files:"
echo "  - Summary: $REPORT_DIR/SUMMARY.md"
echo "  - Dashboard: $REPORT_DIR/performance-dashboard.html"
echo "  - Flamegraphs: $REPORT_DIR/flamegraph_*.svg"

# Open dashboard if possible
if command -v open &> /dev/null; then
    open "$REPORT_DIR/performance-dashboard.html"
elif command -v xdg-open &> /dev/null; then
    xdg-open "$REPORT_DIR/performance-dashboard.html"
fi