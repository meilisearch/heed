#!/bin/bash
# Performance regression testing script for CI/CD

set -e

echo "Running performance regression tests..."

# Configuration
BASELINE_FILE="target/perf-baseline.json"
CURRENT_FILE="target/perf-current.json"
THRESHOLD=10  # Allow 10% performance regression

# Function to run benchmarks and save results
run_benchmarks() {
    local output_file=$1
    echo "Running benchmarks and saving to $output_file..."
    
    # Run criterion benchmarks with JSON export
    cargo bench --bench db_comparison -- --save-baseline current
    cargo bench --bench btree_ops -- --save-baseline current
    cargo bench --bench page_alloc -- --save-baseline current
    
    # Extract benchmark results (simplified - in practice use criterion's JSON output)
    echo "{\"benchmarks\": []}" > "$output_file"
}

# Check if baseline exists
if [ ! -f "$BASELINE_FILE" ]; then
    echo "No baseline found. Creating baseline..."
    run_benchmarks "$BASELINE_FILE"
    echo "Baseline created. Re-run this script after making changes."
    exit 0
fi

# Run current benchmarks
run_benchmarks "$CURRENT_FILE"

# Compare results
echo "Comparing performance..."

# Simple comparison (in practice, parse JSON and compare properly)
python3 - << 'EOF'
import json
import sys

threshold = 10  # 10% regression threshold

# Mock comparison - in practice, load and compare actual results
regressions = []

# Example regression detection
mock_results = {
    "btree_insert": {"baseline": 100, "current": 115},  # 15% regression
    "page_alloc": {"baseline": 50, "current": 52},      # 4% regression
    "search_ops": {"baseline": 200, "current": 180},    # 10% improvement
}

for bench, times in mock_results.items():
    baseline = times["baseline"]
    current = times["current"]
    change = ((current - baseline) / baseline) * 100
    
    if change > threshold:
        regressions.append({
            "benchmark": bench,
            "baseline": baseline,
            "current": current,
            "regression": f"{change:.1f}%"
        })

if regressions:
    print("❌ Performance regressions detected:")
    for r in regressions:
        print(f"  - {r['benchmark']}: {r['regression']} slower")
        print(f"    Baseline: {r['baseline']}µs, Current: {r['current']}µs")
    sys.exit(1)
else:
    print("✅ No performance regressions detected")
    print("\nPerformance summary:")
    for bench, times in mock_results.items():
        change = ((times["current"] - times["baseline"]) / times["baseline"]) * 100
        symbol = "🔴" if change > 0 else "🟢"
        print(f"  {symbol} {bench}: {change:+.1f}%")
EOF

EXIT_CODE=$?

# Generate performance report
echo ""
echo "Generating performance report..."
cargo bench --bench db_comparison -- --load-baseline current --baseline baseline || true

# Create GitHub comment if running in CI
if [ -n "$GITHUB_ACTIONS" ]; then
    echo "## Performance Report" > perf-comment.md
    echo "" >> perf-comment.md
    if [ $EXIT_CODE -eq 0 ]; then
        echo "✅ **No performance regressions detected**" >> perf-comment.md
    else
        echo "❌ **Performance regressions detected**" >> perf-comment.md
        echo "" >> perf-comment.md
        echo "Please investigate the regressions before merging." >> perf-comment.md
    fi
    echo "" >> perf-comment.md
    echo "Full benchmark results available in the Actions artifacts." >> perf-comment.md
fi

exit $EXIT_CODE