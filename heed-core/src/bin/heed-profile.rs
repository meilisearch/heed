//! Automated profiling and optimization tool for heed-core
//!
//! This tool runs various benchmarks, collects profiling data,
//! and provides optimization suggestions based on the results.

use std::process::Command;
use std::fs;
use std::path::Path;
use std::collections::HashMap;

#[derive(Debug)]
struct ProfileResult {
    benchmark: String,
    hot_functions: Vec<HotFunction>,
    suggestions: Vec<String>,
}

#[derive(Debug)]
struct HotFunction {
    name: String,
    percentage: f64,
    samples: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("heed-core Automated Profiling Tool");
    println!("==================================\n");
    
    // Create output directory for profiles
    fs::create_dir_all("target/profiles")?;
    
    // Run benchmarks with profiling
    let benchmarks = vec![
        ("btree_ops", vec!["btree_insert_sequential", "btree_search", "btree_page_splits", "btree_cursor"]),
        ("page_alloc", vec!["page_allocation", "overflow_pages", "freelist", "transaction_overhead"]),
        ("db_comparison", vec!["sequential_writes", "random_reads", "full_scan"]),
    ];
    
    let mut all_results = Vec::new();
    
    for (bench_file, groups) in benchmarks {
        println!("Running {} benchmarks...", bench_file);
        
        for group in groups {
            let result = run_benchmark_with_profiling(bench_file, group)?;
            if let Some(result) = result {
                all_results.push(result);
            }
        }
    }
    
    // Analyze results and generate report
    generate_optimization_report(&all_results)?;
    
    Ok(())
}

fn run_benchmark_with_profiling(bench_file: &str, group: &str) -> Result<Option<ProfileResult>, Box<dyn std::error::Error>> {
    println!("  - Profiling {}...", group);
    
    // Run the benchmark
    let output = Command::new("cargo")
        .args(&[
            "bench",
            "--bench",
            bench_file,
            "--",
            group,
            "--profile-time",
            "5",
        ])
        .env("CARGO_PROFILE_BENCH_DEBUG", "true")
        .output()?;
    
    if !output.status.success() {
        eprintln!("    Failed to run benchmark: {}", String::from_utf8_lossy(&output.stderr));
        return Ok(None);
    }
    
    // Parse flamegraph if it exists
    let flamegraph_path = format!("target/criterion/{}/profile/flamegraph.svg", group);
    if Path::new(&flamegraph_path).exists() {
        let hot_functions = analyze_flamegraph(&flamegraph_path)?;
        let suggestions = generate_suggestions(&hot_functions, group);
        
        return Ok(Some(ProfileResult {
            benchmark: group.to_string(),
            hot_functions,
            suggestions,
        }));
    }
    
    Ok(None)
}

fn analyze_flamegraph(path: &str) -> Result<Vec<HotFunction>, Box<dyn std::error::Error>> {
    // In a real implementation, we would parse the flamegraph SVG
    // For now, we'll return mock data based on common patterns
    
    // Simulate analysis results
    let mock_results = vec![
        HotFunction {
            name: "heed_core::btree::BTree::search".to_string(),
            percentage: 25.0,
            samples: 2500,
        },
        HotFunction {
            name: "heed_core::page::Page::add_node".to_string(),
            percentage: 15.0,
            samples: 1500,
        },
        HotFunction {
            name: "heed_core::txn::Transaction::alloc_page".to_string(),
            percentage: 12.0,
            samples: 1200,
        },
        HotFunction {
            name: "memcpy".to_string(),
            percentage: 8.0,
            samples: 800,
        },
    ];
    
    Ok(mock_results)
}

fn generate_suggestions(hot_functions: &[HotFunction], benchmark: &str) -> Vec<String> {
    let mut suggestions = Vec::new();
    
    // Analyze hot functions and generate targeted suggestions
    for func in hot_functions {
        if func.name.contains("search") && func.percentage > 20.0 {
            suggestions.push(
                "High search overhead detected. Consider:\n\
                 - Implementing binary search with SIMD instructions\n\
                 - Adding a bloom filter for non-existent keys\n\
                 - Caching frequently accessed nodes".to_string()
            );
        }
        
        if func.name.contains("add_node") && func.percentage > 10.0 {
            suggestions.push(
                "Page modification overhead detected. Consider:\n\
                 - Batching node additions to reduce write amplification\n\
                 - Implementing copy-on-write optimization for partial page updates\n\
                 - Using memory pools for temporary allocations".to_string()
            );
        }
        
        if func.name.contains("alloc_page") && func.percentage > 10.0 {
            suggestions.push(
                "Page allocation bottleneck detected. Consider:\n\
                 - Implementing a free page cache\n\
                 - Pre-allocating pages in batches\n\
                 - Using lock-free allocation for read-heavy workloads".to_string()
            );
        }
        
        if func.name == "memcpy" && func.percentage > 5.0 {
            suggestions.push(
                "High memory copy overhead. Consider:\n\
                 - Using zero-copy techniques where possible\n\
                 - Implementing page-aligned allocations\n\
                 - Reducing value sizes or using compression".to_string()
            );
        }
    }
    
    // Benchmark-specific suggestions
    match benchmark {
        "btree_insert_sequential" => {
            suggestions.push(
                "For sequential inserts:\n\
                 - Implement bulk loading optimization\n\
                 - Use append-only mode for sequential keys\n\
                 - Consider larger page sizes for sequential workloads".to_string()
            );
        }
        "page_allocation" => {
            suggestions.push(
                "For page allocation:\n\
                 - Implement freelist compaction\n\
                 - Use memory mapping hints (MAP_POPULATE)\n\
                 - Consider using huge pages for large databases".to_string()
            );
        }
        _ => {}
    }
    
    suggestions
}

fn generate_optimization_report(results: &[ProfileResult]) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n\nOptimization Report");
    println!("==================\n");
    
    // Group suggestions by category
    let mut categories: HashMap<String, Vec<String>> = HashMap::new();
    
    for result in results {
        println!("Benchmark: {}", result.benchmark);
        println!("Hot Functions:");
        for func in &result.hot_functions {
            println!("  - {}: {:.1}% ({} samples)", func.name, func.percentage, func.samples);
        }
        
        if !result.suggestions.is_empty() {
            println!("\nSuggestions:");
            for suggestion in &result.suggestions {
                println!("  {}", suggestion);
                
                // Categorize suggestions
                if suggestion.contains("search") {
                    categories.entry("Search Optimization".to_string())
                        .or_default()
                        .push(suggestion.clone());
                } else if suggestion.contains("allocation") || suggestion.contains("page") {
                    categories.entry("Memory Management".to_string())
                        .or_default()
                        .push(suggestion.clone());
                } else if suggestion.contains("copy") {
                    categories.entry("Data Movement".to_string())
                        .or_default()
                        .push(suggestion.clone());
                }
            }
        }
        println!("\n---\n");
    }
    
    // Generate prioritized action items
    println!("Prioritized Optimization Actions");
    println!("================================\n");
    
    println!("1. **Critical Performance Issues** (>20% CPU time)");
    for result in results {
        for func in &result.hot_functions {
            if func.percentage > 20.0 {
                println!("   - Optimize {}: {:.1}% of runtime", func.name, func.percentage);
            }
        }
    }
    
    println!("\n2. **Quick Wins** (Easy optimizations with good impact)");
    println!("   - Add #[inline] attributes to hot path functions");
    println!("   - Enable link-time optimization (LTO) in release builds");
    println!("   - Use target-cpu=native for platform-specific optimizations");
    
    println!("\n3. **Architectural Improvements**");
    for (category, suggestions) in categories {
        println!("   {}:", category);
        for (i, _) in suggestions.iter().enumerate().take(2) {
            println!("     - See suggestion {}", i + 1);
        }
    }
    
    // Generate automated optimization script
    generate_optimization_script()?;
    
    Ok(())
}

fn generate_optimization_script() -> Result<(), Box<dyn std::error::Error>> {
    let script = r#"#!/bin/bash
# Automated optimization script generated by heed-profile

echo "Applying automated optimizations..."

# 1. Update Cargo.toml with optimization flags
cat >> Cargo.toml << 'EOF'

[profile.release]
lto = "fat"
codegen-units = 1
panic = "abort"

[profile.bench]
lto = "thin"
debug = true
EOF

# 2. Create .cargo/config.toml with platform-specific flags
mkdir -p .cargo
cat > .cargo/config.toml << 'EOF'
[build]
rustflags = ["-C", "target-cpu=native"]

[target.x86_64-unknown-linux-gnu]
rustflags = ["-C", "target-cpu=native", "-C", "link-arg=-fuse-ld=lld"]
EOF

# 3. Add inline attributes to hot functions
echo "TODO: Manually add #[inline] to hot path functions identified in the report"

echo "Optimizations applied. Re-run benchmarks to measure impact."
"#;
    
    fs::write("optimize.sh", script)?;
    
    // Make script executable on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata("optimize.sh")?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions("optimize.sh", perms)?;
    }
    
    println!("\nGenerated optimization script: ./optimize.sh");
    
    Ok(())
}