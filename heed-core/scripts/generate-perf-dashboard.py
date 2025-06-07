#!/usr/bin/env python3
"""
Generate a performance dashboard from benchmark results
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

# HTML template for the dashboard
DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>heed-core Performance Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #333;
            border-bottom: 2px solid #007bff;
            padding-bottom: 10px;
        }
        .metric-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        .metric-card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .metric-title {
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 10px;
            color: #555;
        }
        .metric-value {
            font-size: 36px;
            font-weight: 700;
            color: #007bff;
        }
        .metric-change {
            font-size: 14px;
            margin-top: 5px;
        }
        .metric-change.positive {
            color: #28a745;
        }
        .metric-change.negative {
            color: #dc3545;
        }
        .chart-container {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .recommendations {
            background: #fff3cd;
            border: 1px solid #ffeeba;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
        }
        .recommendations h2 {
            color: #856404;
            margin-top: 0;
        }
        .recommendations ul {
            margin: 10px 0;
            padding-left: 20px;
        }
        .recommendations li {
            margin: 5px 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>heed-core Performance Dashboard</h1>
        <p>Last updated: {timestamp}</p>
        
        <div class="metric-grid">
            {metrics}
        </div>
        
        <div class="chart-container">
            <h2>Performance Trends</h2>
            <canvas id="performanceChart"></canvas>
        </div>
        
        <div class="chart-container">
            <h2>Operation Breakdown</h2>
            <canvas id="breakdownChart"></canvas>
        </div>
        
        <div class="recommendations">
            <h2>Performance Recommendations</h2>
            <ul>
                {recommendations}
            </ul>
        </div>
    </div>
    
    <script>
        {chart_scripts}
    </script>
</body>
</html>
"""

def load_benchmark_results():
    """Load benchmark results from criterion output"""
    # Mock data - in practice, parse actual criterion JSON output
    return {
        "sequential_writes": {
            "current": 572.98,  # microseconds
            "baseline": 600.0,
            "unit": "µs",
            "samples": 5050
        },
        "random_reads": {
            "current": 125.5,
            "baseline": 120.0,
            "unit": "µs",
            "samples": 8000
        },
        "full_scan": {
            "current": 2500.0,
            "baseline": 2600.0,
            "unit": "µs",
            "samples": 100
        },
        "btree_search": {
            "current": 85.2,
            "baseline": 90.0,
            "unit": "ns",
            "samples": 10000
        },
        "page_allocation": {
            "current": 450.0,
            "baseline": 440.0,
            "unit": "ns",
            "samples": 5000
        }
    }

def generate_metrics_html(results):
    """Generate HTML for metric cards"""
    metrics_html = []
    
    for name, data in results.items():
        current = data["current"]
        baseline = data["baseline"]
        change = ((current - baseline) / baseline) * 100
        
        change_class = "positive" if change < 0 else "negative"
        change_symbol = "↓" if change < 0 else "↑"
        
        metric_html = f"""
        <div class="metric-card">
            <div class="metric-title">{name.replace('_', ' ').title()}</div>
            <div class="metric-value">{current:.1f} {data['unit']}</div>
            <div class="metric-change {change_class}">
                {change_symbol} {abs(change):.1f}% from baseline
            </div>
        </div>
        """
        metrics_html.append(metric_html)
    
    return "\n".join(metrics_html)

def generate_chart_scripts(results):
    """Generate Chart.js scripts for visualizations"""
    # Performance trends chart
    labels = list(results.keys())
    current_values = [r["current"] for r in results.values()]
    baseline_values = [r["baseline"] for r in results.values()]
    
    performance_chart = f"""
    const perfCtx = document.getElementById('performanceChart').getContext('2d');
    new Chart(perfCtx, {{
        type: 'bar',
        data: {{
            labels: {json.dumps(labels)},
            datasets: [{{
                label: 'Current',
                data: {json.dumps(current_values)},
                backgroundColor: 'rgba(0, 123, 255, 0.8)',
                borderColor: 'rgba(0, 123, 255, 1)',
                borderWidth: 1
            }}, {{
                label: 'Baseline',
                data: {json.dumps(baseline_values)},
                backgroundColor: 'rgba(108, 117, 125, 0.5)',
                borderColor: 'rgba(108, 117, 125, 1)',
                borderWidth: 1
            }}]
        }},
        options: {{
            responsive: true,
            scales: {{
                y: {{
                    beginAtZero: true,
                    title: {{
                        display: true,
                        text: 'Time (µs)'
                    }}
                }}
            }}
        }}
    }});
    """
    
    # Operation breakdown pie chart
    breakdown_chart = f"""
    const breakdownCtx = document.getElementById('breakdownChart').getContext('2d');
    new Chart(breakdownCtx, {{
        type: 'doughnut',
        data: {{
            labels: {json.dumps(labels)},
            datasets: [{{
                data: {json.dumps(current_values)},
                backgroundColor: [
                    'rgba(255, 99, 132, 0.8)',
                    'rgba(54, 162, 235, 0.8)',
                    'rgba(255, 206, 86, 0.8)',
                    'rgba(75, 192, 192, 0.8)',
                    'rgba(153, 102, 255, 0.8)'
                ]
            }}]
        }},
        options: {{
            responsive: true,
            plugins: {{
                legend: {{
                    position: 'right'
                }},
                title: {{
                    display: true,
                    text: 'Time Distribution by Operation'
                }}
            }}
        }}
    }});
    """
    
    return performance_chart + "\n" + breakdown_chart

def generate_recommendations(results):
    """Generate performance recommendations based on results"""
    recommendations = []
    
    # Check for regressions
    for name, data in results.items():
        change = ((data["current"] - data["baseline"]) / data["baseline"]) * 100
        if change > 10:
            recommendations.append(
                f"<li><strong>{name}:</strong> {change:.1f}% regression detected. "
                f"Consider profiling this operation specifically.</li>"
            )
    
    # General recommendations based on patterns
    if results.get("sequential_writes", {}).get("current", 0) > 500:
        recommendations.append(
            "<li><strong>Sequential writes:</strong> Consider implementing bulk insert optimization "
            "or increasing page size for sequential workloads.</li>"
        )
    
    if results.get("random_reads", {}).get("current", 0) > 100:
        recommendations.append(
            "<li><strong>Random reads:</strong> Implement caching for frequently accessed pages "
            "or add a bloom filter for non-existent keys.</li>"
        )
    
    if results.get("page_allocation", {}).get("current", 0) > 400:
        recommendations.append(
            "<li><strong>Page allocation:</strong> Implement a freelist cache "
            "or batch page allocations to reduce overhead.</li>"
        )
    
    if not recommendations:
        recommendations.append("<li>All operations are performing within expected bounds.</li>")
    
    return "\n".join(recommendations)

def main():
    """Generate the performance dashboard"""
    results = load_benchmark_results()
    
    # Generate HTML components
    metrics_html = generate_metrics_html(results)
    chart_scripts = generate_chart_scripts(results)
    recommendations = generate_recommendations(results)
    
    # Fill in the template
    dashboard_html = DASHBOARD_TEMPLATE.format(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        metrics=metrics_html,
        chart_scripts=chart_scripts,
        recommendations=recommendations
    )
    
    # Write to file
    output_path = Path("target/performance-dashboard.html")
    output_path.parent.mkdir(exist_ok=True)
    output_path.write_text(dashboard_html)
    
    print(f"Performance dashboard generated: {output_path}")
    print("Open in a web browser to view the results.")

if __name__ == "__main__":
    main()