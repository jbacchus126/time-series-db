#!/usr/bin/env python3
"""
Display TSDB-Specific Metrics from OpenSearch Benchmark Results
Shows metrics from the JSON files created by custom runners
"""

import json
import sys
import os
from pathlib import Path


def find_latest_tsdb_metrics():
    """Find the most recent TSDB metrics JSON file"""
    results_dir = Path("results")
    if not results_dir.exists():
        return None
    
    # Find all TSDB metrics JSON files
    metrics_files = list(results_dir.glob("tsdb-*.json"))
    if not metrics_files:
        return None
    
    # Return most recent
    return max(metrics_files, key=lambda p: p.stat().st_mtime)


def display_tsdb_metrics():
    """Display TSDB metrics from latest JSON file"""
    
    print("📊 TSDB-Specific Metrics from Latest Run")
    print("=" * 70)
    
    # Find latest TSDB metrics
    latest_metrics = find_latest_tsdb_metrics()
    
    if not latest_metrics:
        print("❌ No TSDB metrics found in results/")
        print("   Run a benchmark with the M3QL runners first")
        print("   Example: ./run_query_benchmark.sh")
        return False
    
    print(f"\n📄 Latest Metrics: {latest_metrics.name}")
    print(f"   Modified: {latest_metrics.stat().st_mtime}")
    print()
    
    # Load and display metrics
    with open(latest_metrics, 'r') as f:
        data = json.load(f)
    
    summary = data.get("summary", {})
    averages = data.get("averages", {})
    
    print("📈 Summary Statistics:")
    print("-" * 70)
    print(f"   Total Queries:        {summary.get('total_queries', 0):,}")
    print(f"   Total Series:         {summary.get('total_series', 0):,}")
    print(f"   Total Samples:        {summary.get('total_samples', 0):,}")
    print(f"   Total Bytes:          {summary.get('total_bytes', 0):,}")
    print()
    print(f"   Avg Series/Query:     {averages.get('avg_series_per_query', 0):.1f}")
    print(f"   Avg Samples/Query:    {averages.get('avg_samples_per_query', 0):.1f}")
    print(f"   Avg Bytes/Query:      {averages.get('avg_bytes_per_query', 0):.1f}")
    print("-" * 70)
    
    # Show sample of detailed metrics
    detailed = data.get("detailed_metrics", [])
    if detailed:
        print()
        print("📝 Sample Query Results (first 3):")
        print("-" * 70)
        for i, metric in enumerate(detailed[:3]):
            print(f"\n   Query {i+1}: {metric.get('query', 'N/A')}")
            print(f"      Series: {metric.get('series_count', 0):,}")
            print(f"      Samples: {metric.get('total_samples', 0):,}")
            print(f"      Bytes: {metric.get('response_size_bytes', 0):,}")
            print(f"      Service Time: {metric.get('service_time_ms', 0):.2f} ms")
    
    print()
    print("=" * 70)
    print()
    print(f"📋 Full metrics: cat {latest_metrics}")
    print()
    
    return True


def main():
    """Main entry point"""
    # Change to benchmarks directory if needed
    if os.path.exists("benchmarks/results"):
        os.chdir("benchmarks")
    elif not os.path.exists("results"):
        print("❌ Cannot find results directory")
        print("   Run from: /Users/jbacchus/Uber/opensearch/opensearch-tsdb-internal/benchmarks")
        return 1
    
    success = display_tsdb_metrics()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
