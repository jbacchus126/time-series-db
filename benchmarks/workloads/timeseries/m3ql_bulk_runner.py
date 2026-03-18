"""
M3QL Bulk Query Runner for OpenSearch Benchmark
Efficiently runs multiple M3QL queries from a file with performance optimization
"""

import json
import os
import time
from typing import Dict, List, Any
from osbenchmark.worker_coordinator.runner import Runner, request_context_holder


# Global storage for metrics
_metrics_log = []
_metrics_summary = {
    "total_queries": 0,
    "total_series": 0,
    "total_samples": 0,
    "total_bytes": 0
}
_metrics_saved = False
_first_query_time = None


def register(registry):
    """Register parameter source and runner with OpenSearch Benchmark"""
    # Parameter source
    registry.register_param_source("m3ql-bulk-query-source", M3QLBulkQuerySource)
    
    # Custom runner - Runner base class is already async-compatible
    registry.register_runner("m3ql-query", M3QLBulkQueryRunner(), async_runner=True)


class M3QLBulkQuerySource:
    """
    Parameter source that loads M3QL queries from a JSON file.
    Uses sequential query selection for consistent benchmarking.
    """
    
    def __init__(self, workload, params, **kwargs):
        """
        Initialize the parameter source.
        
        Args:
            workload: OpenSearch Benchmark workload
            params: Operation parameters
        """
        # Get queries file path
        queries_file = params.get("queries-file", "m3ql_queries_list.json")
        
        # Get queries file path (same directory as this runner)
        workload_path = os.path.dirname(os.path.abspath(__file__))
        queries_path = os.path.join(workload_path, queries_file)
        
        # Load queries
        with open(queries_path, 'r') as f:
            self.queries = json.load(f)
        
        if not self.queries:
            raise ValueError(f"No queries found in {queries_path}")
        
        # Configuration
        self.default_start = params.get("start", "now-1h")
        self.default_end = params.get("end", "now")
        self.default_step = params.get("step", 60000)
        self.index = params.get("index", "timeseries")
        
        # State for sequential mode
        self.current_index = 0
        
        print(f"✅ Loaded {len(self.queries)} M3QL queries from {queries_file}")
    
    def params(self) -> Dict[str, Any]:
        """
        Return parameters for next query execution (sequential order).
        
        Returns:
            dict: Query parameters
        """
        # Select query sequentially
        query_def = self.queries[self.current_index % len(self.queries)]
        self.current_index += 1

        # Check for required parameters
        missing = []
        for key in ["query", "start", "end", "step"]:
            if key not in query_def:
                missing.append(key)
        if missing:
            raise ValueError(f"Query definition missing required parameters: {', '.join(missing)}. Query: {query_def}")

        return {
            "query": query_def["query"],
            "query_name": query_def.get("name", "unnamed"),
            "start": query_def["start"],
            "end": query_def["end"],
            "step": query_def["step"],
            "index": self.index
        }
    
    def partition(self, total_clients, client_index):
        """
        Partition queries among multiple clients for parallel execution.
        All clients get access to all queries in sequential order.
        
        Args:
            total_clients: Total number of parallel clients
            client_index: Index of this client (0-based)
        
        Returns:
            self: This param source instance
        """
        # All clients access all queries sequentially
        return self


class M3QLBulkQueryRunner(Runner):
    """
    Custom runner for M3QL bulk queries that returns TSDB-specific metrics.
    Extends OSB's Runner base class.
    """
    
    def __init__(self):
        """Initialize the runner"""
        super().__init__()
    
    async def __call__(self, opensearch, params):
        """
        Execute a single M3QL query.
        
        Args:
            opensearch: OpenSearch client
            params: Query parameters
        
        Returns:
            dict: Standard OSB format with weight and unit
        """
        query = params["query"]
        query_name = params.get("query_name", "unnamed")
        start = params["start"]
        end = params["end"]
        step = params["step"]
        index = params.get("index", "timeseries")
        
        if not query:
            raise ValueError("M3QL query parameter is required")
        
        # Build request
        body = {"query": query}
        request_params = {"start": start, "end": end, "step": step}
        
        # Use transport.perform_request with proper context management
        request_context_holder.on_client_request_start()
        start_time = time.time()
        response = await opensearch.transport.perform_request(
            "POST",
            "/_m3ql",
            params=request_params,
            body=body
        )
        request_context_holder.on_client_request_end()
        latency_ms = (time.time() - start_time) * 1000
        
        # Extract TSDB metrics from response
        metrics = extract_tsdb_metrics(response, latency_ms)
        
        # Update global summary stats
        global _metrics_log, _metrics_summary, _first_query_time
        
        # Capture timestamp of first query for filename
        if _first_query_time is None:
            _first_query_time = time.time()
        
        _metrics_summary["total_queries"] += 1
        _metrics_summary["total_series"] += metrics["series_count"]
        _metrics_summary["total_samples"] += metrics["total_samples"]
        _metrics_summary["total_bytes"] += metrics["response_size_bytes"]
        
        # Log to global storage for detailed analysis
        log_entry = metrics.copy()
        log_entry["timestamp"] = time.time()
        log_entry["query_name"] = query_name
        log_entry["query"] = query[:100] 
        _metrics_log.append(log_entry)
        
        # Save metrics every 5 queries to ensure incremental capture
        if _metrics_summary['total_queries'] % 5 == 0:
            save_metrics_summary()
        
        # Return metrics in OSB format (weight and unit are required)
        # Additional metrics will be captured in summary stats
        result = {
            "weight": 1,
            "unit": "ops",
            "tsdb_series_count": metrics["series_count"],
            "tsdb_total_samples": metrics["total_samples"],
            "tsdb_response_bytes": metrics["response_size_bytes"]
        }
        
        return result
    
    def __repr__(self):
        return "m3ql-query"


def extract_tsdb_metrics(response: Dict[str, Any], latency_ms: float) -> Dict[str, Any]:
    """
    Extract TSDB-specific metrics from M3QL response.
    
    Args:
        response: M3QL query response
        latency_ms: Query latency in milliseconds
    
    Returns:
        dict: Extracted metrics
    """
    if "took" in response:
        latency_ms = response["took"]
    elif "data" in response and "took" in response["data"]:
        latency_ms = response["data"]["took"]
    
    metrics = {
        "latency_ms": latency_ms,
        "series_count": 0,
        "total_samples": 0,
        "min_samples_per_series": 0,
        "max_samples_per_series": 0,
        "avg_samples_per_series": 0,
        "response_size_bytes": 0
    }
    
    # Extract series information
    if "data" in response and "result" in response["data"]:
        results = response["data"]["result"]
        metrics["series_count"] = len(results)
        
        if metrics["series_count"] > 0:
            sample_counts = []
            
            for series in results:
                if "values" in series:
                    sample_count = len(series["values"])
                    sample_counts.append(sample_count)
                    metrics["total_samples"] += sample_count
            
            if sample_counts:
                metrics["min_samples_per_series"] = min(sample_counts)
                metrics["max_samples_per_series"] = max(sample_counts)
                metrics["avg_samples_per_series"] = sum(sample_counts) / len(sample_counts)
    
    # Estimate response size
    response_str = json.dumps(response)
    metrics["response_size_bytes"] = len(response_str.encode('utf-8'))
    
    return metrics


def save_metrics_summary():
    """Save aggregated TSDB metrics to a file"""
    global _first_query_time
    
    if not _metrics_log:
        return
    
    # Use timestamp from first query
    if _first_query_time:
        timestamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(_first_query_time))
    else:
        timestamp = time.strftime("%Y%m%d-%H%M%S")
    
    metrics_file = f"results/tsdb-metrics-{timestamp}.json"
    
    summary = {
        "summary": _metrics_summary,
        "averages": {
            "avg_series_per_query": _metrics_summary["total_series"] / max(_metrics_summary["total_queries"], 1),
            "avg_samples_per_query": _metrics_summary["total_samples"] / max(_metrics_summary["total_queries"], 1),
            "avg_bytes_per_query": _metrics_summary["total_bytes"] / max(_metrics_summary["total_queries"], 1)
        },
        "detailed_metrics": _metrics_log
    }
    
    os.makedirs("results", exist_ok=True)
    with open(metrics_file, 'w') as f:
        json.dump(summary, f, indent=2)
    


# Register cleanup handler to save metrics at the end
import atexit
atexit.register(save_metrics_summary)


def save_metrics_log(filename="tsdb_metrics_log.json"):
    """
    Save collected TSDB metrics to a JSON file.
    Call this after benchmark completion to persist metrics.
    """
    global _metrics_log
    
    if not _metrics_log:
        return
    
    output_path = os.path.join("results", filename)
    os.makedirs("results", exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(_metrics_log, f, indent=2)
    
    print(f"📊 TSDB metrics saved to: {output_path}")
    
    # Print summary
    if _metrics_log:
        total_series = sum(m.get("series_count", 0) for m in _metrics_log)
        total_samples = sum(m.get("total_samples", 0) for m in _metrics_log)
        avg_series = total_series / len(_metrics_log) if _metrics_log else 0
        avg_samples = total_samples / len(_metrics_log) if _metrics_log else 0
        
        print(f"   Total queries: {len(_metrics_log)}")
        print(f"   Avg series per query: {avg_series:.1f}")
        print(f"   Avg samples per query: {avg_samples:.1f}")
