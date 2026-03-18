## Timeseries Workload

A benchmark workload for testing the OpenSearch TSDB engine with example monitoring data.

### Dataset

The workload includes a small example dataset with 793 documents covering 1 hour of metrics:

- **Metrics**: CPU usage, memory usage, HTTP request counts
- **Time range**: 2025-01-01 00:00:00 to 01:00:00 (1 hour)
- **Step**: 60 seconds
- **Hosts**: 5 (host_0 through host_4)
- **Regions**: reg0, reg1
- **Datacenters**: dc0, dc1, dc2

### Document Format

```json
{
  "labels": "name cpu field usage_user hostname host_0 region reg0 datacenter dc0 team platform",
  "timestamp": 1735718400000,
  "value": 24.0
}
```

### Example Queries

The workload includes 9 queries demonstrating M3QL capabilities:

1. **cpu-usage-by-host**: `fetch name:cpu field:usage_user | avg hostname`
2. **cpu-high-usage-alert**: CPU alert with threshold
3. **memory-by-region**: Memory aggregated by region
4. **cpu-by-datacenter**: CPU by datacenter
5. **request-rate**: HTTP requests per second
6. **cpu-scaled**: Scaled CPU values
7. **memory-with-gap-filling**: Gap-filling with keepLastValue
8. **cpu-by-region**: Filter by region
9. **all-metrics-count**: Count all metrics

### Running the Benchmark

The easiest way to run benchmarks is using the provided script:

```bash
cd benchmarks
./run_full_benchmark.sh
```

This will:
1. Ingest the timeseries data
2. Run all M3QL queries
3. Display TSDB-specific metrics

#### Options

```bash
# Restart OpenSearch cluster before benchmarking (clean state)
./run_full_benchmark.sh --restart

# Ingest only a percentage of the data
./run_full_benchmark.sh --ingest-percentage 50
```

### Results

Results are saved to `benchmarks/results/`:
- `ingest-<timestamp>.md` - Ingestion metrics
- `m3ql-queries-<timestamp>.md` - Query performance
- `tsdb-metrics-<timestamp>.json` - Detailed TSDB metrics (series count, samples, etc.)

### Parameters

This workload supports custom parameters via `--workload-params`:

* `bulk_size` (default: 10000)
* `ingest_percentage` (default: 100)
* `number_of_replicas` (default: 0)
* `number_of_shards` (default: 1)
* `query_cache_enabled` (default: false)
* `requests_cache_enabled` (default: false)

Example:
```bash
cd benchmarks
opensearch-benchmark run \
  --pipeline=benchmark-only \
  --target-hosts=localhost:9200 \
  --workload-path=workloads/timeseries \
  --test-procedure=ingest \
  --workload-params='{"bulk_size": 5000}'
```
