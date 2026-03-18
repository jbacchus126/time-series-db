#!/bin/bash
# Run full TSDB benchmark: ingestion + queries
# This ensures data is loaded before running query benchmarks

set -e

# Parse arguments
RESTART_CLUSTER=false
INGEST_PERCENTAGE=100

while [[ $# -gt 0 ]]; do
    case $1 in
        --restart)
            RESTART_CLUSTER=true
            shift
            ;;
        --ingest-percentage)
            INGEST_PERCENTAGE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--restart] [--ingest-percentage <1-100>]"
            echo ""
            echo "Options:"
            echo "  --restart                Restart OpenSearch cluster before benchmark"
            echo "  --ingest-percentage <n>  Percentage of data to ingest (default: 100)"
            exit 1
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Restart OpenSearch cluster if requested
if [[ "$RESTART_CLUSTER" == true ]]; then
    echo "🔄 Restarting OpenSearch cluster for clean benchmark state..."
    docker compose -f docker-compose.yml down -v
    docker compose -f docker-compose.yml up -d
    
    echo "⏳ Waiting for OpenSearch to be ready..."
    sleep 30
    
    # Wait for cluster health
    until curl -s http://localhost:9200/_cluster/health | grep -q '"status":"green"\|"status":"yellow"'; do
        echo "   Waiting for cluster..."
        sleep 5
    done
    
    # Additional wait for system indices initialization to complete
    echo "   Waiting for system indices to stabilize..."
    sleep 15
    
    # Delete any existing timeseries index
    curl -s -X DELETE "http://localhost:9200/timeseries" > /dev/null 2>&1 || true
    
    # Final wait for metrics to settle
    echo "   Allowing metrics to settle..."
    sleep 5
    
    echo "✅ OpenSearch is ready!"
    echo ""
fi

# Activate Python venv
source venv/bin/activate

export THESPIAN_BASE_IPADDR=127.0.0.1
export THESPIAN_SYSTEM_BASE=multiprocQueueBase
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

mkdir -p results

TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Step 1: Ingest data
echo "📥 Step 1: Ingesting timeseries data (${INGEST_PERCENTAGE}%)"
echo "=================================================="
echo ""

INGEST_RESULT="results/ingest-${TIMESTAMP}.md"

opensearch-benchmark run \
  --pipeline=benchmark-only \
  --target-hosts=localhost:9200 \
  --workload-path=workloads/timeseries \
  --test-procedure=ingest \
  --workload-params="ingest_percentage:${INGEST_PERCENTAGE}" \
  --kill-running-processes \
  --results-format=markdown \
  --results-file="$INGEST_RESULT"

echo ""
echo "✅ Data ingestion complete!"
echo "📊 Ingestion results: $INGEST_RESULT"
echo ""
echo "⏳ Waiting for data to settle..."
sleep 10

# Step 2: Run query benchmark
echo "🔍 Step 2: Running M3QL query benchmark"
echo "=================================================="
echo ""

QUERY_RESULT="results/m3ql-queries-${TIMESTAMP}.md"

opensearch-benchmark run \
  --pipeline=benchmark-only \
  --target-hosts=localhost:9200 \
  --workload-path=workloads/timeseries \
  --test-procedure=m3ql-query \
  --kill-running-processes \
  --results-format=markdown \
  --results-file="$QUERY_RESULT"

echo ""
echo "✅ Query benchmark complete!"
echo "📊 Query results: $QUERY_RESULT"
echo ""

# Display TSDB metrics
echo "📊 TSDB-Specific Metrics"
echo "=================================================="
echo ""
python3 show_tsdb_metrics.py

echo ""
echo "🎉 Full benchmark complete!"
echo ""
echo "Results:"
echo "  - Ingestion: $INGEST_RESULT"
echo "  - Queries:   $QUERY_RESULT"
echo ""
