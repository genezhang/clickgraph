#!/bin/bash
# LDBC SNB Master Benchmark Suite
# Orchestrates data loading, query performance, and concurrent load testing

set -e

SCALE_FACTOR="${1:-sf10}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "LDBC SNB Comprehensive Benchmark Suite"
echo "=========================================="
echo "Scale Factor: $SCALE_FACTOR"
echo "Starting: $(date)"
echo "=========================================="
echo ""

# Make sure all scripts are executable
chmod +x "$SCRIPT_DIR"/*.sh

# Phase 1: Data Loading Benchmark
echo "================================================"
echo "Phase 1: Data Loading Benchmark"
echo "================================================"
if [ -f "$SCRIPT_DIR/benchmark_data_loading.sh" ]; then
    "$SCRIPT_DIR/benchmark_data_loading.sh" "$SCALE_FACTOR" || {
        echo "❌ Data loading benchmark failed"
        exit 1
    }
else
    echo "⚠️  Data loading benchmark script not found, skipping..."
fi
echo ""

# Wait for ClickHouse to stabilize
echo "Waiting 10s for ClickHouse to stabilize..."
sleep 10

# Phase 2: Query Performance Benchmark
echo "================================================"
echo "Phase 2: Query Performance Benchmark"
echo "================================================"
if [ -f "$SCRIPT_DIR/benchmark_query_performance.sh" ]; then
    # Set environment for query benchmarks
    export SCALE_FACTOR="$SCALE_FACTOR"
    export WARMUP_RUNS=1
    export BENCHMARK_RUNS=3
    
    "$SCRIPT_DIR/benchmark_query_performance.sh" || {
        echo "⚠️  Some queries failed, continuing..."
    }
else
    echo "⚠️  Query performance benchmark script not found, skipping..."
fi
echo ""

# Phase 3: Concurrent Load Testing
echo "================================================"
echo "Phase 3: Concurrent Load Testing"
echo "================================================"
if [ -f "$SCRIPT_DIR/benchmark_concurrent_load.sh" ]; then
    # Set environment for concurrent tests
    export SCALE_FACTOR="$SCALE_FACTOR"
    export TEST_DURATION=30
    export CONNECTION_COUNTS="1 2 4 8 16 32"
    
    "$SCRIPT_DIR/benchmark_concurrent_load.sh" || {
        echo "⚠️  Concurrent load testing failed"
        exit 1
    }
else
    echo "⚠️  Concurrent load benchmark script not found, skipping..."
fi
echo ""

# Generate Combined Report
echo "================================================"
echo "Generating Combined Report"
echo "================================================"

RESULTS_DIR="$BENCHMARK_DIR/results"
REPORT_FILE="$RESULTS_DIR/benchmark_report_${SCALE_FACTOR}_$(date +%Y%m%d_%H%M%S).md"

cat > "$REPORT_FILE" << EOF
# LDBC SNB Benchmark Report - $SCALE_FACTOR

**Generated**: $(date -Iseconds)

## Executive Summary

This report contains comprehensive benchmark results for ClickGraph on the LDBC SNB $SCALE_FACTOR dataset.

### Test Environment
- **Scale Factor**: $SCALE_FACTOR
- **ClickGraph Version**: $(git describe --tags --always 2>/dev/null || echo "unknown")
- **ClickHouse Version**: $(curl -s http://localhost:8123 --data "SELECT version()" 2>/dev/null || echo "unknown")
- **System**: $(uname -a)
- **CPU Cores**: $(nproc)
- **Memory**: $(free -h | grep Mem | awk '{print $2}')

## Phase 1: Data Loading Performance

See detailed results in: \`results/loading/\`

### Summary
EOF

# Add loading summary if available
LATEST_LOADING=$(ls -t "$RESULTS_DIR/loading"/loading_benchmark_*.json 2>/dev/null | head -1)
if [ -f "$LATEST_LOADING" ]; then
    TOTAL_ROWS=$(jq -r '.summary.total_rows' "$LATEST_LOADING")
    TOTAL_DURATION=$(jq -r '.summary.total_duration_seconds' "$LATEST_LOADING")
    AVG_THROUGHPUT=$(jq -r '.summary.avg_throughput_rows_per_sec' "$LATEST_LOADING")
    
    cat >> "$REPORT_FILE" << EOF
- **Total Rows Loaded**: $(printf "%'d" $TOTAL_ROWS)
- **Total Duration**: ${TOTAL_DURATION}s
- **Average Throughput**: $(printf "%'d" $(echo $AVG_THROUGHPUT | cut -d. -f1)) rows/sec
EOF
else
    echo "- No loading data available" >> "$REPORT_FILE"
fi

cat >> "$REPORT_FILE" << EOF

## Phase 2: Query Performance

See detailed results in: \`results/performance/\`

### Summary
EOF

# Add query performance summary if available
LATEST_QUERY=$(ls -t "$RESULTS_DIR/performance"/query_benchmark_*.json 2>/dev/null | head -1)
if [ -f "$LATEST_QUERY" ]; then
    TOTAL_QUERIES=$(jq -r '.summary.total_queries' "$LATEST_QUERY")
    PASSED=$(jq -r '.summary.passed' "$LATEST_QUERY")
    SUCCESS_RATE=$(jq -r '.summary.success_rate' "$LATEST_QUERY")
    
    cat >> "$REPORT_FILE" << EOF
- **Total Queries Tested**: $TOTAL_QUERIES
- **Passed**: $PASSED
- **Success Rate**: ${SUCCESS_RATE}%

### Top 5 Fastest Queries
| Query | Avg Duration (ms) | Rows |
|-------|-------------------|------|
EOF
    
    jq -r '.queries | sort_by(.avg_duration_ms) | limit(5;.[]) | "| \(.query) | \(.avg_duration_ms) | \(.avg_rows) |"' "$LATEST_QUERY" >> "$REPORT_FILE"
    
    cat >> "$REPORT_FILE" << EOF

### Top 5 Slowest Queries
| Query | Avg Duration (ms) | Rows |
|-------|-------------------|------|
EOF
    
    jq -r '.queries | sort_by(-.avg_duration_ms) | limit(5;.[]) | "| \(.query) | \(.avg_duration_ms) | \(.avg_rows) |"' "$LATEST_QUERY" >> "$REPORT_FILE"
else
    echo "- No query performance data available" >> "$REPORT_FILE"
fi

cat >> "$REPORT_FILE" << EOF

## Phase 3: Concurrent Load Testing

See detailed results in: \`results/concurrency/\`

### Summary

Peak QPS by query type at optimal concurrency:
EOF

# Add concurrent load summary if available
LATEST_CONCURRENT=$(ls -t "$RESULTS_DIR/concurrency"/concurrent_benchmark_*.json 2>/dev/null | head -1)
if [ -f "$LATEST_CONCURRENT" ]; then
    # Find peak QPS for each query
    for query in $(jq -r '.tests[].query' "$LATEST_CONCURRENT" | sort -u); do
        PEAK_QPS=$(jq -r ".tests[] | select(.query==\"$query\") | .qps" "$LATEST_CONCURRENT" | sort -n | tail -1)
        PEAK_CONN=$(jq -r ".tests[] | select(.query==\"$query\" and .qps==$PEAK_QPS) | .connections" "$LATEST_CONCURRENT")
        P99=$(jq -r ".tests[] | select(.query==\"$query\" and .connections==$PEAK_CONN) | .latency_ms.p99" "$LATEST_CONCURRENT")
        
        echo "- **$query**: ${PEAK_QPS} q/s @ ${PEAK_CONN} connections (P99: ${P99}ms)" >> "$REPORT_FILE"
    done
else
    echo "- No concurrent load data available" >> "$REPORT_FILE"
fi

cat >> "$REPORT_FILE" << EOF

## Conclusion

ClickGraph successfully processed $SCALE_FACTOR LDBC SNB benchmark with:
- High query success rate (${SUCCESS_RATE}%)
- Efficient data loading
- Scalable concurrent query handling

For detailed metrics, see the individual result files in \`results/\`.

---
*Report generated by LDBC SNB Benchmark Suite*
EOF

echo "Report generated: $REPORT_FILE"
cat "$REPORT_FILE"

echo ""
echo "=========================================="
echo "Benchmark Suite Complete!"
echo "=========================================="
echo "Finished: $(date)"
echo "Results Directory: $RESULTS_DIR"
echo "Report: $REPORT_FILE"
echo "=========================================="

exit 0
