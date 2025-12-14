#!/bin/bash
# LDBC SNB Concurrent Load Testing
# Measures QPS (Queries Per Second) with varying connection counts

set -e

CLICKGRAPH_URL="${CLICKGRAPH_URL:-http://localhost:8080}"
SCHEMA_NAME="${SCHEMA_NAME:-ldbc_snb}"
SCALE_FACTOR="${SCALE_FACTOR:-sf10}"
TEST_DURATION="${TEST_DURATION:-30}"  # seconds
CONNECTION_COUNTS="${CONNECTION_COUNTS:-1 2 4 8 16 32}"

RESULTS_DIR="./benchmarks/ldbc_snb/results/concurrency"
mkdir -p "$RESULTS_DIR"
RESULTS_FILE="${RESULTS_DIR}/concurrent_benchmark_${SCALE_FACTOR}_$(date +%Y%m%d_%H%M%S).json"
SUMMARY_FILE="${RESULTS_DIR}/concurrent_summary_${SCALE_FACTOR}_$(date +%Y%m%d_%H%M%S).txt"

echo "=========================================="
echo "LDBC SNB Concurrent Load Testing"
echo "=========================================="
echo "Scale Factor: $SCALE_FACTOR"
echo "Test Duration: ${TEST_DURATION}s per concurrency level"
echo "Connection Counts: $CONNECTION_COUNTS"
echo "Results: $RESULTS_FILE"
echo "=========================================="
echo ""

# Check server is running
if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "❌ ClickGraph server is not running"
    echo "Start with:"
    echo "  export CLICKHOUSE_URL='http://localhost:8123' CLICKHOUSE_USER='default' CLICKHOUSE_PASSWORD='default' CLICKHOUSE_DATABASE='ldbc' GRAPH_CONFIG_PATH='./benchmarks/ldbc_snb/schemas/ldbc_snb.yaml'"
    echo "  cargo run --release &"
    exit 1
fi

# Test queries (mix of simple and complex)
declare -A TEST_QUERIES
TEST_QUERIES["simple_count"]='MATCH (p:Person) RETURN count(p)'
TEST_QUERIES["simple_filter"]='MATCH (p:Person) WHERE p.id = 933 RETURN p.firstName, p.lastName'
TEST_QUERIES["one_hop"]='MATCH (p:Person {id: 933})-[:KNOWS]->(friend:Person) RETURN friend.firstName, friend.lastName LIMIT 10'
TEST_QUERIES["two_hop"]='MATCH (p:Person {id: 933})-[:KNOWS]->()-[:KNOWS]->(foaf:Person) WHERE foaf.id <> 933 RETURN DISTINCT foaf.firstName, foaf.lastName LIMIT 10'
TEST_QUERIES["is2"]='MATCH (p:Person {id: 933})-[:IS_LOCATED_IN]->(city)-[:IS_PART_OF]->(country) RETURN p.firstName, p.lastName, city.name, country.name'

# Initialize results JSON
cat > "$RESULTS_FILE" << 'EOF'
{
  "metadata": {
    "timestamp": "",
    "scale_factor": "",
    "test_duration_seconds": 0,
    "connection_counts": [],
    "clickgraph_url": ""
  },
  "tests": [
EOF

# Update metadata
TIMESTAMP=$(date -Iseconds)
CONN_ARRAY=$(echo "$CONNECTION_COUNTS" | jq -Rc 'split(" ") | map(tonumber)')
sed -i "s|\"timestamp\": \"\"|\"timestamp\": \"$TIMESTAMP\"|" "$RESULTS_FILE"
sed -i "s|\"scale_factor\": \"\"|\"scale_factor\": \"$SCALE_FACTOR\"|" "$RESULTS_FILE"
sed -i "s|\"test_duration_seconds\": 0|\"test_duration_seconds\": $TEST_DURATION|" "$RESULTS_FILE"
sed -i "s|\"connection_counts\": \[\]|\"connection_counts\": $CONN_ARRAY|" "$RESULTS_FILE"
sed -i "s|\"clickgraph_url\": \"\"|\"clickgraph_url\": \"$CLICKGRAPH_URL\"|" "$RESULTS_FILE"

FIRST_TEST=true

# Worker function (runs in parallel)
worker() {
    local worker_id=$1
    local query_name=$2
    local query=$3
    local duration=$4
    local result_file=$5
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration))
    local success_count=0
    local error_count=0
    local total_response_time_ms=0
    local latencies=()
    
    while [ $(date +%s) -lt $end_time ]; do
        local req_start=$(date +%s%N)
        
        local response=$(curl -s -X POST "$CLICKGRAPH_URL/query" \
            -H "Content-Type: application/json" \
            -d "{\"query\": $(echo "$query" | jq -Rs .), \"schema_name\": \"$SCHEMA_NAME\"}" \
            2>/dev/null)
        
        local req_end=$(date +%s%N)
        local req_duration_ms=$(( ($req_end - $req_start) / 1000000 ))
        
        # Check if query succeeded
        local has_results=$(echo "$response" | jq -r 'has("results")' 2>/dev/null || echo "false")
        
        if [ "$has_results" = "true" ]; then
            ((success_count++))
            total_response_time_ms=$((total_response_time_ms + req_duration_ms))
            latencies+=($req_duration_ms)
        else
            ((error_count++))
        fi
    done
    
    # Write worker results
    echo "$worker_id,$success_count,$error_count,$total_response_time_ms,${latencies[*]}" >> "$result_file"
}

# Benchmark function
benchmark_concurrency() {
    local query_name=$1
    local query=$2
    local connections=$3
    
    echo "Testing $query_name with $connections connections..."
    
    # Temporary file for worker results
    local temp_results=$(mktemp)
    
    # Start workers
    local pids=()
    for ((i=1; i<=connections; i++)); do
        worker "$i" "$query_name" "$query" "$TEST_DURATION" "$temp_results" &
        pids+=($!)
    done
    
    # Wait for all workers
    for pid in "${pids[@]}"; do
        wait $pid
    done
    
    # Aggregate results
    local total_requests=0
    local total_errors=0
    local total_response_time=0
    local all_latencies=()
    
    while IFS=, read -r worker_id success_count error_count response_time latencies; do
        total_requests=$((total_requests + success_count))
        total_errors=$((total_errors + error_count))
        total_response_time=$((total_response_time + response_time))
        
        # Parse latencies
        for lat in $latencies; do
            all_latencies+=($lat)
        done
    done < "$temp_results"
    
    rm "$temp_results"
    
    # Calculate metrics
    local qps=$(awk "BEGIN {printf \"%.2f\", $total_requests/$TEST_DURATION}")
    local avg_latency_ms=0
    local p50_ms=0
    local p90_ms=0
    local p99_ms=0
    local min_ms=0
    local max_ms=0
    
    if [ $total_requests -gt 0 ]; then
        avg_latency_ms=$(awk "BEGIN {printf \"%.2f\", $total_response_time/$total_requests}")
        
        # Sort latencies for percentile calculation
        IFS=$'\n' sorted_latencies=($(printf '%s\n' "${all_latencies[@]}" | sort -n))
        
        local count=${#sorted_latencies[@]}
        min_ms=${sorted_latencies[0]}
        max_ms=${sorted_latencies[$((count-1))]}
        
        local p50_idx=$(awk "BEGIN {printf \"%.0f\", $count*0.50}")
        local p90_idx=$(awk "BEGIN {printf \"%.0f\", $count*0.90}")
        local p99_idx=$(awk "BEGIN {printf \"%.0f\", $count*0.99}")
        
        p50_ms=${sorted_latencies[$p50_idx]:-0}
        p90_ms=${sorted_latencies[$p90_idx]:-0}
        p99_ms=${sorted_latencies[$p99_idx]:-0}
    fi
    
    local error_rate=0
    if [ $((total_requests + total_errors)) -gt 0 ]; then
        error_rate=$(awk "BEGIN {printf \"%.2f\", ($total_errors/($total_requests+$total_errors))*100}")
    fi
    
    echo "  QPS: $qps, Avg Latency: ${avg_latency_ms}ms, P50: ${p50_ms}ms, P90: ${p90_ms}ms, P99: ${p99_ms}ms, Errors: ${error_rate}%"
    
    # Write to JSON
    if [ "$FIRST_TEST" = false ]; then
        echo "    ," >> "$RESULTS_FILE"
    fi
    FIRST_TEST=false
    
    cat >> "$RESULTS_FILE" << EOF
    {
      "query": "$query_name",
      "connections": $connections,
      "duration_seconds": $TEST_DURATION,
      "total_requests": $total_requests,
      "total_errors": $total_errors,
      "qps": $qps,
      "error_rate_percent": $error_rate,
      "latency_ms": {
        "avg": $avg_latency_ms,
        "min": $min_ms,
        "max": $max_ms,
        "p50": $p50_ms,
        "p90": $p90_ms,
        "p99": $p99_ms
      }
    }
EOF
}

# Run benchmarks for each query at each concurrency level
for query_name in "${!TEST_QUERIES[@]}"; do
    echo "=== Query: $query_name ==="
    for connections in $CONNECTION_COUNTS; do
        benchmark_concurrency "$query_name" "${TEST_QUERIES[$query_name]}" "$connections"
    done
    echo ""
done

# Close JSON
echo "" >> "$RESULTS_FILE"
echo "  ]" >> "$RESULTS_FILE"
echo "}" >> "$RESULTS_FILE"

# Generate summary report
{
    echo "=========================================================================="
    echo "LDBC SNB Concurrent Load Testing Summary"
    echo "=========================================================================="
    echo "Timestamp: $TIMESTAMP"
    echo "Scale Factor: $SCALE_FACTOR"
    echo "Test Duration: ${TEST_DURATION}s per test"
    echo ""
    echo "Results by Query and Concurrency:"
    echo "---"
    
    # Parse JSON results for summary
    for query_name in "${!TEST_QUERIES[@]}"; do
        echo ""
        echo "Query: $query_name"
        echo "  Connections | QPS      | Avg Latency | P99 Latency | Error Rate"
        echo "  ------------|----------|-------------|-------------|------------"
        
        for connections in $CONNECTION_COUNTS; do
            local qps=$(jq -r ".tests[] | select(.query==\"$query_name\" and .connections==$connections) | .qps" "$RESULTS_FILE")
            local avg_lat=$(jq -r ".tests[] | select(.query==\"$query_name\" and .connections==$connections) | .latency_ms.avg" "$RESULTS_FILE")
            local p99_lat=$(jq -r ".tests[] | select(.query==\"$query_name\" and .connections==$connections) | .latency_ms.p99" "$RESULTS_FILE")
            local err_rate=$(jq -r ".tests[] | select(.query==\"$query_name\" and .connections==$connections) | .error_rate_percent" "$RESULTS_FILE")
            
            printf "  %-11s | %-8s | %-11s | %-11s | %-10s\n" "$connections" "${qps}q/s" "${avg_lat}ms" "${p99_lat}ms" "${err_rate}%"
        done
    done
    
    echo ""
    echo "=========================================================================="
    echo "Detailed Results: $RESULTS_FILE"
    echo "=========================================================================="
} | tee "$SUMMARY_FILE"

echo ""
echo "Results saved to: $RESULTS_FILE"
echo "Summary saved to: $SUMMARY_FILE"

exit 0
