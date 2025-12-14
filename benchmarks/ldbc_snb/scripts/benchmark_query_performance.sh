#!/bin/bash
# LDBC SNB Query Performance Benchmark
# Measures query execution time for all supported queries

set -e

CLICKGRAPH_URL="${CLICKGRAPH_URL:-http://localhost:8080}"
SCHEMA_NAME="${SCHEMA_NAME:-ldbc_snb}"
SCALE_FACTOR="${SCALE_FACTOR:-sf10}"
WARMUP_RUNS="${WARMUP_RUNS:-1}"
BENCHMARK_RUNS="${BENCHMARK_RUNS:-3}"

RESULTS_DIR="./benchmarks/ldbc_snb/results/performance"
mkdir -p "$RESULTS_DIR"
RESULTS_FILE="${RESULTS_DIR}/query_benchmark_${SCALE_FACTOR}_$(date +%Y%m%d_%H%M%S).json"
SUMMARY_FILE="${RESULTS_DIR}/query_summary_${SCALE_FACTOR}_$(date +%Y%m%d_%H%M%S).txt"

echo "=========================================="
echo "LDBC SNB Query Performance Benchmark"
echo "=========================================="
echo "Scale Factor: $SCALE_FACTOR"
echo "Warmup Runs: $WARMUP_RUNS"
echo "Benchmark Runs: $BENCHMARK_RUNS"
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

# Initialize results JSON
cat > "$RESULTS_FILE" << 'EOF'
{
  "metadata": {
    "timestamp": "",
    "scale_factor": "",
    "warmup_runs": 0,
    "benchmark_runs": 0,
    "clickgraph_url": ""
  },
  "queries": [
EOF

# Update metadata
TIMESTAMP=$(date -Iseconds)
sed -i "s|\"timestamp\": \"\"|\"timestamp\": \"$TIMESTAMP\"|" "$RESULTS_FILE"
sed -i "s|\"scale_factor\": \"\"|\"scale_factor\": \"$SCALE_FACTOR\"|" "$RESULTS_FILE"
sed -i "s|\"warmup_runs\": 0|\"warmup_runs\": $WARMUP_RUNS|" "$RESULTS_FILE"
sed -i "s|\"benchmark_runs\": 0|\"benchmark_runs\": $BENCHMARK_RUNS|" "$RESULTS_FILE"
sed -i "s|\"clickgraph_url\": \"\"|\"clickgraph_url\": \"$CLICKGRAPH_URL\"|" "$RESULTS_FILE"

FIRST_QUERY=true
TOTAL_QUERIES=0
PASSED_QUERIES=0
FAILED_QUERIES=0

# Helper to benchmark a query
benchmark_query() {
    local query_name=$1
    local query_file=$2
    local params=${3:-"{}"}
    
    ((TOTAL_QUERIES++))
    
    echo "Benchmarking $query_name..."
    
    local query_content=$(cat "$query_file" | grep -v '^//' | grep -v '^ */\*' | grep -v '^ *\*' | tr '\n' ' ')
    
    # Warmup runs
    for ((i=1; i<=$WARMUP_RUNS; i++)); do
        curl -s -X POST "$CLICKGRAPH_URL/query" \
            -H "Content-Type: application/json" \
            -d "{\"query\": $(echo "$query_content" | jq -Rs .), \"schema_name\": \"$SCHEMA_NAME\", \"parameters\": $params}" \
            > /dev/null 2>&1 || true
    done
    
    # Benchmark runs
    local durations=()
    local row_counts=()
    local success_count=0
    local error_msg=""
    
    for ((i=1; i<=$BENCHMARK_RUNS; i++)); do
        local start_time=$(date +%s%N)
        
        local response=$(curl -s -X POST "$CLICKGRAPH_URL/query" \
            -H "Content-Type: application/json" \
            -d "{\"query\": $(echo "$query_content" | jq -Rs .), \"schema_name\": \"$SCHEMA_NAME\", \"parameters\": $params}")
        
        local end_time=$(date +%s%N)
        local duration_ms=$(( ($end_time - $start_time) / 1000000 ))
        
        # Check if query succeeded
        local has_results=$(echo "$response" | jq -r 'has("results")' 2>/dev/null || echo "false")
        
        if [ "$has_results" = "true" ]; then
            ((success_count++))
            durations+=($duration_ms)
            local row_count=$(echo "$response" | jq '.results | length' 2>/dev/null || echo 0)
            row_counts+=($row_count)
        else
            error_msg=$(echo "$response" | jq -r '.error // "Unknown error"' 2>/dev/null || echo "Parse error")
        fi
    done
    
    # Calculate statistics
    if [ $success_count -gt 0 ]; then
        ((PASSED_QUERIES++))
        
        # Calculate min, max, avg, median
        local min_ms=$(printf '%s\n' "${durations[@]}" | sort -n | head -1)
        local max_ms=$(printf '%s\n' "${durations[@]}" | sort -n | tail -1)
        local sum_ms=$(IFS=+; echo "$((${durations[*]}))")
        local avg_ms=$(awk "BEGIN {printf \"%.2f\", $sum_ms/$success_count}")
        
        local median_ms
        if [ ${#durations[@]} -eq 1 ]; then
            median_ms=${durations[0]}
        elif [ $((${#durations[@]} % 2)) -eq 1 ]; then
            median_ms=$(printf '%s\n' "${durations[@]}" | sort -n | sed -n "$((${#durations[@]}/2 + 1))p")
        else
            local mid1=$(printf '%s\n' "${durations[@]}" | sort -n | sed -n "$((${#durations[@]}/2))p")
            local mid2=$(printf '%s\n' "${durations[@]}" | sort -n | sed -n "$((${#durations[@]}/2 + 1))p")
            median_ms=$(awk "BEGIN {printf \"%.0f\", ($mid1+$mid2)/2}")
        fi
        
        local avg_rows=$(IFS=+; awk "BEGIN {printf \"%.0f\", (${row_counts[*]})/$success_count}")
        
        echo "  ✅ PASS - Avg: ${avg_ms}ms, Median: ${median_ms}ms, Min: ${min_ms}ms, Max: ${max_ms}ms, Rows: ${avg_rows}"
        
        # Write to JSON
        if [ "$FIRST_QUERY" = false ]; then
            echo "    ," >> "$RESULTS_FILE"
        fi
        FIRST_QUERY=false
        
        cat >> "$RESULTS_FILE" << EOF
    {
      "query": "$query_name",
      "status": "PASS",
      "runs": $success_count,
      "avg_duration_ms": $avg_ms,
      "median_duration_ms": $median_ms,
      "min_duration_ms": $min_ms,
      "max_duration_ms": $max_ms,
      "avg_rows": $avg_rows
    }
EOF
    else
        ((FAILED_QUERIES++))
        echo "  ❌ FAIL - $error_msg"
        
        if [ "$FIRST_QUERY" = false ]; then
            echo "    ," >> "$RESULTS_FILE"
        fi
        FIRST_QUERY=false
        
        cat >> "$RESULTS_FILE" << EOF
    {
      "query": "$query_name",
      "status": "FAIL",
      "error": $(echo "$error_msg" | jq -Rs .)
    }
EOF
    fi
}

# Benchmark Interactive Short queries
echo "=== Interactive Short Queries (IS1-IS7) ==="
benchmark_query "IS1" "./queries/official/interactive/short-1.cypher" '{"personId": 933}'
benchmark_query "IS2" "./queries/official/interactive/short-2.cypher" '{"personId": 933}'
benchmark_query "IS3" "./queries/official/interactive/short-3.cypher" '{"personId": 933}'
benchmark_query "IS4" "./queries/official/interactive/short-4.cypher" '{"messageId": 206158431390}'
benchmark_query "IS5" "./queries/official/interactive/short-5.cypher" '{"messageId": 206158431390}'
benchmark_query "IS6" "./queries/official/interactive/short-6.cypher" '{"messageId": 206158431390}'
benchmark_query "IS7" "./queries/official/interactive/short-7.cypher" '{"messageId": 206158431390}'

echo ""
echo "=== Interactive Complex Queries (IC1-IC13) ==="
benchmark_query "IC1" "./queries/official/interactive/complex-1.cypher" '{"personId": 933, "firstName": "John"}'
benchmark_query "IC2" "./queries/official/interactive/complex-2.cypher" '{"personId": 933, "maxDate": "2012-11-23T00:00:00.000+00:00"}'
benchmark_query "IC3" "./queries/official/interactive/complex-3.cypher" '{"personId": 933, "countryXName": "Angola", "countryYName": "Colombia", "startDate": 1275393600000, "endDate": 1277812800000}'
benchmark_query "IC4" "./queries/official/interactive/complex-4.cypher" '{"personId": 933, "startDate": "2011-07-22T00:00:00.000+00:00", "duration": 30}'
benchmark_query "IC5" "./queries/official/interactive/complex-5.cypher" '{"personId": 933, "minDate": "2011-07-22T00:00:00.000+00:00"}'
benchmark_query "IC6" "./queries/official/interactive/complex-6.cypher" '{"personId": 933, "tagName": "Che_Guevara"}'
benchmark_query "IC7" "./queries/official/interactive/complex-7.cypher" '{"personId": 933}'
benchmark_query "IC8" "./queries/official/interactive/complex-8.cypher" '{"personId": 933}'
benchmark_query "IC9" "./queries/official/interactive/complex-9.cypher" '{"personId": 933, "maxDate": "2012-11-23T00:00:00.000+00:00"}'
benchmark_query "IC11" "./queries/official/interactive/complex-11.cypher" '{"personId": 933, "countryName": "Angola", "workFromYear": 2005}'
benchmark_query "IC12" "./queries/official/interactive/complex-12.cypher" '{"personId": 933, "tagClassName": "MusicalArtist"}'
benchmark_query "IC13" "./queries/official/interactive/complex-13.cypher" '{"person1Id": 933, "person2Id": 10995116278874}'

echo ""
echo "=== Business Intelligence Queries (Selected) ==="
benchmark_query "BI1" "./queries/official/bi/bi-1.cypher" '{"datetime": "2011-12-01T00:00:00.000+00:00"}'
benchmark_query "BI2" "./queries/official/bi/bi-2.cypher" '{"date": "2011-07-22T00:00:00.000+00:00", "tagClass": "MusicalArtist", "countries": ["Angola", "Colombia"]}'
benchmark_query "BI3" "./queries/official/bi/bi-3.cypher" '{"tagClass": "MusicalArtist", "country": "Angola"}'
benchmark_query "BI5" "./queries/official/bi/bi-5.cypher" '{"tag": "Che_Guevara"}'
benchmark_query "BI6" "./queries/official/bi/bi-6.cypher" '{"tag": "Che_Guevara"}'
benchmark_query "BI7" "./queries/official/bi/bi-7.cypher" '{"tag": "Che_Guevara"}'
benchmark_query "BI9" "./queries/official/bi/bi-9.cypher" '{"tagClass1": "MusicalArtist", "tagClass2": "OfficeHolder", "threshold": 100}'
benchmark_query "BI11" "./queries/official/bi/bi-11.cypher" '{"country": "Angola"}'
benchmark_query "BI13" "./queries/official/bi/bi-13.cypher" '{"country": "Angola", "endDate": "2012-01-01T00:00:00.000+00:00"}'
benchmark_query "BI14" "./queries/official/bi/bi-14.cypher" '{"country1": "Angola", "country2": "Colombia"}'
benchmark_query "BI17" "./queries/official/bi/bi-17.cypher" '{"tag": "Che_Guevara"}'
benchmark_query "BI18" "./queries/official/bi/bi-18.cypher" '{"person": 933, "tag": "Che_Guevara", "limit": 4}'

# Close JSON
echo "" >> "$RESULTS_FILE"
echo "  ]," >> "$RESULTS_FILE"

# Calculate summary statistics
SUCCESS_RATE=$(awk "BEGIN {printf \"%.1f\", ($PASSED_QUERIES/$TOTAL_QUERIES)*100}")

cat >> "$RESULTS_FILE" << EOF
  "summary": {
    "total_queries": $TOTAL_QUERIES,
    "passed": $PASSED_QUERIES,
    "failed": $FAILED_QUERIES,
    "success_rate": $SUCCESS_RATE
  }
}
EOF

# Generate summary report
cat > "$SUMMARY_FILE" << EOF
==========================================================================
LDBC SNB Query Performance Benchmark Summary
==========================================================================
Timestamp: $TIMESTAMP
Scale Factor: $SCALE_FACTOR
Warmup Runs: $WARMUP_RUNS
Benchmark Runs: $BENCHMARK_RUNS

Results:
  Total Queries: $TOTAL_QUERIES
  Passed: $PASSED_QUERIES
  Failed: $FAILED_QUERIES
  Success Rate: ${SUCCESS_RATE}%

Detailed Results: $RESULTS_FILE
==========================================================================
EOF

cat "$SUMMARY_FILE"
echo ""
echo "Results saved to: $RESULTS_FILE"
echo "Summary saved to: $SUMMARY_FILE"

# Exit with error if any queries failed
[ $FAILED_QUERIES -eq 0 ] && exit 0 || exit 1
