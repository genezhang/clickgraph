#!/bin/bash
# Test the 5 LDBC IC queries that use WITH + aggregation
# These were previously failing due to column name resolution issues

set -e

BASE_URL="http://localhost:8080/query"
QUERY_DIR="/home/gz/clickgraph/benchmarks/ldbc_snb/queries/official/interactive"

echo "Testing IC queries with WITH + aggregation patterns"
echo "===================================================="
echo ""

test_query() {
    local query_name=$1
    local query_file=$2
    
    echo "Testing $query_name..."
    
    # Read query file and remove comments
    query=$(cat "$QUERY_DIR/$query_file" | sed 's|//.*||' | tr '\n' ' ')
    
    # Execute query
    response=$(curl -s -X POST "$BASE_URL" \
        -H "Content-Type: application/json" \
        -d "{\"query\":\"$query\"}")
    
    # Check for errors
    if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
        error=$(echo "$response" | jq -r '.error')
        echo "  ❌ FAILED: $error"
        return 1
    elif echo "$response" | jq -e '.results' > /dev/null 2>&1; then
        count=$(echo "$response" | jq '.results | length')
        echo "  ✅ PASSED: Returned $count rows"
        return 0
    else
        echo "  ⚠️  UNKNOWN: $response"
        return 1
    fi
}

# Test the 5 IC queries that were failing
passed=0
failed=0

test_query "IC1" "complex-1.cypher" && ((passed++)) || ((failed++))
test_query "IC3" "complex-3.cypher" && ((passed++)) || ((failed++))
test_query "IC4" "complex-4.cypher" && ((passed++)) || ((failed++))
test_query "IC7" "complex-7.cypher" && ((passed++)) || ((failed++))
test_query "IC8" "complex-8.cypher" && ((passed++)) || ((failed++))

echo ""
echo "===================================================="
echo "Results: $passed passed, $failed failed (total: 5)"
echo "===================================================="

if [ $failed -eq 0 ]; then
    echo "🎉 All WITH + aggregation queries now passing!"
    exit 0
else
    echo "⚠️  Some queries still failing"
    exit 1
fi
