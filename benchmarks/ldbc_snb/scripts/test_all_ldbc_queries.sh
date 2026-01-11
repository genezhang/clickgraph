#!/bin/bash
# Test all LDBC queries systematically and update status matrix
# Usage: ./test_all_ldbc_queries.sh

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
CLICKGRAPH_URL="http://localhost:8080"
QUERY_BASE="benchmarks/ldbc_snb/queries/official"
RESULTS_FILE="benchmarks/ldbc_snb/test_results_$(date +%Y%m%d_%H%M%S).json"

# Initialize results
echo "{" > "$RESULTS_FILE"
echo "  \"timestamp\": \"$(date -Iseconds)\"," >> "$RESULTS_FILE"
echo "  \"results\": [" >> "$RESULTS_FILE"

total=0
pass=0
fail=0
first=true

test_query() {
    local query_file="$1"
    local query_name=$(basename "$query_file" .cypher)
    local category=$(basename $(dirname "$query_file"))
    
    echo -e "${YELLOW}Testing: $category/$query_name${NC}"
    
    # Read query
    query=$(cat "$query_file")
    
    # Extract parameters from :params comment (e.g., :params { personId: 10995116277794 })
    params=$(echo "$query" | grep -oP '(?<=:params\s).*' | head -1 | tr -d ' ' || echo "{}")
    if [ "$params" = "{}" ]; then
        params_json="null"
    else
        params_json="$params"
    fi
    
    # Remove comments from query
    query_clean=$(echo "$query" | grep -v '^//' | grep -v '^/\*' | grep -v '^\*/' | grep -v '^:params')
    
    # Test query with schema_name
    response=$(curl -s -X POST "$CLICKGRAPH_URL/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\": $(echo "$query_clean" | jq -Rs .), \"parameters\": $params_json, \"schema_name\": \"ldbc_snb\"}" \
        2>&1) || true
    
    # Check if successful
    if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
        status="FAIL"
        error=$(echo "$response" | jq -r '.error')
        echo -e "${RED}  ✗ FAIL: $error${NC}"
        ((fail++))
    elif echo "$response" | jq -e '.results' > /dev/null 2>&1; then
        status="PASS"
        rows=$(echo "$response" | jq '.results | length')
        echo -e "${GREEN}  ✓ PASS ($rows rows)${NC}"
        ((pass++))
    else
        status="ERROR"
        error="Invalid response"
        echo -e "${RED}  ✗ ERROR: $response${NC}"
        ((fail++))
    fi
    
    ((total++))
    
    # Add to results JSON
    if [ "$first" = false ]; then
        echo "," >> "$RESULTS_FILE"
    fi
    first=false
    
    cat >> "$RESULTS_FILE" << EOF
    {
      "category": "$category",
      "query": "$query_name",
      "status": "$status",
      "error": $(echo "${error:-null}" | jq -Rs .)
    }
EOF
}

# Test Interactive Short queries
echo ""
echo "========================================="
echo "Testing Interactive Short (IS) Queries"
echo "========================================="
for query in "$QUERY_BASE/interactive/short-"*.cypher; do
    [ -f "$query" ] && test_query "$query"
done

# Test Interactive Complex queries
echo ""
echo "========================================="
echo "Testing Interactive Complex (IC) Queries"
echo "========================================="
for query in "$QUERY_BASE/interactive/complex-"*.cypher; do
    [ -f "$query" ] && test_query "$query"
done

# Test BI queries
echo ""
echo "========================================="
echo "Testing Business Intelligence (BI) Queries"
echo "========================================="
for query in "$QUERY_BASE/bi/"*.cypher; do
    [ -f "$query" ] && test_query "$query"
done

# Finalize results JSON
echo "" >> "$RESULTS_FILE"
echo "  ]," >> "$RESULTS_FILE"
echo "  \"summary\": {" >> "$RESULTS_FILE"
echo "    \"total\": $total," >> "$RESULTS_FILE"
echo "    \"pass\": $pass," >> "$RESULTS_FILE"
echo "    \"fail\": $fail," >> "$RESULTS_FILE"
echo "    \"pass_rate\": \"$(awk "BEGIN {printf \"%.1f\", ($pass/$total)*100}")%\"" >> "$RESULTS_FILE"
echo "  }" >> "$RESULTS_FILE"
echo "}" >> "$RESULTS_FILE"

# Summary
echo ""
echo "========================================="
echo "Summary"
echo "========================================="
echo -e "Total:     $total"
echo -e "${GREEN}Pass:      $pass${NC}"
echo -e "${RED}Fail:      $fail${NC}"
echo -e "Pass Rate: $(awk "BEGIN {printf \"%.1f\", ($pass/$total)*100}")%"
echo ""
echo "Results saved to: $RESULTS_FILE"
echo ""
echo "View summary:"
echo "  jq '.summary' $RESULTS_FILE"
echo ""
echo "View failures:"
echo "  jq '.results[] | select(.status==\"FAIL\")' $RESULTS_FILE"
