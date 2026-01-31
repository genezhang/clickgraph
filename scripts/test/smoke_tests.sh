#!/bin/bash
# Critical Smoke Tests - Run before any commit to catch regressions
# These test the most commonly broken features after refactoring
# Note: We don't use 'set -e' to run all tests and report comprehensive results

CLICKGRAPH_URL="${CLICKGRAPH_URL:-http://localhost:8080}"
SCHEMA="${SCHEMA:-social_benchmark}"

echo "🔥 Running Critical Smoke Tests..."
echo "Server: $CLICKGRAPH_URL"
echo "Schema: $SCHEMA"
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PASSED=0
FAILED=0

run_test() {
    local test_name="$1"
    local query="$2"
    local description="$3"
    
    echo -n "Testing: $description ... "
    
    response=$(curl -s -X POST "$CLICKGRAPH_URL/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\":\"$query\",\"schema_name\":\"$SCHEMA\"}")
    
    if echo "$response" | jq -e '.error == null' > /dev/null 2>&1; then
        echo -e "${GREEN}✅ PASS${NC}"
        PASSED=$((PASSED + 1))
        return 0
    else
        echo -e "${RED}❌ FAIL${NC}"
        echo -e "${YELLOW}Query: $query${NC}"
        echo "$response" | jq -r '.error // .generated_sql' | head -5
        echo ""
        FAILED=$((FAILED + 1))
        return 1
    fi
}

# Test 1: VLP Chained Join (Exact Hops) - Bug #1
run_test "vlp_exact_2" \
    "MATCH (a:User)-[*2]->(b:User) RETURN a.user_id, b.user_id LIMIT 1" \
    "VLP chained join (*2 pattern)"

# Test 2: VLP Range Pattern
run_test "vlp_range" \
    "MATCH (a:User)-[r:FOLLOWS*1..3]->(b:User) RETURN a.user_id, b.user_id LIMIT 1" \
    "VLP range pattern (*1..3)"

# Test 3: Multi-Type Relationships - Bug #2
run_test "multi_type" \
    "MATCH (a:User)-[r:FOLLOWS|AUTHORED]->(b) RETURN type(r), count(*) AS cnt" \
    "Multi-type relationships (|)"

# Test 4: VLP with Path Variable
run_test "vlp_path" \
    "MATCH p = (a:User)-[r:FOLLOWS*1..2]->(b:User) RETURN length(p) LIMIT 1" \
    "VLP with path variable"

# Test 5: Single-hop Relationship (baseline)
run_test "single_hop" \
    "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN a.user_id, b.user_id LIMIT 1" \
    "Single-hop relationship"

# Test 6: Node scan with property (baseline)
run_test "node_scan" \
    "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name" \
    "Node scan with property"

# Test 7: Aggregation
run_test "aggregation" \
    "MATCH (u:User) RETURN count(*) AS user_count" \
    "Basic aggregation"

# Test 8: Multi-hop pattern
run_test "multi_hop" \
    "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) RETURN a.user_id LIMIT 1" \
    "Multi-hop pattern"

# Test 9: OPTIONAL MATCH
run_test "optional_match" \
    "MATCH (u:User) OPTIONAL MATCH (u)-[r:FOLLOWS]->(f) RETURN u.user_id, count(r) AS cnt" \
    "OPTIONAL MATCH with aggregation"

# Test 10: WITH clause variable renaming
run_test "with_rename" \
    "MATCH (u:User) WITH u AS person RETURN person.name LIMIT 1" \
    "WITH clause variable renaming"

echo ""
echo "================================"
echo "Smoke Test Results"
echo "================================"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -gt 0 ]; then
    echo -e "${RED}❌ SMOKE TESTS FAILED${NC}"
    echo "DO NOT COMMIT until all smoke tests pass!"
    echo "See REGRESSION_BUGS.md for common issues"
    exit 1
else
    echo -e "${GREEN}✅ ALL SMOKE TESTS PASSED${NC}"
    echo "Safe to proceed with commit"
    exit 0
fi
