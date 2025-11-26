#!/bin/bash
# Query Pattern Validation Test Runner
# Systematically tests all query patterns from QUERY_PATTERN_CHECKLIST.md
# 
# Usage:
#   ./scripts/test/run_pattern_validation.sh [category]
#
# Examples:
#   ./scripts/test/run_pattern_validation.sh              # Run all tests
#   ./scripts/test/run_pattern_validation.sh basic        # Just basic patterns
#   ./scripts/test/run_pattern_validation.sh aggregations # Just aggregations

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

CATEGORY="${1:-all}"
SERVER_PORT=8080
SERVER_URL="http://localhost:${SERVER_PORT}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Query Pattern Validation Test Runner${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if server is running
if ! curl -s -f "${SERVER_URL}/health" > /dev/null 2>&1; then
    echo -e "${RED}Error: ClickGraph server not running on port ${SERVER_PORT}${NC}"
    echo "Start server with:"
    echo "  cargo run --release --bin clickgraph -- --http-port ${SERVER_PORT}"
    exit 1
fi

echo -e "${GREEN}✓ Server is running${NC}"
echo ""

# Test counters
TOTAL=0
PASSED=0
FAILED=0
SKIPPED=0

# Test function
test_query() {
    local name="$1"
    local query="$2"
    local schema="$3"
    local expected_status="${4:-success}"
    
    TOTAL=$((TOTAL + 1))
    
    echo -n "Testing: ${name} (${schema})... "
    
    response=$(curl -s -X POST "${SERVER_URL}/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\":\"${query}\"}" 2>&1)
    
    if echo "$response" | grep -q '"results"'; then
        if [ "$expected_status" = "success" ]; then
            echo -e "${GREEN}✓ PASS${NC}"
            PASSED=$((PASSED + 1))
        else
            echo -e "${YELLOW}⚠ UNEXPECTED SUCCESS (expected error)${NC}"
            FAILED=$((FAILED + 1))
        fi
    elif echo "$response" | grep -qi "error"; then
        if [ "$expected_status" = "error" ]; then
            echo -e "${GREEN}✓ PASS (expected error)${NC}"
            PASSED=$((PASSED + 1))
        else
            echo -e "${RED}✗ FAIL${NC}"
            echo "  Error: $(echo "$response" | head -1)"
            FAILED=$((FAILED + 1))
        fi
    else
        echo -e "${RED}✗ FAIL (no response)${NC}"
        FAILED=$((FAILED + 1))
    fi
}

# Test function (SQL generation only)
test_sql_gen() {
    local name="$1"
    local query="$2"
    local schema="$3"
    
    TOTAL=$((TOTAL + 1))
    
    echo -n "Testing SQL: ${name} (${schema})... "
    
    response=$(curl -s -X POST "${SERVER_URL}/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\":\"${query}\", \"sql_only\": true}" 2>&1)
    
    if echo "$response" | grep -q '"generated_sql"'; then
        echo -e "${GREEN}✓ PASS${NC}"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}✗ FAIL${NC}"
        echo "  Response: $(echo "$response" | head -1)"
        FAILED=$((FAILED + 1))
    fi
}

# ============================================
# CATEGORY 1: Basic Node Patterns
# ============================================
if [ "$CATEGORY" = "all" ] || [ "$CATEGORY" = "basic" ]; then
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}1. Basic Node Patterns${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    
    test_query "match_all_nodes" \
        "MATCH (n:User) RETURN n.user_id LIMIT 5" \
        "standard"
    
    test_query "node_with_property_filter" \
        "MATCH (n:User) WHERE n.user_id = 1 RETURN n.name" \
        "standard"
    
    test_query "mixed_property_expression" \
        "MATCH (u1:User), (u2:User) WHERE u1.user_id + u2.user_id < 10 RETURN u1.name, u2.name" \
        "standard"
    
    echo ""
fi

# ============================================
# CATEGORY 2: Relationship Patterns
# ============================================
if [ "$CATEGORY" = "all" ] || [ "$CATEGORY" = "relationships" ]; then
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}2. Relationship Patterns${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    
    test_query "simple_directed_relationship" \
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name LIMIT 5" \
        "standard"
    
    test_query "undirected_relationship" \
        "MATCH (u1:User)-[:FOLLOWS]-(u2:User) WHERE u1.user_id = 1 RETURN u2.name LIMIT 5" \
        "standard"
    
    test_query "multi_type_relationship" \
        "MATCH (u1:User)-[:FOLLOWS|FRIENDS_WITH]->(u2:User) RETURN u2.name LIMIT 5" \
        "standard" \
        "error"  # May not be implemented
    
    test_query "two_hop_traversal" \
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u3:User) WHERE u1.user_id = 1 RETURN u3.name LIMIT 5" \
        "standard"
    
    echo ""
fi

# ============================================
# CATEGORY 3: Mixed Property Expressions
# ============================================
if [ "$CATEGORY" = "all" ] || [ "$CATEGORY" = "expressions" ]; then
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}3. Mixed Property Expressions${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    
    test_query "where_arithmetic_expression" \
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id + u2.user_id < 10 RETURN u1.name, u2.name LIMIT 5" \
        "standard"
    
    test_query "return_arithmetic_expression" \
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) RETURN u1.user_id + u2.user_id AS sum_ids LIMIT 5" \
        "standard"
    
    test_query "order_by_expression" \
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) RETURN u1.name, u2.name ORDER BY u1.user_id + u2.user_id LIMIT 5" \
        "standard"
    
    test_query "string_concat_expression" \
        "MATCH (u1:User)-[:FOLLOWS]->(u2:User) RETURN concat(u1.name, ' follows ', u2.name) AS description LIMIT 5" \
        "standard"
    
    echo ""
fi

# ============================================
# CATEGORY 4: Variable-Length Paths
# ============================================
if [ "$CATEGORY" = "all" ] || [ "$CATEGORY" = "varpaths" ]; then
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}4. Variable-Length Paths${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    
    test_query "varpath_exact_2_hops" \
        "MATCH (u1:User)-[:FOLLOWS*2]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name LIMIT 10" \
        "standard"
    
    test_query "varpath_range_1_to_3" \
        "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name LIMIT 10" \
        "standard"
    
    test_query "varpath_max_depth_5" \
        "MATCH (u1:User)-[:FOLLOWS*..5]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name LIMIT 10" \
        "standard"
    
    echo ""
fi

# ============================================
# CATEGORY 5: Aggregations
# ============================================
if [ "$CATEGORY" = "all" ] || [ "$CATEGORY" = "aggregations" ]; then
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}5. Aggregations${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    
    test_query "count_all" \
        "MATCH (n:User) RETURN COUNT(*) AS total" \
        "standard"
    
    test_query "count_with_where" \
        "MATCH (n:User) WHERE n.user_id < 100 RETURN COUNT(*) AS count" \
        "standard"
    
    test_query "group_by_with_count" \
        "MATCH (u:User)<-[:FOLLOWS]-(follower) RETURN u.name, COUNT(follower) AS count ORDER BY count DESC LIMIT 10" \
        "standard"
    
    test_query "min_max_aggregation" \
        "MATCH (n:User) RETURN MIN(n.user_id) AS min_id, MAX(n.user_id) AS max_id" \
        "standard"
    
    echo ""
fi

# ============================================
# CATEGORY 6: Optional Match
# ============================================
if [ "$CATEGORY" = "all" ] || [ "$CATEGORY" = "optional" ]; then
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}6. OPTIONAL MATCH${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    
    test_query "optional_match_basic" \
        "MATCH (u:User) WHERE u.user_id = 1 OPTIONAL MATCH (u)-[:FOLLOWS]->(friend) RETURN u.name, friend.name LIMIT 5" \
        "standard"
    
    test_query "optional_match_no_results" \
        "MATCH (u:User) WHERE u.user_id = 99999 OPTIONAL MATCH (u)-[:FOLLOWS]->(friend) RETURN u.name, friend.name" \
        "standard"
    
    echo ""
fi

# ============================================
# Summary
# ============================================
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Test Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Total Tests:  $TOTAL"
echo -e "${GREEN}Passed:       $PASSED${NC}"
echo -e "${RED}Failed:       $FAILED${NC}"
echo -e "${YELLOW}Skipped:      $SKIPPED${NC}"
echo ""

PASS_RATE=$((PASSED * 100 / TOTAL))
echo "Pass Rate: ${PASS_RATE}%"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests failed${NC}"
    exit 1
fi
