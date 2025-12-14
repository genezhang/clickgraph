#!/bin/bash
# Quick test of LDBC queries - start small with known-working patterns

set -e

CLICKGRAPH_URL="${CLICKGRAPH_URL:-http://localhost:8080}"

echo "Waiting for ClickGraph server..."
for i in {1..30}; do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ Server is ready!"
        break
    fi
    echo -n "."
    sleep 1
done
echo ""

test_query() {
    local name="$1"
    local query="$2"
    echo "Testing: $name"
    curl -s -X POST "$CLICKGRAPH_URL/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\": $(echo "$query" | jq -Rs .), \"schema_name\": \"ldbc_snb\"}" | \
        jq -r 'if .success then "✅ PASS - Rows: \(.data | length)" else "❌ FAIL - \(.error)" end'
    echo ""
}

echo "=== LDBC Quick Tests ==="
echo ""

# Basic counts
test_query "Count Persons" "MATCH (p:Person) RETURN count(p) AS cnt"
test_query "Count Posts" "MATCH (p:Post) RETURN count(p) AS cnt"  
test_query "Count Comments" "MATCH (c:Comment) RETURN count(c) AS cnt"

# Simple patterns
test_query "Sample Persons" "MATCH (p:Person) RETURN p.firstName, p.lastName LIMIT 5"
test_query "KNOWS relationships" "MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN p1.firstName, p2.firstName LIMIT 5"
test_query "Post creators" "MATCH (p:Person)<-[:HAS_CREATOR]-(post:Post) RETURN p.firstName, count(post) AS posts ORDER BY posts DESC LIMIT 5"

# More complex
test_query "Friends of friends" "MATCH (p:Person {id: 933})-[:KNOWS*1..2]->(friend) RETURN DISTINCT friend.firstName, friend.lastName LIMIT 10"

echo "=== Tests Complete ==="
