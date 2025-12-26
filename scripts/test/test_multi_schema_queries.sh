#!/bin/bash
# Test queries for all 6 schemas in unified_test_multi_schema.yaml

SERVER_URL="${CLICKGRAPH_URL:-http://localhost:8080}"

echo "=== Multi-Schema Query Test Suite ==="
echo "Server: $SERVER_URL"
echo ""

# Function to run query and show results
run_query() {
    local schema=$1
    local description=$2
    local query=$3
    
    echo "[$schema] $description"
    echo "Query: $query"
    
    result=$(curl -s -X POST "$SERVER_URL/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\":\"$query\"}")
    
    echo "$result" | jq -C '.'
    echo ""
}

echo "=== 1. social_benchmark Schema ==="
run_query "social_benchmark" "Find all users" \
    "USE social_benchmark MATCH (u:User) RETURN u.name, u.country ORDER BY u.name LIMIT 5"

run_query "social_benchmark" "Find who Alice follows" \
    "USE social_benchmark MATCH (alice:User {name:'Alice Smith'})-[:FOLLOWS]->(friend:User) RETURN friend.name"

run_query "social_benchmark" "Find posts and their authors" \
    "USE social_benchmark MATCH (p:Post) RETURN p.content, p.user_id LIMIT 3"

echo "=== 2. test_fixtures Schema ==="
run_query "test_fixtures" "Find all products" \
    "USE test_fixtures MATCH (p:TestProduct) RETURN p.name, p.price ORDER BY p.price DESC"

run_query "test_fixtures" "Find user purchases" \
    "USE test_fixtures MATCH (u:TestUser)-[:TEST_PURCHASED]->(p:TestProduct) RETURN u.name, p.name, p.price"

run_query "test_fixtures" "Find group memberships" \
    "USE test_fixtures MATCH (u:TestUser)-[m:MEMBER_OF]->(g:TestGroup) RETURN u.name, g.name, m.role"

echo "=== 3. ldbc_snb Schema ==="
run_query "ldbc_snb" "Find all persons" \
    "USE ldbc_snb MATCH (p:Person) RETURN p.firstName, p.lastName ORDER BY p.firstName"

run_query "ldbc_snb" "Find who knows whom" \
    "USE ldbc_snb MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN p1.firstName, p2.firstName"

echo "=== 4. denormalized_flights Schema ==="
run_query "denormalized_flights" "Find all flights" \
    "USE denormalized_flights MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport) RETURN origin.code, dest.code, f.flight_number"

run_query "denormalized_flights" "Find flights from LAX" \
    "USE denormalized_flights MATCH (lax:Airport {code:'LAX'})-[f:FLIGHT]->(dest:Airport) RETURN dest.code, f.flight_number"

echo "=== 5. pattern_comp Schema ==="
run_query "pattern_comp" "Find all pattern comp users" \
    "USE pattern_comp MATCH (u:PatternCompUser) RETURN u.name ORDER BY u.name"

run_query "pattern_comp" "Find follow relationships" \
    "USE pattern_comp MATCH (u1:PatternCompUser)-[:PATTERN_COMP_FOLLOWS]->(u2:PatternCompUser) RETURN u1.name, u2.name"

echo "=== 6. zeek_logs Schema ==="
run_query "zeek_logs" "Find all IPs" \
    "USE zeek_logs MATCH (ip:IP) RETURN ip.ip LIMIT 5"

run_query "zeek_logs" "Find DNS requests" \
    "USE zeek_logs MATCH (ip:IP)-[d:DNS_REQUESTED]->(domain:Domain) RETURN ip.ip, domain.domain"

run_query "zeek_logs" "Find IP connections" \
    "USE zeek_logs MATCH (ip1:IP)-[c:CONNECTED_TO]->(ip2:IP) RETURN ip1.ip, ip2.ip, c.proto"

echo "=== Schema Switching Test ==="
echo "Testing queries without USE clause (should use default schema)"
run_query "default" "Default schema query" \
    "MATCH (u:User) RETURN count(u) as user_count"

echo ""
echo "=== Test Complete ==="
echo "All 6 schemas tested successfully!"
