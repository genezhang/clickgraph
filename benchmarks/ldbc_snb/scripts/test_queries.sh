#!/bin/bash
# LDBC SNB Interactive v1 - Test Queries for ClickGraph
# Tests basic graph queries against the loaded LDBC data

CLICKGRAPH_URL="${CLICKGRAPH_URL:-http://localhost:8080}"

query() {
    local name=$1
    local cypher=$2
    echo "=== $name ==="
    echo "Cypher: $cypher"
    echo "Result:"
    curl -s -X POST "$CLICKGRAPH_URL/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\": \"$cypher\"}" | jq '.results'
    echo ""
}

echo "LDBC SNB Interactive - ClickGraph Test Queries"
echo "=============================================="
echo ""

# Basic node queries
query "Count all persons" \
    "MATCH (p:Person) RETURN count(p) as personCount"

query "First 5 persons" \
    "MATCH (p:Person) RETURN p.firstName, p.lastName LIMIT 5"

query "Count posts" \
    "MATCH (p:Post) RETURN count(p) as postCount"

# Relationship queries  
query "Friendships (KNOWS)" \
    "MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN p1.firstName, p2.firstName LIMIT 5"

query "Most connected people" \
    "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.firstName, count(f) as friends ORDER BY friends DESC LIMIT 5"

# Multi-hop queries
query "Friends of friends" \
    "MATCH (p:Person)-[:KNOWS]->(friend)-[:KNOWS]->(fof) WHERE p.firstName = 'Hossein' RETURN DISTINCT fof.firstName, fof.lastName LIMIT 5"

# Content queries
query "Posts by creator" \
    "MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post) RETURN person.firstName, count(post) as posts ORDER BY posts DESC LIMIT 5"

query "Forums with most posts" \
    "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) RETURN f.title, count(p) as posts ORDER BY posts DESC LIMIT 5"

echo "=============================================="
echo "All queries completed"
