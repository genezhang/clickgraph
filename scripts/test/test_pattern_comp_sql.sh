#!/bin/bash
# Test pattern comprehension SQL generation via HTTP API

URL="http://localhost:8080/query"

echo "=== Pattern Comprehension SQL Generation Test ==="
echo

# Test 1: Simple pattern comprehension
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test 1: Simple pattern comprehension"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
curl -s -X POST "$URL" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, [(u)-[:FOLLOWS]->(f) | f.name] AS friends",
    "sql_only": true
  }' | jq -r '.generated_sql' | head -50
echo
echo

# Test 2: Pattern comprehension with WHERE
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test 2: Pattern comprehension with WHERE clause"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
curl -s -X POST "$URL" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, [(u)-[:FOLLOWS]->(f) WHERE f.country = '\''USA'\'' | f.name] AS us_friends",
    "sql_only": true
  }' | jq -r '.generated_sql' | head -50
echo
echo

# Test 3: Multiple pattern comprehensions
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test 3: Multiple pattern comprehensions"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
curl -s -X POST "$URL" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, [(u)-[:FOLLOWS]->(f) | f.name] AS friends, [(u)<-[:FOLLOWS]-(follower) | follower.name] AS followers",
    "sql_only": true
  }' | jq -r '.generated_sql' | head -60
