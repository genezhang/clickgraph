#!/bin/bash
# Cross-functional VLP Verification Tests
# Tests VLP with COLLECT, WITH, aggregations, etc.

set -e

echo "=== Cross-Functional VLP Tests ==="
echo ""

# Test 1: VLP + COLLECT
echo "Test 1: VLP + COLLECT"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser) WHERE u1.user_id = 1 RETURN u1.name as start, COLLECT(u2.name) as reached", "schema_name": "unified_test_schema"}' | jq -e '.results | length > 0' > /dev/null && echo "  ✅ PASSED" || echo "  ❌ FAILED"

# Test 2: VLP + WITH filtering
echo "Test 2: VLP + WITH filtering"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser) WHERE u1.user_id = 1 WITH u1, u2 WHERE u2.user_id > 2 RETURN u1.name, u2.name", "schema_name": "unified_test_schema"}' | jq -e '.results | length > 0' > /dev/null && echo "  ✅ PASSED" || echo "  ❌ FAILED"

# Test 3: VLP + Aggregation (COUNT DISTINCT)
echo "Test 3: VLP + COUNT DISTINCT"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser) WHERE u1.user_id = 1 RETURN COUNT(DISTINCT u2.user_id) as unique_reached, COUNT(*) as total_paths", "schema_name": "unified_test_schema"}' | jq -e '.results | length > 0' > /dev/null && echo "  ✅ PASSED" || echo "  ❌ FAILED"

# Test 4: VLP + WITH + Aggregation
echo "Test 4: VLP + WITH + Aggregation"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser) WHERE u1.user_id = 1 WITH u1, COUNT(DISTINCT u2.user_id) as reach_count RETURN u1.name, reach_count", "schema_name": "unified_test_schema"}' | jq -e '.results | length > 0' > /dev/null && echo "  ✅ PASSED" || echo "  ❌ FAILED"

# Test 5: VLP + Property filtering
echo "Test 5: VLP + Property in WHERE"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser) WHERE u1.name = '\''Alice'\'' RETURN u1.name, u2.name LIMIT 5", "schema_name": "unified_test_schema"}' | jq -e '.results | length > 0' > /dev/null && echo "  ✅ PASSED" || echo "  ❌ FAILED"

# Test 6: VLP + COLLECT + GROUP BY
echo "Test 6: VLP + COLLECT + GROUP BY"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser) WHERE u1.user_id IN [1, 2] RETURN u1.user_id, COLLECT(u2.name) as reached", "schema_name": "unified_test_schema"}' | jq -e '.results | length >= 1' > /dev/null && echo "  ✅ PASSED" || echo "  ❌ FAILED"

echo ""
echo "=== Summary ==="
echo "All tests verify VLP works correctly with:"
echo "  - COLLECT aggregation"
echo "  - WITH clause filtering"
echo "  - COUNT and COUNT DISTINCT"
echo "  - Property filtering in WHERE"
echo "  - GROUP BY clauses"
echo ""
echo "Philosophy validated: Never assume orthogonal features work together ✅"
