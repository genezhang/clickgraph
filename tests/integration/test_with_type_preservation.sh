#!/bin/bash
# Test: WITH clause type preservation fix (December 2025)
# 
# **Bug**: After WITH DISTINCT tag, the analyzer lost node type information
# **Symptom**: "Property 'name' not found on node 'tag'" even though Tag.name exists in schema
# **Root Cause**: WITH clause exported aliases without preserving their node/relationship labels
# **Fix**: Added cte_entity_types tracking to PlanCtx, registered in CteSchemaResolver
#
# This script tests that node/relationship types are preserved across WITH boundaries

set -e

BASE_URL="http://localhost:8080"

echo "🧪 Test 1: Simple WITH preserves node type"
RESULT=$(curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (p1:Person)-[:KNOWS]->(p2:Person) WITH DISTINCT p2 RETURN p2.firstName LIMIT 3"}')

if echo "$RESULT" | grep -q '"p2.firstName"'; then
    echo "✅ Test 1 PASSED: p2.firstName accessible after WITH"
else
    echo "❌ Test 1 FAILED: $RESULT"
    exit 1
fi

echo ""
echo "🧪 Test 2: Multiple WITH clauses preserve types"
RESULT=$(curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (p1:Person)-[:KNOWS]->(p2:Person) WITH p2 WITH p2 RETURN p2.lastName LIMIT 3"}')

if echo "$RESULT" | grep -q '"p2.lastName"'; then
    echo "✅ Test 2 PASSED: p2.lastName accessible after double WITH"
else
    echo "❌ Test 2 FAILED: $RESULT"
    exit 1
fi

echo ""
echo "🧪 Test 3: WITH DISTINCT preserves types for multiple aliases"
RESULT=$(curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (p1:Person)-[:KNOWS]->(p2:Person) WITH DISTINCT p1, p2 RETURN p1.firstName, p2.firstName LIMIT 2"}')

if echo "$RESULT" | grep -q '"p1.firstName"' && echo "$RESULT" | grep -q '"p2.firstName"'; then
    echo "✅ Test 3 PASSED: Both p1 and p2 types preserved"
else
    echo "❌ Test 3 FAILED: $RESULT"
    exit 1
fi

echo ""
echo "================================================"
echo "✅ All WITH type preservation tests passed!"
echo "================================================"
echo ""
echo "Implementation Details:"
echo "- Added cte_entity_types: HashMap<String, HashMap<String, (bool, Option<Vec<String>>)>> to PlanCtx"
echo "- CteSchemaResolver.register_with_clause_schema() now calls register_cte_entity_types()"
echo "- TableCtx::new_with_cte_reference() looks up entity types from registry"
echo "- Result: Node/relationship labels preserved across WITH boundaries"
