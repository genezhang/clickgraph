#!/bin/bash
#
# Property Pruning Validation Script
#
# Tests that PropertyRequirementsAnalyzer correctly identifies which properties
# are needed and that the renderer uses this information to prune unnecessary columns.
#
# Expected behavior:
# - PropertyRequirementsAnalyzer logs: "Found requirements for N aliases"
# - Expansion logs: "Only X properties required" when pruning enabled
# - SQL should only include needed columns (not all 50-200 properties)
#
# Usage: ./scripts/debug/test_property_pruning.sh

set -e

# Configuration
CLICKGRAPH_URL="${CLICKGRAPH_URL:-http://localhost:8080}"
GRAPH_CONFIG_PATH="${GRAPH_CONFIG_PATH:-./benchmarks/social_network/schemas/social_benchmark.yaml}"

echo "🧪 Property Pruning Validation Tests"
echo "====================================="
echo ""
echo "Configuration:"
echo "  ClickGraph URL: $CLICKGRAPH_URL"
echo "  Schema: $GRAPH_CONFIG_PATH"
echo ""

# Ensure server is running
if ! curl -s "$CLICKGRAPH_URL/health" > /dev/null 2>&1; then
    echo "❌ ClickGraph server not running at $CLICKGRAPH_URL"
    echo "   Start with: cargo run --bin clickgraph"
    exit 1
fi

echo "✅ ClickGraph server is running"
echo ""

# Test 1: Basic property selection (should only include user_id, name)
echo "📋 Test 1: Basic Property Selection"
echo "Query: MATCH (u:User) WHERE u.user_id = 1 RETURN u.name"
echo ""

RESPONSE=$(curl -s -X POST "$CLICKGRAPH_URL/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name",
    "database": "brahmand"
  }')

# Check if query succeeded
if echo "$RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
    echo "❌ Query failed: $(echo "$RESPONSE" | jq -r '.error')"
else
    echo "✅ Query succeeded"
    RESULT_COUNT=$(echo "$RESPONSE" | jq '.results | length')
    echo "   Results: $RESULT_COUNT rows"
fi

# Check generated SQL (requires sql_only mode)
SQL_RESPONSE=$(curl -s -X POST "$CLICKGRAPH_URL/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name",
    "database": "brahmand",
    "sql_only": true
  }')

SQL=$(echo "$SQL_RESPONSE" | jq -r '.sql // .query // "N/A"')
echo ""
echo "Generated SQL:"
echo "$SQL"
echo ""

# Check if SQL contains only needed columns
if echo "$SQL" | grep -q "full_name"; then
    echo "✅ SQL includes full_name (mapped from u.name)"
else
    echo "⚠️  SQL missing full_name column"
fi

if echo "$SQL" | grep -q "SELECT.*\*" || echo "$SQL" | grep -qE "email_address|registration_date|is_active|country|city"; then
    echo "⚠️  SQL may include unnecessary columns (property pruning not working?)"
else
    echo "✅ SQL appears to include only necessary columns"
fi

echo ""
echo "---"
echo ""

# Test 2: collect() aggregation (should only materialize needed properties)
echo "📋 Test 2: collect() with Property Selection"
echo "Query: MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.user_id = 1 RETURN collect(f)[0].name"
echo ""

SQL_RESPONSE=$(curl -s -X POST "$CLICKGRAPH_URL/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.user_id = 1 RETURN collect(f)[0].name AS first_friend",
    "database": "brahmand",
    "sql_only": true
  }')

SQL=$(echo "$SQL_RESPONSE" | jq -r '.sql // .query // "N/A"')
echo "Generated SQL:"
echo "$SQL"
echo ""

# Check if collect() includes only needed columns
if echo "$SQL" | grep -qE "groupArray.*full_name"; then
    echo "✅ collect() appears to include name/full_name"
else
    echo "⚠️  collect() may not include needed property"
fi

# Count how many properties are in the groupArray
PROP_COUNT=$(echo "$SQL" | grep -oE "anyLast\([^)]+\)" | wc -l)
echo "   Properties in aggregation: $PROP_COUNT"
if [ "$PROP_COUNT" -le 3 ]; then
    echo "✅ Property pruning working! (3 or fewer properties)"
else
    echo "⚠️  Many properties in aggregation ($PROP_COUNT) - pruning may not be working"
fi

echo ""
echo "---"
echo ""

# Test 3: WITH clause propagation
echo "📋 Test 3: WITH Clause Property Propagation"
echo "Query: MATCH (u:User)-[:FOLLOWS]->(f:User) WITH f WHERE f.country = 'USA' RETURN f.name, f.email"
echo ""

SQL_RESPONSE=$(curl -s -X POST "$CLICKGRAPH_URL/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) WITH f WHERE f.country = '\''USA'\'' RETURN f.name, f.email",
    "database": "brahmand",
    "sql_only": true
  }')

SQL=$(echo "$SQL_RESPONSE" | jq -r '.sql // .query // "N/A"')
echo "Generated SQL:"
echo "$SQL"
echo ""

# Requirements should include: name, email (RETURN), country (WHERE), user_id (ID)
if echo "$SQL" | grep -q "full_name" && echo "$SQL" | grep -q "email_address" && echo "$SQL" | grep -q "country"; then
    echo "✅ SQL includes all required properties (name, email, country)"
else
    echo "⚠️  SQL may be missing required properties"
fi

echo ""
echo "---"
echo ""

# Test 4: Wildcard (should expand ALL properties)
echo "📋 Test 4: Wildcard Return (No Pruning)"
echo "Query: MATCH (u:User) WHERE u.user_id = 1 RETURN u"
echo ""

SQL_RESPONSE=$(curl -s -X POST "$CLICKGRAPH_URL/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u",
    "database": "brahmand",
    "sql_only": true
  }')

SQL=$(echo "$SQL_RESPONSE" | jq -r '.sql // .query // "N/A"')
echo "Generated SQL:"
echo "$SQL"
echo ""

# Should include ALL user properties
EXPECTED_PROPS=("user_id" "full_name" "email_address" "registration_date" "is_active" "country" "city")
FOUND=0
for prop in "${EXPECTED_PROPS[@]}"; do
    if echo "$SQL" | grep -q "$prop"; then
        ((FOUND++))
    fi
done

echo "   Found $FOUND/${#EXPECTED_PROPS[@]} expected properties"
if [ "$FOUND" -ge 5 ]; then
    echo "✅ Wildcard expansion working (includes many properties)"
else
    echo "⚠️  Wildcard may not be expanding all properties"
fi

echo ""
echo "========================================="
echo "✅ Property Pruning Validation Complete"
echo ""
echo "Summary:"
echo "  - Test 1: Basic property selection"
echo "  - Test 2: collect() aggregation pruning"
echo "  - Test 3: WITH clause propagation"
echo "  - Test 4: Wildcard expansion (no pruning)"
echo ""
echo "Check server logs for PropertyRequirementsAnalyzer output:"
echo "  - Look for '🔍 PropertyRequirementsAnalyzer: Found requirements for X aliases'"
echo "  - Look for '📋 alias: N properties: [...]'"
echo ""
