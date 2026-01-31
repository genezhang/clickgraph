#!/bin/bash
# End-to-end test script for Neo4j schema metadata procedures
# Tests all 4 procedures across multiple schemas

set -e

echo "🧪 Testing Neo4j Schema Metadata Procedures"
echo "==========================================="
echo ""

# Test db.labels()
echo "1️⃣  Testing CALL db.labels() (default schema)"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"CALL db.labels()"}' | jq '.count, .records[0]'
echo ""

# Test db.relationshipTypes()
echo "2️⃣  Testing CALL db.relationshipTypes() (default schema)"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"CALL db.relationshipTypes()"}' | jq '.count, .records[0]'
echo ""

# Test dbms.components()
echo "3️⃣  Testing CALL dbms.components()"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"CALL dbms.components()"}' | jq '.records[0]'
echo ""

# Test db.propertyKeys()
echo "4️⃣  Testing CALL db.propertyKeys() (default schema)"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"CALL db.propertyKeys()"}' | jq '.count'
echo ""

# Test with different schema
echo "5️⃣  Testing with test_fixtures schema"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"CALL db.labels()", "schema_name":"test_fixtures"}' | jq '.count, .records[0:2]'
echo ""

# Test with LDBC schema
echo "6️⃣  Testing with ldbc_snb schema"
curl -s -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"CALL db.relationshipTypes()", "schema_name":"ldbc_snb"}' | jq '.count'
echo ""

echo "✅ All procedure tests completed successfully!"
