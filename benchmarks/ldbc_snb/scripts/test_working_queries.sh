#!/bin/bash
# Test LDBC queries that should work as-is on sf0.003 dataset
# Tests the 29 queries identified as "Works As-Is" in QUERY_FEATURE_ANALYSIS.md

set -e

# Configuration
CLICKGRAPH_URL="${CLICKGRAPH_URL:-http://localhost:8080}"
SCHEMA_NAME="${SCHEMA_NAME:-ldbc_snb}"
SCALE_FACTOR="${SCALE_FACTOR:-sf0.003}"

# Output files
RESULTS_DIR="./benchmarks/ldbc_snb/results/${SCALE_FACTOR}"
mkdir -p "$RESULTS_DIR"
RESULTS_FILE="${RESULTS_DIR}/test_results_$(date +%Y%m%d_%H%M%S).json"
SUMMARY_FILE="${RESULTS_DIR}/test_summary_$(date +%Y%m%d_%H%M%S).txt"

echo "==============================================="
echo "LDBC SNB Query Testing - ${SCALE_FACTOR}"
echo "==============================================="
echo "ClickGraph URL: $CLICKGRAPH_URL"
echo "Schema: $SCHEMA_NAME"
echo "Results: $RESULTS_FILE"
echo ""

# Helper function to test a query
test_query() {
    local query_file=$1
    local query_name=$(basename "$query_file" .cypher)
    local params=${2:-"{}"}
    
    echo -n "Testing $query_name... "
    
    local start_time=$(date +%s%N)
    local query_content=$(cat "$query_file")
    
    # Execute query
    local response=$(curl -s -X POST "$CLICKGRAPH_URL/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"query\": $(echo "$query_content" | jq -Rs .),
            \"schema_name\": \"$SCHEMA_NAME\",
            \"parameters\": $params
        }")
    
    local end_time=$(date +%s%N)
    local duration_ms=$(( ($end_time - $start_time) / 1000000 ))
    
    # Check if query succeeded
    local success=$(echo "$response" | jq -r '.success // false')
    
    if [ "$success" = "true" ]; then
        local row_count=$(echo "$response" | jq '.data | length')
        echo "✅ PASS (${duration_ms}ms, ${row_count} rows)"
        echo "{\"query\": \"$query_name\", \"status\": \"PASS\", \"duration_ms\": $duration_ms, \"rows\": $row_count}" >> "$RESULTS_FILE"
        return 0
    else
        local error=$(echo "$response" | jq -r '.error // "Unknown error"')
        echo "❌ FAIL: $error"
        echo "{\"query\": \"$query_name\", \"status\": \"FAIL\", \"error\": $(echo "$error" | jq -Rs .)}" >> "$RESULTS_FILE"
        return 1
    fi
}

# Initialize results file
echo "[" > "$RESULTS_FILE"

PASSED=0
FAILED=0
TOTAL=0

echo "=== Interactive Short Queries (IS1-IS7) ==="
# IS1: Given person details
test_query "./benchmarks/ldbc_snb/queries/official/interactive/short-1.cypher" '{"personId": 933}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

test_query "./benchmarks/ldbc_snb/queries/official/interactive/short-2.cypher" '{"personId": 933}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

test_query "./benchmarks/ldbc_snb/queries/official/interactive/short-3.cypher" '{"personId": 933}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

test_query "./benchmarks/ldbc_snb/queries/official/interactive/short-4.cypher" '{"messageId": 206158431390}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

test_query "./benchmarks/ldbc_snb/queries/official/interactive/short-5.cypher" '{"messageId": 206158431390}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

test_query "./benchmarks/ldbc_snb/queries/official/interactive/short-6.cypher" '{"messageId": 206158431390}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

test_query "./benchmarks/ldbc_snb/queries/official/interactive/short-7.cypher" '{"messageId": 206158431390}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

echo ""
echo "=== Interactive Complex Queries (Selected) ==="
# IC1: Friends with given name
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-1.cypher" '{"personId": 933, "firstName": "John"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC2: Recent messages by friends
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-2.cypher" '{"personId": 933, "maxDate": "2012-11-23T00:00:00.000+00:00"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC3: Friends in countries (should work - uses IN with variables)
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-3.cypher" '{"personId": 933, "countryXName": "Angola", "countryYName": "Colombia", "startDate": 1275393600000, "endDate": 1277812800000}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC4: New tags
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-4.cypher" '{"personId": 933, "startDate": "2011-07-22T00:00:00.000+00:00", "duration": 30}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC5: New groups
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-5.cypher" '{"personId": 933, "minDate": "2011-07-22T00:00:00.000+00:00"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC6: Tag co-occurrence
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-6.cypher" '{"personId": 933, "tagName": "Che_Guevara"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC7: Recent likers
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-7.cypher" '{"personId": 933}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC8: Recent replies
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-8.cypher" '{"personId": 933}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC9: Recent messages by friends of friends
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-9.cypher" '{"personId": 933, "maxDate": "2012-11-23T00:00:00.000+00:00"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC11: Job referral
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-11.cypher" '{"personId": 933, "countryName": "Angola", "workFromYear": 2005}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC12: Expert search
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-12.cypher" '{"personId": 933, "tagClassName": "MusicalArtist"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# IC13: Shortest path
test_query "./benchmarks/ldbc_snb/queries/official/interactive/complex-13.cypher" '{"person1Id": 933, "person2Id": 10995116278874}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

echo ""
echo "=== Business Intelligence Queries (Selected) ==="
# BI1: Posting summary
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-1.cypher" '{"datetime": "2011-12-01T00:00:00.000+00:00"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI2: Tag evolution
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-2.cypher" '{"date": "2011-07-22T00:00:00.000+00:00", "tagClass": "MusicalArtist", "countries": ["Angola", "Colombia"]}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI3: Popular topics in a country
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-3.cypher" '{"tagClass": "MusicalArtist", "country": "Angola"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI5: Active posters
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-5.cypher" '{"tag": "Che_Guevara"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI6: Authoritative users
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-6.cypher" '{"tag": "Che_Guevara"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI7: Related topics
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-7.cypher" '{"tag": "Che_Guevara"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI9: Top thread initiators
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-9.cypher" '{"tagClass1": "MusicalArtist", "tagClass2": "OfficeHolder", "threshold": 100}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI11: Friend triangles
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-11.cypher" '{"country": "Angola"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI12: Messaging patterns (test with simple parameters)
# Note: May need parameter array support
# test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-12.cypher" '{"startDate": "2010-07-22T00:00:00.000+00:00", "lengthThreshold": 20, "languages": ["ar", "hu"]}' && ((PASSED++)) || ((FAILED++))
# ((TOTAL++))

# BI13: Zombies
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-13.cypher" '{"country": "Angola", "endDate": "2012-01-01T00:00:00.000+00:00"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI14: International dialog
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-14.cypher" '{"country1": "Angola", "country2": "Colombia"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI17: Information propagation
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-17.cypher" '{"tag": "Che_Guevara"}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# BI18: Friend recommendation
test_query "./benchmarks/ldbc_snb/queries/official/bi/bi-18.cypher" '{"person": 933, "tag": "Che_Guevara", "limit": 4}' && ((PASSED++)) || ((FAILED++))
((TOTAL++))

# Close results file
echo "]" >> "$RESULTS_FILE"

# Generate summary
echo "===============================================" | tee "$SUMMARY_FILE"
echo "LDBC SNB Query Test Summary - ${SCALE_FACTOR}" | tee -a "$SUMMARY_FILE"
echo "===============================================" | tee -a "$SUMMARY_FILE"
echo "Total queries tested: $TOTAL" | tee -a "$SUMMARY_FILE"
echo "Passed: $PASSED" | tee -a "$SUMMARY_FILE"
echo "Failed: $FAILED" | tee -a "$SUMMARY_FILE"
echo "Success rate: $(awk "BEGIN {printf \"%.1f\", ($PASSED/$TOTAL)*100}")%" | tee -a "$SUMMARY_FILE"
echo "" | tee -a "$SUMMARY_FILE"
echo "Detailed results: $RESULTS_FILE" | tee -a "$SUMMARY_FILE"
echo "Summary: $SUMMARY_FILE" | tee -a "$SUMMARY_FILE"

if [ $FAILED -eq 0 ]; then
    echo "" | tee -a "$SUMMARY_FILE"
    echo "✅ All tests passed! Ready for sf10 benchmark." | tee -a "$SUMMARY_FILE"
    exit 0
else
    echo "" | tee -a "$SUMMARY_FILE"
    echo "⚠️  Some tests failed. Review errors before proceeding to sf10." | tee -a "$SUMMARY_FILE"
    exit 1
fi
