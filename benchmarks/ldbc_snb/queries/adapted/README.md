# LDBC SNB Adapted Queries

This directory contains adapted versions of LDBC SNB queries that use workarounds for features not yet fully supported in ClickGraph.

## Query Status

### ✅ Works As-Is (No Adaptation Needed)
These queries should work without modification:
- **Interactive Short**: IS1-IS7 (all 7 queries)
- **Interactive Complex**: IC1, IC2, IC3, IC4, IC5, IC6, IC7, IC8, IC9, IC11, IC12, IC13 (11 queries)
- **Business Intelligence**: BI1, BI2, BI3, BI5, BI6, BI7, BI9, BI11, BI12, BI13, BI14, BI17, BI18 (13 queries)

### ⚠️ Workaround Provided
These queries have adapted versions with workarounds:

#### IC10 - Friend Recommendation
**File**: `interactive-complex-10-workaround.cypher`  
**Issue**: Pattern comprehension `size([p IN posts WHERE (p)-[:HAS_TAG]->()...])`  
**Workaround**: Use OPTIONAL MATCH + count() instead of pattern comprehension

#### BI8 - Central Person for Tag
**File**: `bi-8-workaround.cypher`  
**Issue**: Pattern comprehension in size calculation  
**Workaround**: Use `size()` on patterns directly (supported since Dec 11, 2025)

#### BI4 - Top Message Creators
**File**: `bi-4-workaround.cypher`  
**Issue**: CALL subquery with UNION ALL  
**Workaround**: Restructure as separate queries with UNION ALL (partial - may need further adjustment)

### 🚫 Blocked by External Libraries
These queries cannot be adapted as they require Neo4j-specific extensions:
- **IC14**: Requires `gds.shortestPath.dijkstra` and `gds.graph.project`
- **BI10**: Requires `apoc.path.subgraphNodes`
- **BI15, BI19, BI20**: Require Neo4j Graph Data Science library

## Testing

To test adapted queries:

```bash
# Set environment variables
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_DATABASE="ldbc_snb"
export GRAPH_CONFIG_PATH="./benchmarks/ldbc_snb/schemas/ldbc_schema.yaml"

# Test query (example)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "<query_content>",
    "parameters": {
      "personId": 4398046511333,
      "month": 5
    }
  }'
```

## Summary

- **36 of 41 queries** (88%) have complete or workaround support
- **5 queries** (12%) blocked by external library dependencies
- **34 of 36 non-blocked queries** (94%) can be executed with existing features or workarounds
