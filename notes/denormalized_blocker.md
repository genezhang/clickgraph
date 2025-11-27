# Denormalized Edge Table Feature - Implementation Status

## Status: Single-Hop Working ✅, Multi-Hop Blocked ❌

*Updated: November 27, 2025*

### ✅ What Works (Fixed Nov 27, 2025)

1. **Schema Architecture**: Node-level `from_node_properties` and `to_node_properties` implemented
2. **Schema Loading**: YAML schemas load correctly with denormalized node definitions
3. **Property Resolution Function**: Enhanced to check node-level denormalized properties with role-awareness
4. **Schema Lookup**: Fixed to search all loaded schemas (not just "default")
5. **Single-Hop Patterns**: Destination node properties now correctly map to `to_node_properties`
6. **Aggregations**: COUNT, SUM, AVG, etc. work correctly on denormalized patterns

**Example (Working)**:
```cypher
MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)
WHERE a.city = "Seattle"
RETURN a.code, b.code, f.carrier
```
Generates correct SQL:
```sql
SELECT f.Origin AS "a.code", f.Dest AS "b.code", f.Carrier AS "f.carrier"
FROM flights AS f
WHERE f.OriginCityName = 'Seattle'
```

### ❌ Remaining Blocker: Multi-Hop Pattern SQL Generation

#### The Problem
For 2+ hop denormalized patterns, SQL generation is incomplete:

**Query**:
```cypher
MATCH (a:Airport)-[f1:FLIGHT]->(b:Airport)-[f2:FLIGHT]->(c:Airport)
RETURN a.code, b.code, c.code
```

**Current (WRONG)**:
```sql
SELECT f1.Origin, f1.Dest, f2.Dest
FROM flights AS f2  -- Missing f1 table and JOIN!
```

**Required (CORRECT)**:
```sql
SELECT f1.Origin AS "a.code", f1.Dest AS "b.code", f2.Dest AS "c.code"
FROM flights AS f1
INNER JOIN flights AS f2 ON f1.Dest = f2.Origin
```

### Files Modified

**Schema Loading** (✅ Complete):
- `src/graph_catalog/graph_schema.rs` - Added denormalized fields to NodeSchema
- `src/graph_catalog/config.rs` - Added denormalized fields to NodeDefinition
- `schemas/tests/denormalized_flights.yaml` - Node-level properties
- `schemas/examples/ontime_denormalized.yaml` - Node-level properties

**Property Resolution** (✅ Complete - Fixed Nov 27, 2025):
- `src/query_planner/analyzer/filter_tagging.rs` - Uses `find_denormalized_context()` for node role
- `src/query_planner/analyzer/view_resolver.rs` - Added `resolve_node_property_with_role()`
  - Role-aware property resolution (From vs To node position)

**SQL Generation - Single Hop** (✅ Working):
- Single relationship patterns generate correct SQL
- Property mappings correctly use relationship table alias

**SQL Generation - Multi-Hop** (❌ BLOCKED):
- 2+ hop patterns missing FROM clause for first relationship
- JOIN conditions not generated between consecutive relationships

### Test Results (Nov 27, 2025)

**Single-Hop Patterns**: ✅ All Working
- Source node properties → from_node_properties ✅
- Destination node properties → to_node_properties ✅
- Relationship properties → edge properties ✅
- Filters on source/destination nodes ✅
- Aggregations (COUNT, SUM, AVG) ✅

**Multi-Hop Patterns**: ❌ SQL Generation Incomplete

### Next Steps for Multi-Hop Fix

1. Trace how multi-hop patterns are planned in `match_clause.rs`
2. Check GraphRel chaining in the logical plan
3. Fix FROM/JOIN generation in `plan_builder.rs`
4. Test with 2-hop and 3-hop patterns

---

*Date*: Nov 27, 2025 (updated with single-hop fix)
*Context*: Implementing denormalized edge table feature (OnTime-style schema pattern)
