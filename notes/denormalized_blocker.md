# Denormalized Edge Table Feature - Implementation Blockers

## Status: Schema Structure Complete, SQL Generation Blocked

### ✅ What Works
1. **Schema Architecture**: Node-level `from_node_properties` and `to_node_properties` implemented
2. **Schema Loading**: YAML schemas load correctly with denormalized node definitions
3. **Property Resolution Function**: Enhanced to check node-level denormalized properties
4. **Schema Lookup**: Fixed to search all loaded schemas (not just "default")

### ❌ Core Blocker: JOIN Generation Architecture

#### The Problem
For denormalized nodes, the current SQL generation creates unnecessary table aliases and JOINs:

**Current (WRONG)**:
```sql
SELECT a.code AS "a.code", b.code AS "b.code"
FROM flights AS a              ← Should NOT exist!
INNER JOIN flights AS f ON ... ← Only table needed
INNER JOIN flights AS b ON ... ← Should NOT exist!
```

**Required (CORRECT)**:
```sql
SELECT f.origin_code AS "a.code", f.dest_code AS "b.code"
FROM flights AS f
```

#### Root Cause
The rendering pipeline assumes all nodes have separate tables:
1. **ViewScan** creates table alias for each node (e.g., `AS a`, `AS b`)
2. **JOIN generation** links node tables to relationship table
3. **SELECT builder** references node table aliases

For denormalized nodes:
- Nodes have NO separate physical table (virtual nodes)
- All node data comes from the relationship table itself
- No JOINs should be generated - nodes and edges share the same table

### Required Architecture Changes

#### Option 1: Denormalized-Aware ViewScan (Recommended)
**Location**: `src/render_plan/plan_builder.rs` - ViewScan rendering
**Changes**:
1. Check if node is denormalized during ViewScan creation
2. If denormalized, **skip table alias creation** entirely
3. Mark node as "virtual" in rendering context
4. During SELECT building, use relationship table alias + denormalized column mapping

**Pros**: Clean, follows data model
**Cons**: Requires refactoring ViewScan logic

#### Option 2: Post-Processing Optimization Pass
**Location**: `src/query_planner/optimizer/` - new pass
**Changes**:
1. Detect denormalized node patterns in logical plan
2. Replace ViewScan + GraphRel with simplified "DenormalizedRelScan"
3. Generate SQL directly from relationship table

**Pros**: Less invasive to existing code
**Cons**: Adds complexity to optimizer

#### Option 3: Denormalized-Specific Logical Plan Node
**Location**: `src/query_planner/logical_plan/` - new plan type
**Changes**:
1. Create `DenormalizedPattern` logical plan node
2. Detect during planning (in analyzer)
3. Render directly to SQL without JOINs

**Pros**: Most explicit, easiest to understand
**Cons**: Requires changes across multiple layers

### Next Steps

1. **Decision**: Choose architecture approach (recommend Option 1)
2. **Prototype**: Implement denormalized ViewScan rendering
3. **Test**: Verify SQL generation matches expected patterns
4. **Iterate**: Handle edge cases (filters, ordering, etc.)

### Files Modified So Far

**Schema Loading** (✅ Complete):
- `src/graph_catalog/graph_schema.rs` - Added denormalized fields to NodeSchema
- `src/graph_catalog/config.rs` - Added denormalized fields to NodeDefinition, populates from YAML
- `schemas/tests/denormalized_flights.yaml` - Refactored to node-level properties
- `schemas/examples/ontime_denormalized.yaml` - Refactored to node-level properties

**Property Resolution** (✅ Complete):
- `src/render_plan/cte_generation.rs` - Enhanced `map_property_to_column_with_relationship_context()`
  - Added node-level property checking (lines 569-592)
  - Fixed schema lookup to search all schemas (lines 556-571)

**SQL Generation** (❌ BLOCKED):
- Needs architectural redesign - see options above

### Test Files Created
- `test_node_denorm_schema.py` - Schema load validation (✅ PASSING)
- `test_denorm_property_resolution.py` - Full query test (❌ ClickHouse auth issue)
- `test_denorm_sql_only.py` - SQL generation test (❌ Shows JOIN generation problem)

### Key Insight
Property resolution works fine - the issue is that we're generating table aliases for virtual nodes. The SELECT clause is being built with node table aliases that shouldn't exist. This requires rethinking how ViewScan and JOIN generation work for denormalized patterns.

---

**Date**: Nov 22, 2025 (continued from earlier session)
**Context**: Implementing denormalized edge table feature (OnTime-style schema pattern)
