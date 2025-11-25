# PropertyResolver Integration Guide

**Status**: Foundation Complete (Phase 1-2), Ready for SQL Generation Integration
**Date**: November 24, 2025

## Overview

PropertyResolver is a unified component for graph→SQL translation that handles property mapping and alias resolution for all three schema patterns (standard, denormalized, polymorphic).

## Implementation Status

### ✅ Completed (Phases 1-2)

1. **PropertyResolver Module** (`src/query_planner/translator/property_resolver.rs`)
   - Core structures: `PropertyResolver`, `AliasMapping`, `PropertyResolution`, `NodePosition`
   - Unified property resolution for all 3 patterns
   - **8/8 unit tests passing**, including denormalized multi-hop tests

2. **ViewScan Enhancement** (`src/query_planner/logical_plan/view_scan.rs`)
   - Added 6 new fields:
     - `from_node_properties`, `to_node_properties` (denormalized)
     - `type_column`, `type_values`, `from_label_column`, `to_label_column` (polymorphic)
   - All constructors updated
   - Preservation in `with_additional_filter()`

3. **ViewScan Population** (`src/query_planner/logical_plan/match_clause.rs`)
   - Node ViewScans: Populate from/to properties from `NodeSchema`
   - Relationship ViewScans: Populate polymorphic fields from `RelationshipSchema`
   - Metadata flows from YAML schema → NodeSchema/RelationshipSchema → ViewScan

### 🔄 Current State: ViewResolver Still Active

The existing filter_tagging logic with `ViewResolver` continues to work because:
- ViewScans now have complete metadata (from/to properties, type columns)
- ViewResolver can read this metadata
- No breaking changes to existing query flow

PropertyResolver is a **parallel system** ready for SQL generation integration.

## Architecture: Two-Phase Translation

```
Graph Layer (Cypher)
    ↓
ViewScan (enriched with from/to properties, type filters)
    ↓
PropertyResolver (new unified translator)
    ↓
SQL Layer (ClickHouse)
```

### Key Design Principle

**Single Source of Truth**: ViewScan contains ALL schema metadata needed for translation:
- Standard: `property_mapping`
- Denormalized: `from_node_properties`, `to_node_properties`
- Polymorphic: `type_column`, `type_values`, `from_label_column`, `to_label_column`

PropertyResolver reads from ViewScan, not from schema directly.

## Usage Example

### Denormalized Property Resolution

```cypher
-- Query: MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) WHERE a.city = 'LAX' AND b.city = 'NYC'
-- Schema: Airport nodes denormalized on flights table
```

**Setup PropertyResolver**:
```rust
let mut resolver = PropertyResolver::new();

// Register ViewScan for node 'a' (FROM position in edge 'f')
let mut a_view_scan = ViewScan::new(...);
a_view_scan.is_denormalized = true;
a_view_scan.from_node_properties = Some(hashmap!{
    "code" => PropertyValue::Column("Origin"),
    "city" => PropertyValue::Column("OriginCityName"),
});
resolver.register_view_scan("a", a_view_scan);

// Register alias mapping: 'a' maps to SQL alias 'f' (edge table)
resolver.register_alias("a", AliasMapping {
    sql_alias: "f",
    position: NodePosition::From,
    is_denormalized: true,
    edge_alias: Some("f"),
    ...
});

// Register ViewScan for node 'b' (TO position in edge 'f')
let mut b_view_scan = ViewScan::new(...);
b_view_scan.is_denormalized = true;
b_view_scan.to_node_properties = Some(hashmap!{
    "code" => PropertyValue::Column("Dest"),
    "city" => PropertyValue::Column("DestCityName"),
});
resolver.register_view_scan("b", b_view_scan);

// Register alias mapping: 'b' maps to SQL alias 'f' (edge table)
resolver.register_alias("b", AliasMapping {
    sql_alias: "f",
    position: NodePosition::To,
    is_denormalized: true,
    edge_alias: Some("f"),
    ...
});
```

**Resolve Properties**:
```rust
// WHERE a.city = 'LAX'
let result = resolver.resolve_property("a", "city", Some("f"))?;
// → table_alias: "f", property_value: Column("OriginCityName")
// SQL: WHERE f.OriginCityName = 'LAX'

// WHERE b.city = 'NYC'
let result = resolver.resolve_property("b", "city", Some("f"))?;
// → table_alias: "f", property_value: Column("DestCityName")
// SQL: WHERE f.DestCityName = 'NYC'
```

### Denormalized Multi-Hop

```cypher
-- MATCH (a)-[f]->(b)-[g]->(c) WHERE b.city = 'NYC'
-- Node 'b' plays TWO roles: TO in 'f', FROM in 'g'
```

**Setup**:
```rust
// Register 'b' with TWO mappings (one per edge)
resolver.register_alias("b", AliasMapping {
    sql_alias: "f",
    position: NodePosition::To,
    edge_alias: Some("f"),
    ...
});
resolver.register_alias("b", AliasMapping {
    sql_alias: "g",
    position: NodePosition::From,
    edge_alias: Some("g"),
    ...
});
```

**Resolve**:
```rust
// b.city in edge 'f' context (TO position)
resolver.resolve_property("b", "city", Some("f"))?;
// → table_alias: "f", property_value: Column("DestCityName")

// b.city in edge 'g' context (FROM position)
resolver.resolve_property("b", "city", Some("g"))?;
// → table_alias: "g", property_value: Column("OriginCityName")
```

**Key Insight**: Same node alias, same property, but DIFFERENT columns based on edge context!

## Integration Points

### Where PropertyResolver Will Be Used

1. **SQL Generation** (`src/clickhouse_query_generator/`)
   - Replace direct ViewScan.property_mapping lookups
   - Use `resolver.resolve_property()` with edge context
   
2. **Filter Rendering** (`src/render_plan/filter_pipeline.rs`)
   - When converting LogicalExpr → RenderExpr
   - Pass edge context for denormalized disambiguation

3. **Projection Rendering** (`src/render_plan/projection_builder.rs`)
   - When building SELECT clauses
   - Resolve properties with proper aliases

4. **RenderPlan Building** (`src/render_plan/plan_builder.rs`)
   - Build PropertyResolver during RenderPlan construction
   - Populate from LogicalPlan tree (ViewScans, GraphNodes, GraphRels)

### How to Build PropertyResolver from LogicalPlan

```rust
fn build_property_resolver(logical_plan: &LogicalPlan) -> PropertyResolver {
    let mut resolver = PropertyResolver::new();
    
    // Walk the LogicalPlan tree
    walk_logical_plan(logical_plan, &mut |plan| {
        match plan {
            LogicalPlan::ViewScan(view_scan) => {
                // Register ViewScan (already has all metadata)
                let alias = get_alias_for_view_scan(plan);
                resolver.register_view_scan(alias, view_scan.clone());
            }
            LogicalPlan::GraphNode(node) => {
                // Register alias mapping
                let mapping = AliasMapping {
                    sql_alias: node.alias.clone(),
                    position: NodePosition::Standalone,
                    is_denormalized: node.is_denormalized,
                    ...
                };
                resolver.register_alias(node.alias.clone(), mapping);
            }
            LogicalPlan::GraphRel(rel) => {
                // Register denormalized nodes with edge context
                if is_denormalized(&rel.left) {
                    let mapping = AliasMapping {
                        sql_alias: rel.alias.clone(),  // Use edge alias
                        position: NodePosition::From,
                        is_denormalized: true,
                        edge_alias: Some(rel.alias.clone()),
                        ...
                    };
                    resolver.register_alias(rel.left_connection.clone(), mapping);
                }
                if is_denormalized(&rel.right) {
                    let mapping = AliasMapping {
                        sql_alias: rel.alias.clone(),
                        position: NodePosition::To,
                        is_denormalized: true,
                        edge_alias: Some(rel.alias.clone()),
                        ...
                    };
                    resolver.register_alias(rel.right_connection.clone(), mapping);
                }
            }
            _ => {}
        }
    });
    
    resolver
}
```

## Testing Strategy

### Unit Tests (✅ Complete)

8/8 tests passing in `src/query_planner/translator/property_resolver.rs`:
- Standard property resolution
- Denormalized FROM/TO property resolution
- Denormalized multi-hop disambiguation
- Polymorphic with type filters
- Error handling (missing properties, invalid aliases)

### Integration Tests (Next Phase)

Test denormalized schema end-to-end:
1. Load `schemas/examples/ontime_denormalized.yaml`
2. Run simple query: `MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) WHERE a.city = 'Los Angeles' RETURN b.city`
3. Verify SQL generation uses correct columns (OriginCity vs DestCity)
4. Verify results correctness

## Known Limitations

1. **Variable-Length Paths**: Denormalized/polymorphic patterns NOT supported
   - CTEs only work with standard schema pattern
   - Documented in `docs/RECURSIVE_CTE_SCHEMA_PATTERNS.md`
   - Requires separate CTE generator enhancements (3-5 days)

2. **Schema Pattern Mixing**: Limited testing
   - Standard + Denormalized: Should work (common case)
   - Standard + Polymorphic: Should work
   - Denormalized + Polymorphic: NOT SUPPORTED (too complex)

## Next Steps

1. **SQL Generation Integration** (Phase 4)
   - Add PropertyResolver to RenderPlan
   - Update filter/projection rendering to use resolver
   - Test with denormalized schema

2. **Documentation** (Phase 7)
   - Update STATUS.md with PropertyResolver status
   - Document in CHANGELOG.md
   - Add to KNOWN_ISSUES.md (variable-length limitation)

3. **Future: CTE Enhancement** (Separate Phase)
   - Extend `VariableLengthCteGenerator` to support denormalized patterns
   - Add polymorphic type filtering to CTEs
   - Estimated: 2-4 days additional work

## Design Rationale

### Why Merge AliasResolutionContext into PropertyResolver?

**Original Plan**: Two separate components (AliasResolutionContext + PropertyResolver)
**Simplified**: Single unified component

**Reasons**:
1. They always work together (never used independently)
2. Edge context needed for both alias AND property resolution
3. Simpler to build and maintain (one component, one API)
4. Saved 2 hours of implementation time

### Why Edge Context Parameter?

For denormalized multi-hop, the same node can appear in multiple edges with different roles:
- `(a)-[f]->(b)-[g]->(c)`: Node 'b' is TO in 'f', FROM in 'g'
- Same property `b.city` maps to different columns depending on which edge

Without edge context, ambiguous. With edge context, precise.

### Why ViewScan as Source of Truth?

ViewScan is created during query planning and carries schema metadata through the pipeline:
- Avoids repeated schema lookups
- Enables offline testing (mock ViewScans)
- Clear separation: schema loading → ViewScan → property resolution

## References

- Implementation Plan: `docs/GRAPH_TO_SQL_BOUNDARY_IMPLEMENTATION_PLAN.md`
- Architecture: `docs/UNIFIED_PROPERTY_MAPPING_ARCHITECTURE.md`
- CTE Limitations: `docs/RECURSIVE_CTE_SCHEMA_PATTERNS.md`
- Schema Mixing: `docs/SCHEMA_PATTERN_MIXING_ANALYSIS.md`
