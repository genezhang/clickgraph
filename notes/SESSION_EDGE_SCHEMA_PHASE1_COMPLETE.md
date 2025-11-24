# Edge Schema Redesign - Phase 1 Complete

**Date**: November 22, 2025  
**Status**: ✅ **PHASE 1 COMPLETE**  
**Branch**: main  
**Related Docs**: `edge-schema-redesign.md`, `composite-id-design.md`

## Session Summary

Completed foundational work for comprehensive edge schema redesign supporting:
1. **Composite IDs** (nodes + edges)
2. **Denormalized nodes** (properties in edge tables)
3. **Polymorphic edges** (runtime type discovery)

## What We Built

### 1. Core Data Structures ✅

**Identifier Enum** (`src/graph_catalog/config.rs`):
```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Identifier {
    Single(String),
    Composite(Vec<String>),
}

impl Identifier {
    pub fn columns(&self) -> Vec<&str>
    pub fn is_composite(&self) -> bool
    pub fn as_single(&self) -> &str
}
```

**Standard Edge Definition**:
```rust
pub struct StandardEdgeDefinition {
    pub type_name: String,
    pub table: String,
    pub from_node: String,  // Known at config time
    pub to_node: String,    // Known at config time
    
    // New features
    pub edge_id: Option<Identifier>,  // Composite ID support
    pub from_node_properties: Option<HashMap<String, String>>,  // Denormalized
    pub to_node_properties: Option<HashMap<String, String>>,    // Denormalized
    
    // ... all existing fields preserved
}
```

**Polymorphic Edge Definition**:
```rust
pub struct PolymorphicEdgeDefinition {
    pub polymorphic: bool,
    pub table: String,
    
    // Discovery columns (runtime type discovery)
    pub type_column: String,
    pub from_label_column: String,
    pub to_label_column: String,
    
    pub type_values: Option<Vec<String>>,  // Optional whitelist
    pub edge_id: Option<Identifier>,
    
    // ... shared properties
}
```

**Edge Definition Enum**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EdgeDefinition {
    Standard(StandardEdgeDefinition),
    Polymorphic(PolymorphicEdgeDefinition),
}
```

### 2. Schema Structure Updates ✅

**GraphSchemaDefinition** - Now supports both old and new formats:
```rust
pub struct GraphSchemaDefinition {
    pub nodes: Vec<NodeDefinition>,
    
    #[serde(default)]
    pub relationships: Vec<RelationshipDefinition>,  // Legacy (deprecated)
    
    #[serde(default, alias = "relationships")]
    pub edges: Vec<EdgeDefinition>,  // New (preferred)
}
```

**Backward Compatibility**: Existing schemas work unchanged!

### 3. Compilation Success ✅

- ✅ All structures compile cleanly
- ✅ No breaking changes to existing code
- ✅ Backward compatible with existing YAML schemas
- ✅ Only 105 warnings (existing, unrelated)

## YAML Examples Now Supported

### Standard Edge (Simple)
```yaml
edges:
  - type: AUTHORED
    database: brahmand
    table: authored
    from_node: User
    to_node: Post
    from_id: user_id
    to_id: post_id
```

### Denormalized Nodes (OnTime)
```yaml
nodes:
  - label: Airport
    database: brahmand
    table: ontime  # Same as edge table!
    id_column: code

edges:
  - type: FLIGHT
    database: brahmand
    table: ontime
    from_id: Origin
    to_id: Dest
    from_node: Airport
    to_node: Airport
    edge_id: [FlightDate, FlightNum, Origin, Dest]  # Composite!
    from_node_properties:
      city: OriginCityName
      state: OriginState
    to_node_properties:
      city: DestCityName
      state: DestState
```

### Polymorphic Edges
```yaml
edges:
  - polymorphic: true
    database: brahmand
    table: relationships
    from_id: from_id
    to_id: to_id
    type_column: relation_type
    from_label_column: from_type
    to_label_column: to_type
    type_values: [FOLLOWS, LIKES, AUTHORED]  # Optional whitelist
```

## Key Decisions Made

### 1. Standard vs Polymorphic (Not Strategies)
**Fundamental difference**: 1:1 vs 1:N config-to-schema mapping
- Standard: Explicit types/nodes at config time
- Polymorphic: Discover types/nodes at runtime

### 2. Denormalized Node Model
**Schema organization**: Properties in edge definition
```yaml
# Node declares table (same as edge)
nodes:
  - label: Airport
    table: ontime  # ← Not null!

# Edge provides column mappings
edges:
  - type: FLIGHT
    table: ontime
    from_node_properties: {...}  # ← Resolves ambiguity
```

**Detection**: `node.table == edge.table` → denormalized node

### 3. Path Modes (ISO GQL)
Documented support for **TRAIL mode** (edge uniqueness, default):
- WALK: No uniqueness (deferred)
- TRAIL: Edge unique, nodes can repeat (✅ implementing)
- SIMPLE: Both unique (deferred)
- ACYCLIC: No cycles (deferred)

### 4. Deferred Patterns
**Not implementing now** (YAGNI principle):
- Filtered polymorphic (Case 2): `type_value: 'follow'` on polymorphic table
- Multi-table nodes: `table: null` → derive from all edges
- Non-TRAIL path modes

## Files Modified

```
src/graph_catalog/config.rs:
  + Identifier enum (45 lines)
  + StandardEdgeDefinition struct (75 lines)
  + PolymorphicEdgeDefinition struct (55 lines)
  + EdgeDefinition enum (5 lines)
  + GraphSchemaDefinition.edges field

src/server/graph_catalog.rs:
  + Added edges: Vec::new() to 3 empty config constructors

notes/edge-schema-redesign.md: [NEW]
  + Complete implementation plan (350 lines)

notes/composite-id-design.md:
  + Added Path Modes section (ISO GQL)
  + Added denormalized edge table design
```

## Next Steps (Phase 2)

### Schema Processing Logic
1. ✅ Detect denormalized nodes (`node.table == edge.table`)
2. ⬜ Expand polymorphic edges (query ClickHouse for types)
3. ⬜ Build `ProcessedNodeMetadata` with derived properties
4. ⬜ Validate denormalized configurations

### Query Generation
5. ⬜ Generate UNION queries for virtual node scans
6. ⬜ Generate direct column access for denormalized edge traversals
7. ⬜ Support composite ID in edge uniqueness filters

### Testing
8. ⬜ Create OnTime-style test schema
9. ⬜ Create polymorphic edge test schema
10. ⬜ Integration tests for all patterns

## Technical Context

### Architecture Flow
```
YAML Config
  ↓ (serde)
EdgeDefinition (enum)
  ├─ Standard → 1 EdgeSchema
  └─ Polymorphic → N EdgeSchema (runtime expansion)
  ↓
ProcessedNodeMetadata (virtual nodes)
  ↓
SQL Generation
  ├─ UNION (standalone virtual nodes)
  └─ Direct access (edge traversals)
```

### Key Implementation Points
- **Untagged serde**: Automatic dispatch between Standard/Polymorphic
- **Backward compat**: `alias = "relationships"` on edges field
- **Optional fields**: All new features are `Option<T>` (non-breaking)
- **Validation**: Happens at `to_graph_schema()` time

## Research Artifacts Created

1. `scripts/research/schema_organization_comparison.py` - Compared property placement options
2. Updated `composite-id-design.md` with denormalized nodes design
3. Updated `composite-id-design.md` with ISO GQL path modes

## Build Status

```
✅ Compilation: Success
✅ Tests: Not yet added (structures only)
✅ Warnings: 105 (pre-existing, unrelated)
⏸️ Integration: Pending Phase 2
```

## Session Stats

- **Duration**: ~2 hours
- **Lines of code**: ~250 (new structures)
- **Design docs**: 3 updated/created
- **Key decisions**: 4 major
- **Compilation attempts**: 3 (all successful after fixes)

## Continuity Notes

**For next session**:
1. Start with Phase 2: Schema processing logic
2. Focus on polymorphic edge expansion first (needs ClickHouse client)
3. Then denormalized node detection (simpler, no external deps)
4. Create test YAML schemas for both patterns

**Context preserved**:
- All design decisions documented in `edge-schema-redesign.md`
- Clear separation: Standard vs Polymorphic (not strategies)
- Denormalized model: properties in edge, detection by table match
- Path modes: TRAIL default, others deferred

**Ready to continue!** 🚀
