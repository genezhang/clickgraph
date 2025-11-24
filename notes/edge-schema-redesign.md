# Edge Schema Redesign - Implementation Plan

**Date**: November 22, 2025  
**Status**: 🚀 **READY TO IMPLEMENT**  
**Related**: `composite-id-design.md`

## Executive Summary

Redesigning edge schema to support:
1. **Composite IDs** for nodes and edges
2. **Denormalized nodes** (properties in edge tables)
3. **Polymorphic edges** (discover types from data)

**Key Design Principle**: Keep it simple - implement what we need now, defer complexity until needed.

## Architecture Overview

```
Config Layer (YAML)          Schema Layer (Runtime)         Query Layer (SQL)
==================          ======================         =================

EdgeDefinition              EdgeSchema                     SQL Generator
├─ Standard                 ├─ type_name                   ├─ SELECT ...
│  ├─ type_name             ├─ table                       ├─ FROM table
│  ├─ table                 ├─ from_node                   ├─ WHERE filters
│  ├─ from_node (known)     ├─ to_node                     └─ JOIN logic
│  └─ to_node (known)       ├─ edge_id (composite?)
└─ Polymorphic              ├─ from_node_props             NodeSchema
   ├─ discovers types       └─ to_node_props               ├─ Explicit (has table)
   ├─ discovers nodes                                      └─ Virtual (derived)
   └─ runtime expansion
```

## Schema Structures

### Core Types

```rust
// Composite ID support
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Identifier {
    Single(String),
    Composite(Vec<String>),
}

impl Identifier {
    pub fn columns(&self) -> Vec<&str> {
        match self {
            Identifier::Single(col) => vec![col.as_str()],
            Identifier::Composite(cols) => cols.iter().map(|s| s.as_str()).collect(),
        }
    }
    
    pub fn is_composite(&self) -> bool {
        matches!(self, Identifier::Composite(_))
    }
}
```

### Edge Definitions (Config Layer)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EdgeDefinition {
    Standard(StandardEdgeDefinition),
    Polymorphic(PolymorphicEdgeDefinition),
}

// Pattern 1: Standard (explicit, known at config time)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StandardEdgeDefinition {
    #[serde(rename = "type")]
    pub type_name: String,
    pub database: String,
    pub table: String,
    pub from_id: String,
    pub to_id: String,
    pub from_node: String,  // Known at config time
    pub to_node: String,    // Known at config time
    
    // Optional: Composite ID support
    pub edge_id: Option<Identifier>,
    
    // Optional: Denormalized node properties
    pub from_node_properties: Option<HashMap<String, String>>,
    pub to_node_properties: Option<HashMap<String, String>>,
    
    // Standard properties
    pub properties: HashMap<String, String>,
    
    // Engine options
    pub use_final: Option<bool>,
}

// Pattern 2: Polymorphic (discovered at runtime)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymorphicEdgeDefinition {
    pub polymorphic: bool,  // Always true (marker)
    pub database: String,
    pub table: String,
    pub from_id: String,
    pub to_id: String,
    
    // Discovery columns
    pub type_column: String,
    pub from_label_column: String,
    pub to_label_column: String,
    
    // Optional whitelist (validation)
    pub type_values: Option<Vec<String>>,
    
    // Shared across all discovered types
    pub properties: HashMap<String, String>,
    pub edge_id: Option<Identifier>,
    pub use_final: Option<bool>,
}
```

### Node Schema Enhancement

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDefinition {
    pub label: String,
    pub database: String,
    pub table: String,  // Can be same as edge table (denormalized case)
    
    // If table matches edge table → virtual node (derive from edge)
    // Otherwise → explicit node (has its own table)
    
    pub id_column: String,
    pub properties: HashMap<String, String>,
    
    // ... existing fields ...
}
```

## YAML Examples

### Case 1: Standard Edge (Simple)

```yaml
nodes:
  - label: User
    database: brahmand
    table: users
    id_column: user_id

edges:
  - type: AUTHORED
    database: brahmand
    table: authored
    from_id: user_id
    to_id: post_id
    from_node: User
    to_node: Post
    properties:
      timestamp: created_at
```

### Case 2: Denormalized Nodes (OnTime)

```yaml
nodes:
  - label: Airport
    database: brahmand
    table: ontime  # ← Same as edge table!
    id_column: code

edges:
  - type: FLIGHT
    database: brahmand
    table: ontime
    from_id: Origin
    to_id: Dest
    from_node: Airport
    to_node: Airport
    edge_id: [FlightDate, FlightNum, Origin, Dest]  # ← Composite!
    from_node_properties:
      city: OriginCityName
      state: OriginState
    to_node_properties:
      city: DestCityName
      state: DestState
```

### Case 3: Polymorphic Edges

```yaml
nodes:
  - label: User
    database: brahmand
    table: users
    id_column: user_id
  - label: Post
    database: brahmand
    table: posts
    id_column: post_id

edges:
  - polymorphic: true
    database: brahmand
    table: relationships
    from_id: from_id
    to_id: to_id
    type_column: relation_type
    from_label_column: from_type
    to_label_column: to_type
    type_values: [FOLLOWS, LIKES, AUTHORED, COMMENTED]  # Optional
    properties:
      created_at: created_at
      weight: weight
```

## Processing Logic

### Detection: Denormalized vs Explicit Nodes

```rust
fn is_denormalized_node(
    node: &NodeDefinition,
    edges: &[EdgeDefinition]
) -> bool {
    edges.iter().any(|edge| {
        match edge {
            EdgeDefinition::Standard(std) => {
                std.table == node.table &&
                (std.from_node == node.label || std.to_node == node.label)
            }
            EdgeDefinition::Polymorphic(_) => false,  // Handled separately
        }
    })
}
```

### Schema Expansion: Polymorphic → Multiple EdgeSchemas

```rust
async fn expand_polymorphic_edge(
    def: &PolymorphicEdgeDefinition,
    client: &ClickHouseClient
) -> Result<Vec<EdgeSchema>, Error> {
    // Discover unique (type, from_label, to_label) combinations
    let query = format!(
        "SELECT DISTINCT {}, {}, {} FROM {}.{}",
        def.type_column,
        def.from_label_column,
        def.to_label_column,
        def.database,
        def.table
    );
    
    let rows: Vec<(String, String, String)> = client.query(&query).fetch_all().await?;
    
    // Optional: Validate against whitelist
    if let Some(ref whitelist) = def.type_values {
        for (type_val, _, _) in &rows {
            if !whitelist.contains(type_val) {
                warn!("Discovered edge type '{}' not in whitelist", type_val);
            }
        }
    }
    
    // Generate one EdgeSchema per combination
    Ok(rows.into_iter().map(|(type_val, from_label, to_label)| {
        EdgeSchema {
            type_name: type_val.clone(),
            database: def.database.clone(),
            table_name: def.table.clone(),
            from_node: from_label,
            to_node: to_label,
            from_id: def.from_id.clone(),
            to_id: def.to_id.clone(),
            // Add implicit filters for this specific edge type
            implicit_filters: vec![
                format!("{} = '{}'", def.type_column, type_val),
                format!("{} = '{}'", def.from_label_column, from_label),
                format!("{} = '{}'", def.to_label_column, to_label),
            ],
            property_mappings: def.properties.clone(),
            edge_id: def.edge_id.clone(),
            // ...
        }
    }).collect())
}
```

### Query Generation: Virtual Nodes

```cypher
MATCH (a:Airport) WHERE a.city = 'Seattle' RETURN a.code, a.city
```

Generated SQL (UNION from/to roles):
```sql
SELECT Origin as code, OriginCityName as city
FROM ontime
WHERE OriginCityName = 'Seattle'

UNION ALL

SELECT Dest as code, DestCityName as city
FROM ontime
WHERE DestCityName = 'Seattle'
```

### Query Generation: Denormalized Edge Traversal

```cypher
MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)
WHERE a.city = 'Seattle' AND b.city = 'New York'
RETURN f.date, a.city, b.city
```

Generated SQL (single table scan, no JOIN):
```sql
SELECT 
  FlightDate as date,
  OriginCityName as from_city,
  DestCityName as to_city
FROM ontime
WHERE OriginCityName = 'Seattle' AND DestCityName = 'New York'
```

## Implementation Plan

### Phase 1: Core Infrastructure (Current Sprint)

**Files to modify**:
- `src/graph_catalog/graph_schema.rs`
- `src/graph_catalog/config.rs`

**Tasks**:
1. ✅ Add `Identifier` enum
2. ✅ Add `StandardEdgeDefinition` struct
3. ✅ Add `PolymorphicEdgeDefinition` struct
4. ✅ Add `EdgeDefinition` enum (untagged serde)
5. ✅ Add `from_node_properties`, `to_node_properties` to `StandardEdgeDefinition`
6. ✅ Add `edge_id: Option<Identifier>` to both edge types
7. ✅ Update schema parsing (backward compatible with existing YAML)

### Phase 2: Denormalized Nodes (Current Sprint)

**Files to modify**:
- Schema processing logic
- Query planner (node scan logic)
- SQL generator

**Tasks**:
1. ✅ Detection: `node.table == edge.table` → denormalized
2. ✅ Build `ProcessedNodeMetadata` with derived properties
3. ✅ Generate UNION queries for standalone node scans
4. ✅ Generate direct column access for edge traversals
5. ✅ Validation: require `from_node_properties`/`to_node_properties` for denormalized nodes

### Phase 3: Composite IDs (Current Sprint)

**Files to modify**:
- Edge uniqueness filters
- Property access logic

**Tasks**:
1. ✅ Parse `edge_id` from YAML (single string or array)
2. ✅ Generate composite equality filters: `WHERE NOT (r1.col1 = r2.col1 AND r1.col2 = r2.col2)`
3. ✅ Default behavior: use `[from_id, to_id]` if `edge_id` not specified
4. ✅ Support node composite IDs (parse `id_column` as `Identifier`)

### Phase 4: Polymorphic Edges (Next Sprint)

**Files to modify**:
- Schema loader
- Query planner (edge type resolution)

**Tasks**:
1. ✅ Detect `polymorphic: true` in YAML
2. ✅ Query ClickHouse to discover edge types
3. ✅ Expand one config → N runtime schemas
4. ✅ Add implicit filters to generated SQL
5. ✅ Optional: Validate discovered types against whitelist

### Phase 5: Testing & Documentation

**Test cases**:
- Composite IDs (single and multi-column)
- Denormalized nodes (OnTime dataset)
- Virtual node queries (UNION generation)
- Denormalized edge traversals (direct access)
- Polymorphic edge discovery
- Polymorphic edge queries

**Documentation**:
- Update STATUS.md
- Update CHANGELOG.md
- Create migration guide for existing schemas
- Add examples to README.md

## Migration Strategy

### Backward Compatibility

**Old format** (still works):
```yaml
relationships:
  - type: AUTHORED
    database: brahmand
    table: authored
    from_id: user_id
    to_id: post_id
    from_node: User
    to_node: Post
```

**Parsing logic**:
```rust
// Accept both "relationships" and "edges" keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSchemaDefinition {
    pub nodes: Vec<NodeDefinition>,
    
    #[serde(default, alias = "relationships")]
    pub edges: Vec<EdgeDefinition>,
}
```

### Deprecation Timeline

- **v0.2.0**: Introduce new edge schema (backward compatible)
- **v0.3.0**: Deprecation warning for `relationships` key
- **v0.4.0**: Remove `relationships` support (breaking change)

## Validation Rules

### Denormalized Nodes

```rust
if node.table == edge.table {
    // Must have property mappings
    if edge.from_node == node.label && edge.from_node_properties.is_none() {
        return Err("Denormalized node requires from_node_properties");
    }
    if edge.to_node == node.label && edge.to_node_properties.is_none() {
        return Err("Denormalized node requires to_node_properties");
    }
}
```

### Polymorphic Edges

```rust
if let EdgeDefinition::Polymorphic(poly) = edge {
    // Must have discovery columns
    if poly.type_column.is_empty() {
        return Err("Polymorphic edge requires type_column");
    }
    if poly.from_label_column.is_empty() {
        return Err("Polymorphic edge requires from_label_column");
    }
    if poly.to_label_column.is_empty() {
        return Err("Polymorphic edge requires to_label_column");
    }
}
```

### Composite IDs

```rust
if let Some(Identifier::Composite(cols)) = &edge.edge_id {
    if cols.is_empty() {
        return Err("Composite edge_id cannot be empty array");
    }
    // All columns must exist in table
    for col in cols {
        validate_column_exists(&edge.table, col)?;
    }
}
```

## Future Extensions (Deferred)

### Filtered Polymorphic Edges

**Not implementing now** - defer until needed:
```yaml
edges:
  - type: FOLLOWS
    table: interactions
    type_column: interaction_type
    type_value: 'follow'  # ← Filter one type from polymorphic table
```

### Multi-Table Nodes

**Not implementing now** - defer until needed:
```yaml
nodes:
  - label: Airport
    table: null  # ← Derive from ALL edges across multiple tables
```

## Success Criteria

- ✅ OnTime benchmark works (denormalized nodes)
- ✅ Composite IDs supported for nodes and edges
- ✅ Polymorphic edges expand correctly
- ✅ Backward compatible with existing schemas
- ✅ All existing tests pass
- ✅ New integration tests for each feature

**Ready to implement! 🚀**
