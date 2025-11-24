# Composite ID Support - Design Document

**Date**: November 22, 2025  
**Status**: 📋 **DESIGN PHASE**  
**Standard**: ISO/IEC 9075-16:2023 (SQL/PGQ), ISO/IEC 39075:2024 (GQL)

## Terminology

Following ISO standards (SQL/PGQ and GQL), we use:
- **NODE** (not vertex) - Graph elements representing entities
- **EDGE** (not relationship) - Graph elements representing connections

**Note**: Neo4j uses "relationship", but both ISO standards use "edge". We align with standards.

## Path Modes (ISO GQL)

ISO GQL defines multiple path matching modes. We implement **TRAIL mode** (edge uniqueness) as the default:

| Mode | Edge Uniqueness | Node Uniqueness | Use Case |
|------|----------------|-----------------|----------|
| **WALK** | ❌ No | ❌ No | Any path (most permissive) |
| **TRAIL** | ✅ Yes | ❌ No | Standard graph traversal (our default) |
| **SIMPLE** | ✅ Yes | ✅ Yes | True simple paths |
| **ACYCLIC** | ✅ Per direction | ✅ Yes | No cycles |

**Current Implementation**: TRAIL mode
- Edges must be unique (no reuse in pattern)
- Nodes can repeat (allows cycles)
- Matches Neo4j behavior
- Covers 90% of real-world use cases

**Future Extension** (deferred): Add `PathMode` enum and query hints for WALK/SIMPLE/ACYCLIC modes when needed.

## Executive Summary

ClickGraph will support **composite IDs for both nodes and edges**, following the SQL/PGQ and GQL standards rather than Neo4j's single-ID model. This enables natural mapping of existing relational schemas and properly handles temporal/multi-instance edges.

## Standards Alignment

### SQL/PGQ (ISO/IEC 9075-16:2023) ✅ **WE FOLLOW THIS**

**Key Principle**: Graph elements map to relational tables, and the PRIMARY KEY becomes element identity.

```sql
-- SQL/PGQ allows this:
CREATE TABLE transfers (
  from_account INT,
  to_account INT,
  timestamp DATETIME,
  amount DECIMAL,
  PRIMARY KEY (from_account, to_account, timestamp)  -- COMPOSITE!
)
```

**Graph mapping**: The PRIMARY KEY `(from_account, to_account, timestamp)` becomes the edge identity.

### Neo4j/openCypher ❌ **We diverge here (intentionally)**

- Single system-generated IDs only
- Composite keys only through property combinations
- **Not a standard requirement, just an implementation choice**

### Our Decision

**Follow SQL/PGQ standard** because:
1. ✅ It's an ISO standard (ISO/IEC 9075-16:2023)
2. ✅ Matches relational world naturally  
3. ✅ More flexible than Neo4j
4. ✅ Enables zero-modification schema mapping
5. ✅ We're a **relational-to-graph** bridge, not a Neo4j clone

## Schema Design

### Current Schema Format

```yaml
nodes:
  - name: User
    table: users
    id_column: user_id  # Single column

relationships:
  - name: FOLLOWS
    table: user_follows
    from_id: follower_id
    to_id: followed_id
    # No relationship_id field (problematic!)
```

### Proposed Schema Format

```yaml
nodes:
  - name: User
    table: users
    node_id: user_id  # Single column
    # OR
    node_id: [user_id]  # Explicit single-element array
    
  - name: Account
    table: accounts
    node_id: [bank_id, account_number]  # Composite!

edges:  # ← Note: "edges" not "relationships"
  - name: FOLLOWS
    table: user_follows
    from_id: follower_id
    to_id: followed_id
    edge_id: id  # ← Note: "edge_id" not "relationship_id"
    # OR
    edge_id: [id]  # Explicit single-element array
    
  - name: TRANSFER
    table: transfers
    from_id: from_account
    to_id: to_account
    edge_id: [from_account, to_account, timestamp]  # Composite!
    
  - name: KNOWS
    table: friendships
    from_id: person1_id
    to_id: person2_id
    # If omitted, defaults to [from_id, to_id]
    edge_id: [person1_id, person2_id]
```

### Field Semantics

#### `node_id` (new, replaces `id_column`)

**Type**: `string | string[]`

**Semantics**: Columns that uniquely identify a node (corresponds to PRIMARY KEY)

**Examples**:
```yaml
node_id: user_id                      # Single column
node_id: [bank_id, account_number]    # Composite
```

**Required**: Yes (every node must have identity)

#### `edge_id` (new)

**Type**: `string | string[]`

**Semantics**: Columns that uniquely identify an edge instance

**Examples**:
```yaml
edge_id: id                                    # Single column
edge_id: [from_id, to_id]                     # Composite (endpoints)
edge_id: [from_id, to_id, timestamp]          # Composite (temporal)
edge_id: [from_account, to_account, txn_id]   # Composite (transactions)
```

**Default**: `[from_id, to_id]` if omitted

**Required**: Recommended (warn if omitted for undirected edges)

### Backward Compatibility

**Migration path** from current `id_column`:

```yaml
# OLD (still supported with deprecation warning)
nodes:
  - name: User
    id_column: user_id

# NEW (preferred)
nodes:
  - name: User
    node_id: user_id
```

**Implementation**: Accept both, prefer `node_id`, emit warning for `id_column`.

## Implementation Details

### Data Structures

**File**: `brahmand/src/graph_catalog/graph_schema.rs`

```rust
/// Identifier specification - can be single column or composite
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Identifier {
    Single(String),           // "user_id"
    Composite(Vec<String>),   // ["bank_id", "account_number"]
}

impl Identifier {
    /// Get all columns involved in the identifier
    pub fn columns(&self) -> Vec<String> {
        match self {
            Identifier::Single(col) => vec![col.clone()],
            Identifier::Composite(cols) => cols.clone(),
        }
    }
    
    /// Check if this is a composite identifier
    pub fn is_composite(&self) -> bool {
        match self {
            Identifier::Single(_) => false,
            Identifier::Composite(cols) => cols.len() > 1,
        }
    }
}

pub struct NodeConfig {
    pub name: String,
    pub table: String,
    pub node_id: Identifier,  // ← NEW
    // ... other fields
}

pub struct EdgeConfig {
    pub name: String,
    pub table: String,
    pub from_id: String,
    pub to_id: String,
    pub edge_id: Option<Identifier>,  // ← NEW
    // ... other fields
}
```

### SQL Generation

**File**: `src/render_plan/plan_builder.rs`

```rust
struct NodeInfo {
    alias: String,
    label: String,
    id_columns: Vec<String>,  // e.g., ["a.bank_id", "a.account_number"]
}

struct EdgeInfo {
    alias: String,
    label: String,
    is_undirected: bool,
    id_columns: Vec<String>,  // e.g., ["r1.from_id", "r1.to_id", "r1.timestamp"]
    from_id_column: String,
    to_id_column: String,
}

/// Generate uniqueness filter for two edges
fn generate_edge_uniqueness_filter(
    e1: &EdgeInfo,
    e2: &EdgeInfo,
) -> Option<String> {
    // Only needed for undirected patterns
    if !e1.is_undirected && !e2.is_undirected {
        return None;
    }
    
    // Build equality conditions for all ID columns
    let forward_conditions: Vec<String> = e1.id_columns.iter()
        .zip(e2.id_columns.iter())
        .map(|(col1, col2)| format!("{} = {}", col1, col2))
        .collect();
    
    let forward_clause = forward_conditions.join(" AND ");
    
    // For undirected, also check reverse direction
    if e1.is_undirected || e2.is_undirected {
        // Check if same edge used in opposite direction
        // For endpoint-only IDs: (e1.from, e1.to) vs (e2.to, e2.from)
        // For composite IDs with timestamps: only check forward
        
        if e1.id_columns.len() == 2 && e2.id_columns.len() == 2 {
            // Simple case: just endpoints
            let reverse_clause = format!(
                "{} = {} AND {} = {}",
                e1.from_id_column, e2.to_id_column,
                e1.to_id_column, e2.from_id_column
            );
            Some(format!("NOT (({}) OR ({}))", forward_clause, reverse_clause))
        } else {
            // Complex case: composite ID with additional columns
            // Only prevent forward direction match (can't reverse temporal keys)
            Some(format!("NOT ({})", forward_clause))
        }
    } else {
        Some(format!("NOT ({})", forward_clause))
    }
}

/// Generate all pairwise uniqueness filters
fn generate_all_edge_uniqueness_filters(
    edges: &[EdgeInfo],
) -> Vec<String> {
    let mut filters = Vec::new();
    
    for i in 0..edges.len() {
        for j in (i + 1)..edges.len() {
            if let Some(filter) = generate_edge_uniqueness_filter(
                &edges[i],
                &edges[j],
            ) {
                filters.push(filter);
            }
        }
    }
    
    filters
}
```

### Example SQL Generation

#### Single Column IDs

**Schema**:
```yaml
edge_id: id
```

**Generated SQL**:
```sql
WHERE NOT (r1.id = r2.id)
```

#### Composite IDs (Endpoints Only)

**Schema**:
```yaml
edge_id: [from_id, to_id]
```

**Generated SQL** (undirected):
```sql
WHERE NOT (
    (r1.from_id = r2.from_id AND r1.to_id = r2.to_id) OR
    (r1.from_id = r2.to_id AND r1.to_id = r2.from_id)
)
```

#### Composite IDs (Temporal)

**Schema**:
```yaml
edge_id: [from_account, to_account, timestamp]
```

**Generated SQL**:
```sql
WHERE NOT (
    r1.from_account = r2.from_account AND
    r1.to_account = r2.to_account AND
    r1.timestamp = r2.timestamp
)
```

**Note**: For temporal IDs, we DON'T check reverse direction (can't reverse time!).

### Default Behavior

**If `edge_id` is omitted**:

1. **Default to** `[from_id, to_id]`
2. **Emit warning** for undirected edges:
   ```
   ⚠️  WARNING: Edge 'FOLLOWS' has no edge_id defined.
   Defaulting to [from_id, to_id]. This assumes each pair of nodes has
   at most one edge of this type. If multiple edges can exist between
   the same nodes, specify edge_id explicitly:
     edge_id: id
     -- OR --
     edge_id: [from_id, to_id, timestamp]
   ```

3. **Generate SQL** using composite key

## Validation Rules

### Schema Validation

**File**: `brahmand/src/graph_catalog/validation.rs`

```rust
fn validate_node_config(node: &NodeConfig) -> Result<(), ValidationError> {
    // node_id is required
    if node.node_id.columns().is_empty() {
        return Err(ValidationError::MissingNodeId(node.name.clone()));
    }
    
    // All ID columns must exist in table
    for col in node.node_id.columns() {
        // Check against table schema
    }
    
    Ok(())
}

fn validate_edge_config(
    edge: &EdgeConfig,
) -> Result<(), ValidationError> {
    // Emit warning if edge_id is missing
    if edge.edge_id.is_none() {
        warn!(
            "Edge '{}' has no edge_id. \
             Defaulting to [from_id, to_id]. \
             This may give incorrect results if multiple edge \
             instances can exist between the same nodes.",
            edge.name
        );
    }
    
    // If specified, validate columns exist
    if let Some(ref id) = edge.edge_id {
        for col in id.columns() {
            // Check against table schema
        }
    }
    
    Ok(())
}
```

## Use Cases

### Use Case 1: Simple Social Graph

**Schema**:
```yaml
nodes:
  - name: User
    table: users
    node_id: user_id

edges:
  - name: FOLLOWS
    table: user_follows
    from_id: follower_id
    to_id: followed_id
    edge_id: id  # Auto-increment primary key
```

**SQL**:
```sql
WHERE NOT (r1.id = r2.id)  -- Simple and fast ✅
```

### Use Case 2: Temporal Edges

**Schema**:
```yaml
edges:
  - name: FRIENDSHIP
    table: friendship_history
    from_id: person1_id
    to_id: person2_id
    edge_id: [person1_id, person2_id, start_date]
```

**Use**: Track multiple friendship periods between same people.

### Use Case 3: Financial Transactions

**Schema**:
```yaml
nodes:
  - name: Account
    table: accounts
    node_id: [bank_id, account_number]  # Composite node ID

edges:
  - name: TRANSFER
    table: transfers
    from_id: from_account_id
    to_id: to_account_id
    edge_id: [from_account_id, to_account_id, transaction_id]
```

**Use**: Multiple transfers between same accounts.

### Use Case 4: Message Graph

**Schema**:
```yaml
edges:
  - name: SENT_MESSAGE
    table: messages
    from_id: sender_id
    to_id: recipient_id
    edge_id: message_id  # Each message is unique
```

**Use**: Multiple messages between same users.

## Testing Strategy

### Unit Tests

1. **Schema Parsing**:
   - Parse `node_id` as string
   - Parse `node_id` as array
   - Parse `edge_id` as string
   - Parse `edge_id` as array
   - Default behavior when `edge_id` omitted

2. **Filter Generation**:
   - Single column ID filter
   - Composite ID filter (2 columns)
   - Composite ID filter (3+ columns)
   - Undirected with endpoint-only ID
   - Undirected with temporal ID

### Integration Tests

1. **Simple ID Test**: Query with single-column IDs
2. **Composite Node ID Test**: Query graph with composite node IDs
3. **Temporal Edge Test**: Multiple edges between same nodes
4. **Undirected Uniqueness Test**: Verify no edge reuse

### Neo4j Comparison Tests

**Important**: Neo4j doesn't support composite IDs, so we can't do 1:1 comparison.

**Strategy**: 
- Test correctness independently
- Verify uniqueness properties hold
- Document divergence from Neo4j (by design, following SQL/PGQ)

## Migration Plan

### Phase 1: Schema Enhancement (Week 1)

- ✅ Add `Identifier` enum to schema types
- ✅ Update parsers to accept `node_id` and `relationship_id`
- ✅ Support backward compatibility with `id_column`
- ✅ Add validation and warnings

### Phase 2: SQL Generation (Week 1)

- ✅ Update `NodeInfo` and `EdgeInfo` structures
- ✅ Implement composite filter generation
- ✅ Handle undirected special cases
- ✅ Add unit tests

### Phase 3: Integration Testing (Week 2)

- ✅ Create test schemas with composite IDs
- ✅ Test temporal edges
- ✅ Test undirected patterns
- ✅ Performance testing

### Phase 4: Documentation (Week 2)

- ✅ Update schema documentation
- ✅ Add examples for common patterns
- ✅ Document SQL/PGQ and GQL alignment
- ✅ Explain divergence from Neo4j

## Performance Considerations

### Single Column IDs ✅ **FAST**

```sql
WHERE NOT (r1.id = r2.id)
```

- Single comparison
- Index-friendly
- Optimal performance

### Composite IDs (2-3 columns) ✅ **ACCEPTABLE**

```sql
WHERE NOT (r1.col1 = r2.col1 AND r1.col2 = r2.col2 AND r1.col3 = r2.col3)
```

- Multiple comparisons but short-circuit evaluation
- Composite indexes can help
- Acceptable overhead for correctness

### Many Edges (N²) ⚠️ **WATCH**

For pattern with N edges, need O(N²) pairwise filters.

**Example**: 5 edges = 10 filters

**Mitigation**:
- Only generate for undirected edges
- Typical queries have 2-3 edges
- Optimize with CTE structure

## Open Questions

### Q1: Should we support `id()` function in Cypher?

**Options**:
1. Don't support (user uses actual ID columns)
2. Support `id(n)` returning first ID column
3. Support `id(n)` returning composite tuple

**Recommendation**: Start without `id()`, add later if needed.

### Q2: How to handle node/edge identity in RETURN clause?

**Example**:
```cypher
MATCH (a:User)-[r:FOLLOWS]->(b:User)
RETURN a, r, b
```

**Options**:
1. Return all columns (current behavior)
2. Return ID columns + properties
3. Return only properties (identity implicit)

**Recommendation**: Keep current behavior (return all columns).

### Q3: Should defaults differ for directed vs undirected?

**Current proposal**: Same default `[from_id, to_id]` for both

**Alternative**: 
- Directed: default to `[from_id, to_id]`
- Undirected: require explicit `relationship_id`

**Recommendation**: Keep same default, but warn for undirected.

## Denormalized Edge Tables with Virtual Nodes

### Use Case: OnTime Flight Dataset

Real-world data warehouses often have **denormalized tables** where node properties are embedded directly in edge tables:

```
ontime table:
FlightDate | FlightNum | Origin | Dest | OriginCityName | OriginState | DestCityName | DestState
2024-01-15 | AA100     | SEA    | JFK  | Seattle        | WA          | New York     | NY
2024-01-15 | UA200     | JFK    | LAX  | New York       | NY          | Los Angeles  | CA
```

This single table represents:
- **Edges**: Flights from origin to destination
- **Nodes**: Airports (appearing twice per row with different column prefixes)

### Schema Design

**Simple, explicit model** (start with this):

```yaml
nodes:
  - name: Airport
    table: ontime      # ← Explicit table reference (same as edge)

edges:
  - name: FLIGHT
    table: ontime
    from_id: Origin
    to_id: Dest
    from_node: Airport
    to_node: Airport
    edge_id: [FlightDate, FlightNum, Origin, Dest]  # ← Composite ID
    from_node_properties:
      city: OriginCityName
      state: OriginState
    to_node_properties:
      city: DestCityName
      state: DestState
```

**Key Design Decisions**:

1. **Node declares table explicitly** (`table: ontime`)
   - Clear that node and edge share the same physical table
   - No `node_id` or `properties` in node definition (derived from edges)

2. **Properties belong to edge definition**
   - `from_node_properties` and `to_node_properties` resolve column ambiguity
   - Physical location matches logical organization

3. **Detection**: Schema processor checks `node.table == edge.table`
   - If true, node is virtual/denormalized
   - Derive node metadata from edge's property mappings

### Query Processing

**Cypher Query**:
```cypher
MATCH (a:Airport) WHERE a.city = 'Seattle' RETURN a.code, a.city
```

**Generated SQL** (UNION of from/to roles):
```sql
SELECT Origin as code, OriginCityName as city
FROM ontime
WHERE OriginCityName = 'Seattle'

UNION ALL

SELECT Dest as code, DestCityName as city
FROM ontime
WHERE DestCityName = 'Seattle'
```

**Edge Query** (direct access, no JOIN):
```cypher
MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)
WHERE a.city = 'Seattle' AND b.city = 'New York'
RETURN f.date, a.city, b.city
```

**Generated SQL** (single table scan):
```sql
SELECT 
  FlightDate,
  OriginCityName as from_city,
  DestCityName as to_city
FROM ontime
WHERE OriginCityName = 'Seattle' AND DestCityName = 'New York'
```

### Internal Metadata (Schema Processing)

```rust
struct ProcessedNodeMetadata {
    name: String,
    table: String,  // "ontime"
    
    // Derived from edges that reference this node and share the same table
    derived_id_sources: Vec<IdSource>,
    // e.g., [
    //   IdSource { column: "Origin", role: From, edge: "FLIGHT" },
    //   IdSource { column: "Dest", role: To, edge: "FLIGHT" }
    // ]
    
    derived_properties: HashMap<String, Vec<PropertySource>>,
    // e.g., "city" -> [
    //   PropertySource { column: "OriginCityName", role: From, edge: "FLIGHT" },
    //   PropertySource { column: "DestCityName", role: To, edge: "FLIGHT" }
    // ]
}
```

### Future Extension (Deferred)

**Multi-table nodes** (`table: null`):
```yaml
nodes:
  - name: Airport
    table: null  # ← Derive from ALL edges, across multiple tables
```

This would support scenarios where Airport appears in multiple edge tables (flights, cargo, etc.) with potentially different property schemas. **Defer until we have a real use case** - starts simple!

### Validation

During schema processing:
```rust
for node in &config.nodes {
    for edge in &config.edges {
        if node.table == edge.table && 
           (edge.from_node == node.name || edge.to_node == node.name) {
            
            // Denormalized detected!
            if edge.from_node == node.name && edge.from_node_properties.is_none() {
                return Err("Denormalized node requires from_node_properties");
            }
            if edge.to_node == node.name && edge.to_node_properties.is_none() {
                return Err("Denormalized node requires to_node_properties");
            }
        }
    }
}
```

## Conclusion

Supporting composite IDs aligns ClickGraph with:
- ✅ SQL/PGQ standard (ISO/IEC 9075-16:2023)
- ✅ Relational database reality
- ✅ Zero-modification schema mapping
- ✅ Denormalized data warehouse patterns (OnTime, PuppyGraph benchmarks)

This is a **principled divergence from Neo4j** based on following ISO standards and matching the relational-to-graph bridge use case.

**Next Steps**:
1. Implement `Identifier` enum and schema parsing
2. Add `edge_id`, `from_node_properties`, `to_node_properties` to EdgeConfig
3. Implement denormalized node detection and metadata derivation
4. Update SQL generation for composite IDs and virtual nodes
5. Add comprehensive tests (including OnTime-style denormalized data)
6. Document the design and rationale
