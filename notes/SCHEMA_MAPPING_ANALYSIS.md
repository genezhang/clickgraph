# Schema Mapping Architecture Analysis

**Date**: December 17, 2025  
**Purpose**: Comprehensive review of graph↔relational mapping consistency

## Problem Statement

We need **two parallel views** of the same data:
1. **Graph Space**: Labels, types, properties (for Cypher query planning)
2. **Relational Space**: Tables, columns (for SQL generation)

These must be kept **consistent and bidirectional** throughout the system.

---

## Current Schema Representations

### 1. YAML Configuration (User Input)

```yaml
nodes:
  - label: Customer          # 🟢 GRAPH: Node label
    table: customers_mem     # 🔵 RELATIONAL: Table name
    database: brahmand       # 🔵 RELATIONAL: Database
    node_id: customer_id     # 🔵 RELATIONAL: Column name
    property_mappings:
      email: email_address   # 🟢 GRAPH property → 🔵 RELATIONAL column

edges:
  - type: PURCHASED          # 🟢 GRAPH: edge type
    table: orders_mem        # 🔵 RELATIONAL: Table name
    from_node: Customer      # 🟢 GRAPH: Node label
    to_node: Product         # 🟢 GRAPH: Node label
    from_id: customer_id     # 🔵 RELATIONAL: Column name
    to_id: product_id        # 🔵 RELATIONAL: Column name
```

**Analysis**: YAML correctly maintains BOTH graph and relational info.
- `label` / `type` = Graph space
- `table` / `database` / column names = Relational space
- `from_node` / `to_node` = Graph labels (NOT table names)

---

### 2. Internal NodeSchema (Runtime)

```rust
pub struct NodeSchema {
    // RELATIONAL INFO (Primary)
    pub database: String,           // "brahmand"
    pub table_name: String,         // "customers_mem"
    pub column_names: Vec<String>,
    pub node_id: NodeIdSchema,      // Column(s) for PK
    
    // GRAPH ↔ RELATIONAL MAPPING
    pub property_mappings: HashMap<String, PropertyValue>,
    //   🟢 "email" → 🔵 PropertyValue::Column("email_address")
    
    // ... additional fields ...
}
```

**Key Insight**: NodeSchema is keyed by **label** in HashMap but stores **table** internally.

```rust
// From config.rs line 1231:
nodes.insert(node_def.label.clone(), node_schema);
//           ^^^^^^^^^^^^^^^^^^^^ KEY = GRAPH LABEL
//                                     VALUE.table_name = RELATIONAL TABLE
```

This bidirectional mapping works because:
- **Lookup by label** → Get table + property mappings
- **Table stored inside** → Can generate SQL

---

### 3. Internal RelationshipSchema (Runtime) - **INCONSISTENT!**

#### Current State (After My Changes):

```rust
pub struct RelationshipSchema {
    // RELATIONAL INFO
    pub database: String,
    pub table_name: String,
    
    // ⚠️ CONFUSION POINT:
    pub from_node: String,        // Currently stores TABLE NAME
    pub to_node: String,          // Currently stores TABLE NAME
    
    // NEW FIELDS (Just added):
    pub from_node_label: Option<String>,  // Graph label
    pub to_node_label: Option<String>,    // Graph label
    
    // ID COLUMNS (Relational)
    pub from_id: String,          // Column name
    pub to_id: String,            // Column name
    
    // DENORMALIZED NODE PROPERTIES
    pub from_node_properties: Option<HashMap<String, String>>,
    pub to_node_properties: Option<HashMap<String, String>>,
}
```

#### The Inconsistency:

**Original Design Intent** (from YAML):
- `from_node` / `to_node` in YAML = **Graph labels**

**Current Implementation** (after label→table resolution):
- `from_node` / `to_node` in RelationshipSchema = **Table names**

**What's Broken**:
```rust
// In cte_generation.rs line 664:
if rel_schema.from_node == node_label {  // ❌ COMPARING TABLE vs LABEL!
    // Trying to match "users_bench" (table) == "User" (label)
    // This NEVER matches!
}
```

---

## Schema Evolution History

### Phase 1: Simple Schema (Original)
```yaml
relationships:
  - type: FOLLOWS
    table: follows
    from_id: user_id1
    to_id: user_id2
    # NO from_node/to_node specified
```

**Assumption**: One table per label, labels implicit.

### Phase 2: Multi-Label Support
```yaml
nodes:
  - label: University       # Multiple labels...
    table: organisation     # ...same table
  - label: Company
    table: organisation

relationships:
  - type: WORKS_AT
    from_node: Person       # Now NEED explicit labels
    to_node: Company        # To disambiguate
```

**Problem**: `from_node`/`to_node` added, but code treated them as table names.

### Phase 3: Denormalized Nodes (Current)
```yaml
nodes:
  - label: Airport
    table: flights         # NO separate airport table!
    from_node_properties:  # Properties when appearing as FROM
      code: origin_code
    to_node_properties:    # Properties when appearing as TO
      code: dest_code
```

**Problem**: Now we need labels for property resolution, not just table lookup.

---

## Where Mappings Are Used

### Graph Space Operations (Need LABELS):

1. **Query Parsing**: `MATCH (u:User)` → Need to know "User" is a valid label
2. **Property Resolution**: `u.name` → Which column? Depends on label + role (from/to)
3. **Pattern Matching**: `[:FOLLOWS]` → Need to know source/target labels
4. **Denormalized Properties**: Must match label to determine from_props vs to_props

### Relational Space Operations (Need TABLES):

1. **SQL Generation**: `FROM users_bench` ← Need table name
2. **JOIN Construction**: `users_bench.user_id = follows.follower_id`
3. **Column References**: `users_bench.full_name`

---

## Proposed Consistent Architecture

### Option A: Separate Label Fields (Current Approach)

```rust
pub struct RelationshipSchema {
    // RELATIONAL: Table references
    pub from_node: String,              // "users_bench" (table)
    pub to_node: String,                // "posts_bench" (table)
    
    // GRAPH: Label references
    pub from_node_label: String,        // "User" (label) - REQUIRED
    pub to_node_label: String,          // "Post" (label) - REQUIRED
    
    // ... rest unchanged
}
```

**Pros**:
- Clear separation of concerns
- Backward compatible (from_node/to_node stay as tables)
- Easy to understand: label for graph logic, table for SQL

**Cons**:
- Redundant storage
- Need to update both fields consistently
- More fields to maintain

---

### Option B: Store Labels, Resolve Tables On-Demand

```rust
pub struct RelationshipSchema {
    // GRAPH: Primary storage (labels)
    pub from_node_label: String,        // "User" (PRIMARY)
    pub to_node_label: String,          // "Post" (PRIMARY)
    
    // NO from_node/to_node fields
    
    // ... rest unchanged
}

// Helper method:
impl RelationshipSchema {
    pub fn from_table<'a>(&self, schema: &'a GraphSchema) -> &'a str {
        schema.get_node_table(&self.from_node_label)
    }
}
```

**Pros**:
- Single source of truth (labels)
- Matches YAML semantics
- Cleaner conceptually

**Cons**:
- **BREAKING CHANGE** - 50+ usage sites need updates
- Requires schema context for table lookups
- Performance: Extra HashMap lookups

---

### Option C: Store Both Explicitly in YAML

```yaml
edges:
  - type: FOLLOWS
    from_node: User           # Graph label
    from_table: users_bench   # Relational table
    to_node: User
    to_table: users_bench
```

**Pros**:
- Explicit, no ambiguity
- No resolution needed
- User controls mapping

**Cons**:
- More verbose config
- Redundant (table can be inferred from label)
- Violates DRY principle

---

## Recommended Solution: **Option A (Enhanced)**

### Implementation Plan:

#### 1. Make Label Fields Required
```rust
pub struct RelationshipSchema {
    pub from_node: String,              // Table name (for SQL)
    pub to_node: String,                // Table name (for SQL)
    pub from_node_label: String,        // ✅ REQUIRED (not Option)
    pub to_node_label: String,          // ✅ REQUIRED (not Option)
}
```

#### 2. Update Config Loading
```rust
// In build_fk_edge_schema() and build_standard_edge_schema():
Ok(RelationshipSchema {
    from_node: from_node_table,         // Resolved table
    to_node: to_node_table,             // Resolved table
    from_node_label: from_node,         // Original label from YAML
    to_node_label: to_node,             // Original label from YAML
    // ...
})
```

#### 3. Update Usage Sites (50+ locations)

**Pattern 1: Label Comparisons** (for property resolution)
```rust
// BEFORE:
if rel_schema.from_node == node_label { ... }

// AFTER:
if rel_schema.from_node_label == node_label { ... }
```

**Pattern 2: Table Usage** (for SQL generation)
```rust
// KEEP AS-IS:
format!("FROM {}", rel_schema.from_node)  // Still uses table name
```

#### 4. Add Validation
```rust
impl RelationshipSchema {
    pub fn validate(&self, schema: &GraphSchema) -> Result<()> {
        // Ensure labels resolve to tables
        schema.get_node_table(&self.from_node_label)?;
        schema.get_node_table(&self.to_node_label)?;
        Ok(())
    }
}
```

---

## Testing Strategy

### 1. Unit Tests
```rust
#[test]
fn test_relationship_schema_label_resolution() {
    let rel_schema = RelationshipSchema {
        from_node: "users_bench".to_string(),      // Table
        from_node_label: "User".to_string(),       // Label
        // ...
    };
    
    // Label for graph reasoning
    assert_eq!(rel_schema.from_node_label, "User");
    
    // Table for SQL generation
    assert_eq!(rel_schema.from_node, "users_bench");
}
```

### 2. Integration Tests
- VLP + WITH query (currently failing)
- Denormalized node property resolution
- Multi-label scenarios

---

## Migration Path

### Phase 1: Add Fields (Non-Breaking) ✅ DONE
- Added `from_node_label` / `to_node_label` as `Option<String>`
- Populate during config loading
- All existing code still works

### Phase 2: Update Usage Sites (In Progress)
- Identify all 50+ locations using `rel_schema.from_node` for label logic
- Update to use `from_node_label` instead
- Keep table-based usage unchanged

### Phase 3: Make Required (Breaking)
- Change from `Option<String>` to `String`
- Add validation to ensure always populated

### Phase 4: Cleanup
- Document the dual-nature of relationship schema
- Add inline comments explaining graph vs relational fields

---

## Questions for Resolution

1. **Should `from_node_label` be required or optional?**
   - Recommendation: Required (always needed for correct semantics)

2. **Should we rename `from_node` to `from_table` for clarity?**
   - Recommendation: Yes, but in separate refactoring (too many changes)

3. **How to handle polymorphic edges?**
   - Current: `from_label_values: Vec<String>` for allowed labels
   - Seems correct - need to verify consistency

4. **Filter handling with labels?**
   - Filters reference columns (relational space)
   - Applied after label resolution (graph space)
   - Current design seems OK

---

## Conclusion

**Root Cause**: The field named `from_node` has been ambiguous:
- YAML: Uses it for labels (graph space)
- Code: Resolves to tables (relational space)
- Usage: Mixed (some expect labels, some expect tables)

**Solution**: Explicit separation:
- `from_node` / `to_node` = Tables (relational)
- `from_node_label` / `to_node_label` = Labels (graph)

This gives us **two parallel, consistent mappings** at every level.
