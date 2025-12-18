# Composite Node ID Implementation Plan

**Status**: Phase 1 Complete ✅ (Semantic Clarification - December 18, 2025)  
**Target Release**: v0.6.0  
**Estimated Effort**: Phase 1: ✅ Done | Phase 2: 2-3 days remaining  
**Priority**: HIGH (Required for real-world applications)

---

## Phase 1: Semantic Clarification ✅ COMPLETE (December 18, 2025)

**Goal**: Establish that `node_id` represents property names (graph layer), not column names

**Changes Made**:
1. **Auto-Identity Mappings**: `build_node_property_mappings()` auto-generates identity mappings
   - `node_id: user_id` → auto-adds `property_mappings: {user_id: user_id}`
   - Explicit mappings take precedence over auto-generated ones
2. **Documentation**: Updated NodeIdSchema and NodeDefinition comments
3. **Testing**: Added 3 tests for identity mapping behavior (649/649 passing)

**Impact**:
- ✅ Consistent semantics: node_id is always property names
- ✅ Backward compatible: existing schemas work unchanged
- ✅ Denormalized edges naturally supported (already using property names)
- ✅ Aligns with edge_id pattern (both use Identifier type)

**Commit**: `3b2a750` - feat: Clarify node_id semantics as property names

---

## Phase 2: Composite Node ID Support (TODO)

**Goal**: Enable multi-column `node_id` for composite primary keys

---

## Executive Summary

**What**: Support multi-column `node_id` for nodes with composite primary keys  
**Why**: Real-world applications have composite node identifiers (e.g., `[tenant_id, user_id]`, `[bank_id, account_number]`)  
**Status**: Phase 1 complete (semantics unified), Phase 2 pending (SQL generation)  
**Blocker**: Some code uses `NodeIdSchema.column()` which **PANICS** on composite keys

---

## Current State Analysis

### ✅ What Already Works (Composite Edge IDs)

**Edge composite IDs are FULLY implemented** (v0.5.0+):

```yaml
# This already works! ✅
relationships:
  - type: FLIGHT
    table: flights
    from_id: origin_airport
    to_id: dest_airport
    edge_id: [FlightDate, Flight_Number_Reporting_Airline, OriginAirportID, DestAirportID]
```

**Generated SQL** (variable-length paths):
```sql
tuple(rel.FlightDate, rel.Flight_Number_Reporting_Airline, rel.OriginAirportID, rel.DestAirportID)
```

**Evidence**: 8 YAML files use composite `edge_id`, tests passing

---

### ⚠️ What Partially Works (Composite Node IDs)

**Node composite IDs infrastructure is READY** (Phase 1 complete):

```yaml
# This will LOAD correctly but may PANIC at query time ⚠️
nodes:
  - label: Account
    table: accounts
    node_id: [bank_id, account_number]  # Infrastructure exists but unused
```

**Why it panics**:
```rust
// src/graph_catalog/graph_schema.rs:333
pub fn column(&self) -> &str {
    self.id.as_single()  // ⚠️ Panics if Identifier::Composite
}
```

**30+ call sites** throughout codebase use `.column()` - all will panic on composite!

---

## Architecture Analysis

### Type System (Already Complete ✅)

**File**: `src/graph_catalog/config.rs` (lines 13-46)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]  // ← Allows YAML: node_id: user_id OR node_id: [col1, col2]
pub enum Identifier {
    Single(String),
    Composite(Vec<String>),
}

impl Identifier {
    pub fn columns(&self) -> Vec<String>  // ✅ Safe for both
    pub fn is_composite(&self) -> bool    // ✅ Type check
    pub fn as_single(&self) -> &str       // ⚠️ Panics on composite
}
```

**Status**: ✅ Fully functional, tested, used for edge_id

---

### Schema Types (Partially Ready)

**File**: `src/graph_catalog/graph_schema.rs` (lines 300-343)

```rust
pub struct NodeIdSchema {
    pub id: Identifier,  // ✅ Already supports Composite!
    pub dtype: String,
}

impl NodeIdSchema {
    pub fn single(column: String, dtype: String) -> Self { ... }     // ✅ Used
    pub fn composite(columns: Vec<String>, dtype: String) -> Self { ... }  // ❌ UNUSED
    
    pub fn column(&self) -> &str {
        self.id.as_single()  // ⚠️ PANIC SITE
    }
    
    pub fn is_composite(&self) -> bool { ... }  // ✅ Available
}
```

**Issues**:
1. `composite()` constructor exists but **never called** in codebase
2. `column()` method **panics** on composite - used everywhere
3. Need new composite-safe accessor methods

---

### Critical Call Sites Audit (30+ locations)

**Grep results**: `node_id.column()` appears 30+ times

#### **Category 1: SQL Generation (render_plan/)**

**render_expr.rs** (8 matches, lines 58, 64, 160, 166, 173, 179, 323, 329, 335, 341):
```rust
// Pattern matching: size(), EXISTS, NOT EXISTS
node_schema.node_id.column().to_string()  // ⚠️ Used in JOIN conditions
```

**Impact**: All pattern count/exists queries will panic  
**Fix**: Replace with composite-aware tuple generation

---

**cte_extraction.rs** (line 459):
```rust
// Variable-length path ID column lookup
return node_schema.node_id.column().to_string();
```

**Impact**: VLP queries will panic  
**Fix**: Return `Vec<String>` and handle tuple comparison

---

**plan_builder_helpers.rs** (lines 1075, 1139):
```rust
// Helper function for ID column lookup
return user_node.node_id.column().to_string();
```

**Impact**: All relationship traversals will panic  
**Fix**: Refactor helpers to return `Vec<String>`

---

#### **Category 2: Query Planning (query_planner/)**

**analyzer/graph_context.rs** (lines 175-176):
```rust
let left_node_id_column = left_schema.node_id.column().to_string();
let right_node_id_column = right_schema.node_id.column().to_string();
// Used in JOIN condition generation
```

**Impact**: All relationship JOINs will panic  
**Fix**: Generate tuple equality for composite

---

**logical_plan/match_clause.rs** (line 794):
```rust
node_schema.node_id.column().to_string(), // ID column from schema
```

**Impact**: MATCH clause planning will panic  
**Fix**: Store `Vec<String>` in logical plan

---

**analyzer/projection_tagging.rs** (lines 589, 814, 851):
```rust
node_schema.node_id.column().to_string()
```

**Impact**: Property access tagging will panic  
**Fix**: Handle composite ID property access

---

**analyzer/filter_tagging.rs** (lines 735, 750):
```rust
node_schema.node_id.column().to_string()
```

**Impact**: WHERE clause tagging will panic  
**Fix**: Tag composite columns separately

---

#### **Category 3: Schema Catalog (graph_catalog/)**

**pattern_schema.rs** (lines 629, 636, 653, 668, 704):
```rust
id_column: left_node_schema.node_id.column().to_string(),
```

**Impact**: Pattern schema compilation will panic  
**Fix**: Store `Vec<String>` in PatternNode

---

**config.rs** (line 600):
```rust
primary_keys: node_def.node_id.as_single().to_string(),
```

**Impact**: Schema config generation will panic  
**Fix**: Generate comma-separated list for composite

---

#### **Category 4: ClickHouse Query Generator**

**clickhouse_query_generator/pagerank.rs** (lines 221, 225):
```rust
node_schema.node_id.column(), node_schema.table_name
```

**Impact**: PageRank algorithm will panic  
**Fix**: PageRank needs special handling for composite IDs

---

### Summary of Impact

| Component | Call Sites | Risk | Effort |
|-----------|-----------|------|--------|
| render_expr.rs | 10 | 🔴 HIGH (pattern matching) | 2-3 hours |
| cte_extraction.rs | 1 | 🔴 HIGH (VLP core) | 1-2 hours |
| plan_builder_helpers.rs | 2 | 🟡 MEDIUM | 1 hour |
| graph_context.rs | 2 | 🔴 HIGH (JOIN generation) | 2 hours |
| pattern_schema.rs | 5 | 🟡 MEDIUM | 1 hour |
| projection_tagging.rs | 3 | 🟡 MEDIUM | 1 hour |
| filter_tagging.rs | 2 | 🟡 MEDIUM | 1 hour |
| match_clause.rs | 1 | 🟡 MEDIUM | 30 min |
| pagerank.rs | 2 | 🟠 LOW (algorithm-specific) | 1 hour |
| config.rs | 1 | 🟠 LOW (metadata) | 30 min |

**Total Estimated Effort**: 12-15 hours (2 days)

---

## Design Decisions

### Decision 1: JOIN Syntax for Composite Node IDs

**Problem**: How to compare composite node IDs in JOINs?

**Options**:
1. ✅ **Tuple equality** (ClickHouse native): `(a.c1, a.c2) = (b.c1, b.c2)`
2. ❌ AND chain: `a.c1 = b.c1 AND a.c2 = b.c2`

**Choice**: **Tuple equality** (Option 1)

**Rationale**:
- Already used for composite `edge_id` (proven pattern)
- ClickHouse optimizes tuples well
- Cleaner SQL generation
- Consistent with edge_id implementation

**Example**:
```sql
-- Single node_id (current)
INNER JOIN users_bench u2 ON r.followed_id = u2.user_id

-- Composite node_id (new)
INNER JOIN accounts a2 ON (r.to_bank_id, r.to_account_num) = (a2.bank_id, a2.account_number)
```

---

### Decision 2: API Design for Composite-Safe Accessors

**Problem**: `column()` panics, need safe alternatives

**Proposed API**:

```rust
impl NodeIdSchema {
    // Keep existing methods
    pub fn column(&self) -> &str { self.id.as_single() }  // Keep for backwards compat, document panic
    
    // NEW: Composite-safe accessors
    pub fn columns(&self) -> Vec<String> {
        self.id.columns()
    }
    
    pub fn columns_with_alias(&self, alias: &str) -> Vec<String> {
        self.id.columns().iter()
            .map(|col| format!("{}.{}", alias, col))
            .collect()
    }
    
    pub fn sql_tuple(&self, alias: &str) -> String {
        let cols = self.columns_with_alias(alias);
        if cols.len() == 1 {
            cols[0].clone()  // No tuple for single column
        } else {
            format!("({})", cols.join(", "))
        }
    }
    
    pub fn sql_equality(&self, left_alias: &str, right_alias: &str) -> String {
        let left = self.sql_tuple(left_alias);
        let right = self.sql_tuple(right_alias);
        format!("{} = {}", left, right)
    }
}
```

**Usage**:
```rust
// Old (panics on composite)
let id_col = node_schema.node_id.column();
sql.push_str(&format!("ON r.to_id = {}.{}", alias, id_col));

// New (composite-safe)
let join_cond = node_schema.node_id.sql_equality("r", &alias);
sql.push_str(&format!("ON {}", join_cond));
```

---

### Decision 3: Property Access for Composite IDs

**Problem**: What does `n.id` return when `node_id: [col1, col2]`?

**Options**:
1. ❌ Error: "Cannot access composite ID as property"
2. ❌ Return first column only (confusing)
3. ✅ **Return tuple**: `tuple(n.col1, n.col2)`
4. ⏸️ Deferred: Allow `n.id[0]`, `n.id[1]` (array syntax)

**Choice**: **Return tuple** (Option 3)

**Rationale**:
- Consistent with edge_id behavior (already returns tuples)
- Preserves full identity information
- Works with ClickHouse tuple functions
- Simple to implement

**Example**:
```cypher
MATCH (a:Account) WHERE a.bank_id = 'chase' RETURN a.id, a.balance

-- Generated SQL
SELECT 
  tuple(a.bank_id, a.account_number) as id,  -- Composite ID as tuple
  a.balance
FROM accounts a
WHERE a.bank_id = 'chase'
```

---

### Decision 4: YAML Syntax

**Already decided** (by existing Identifier implementation):

```yaml
# Single column (backwards compatible)
node_id: user_id

# Composite (array syntax)
node_id: [bank_id, account_number]
```

**No changes needed** - serde untagged parsing already works!

---

## Implementation Plan

### Phase 1: API Design & Core Infrastructure (4 hours)

**Files**:
- `src/graph_catalog/graph_schema.rs`
- `src/graph_catalog/config.rs`

**Tasks**:
1. Add new methods to `NodeIdSchema`:
   - `columns()` - returns `Vec<String>`
   - `columns_with_alias(&str)` - returns `Vec<String>` with table prefix
   - `sql_tuple(&str)` - returns `String` (single col or tuple)
   - `sql_equality(&str, &str)` - returns `String` JOIN condition
   
2. Update `NodeIdSchema::column()` documentation:
   ```rust
   /// Get the column name for single-column identifiers.
   /// **PANICS** if called on composite identifier.
   /// For composite-safe access, use `columns()` or `sql_tuple()`.
   pub fn column(&self) -> &str
   ```

3. Add helper to `Identifier`:
   ```rust
   pub fn to_sql_tuple(&self, alias: &str) -> String {
       match self {
           Identifier::Single(col) => format!("{}.{}", alias, col),
           Identifier::Composite(cols) => {
               let fields = cols.iter()
                   .map(|c| format!("{}.{}", alias, c))
                   .collect::<Vec<_>>()
                   .join(", ");
               format!("({})", fields)
           }
       }
   }
   ```

4. Write unit tests:
   - Test `sql_tuple()` with single column → no tuple wrapper
   - Test `sql_tuple()` with 2 columns → tuple wrapper
   - Test `sql_equality()` generation

**Validation**: Run `cargo test graph_catalog`

---

### Phase 2: SQL Generation - JOIN Conditions (3 hours)

**Files**:
- `src/render_plan/plan_builder_helpers.rs`
- `src/query_planner/analyzer/graph_context.rs`

**Tasks**:

1. **plan_builder_helpers.rs** (lines 1075, 1139):
   
   **Current**:
   ```rust
   fn table_to_id_column(...) -> String {
       return user_node.node_id.column().to_string();
   }
   ```
   
   **Replace with**:
   ```rust
   fn table_to_id_columns(...) -> Vec<String> {
       return user_node.node_id.columns();
   }
   
   fn generate_node_join_condition(
       rel_alias: &str,
       node_alias: &str,
       node_schema: &NodeSchema,
       from_or_to: &str, // "from_id" or "to_id"
   ) -> String {
       node_schema.node_id.sql_equality(rel_alias, node_alias)
   }
   ```

2. **graph_context.rs** (lines 175-176):
   
   **Current**:
   ```rust
   let left_node_id_column = left_schema.node_id.column().to_string();
   let right_node_id_column = right_schema.node_id.column().to_string();
   
   // Later used in: format!("r.{} = {}.{}", left_node_id_column, ...)
   ```
   
   **Replace with**:
   ```rust
   // Generate full JOIN condition instead of just column names
   let join_condition = format!(
       "{} = {}",
       left_schema.node_id.sql_tuple("r"),
       right_schema.node_id.sql_tuple(&left_alias)
   );
   ```

3. Update all callers to use new `generate_node_join_condition()` helper

**Validation**: 
- Run integration test: `cargo test test_basic_match`
- Verify generated SQL has tuple JOINs for composite IDs

---

### Phase 3: Pattern Matching (size/EXISTS) (3 hours)

**Files**:
- `src/render_plan/render_expr.rs`

**Tasks**:

1. **generate_pattern_count_sql()** (lines 149-185):
   
   **Current** (lines 160, 166):
   ```rust
   node_schema.node_id.column().to_string()
   ```
   
   **Replace with**:
   ```rust
   // For FROM node JOIN
   let from_join = format!(
       "INNER JOIN {} {} ON {}",
       from_schema.table_name,
       from_alias,
       rel_schema.from_node.node_id.sql_equality(rel_alias, from_alias)
   );
   
   // For TO node JOIN
   let to_join = format!(
       "INNER JOIN {} {} ON {}",
       to_schema.table_name,
       to_alias,
       rel_schema.to_node.node_id.sql_equality(rel_alias, to_alias)
   );
   ```

2. **generate_exists_sql()** (lines 290-360):
   - Same pattern as above
   - Update lines 323, 329, 335, 341

3. **generate_not_exists_sql()** (if exists, similar pattern)

**Validation**:
- Run test: `cargo test test_size_on_patterns`
- Verify SQL generation with composite node IDs

---

### Phase 4: Variable-Length Paths (2 hours)

**Files**:
- `src/render_plan/cte_extraction.rs`

**Tasks**:

1. **table_to_id_column()** (line 459):
   
   **Current**:
   ```rust
   fn table_to_id_column(...) -> String {
       node_schema.node_id.column().to_string()
   }
   ```
   
   **Replace with**:
   ```rust
   fn table_to_id_columns(...) -> Vec<String> {
       node_schema.node_id.columns()
   }
   
   fn table_to_id_tuple(...) -> String {
       node_schema.node_id.sql_tuple(alias)
   }
   ```

2. Update VLP CTE generation to use tuple comparisons:
   ```sql
   -- Base case
   WHERE (start.col1, start.col2) = (@param_col1, @param_col2)
   
   -- Recursive case JOIN
   INNER JOIN nodes n ON (vp.end_col1, vp.end_col2) = (n.col1, n.col2)
   ```

3. Update cycle prevention filters for composite node IDs

**Validation**:
- Run VLP test: `cargo test test_variable_length_path`
- Test with composite node IDs

---

### Phase 5: Query Planning Updates (2 hours)

**Files**:
- `src/query_planner/logical_plan/match_clause.rs`
- `src/query_planner/analyzer/projection_tagging.rs`
- `src/query_planner/analyzer/filter_tagging.rs`
- `src/graph_catalog/pattern_schema.rs`

**Tasks**:

1. **match_clause.rs** (line 794):
   - Change field from `id_column: String` to `id_columns: Vec<String>`
   
2. **projection_tagging.rs** (lines 589, 814, 851):
   - Update property tagging to handle composite IDs
   - When accessing `n.id` property, return tuple expression
   
3. **filter_tagging.rs** (lines 735, 750):
   - Update filter tagging to handle composite IDs
   - Generate tuple comparisons for ID filters

4. **pattern_schema.rs** (lines 629, 636, 653, 668, 704):
   - Change `id_column: String` to `id_columns: Vec<String>` in PatternNode
   - Update all constructors

**Validation**:
- Run query planner tests: `cargo test query_planner`
- Test property access: `RETURN n.id`

---

### Phase 6: PageRank Special Handling (1 hour)

**Files**:
- `src/clickhouse_query_generator/pagerank.rs`

**Tasks**:

1. **Lines 221, 225**:
   
   **Issue**: PageRank algorithm needs special handling for composite IDs
   
   **Options**:
   - ❌ Block composite IDs (error message)
   - ✅ Generate tuple-based PageRank (use tuple as node ID)
   
   **Implementation**:
   ```rust
   let node_id_expr = node_schema.node_id.sql_tuple("n");
   
   // Use in PageRank CTE
   SELECT {node_id_expr} as node_id, ...
   ```

2. Update PageRank tests to document composite ID support

**Validation**:
- Run: `cargo test pagerank`
- Document: PageRank works with composite node IDs (uses tuples)

---

### Phase 7: Schema Config Generation (30 min)

**Files**:
- `src/graph_catalog/config.rs`

**Tasks**:

1. **Line 600** - `primary_keys` field:
   
   **Current**:
   ```rust
   primary_keys: node_def.node_id.as_single().to_string(),
   ```
   
   **Replace with**:
   ```rust
   primary_keys: node_def.node_id.columns().join(", "),
   ```

**Validation**: No tests affected (metadata only)

---

### Phase 8: Testing (3 hours)

**Test Schema**: `schemas/test/composite_node_ids.yaml`

```yaml
database: brahmand_test
nodes:
  - label: Account
    table: accounts_composite
    node_id: [bank_id, account_number]
    properties:
      - name: balance
        column: balance
      - name: account_type
        column: account_type
  
  - label: User
    table: users
    node_id: user_id  # Mix single and composite
    properties:
      - name: name
        column: full_name

relationships:
  - type: OWNS
    table: account_ownership
    from_id: [user_id]  # Single column as array (optional syntax)
    to_id: [bank_id, account_number]  # Composite!
    from_label: User
    to_label: Account
```

**Test Cases**:

1. **test_composite_node_basic_match.py**:
   ```cypher
   MATCH (a:Account) WHERE a.bank_id = 'chase' RETURN a.id, a.balance
   ```
   
   **Expected SQL**:
   ```sql
   SELECT 
     tuple(a.bank_id, a.account_number) as id,
     a.balance
   FROM accounts_composite a
   WHERE a.bank_id = 'chase'
   ```

2. **test_composite_node_relationship.py**:
   ```cypher
   MATCH (u:User)-[:OWNS]->(a:Account) WHERE u.user_id = 1 RETURN u.name, a.balance
   ```
   
   **Expected SQL**:
   ```sql
   SELECT u.full_name, a.balance
   FROM users u
   INNER JOIN account_ownership r ON r.user_id = u.user_id
   INNER JOIN accounts_composite a ON (r.bank_id, r.account_number) = (a.bank_id, a.account_number)
   WHERE u.user_id = 1
   ```

3. **test_composite_node_size_pattern.py**:
   ```cypher
   MATCH (u:User) RETURN u.name, size((u)-[:OWNS]->(:Account)) as num_accounts
   ```
   
   **Expected SQL**: Subquery with composite JOIN

4. **test_composite_node_vlp.py**:
   ```cypher
   MATCH (a:Account)-[:LINKED*1..3]->(b:Account) RETURN a.id, b.id
   ```
   
   **Expected SQL**: VLP CTE with tuple comparisons

5. **test_mixed_composite_single.py**:
   ```cypher
   MATCH (u:User)-[:OWNS]->(a:Account)-[:LINKED]->(b:Account)
   WHERE u.user_id = 1 RETURN u.name, a.balance, b.balance
   ```
   
   **Expected SQL**: Mix of single and tuple JOINs

**Validation**:
- All 5 integration tests pass
- Generate correct SQL for all scenarios
- No panics from `.column()` calls

---

### Phase 9: Documentation (2 hours)

**Files to Update**:

1. **CHANGELOG.md** (Unreleased section):
   ```markdown
   ## [Unreleased]
   
   ### 🚀 Features
   - **Composite Node IDs**: Support multi-column `node_id` for nodes with composite primary keys
     - YAML syntax: `node_id: [col1, col2]`
     - Generates tuple equality in JOINs: `(a.c1, a.c2) = (b.c1, b.c2)`
     - Property access `n.id` returns tuple for composite IDs
     - Works with all query features (MATCH, VLP, size(), EXISTS)
   ```

2. **STATUS.md** (What Works section):
   ```markdown
   ### Composite Identifiers
   
   **Composite Edge IDs** ✅ (v0.5.0+)
   - Multi-column `edge_id` for relationships
   - Example: `edge_id: [flight_date, flight_number, origin, dest]`
   - Used in 8+ benchmark schemas
   
   **Composite Node IDs** ✅ (v0.6.0+) **NEW**
   - Multi-column `node_id` for nodes
   - Example: `node_id: [bank_id, account_number]`
   - Automatic tuple equality in JOINs
   - Property access returns tuples
   ```

3. **docs/wiki/Cypher-Language-Reference.md** (new section):
   ```markdown
   ## Composite Identifiers
   
   ### Node Composite IDs
   
   ClickGraph supports multi-column node identifiers for nodes with composite primary keys.
   
   **YAML Configuration**:
   ```yaml
   nodes:
     - label: Account
       table: accounts
       node_id: [bank_id, account_number]  # Composite ID
   ```
   
   **Property Access**:
   ```cypher
   MATCH (a:Account) RETURN a.id
   -- Returns: tuple(a.bank_id, a.account_number)
   ```
   
   **JOIN Behavior**:
   ```cypher
   MATCH (u:User)-[:OWNS]->(a:Account) RETURN u.name, a.balance
   -- Generated SQL uses tuple equality:
   -- INNER JOIN accounts a ON (r.bank_id, r.account_number) = (a.bank_id, a.account_number)
   ```
   ```

4. **Feature Note**: `notes/composite-node-ids.md`
   - Summary of implementation
   - Key design decisions (tuple equality, property access)
   - Limitations (if any)
   - Examples

5. **Schema Reference**: `docs/schema-reference.md`
   - Add composite `node_id` to syntax guide
   - Add examples

**Validation**: Documentation review checklist complete

---

## Risk Assessment

### Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Breaking changes to existing APIs | 🟡 Medium | 🔴 High | Keep `.column()` for backwards compat, add deprecation warning |
| Complex SQL generation bugs | 🟡 Medium | 🔴 High | Comprehensive test coverage, manual SQL validation |
| Performance degradation | 🟢 Low | 🟡 Medium | Tuple equality is optimized by ClickHouse |
| Missing edge cases | 🟡 Medium | 🟡 Medium | Extensive integration tests, real-world schema testing |

### Scope Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| PageRank doesn't support composite IDs | 🟢 Low | 🟠 Low | Document limitation or implement tuple-based PageRank |
| Some functions incompatible with tuples | 🟢 Low | 🟠 Low | Document which functions work with composite IDs |
| YAML parsing ambiguity | 🟢 Low | 🟡 Medium | Already solved by existing Identifier serde implementation |

---

## Success Criteria

### Functional Requirements

✅ **Must Have**:
1. Parse `node_id: [col1, col2]` from YAML without errors
2. Generate correct JOIN conditions with tuple equality
3. Support MATCH queries with composite node IDs
4. Property access `n.id` returns tuple for composite IDs
5. All 30+ `.column()` call sites replaced or validated
6. Zero panics on composite node IDs

✅ **Should Have**:
7. Variable-length paths work with composite node IDs
8. Pattern matching (size/EXISTS) works with composite IDs
9. PageRank supports composite IDs (or clear error message)
10. Mix of single and composite node IDs in same query

🔄 **Nice to Have** (Future):
11. Array syntax for ID access: `n.id[0]`, `n.id[1]`
12. Named tuple fields: `n.id.bank_id`, `n.id.account_number`

### Testing Requirements

- ✅ Unit tests: 20+ new tests for API methods
- ✅ Integration tests: 5 test cases covering all major features
- ✅ Benchmark test: OnTime flights with composite airport IDs
- ✅ Manual validation: Run generated SQL in ClickHouse
- ✅ Regression tests: All existing tests still pass

### Documentation Requirements

- ✅ CHANGELOG.md updated
- ✅ STATUS.md updated
- ✅ Feature note created
- ✅ Cypher Language Reference section added
- ✅ Schema reference updated with examples
- ✅ API documentation for new methods

---

## Timeline

**Total Estimated Effort**: 20 hours (2.5 days)

### Day 1 (8 hours)
- **Morning** (4h): Phase 1 (API Design) + Phase 2 (JOIN Conditions)
- **Afternoon** (4h): Phase 3 (Pattern Matching) + Phase 4 (VLP)

### Day 2 (8 hours)
- **Morning** (4h): Phase 5 (Query Planning) + Phase 6 (PageRank)
- **Afternoon** (4h): Phase 7 (Config) + Phase 8 (Testing start)

### Day 3 (4 hours)
- **Morning** (2h): Phase 8 (Testing complete)
- **Afternoon** (2h): Phase 9 (Documentation)

---

## Open Questions

### Question 1: Property Access Syntax

**For composite node ID** `[bank_id, account_number]`:

**Current Plan**: `n.id` returns `tuple(n.bank_id, n.account_number)`

**Alternative**: Error with message "Use n.bank_id or n.account_number instead"

**Decision**: Keep tuple return (consistent with edge_id behavior)

---

### Question 2: PageRank Algorithm

**Options**:
1. Block composite IDs with clear error
2. Use tuple as node ID (may affect sorting/aggregation)

**Recommendation**: **Try tuple approach first**, document if limitations found

---

### Question 3: Backwards Compatibility

**Keep** `NodeIdSchema.column()` method?

**Options**:
1. ✅ Keep with panic + deprecation warning (gradual migration)
2. ❌ Remove (breaking change, forces immediate migration)

**Recommendation**: **Keep with clear documentation** - panic is acceptable for known issue

---

## Next Steps

1. **User Approval**: Review plan, answer open questions
2. **Start Implementation**: Begin Phase 1 (API Design)
3. **Incremental Testing**: Test each phase before moving to next
4. **Documentation**: Update as we go, not at the end

**Ready to proceed?** 🚀

---

## Appendix: Example Real-World Schema

**Banking Application** (motivating use case):

```yaml
database: banking_app
nodes:
  - label: Customer
    table: customers
    node_id: customer_id  # Single column
    properties:
      - name: name
        column: full_name
      - name: email
        column: email_address

  - label: Account
    table: accounts
    node_id: [bank_id, account_number]  # Composite! (multi-bank system)
    properties:
      - name: balance
        column: current_balance
      - name: type
        column: account_type

  - label: Transaction
    table: transactions
    node_id: [bank_id, transaction_id]  # Composite! (per-bank transaction IDs)
    properties:
      - name: amount
        column: amount
      - name: timestamp
        column: transaction_timestamp

relationships:
  - type: OWNS
    table: account_ownership
    from_id: customer_id
    to_id: [bank_id, account_number]  # References composite key!
    from_label: Customer
    to_label: Account
    
  - type: SENT_MONEY
    table: transfers
    from_id: [from_bank_id, from_account_num]  # Composite
    to_id: [to_bank_id, to_account_num]        # Composite
    from_label: Account
    to_label: Account
    edge_id: [transfer_id, timestamp]  # Already supported!
```

**Query Example**:
```cypher
MATCH (c:Customer)-[:OWNS]->(a:Account)-[:SENT_MONEY]->(b:Account)
WHERE c.customer_id = 12345
RETURN c.name, a.balance, b.balance
```

**Generated SQL** (with composite node IDs):
```sql
SELECT 
  c.full_name,
  a.current_balance,
  b.current_balance
FROM customers c
INNER JOIN account_ownership owns ON owns.customer_id = c.customer_id
INNER JOIN accounts a ON (owns.bank_id, owns.account_number) = (a.bank_id, a.account_number)
INNER JOIN transfers t ON (t.from_bank_id, t.from_account_num) = (a.bank_id, a.account_number)
INNER JOIN accounts b ON (t.to_bank_id, t.to_account_num) = (b.bank_id, b.account_number)
WHERE c.customer_id = 12345
```

---

**This plan follows the 5-phase development process** from `DEVELOPMENT_PROCESS.md`:
- ✅ Phase 1 (Design): Complete (this document)
- 🔄 Phase 2 (Implement): Ready to start
- ⏸️ Phase 3 (Test): Test cases defined
- ⏸️ Phase 4 (Debug): Debug strategy included
- ⏸️ Phase 5 (Document): Documentation tasks listed
