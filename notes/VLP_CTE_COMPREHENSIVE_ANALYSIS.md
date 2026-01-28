# VLP CTE Comprehensive Analysis - All Schema Variations

**Created**: December 25, 2025  
**Purpose**: Deep dive into VLP CTE implementation for all schema patterns  
**Goal**: Design rock-solid CTE architecture that handles all variations

---

## Executive Summary

**Problem**: VLP CTE property handling works for some schema types but breaks for denormalized edges. Properties are extracted (6) and passed to generator, but don't appear in generated SQL (0).

**Root Cause Hypothesis**: Different schema patterns use different property aliasing strategies:
- **FK-edge**: `start_name`, `end_name` (prefixed)
- **Denormalized**: `Origin`, `OriginCityName` (physical column names)
- **Standard**: `start_node.name`, `end_node.name` (table.column)

**Impact**: Denormalized schema optimization defeated, forcing unnecessary JOINs.

---

## Schema Pattern Matrix

| Pattern | Node Tables | Edge Table | Property Location | Example |
|---------|-------------|------------|-------------------|---------|
| **Standard** | Separate | Separate | Node tables | `users` + `follows` |
| **FK-Edge** | Same table | FK column | Node table | `objects` (parent_id FK) |
| **Denormalized** | Virtual (no table) | Edge table | Edge table columns | `flights` (Origin/Dest) |
| **Partial Denorm** | Separate | Edge table | Some in edge, some in node | `orders` + `customers` |
| **Polymorphic** | Separate or Virtual | Single with type_col | Varies | `interactions` (type_column) |

---

## Current VLP CTE Implementation

### Architecture Overview

```rust
struct VariableLengthCteGenerator {
    // Core identification
    schema: &GraphSchema,
    spec: VariableLengthSpec,
    cte_name: String,
    
    // Node configuration
    start_node_table: String,
    start_node_id_column: String,
    start_node_alias: String,      // "start_node"
    
    // Edge configuration
    relationship_table: String,
    relationship_from_column: String,
    relationship_to_column: String,
    relationship_alias: String,     // "rel"
    
    // Property handling
    properties: Vec<NodeProperty>,   // 🔍 Properties to include in CTE
    start_cypher_alias: String,      // "a", "origin", etc.
    end_cypher_alias: String,        // "b", "dest", etc.
    
    // Schema pattern flags
    is_denormalized: bool,           // BOTH nodes virtual
    start_is_denormalized: bool,     // Start node virtual
    end_is_denormalized: bool,       // End node virtual
    is_fk_edge: bool,                // FK pattern
    
    // ... more fields ...
}

struct NodeProperty {
    cypher_alias: String,  // "origin" - which node
    column_name: String,   // "code" - logical property name
    alias: String,         // "code" - output alias
}
```

### Property Flow (What Works vs. What Breaks)

#### ✅ FK-Edge Pattern (WORKS)

**Base Case** (`generate_fk_edge_base_case`, lines 1695-1710):
```rust
for prop in &self.properties {
    if prop.cypher_alias == self.start_cypher_alias {
        select_items.push(format!(
            "{}.{} as start_{}",  // ✅ Prefixed: start_city
            self.start_node_alias, prop.column_name, prop.alias
        ));
    }
    if prop.cypher_alias == self.end_cypher_alias {
        select_items.push(format!(
            "{}.{} as end_{}",    // ✅ Prefixed: end_city
            self.end_node_alias, prop.column_name, prop.alias
        ));
    }
}
```

**Generated SQL**:
```sql
SELECT 
    start_node.object_id as start_id,
    end_node.object_id as end_id,
    start_node.full_name as start_name,  -- ✅ Works
    end_node.full_name as end_name,      -- ✅ Works
    1 as hop_count,
    ...
FROM objects AS start_node
JOIN objects AS end_node ON start_node.parent_id = end_node.object_id
```

#### ❌ Denormalized Pattern (BROKEN)

**Base Case** (`generate_denormalized_base_case`, lines 1972-2000):
```rust
for prop in &self.properties {
    if prop.cypher_alias == self.start_cypher_alias {
        if let Ok(physical_col) = self.map_denormalized_property(&prop.alias, true) {
            select_items.push(format!(
                "{}.{} as {}",  // ❌ Not prefixed: OriginCityName
                self.relationship_alias, physical_col, physical_col
            ));
        } else {
            log::warn!("Could not map start property {}", prop.alias);  // 🚨 Silent failure?
        }
    }
    // Similar for end properties...
}
```

**Expected SQL** (not generated):
```sql
SELECT 
    rel.Origin as start_id,
    rel.Dest as end_id,
    rel.OriginCityName as OriginCityName,  -- ❌ MISSING
    rel.DestCityName as DestCityName,      -- ❌ MISSING
    1 as hop_count,
    ...
FROM test_integration.flights AS rel
```

**Current SQL** (actual output):
```sql
SELECT 
    rel.Origin as start_id,
    rel.Dest as end_id,
    1 as hop_count,
    -- ❌ Properties completely missing!
    ...
FROM test_integration.flights AS rel
```

---

## Problem Analysis: Why Denormalized Fails

### Issue #1: Property Mapping Complexity

The `map_denormalized_property()` function (lines 694-722) looks up properties:

```rust
fn map_denormalized_property(&self, logical_prop: &str, is_from_node: bool) -> Result<String, String> {
    let node_schemas = self.schema.get_nodes_schemas();
    
    let node_schema = node_schemas
        .values()
        .find(|n| n.table_name == self.relationship_table)  // 🔍 Find schema by table
        .ok_or_else(|| format!("No node schema found for table '{}'", self.relationship_table))?;
    
    let property_map = if is_from_node {
        node_schema.from_properties.as_ref()  // 🔍 Get from_properties map
    } else {
        node_schema.to_properties.as_ref()    // 🔍 Get to_properties map
    };
    
    property_map
        .and_then(|map| map.get(logical_prop))  // 🔍 Look up property
        .map(|col| col.to_string())
        .ok_or_else(|| format!("Property '{}' not found...", logical_prop))
}
```

**Potential Failures**:
1. **Table name mismatch**: `test_integration.flights` vs `flights`
2. **Property name mismatch**: `city` (Cypher) vs `code` (column_name in NodeProperty)
3. **Missing from_properties/to_properties**: Schema not loaded correctly
4. **Silent errors**: `log::warn!` but doesn't propagate error

### Issue #2: Property Aliasing Inconsistency

Different patterns use different aliasing:

| Pattern | Alias Strategy | Example |
|---------|---------------|---------|
| FK-Edge | Prefixed | `start_city`, `end_city` |
| Denormalized | Physical column | `OriginCityName`, `DestCityName` |
| Standard | Table.column | `start_node.city`, `end_node.city` |

**Problem**: Downstream code expects consistent aliasing!

### Issue #3: Recursive Case Mismatch

**FK-Edge Recursive** (works):
```rust
// Carry forward start properties
for prop in &self.properties {
    if prop.cypher_alias == self.start_cypher_alias {
        select_items.push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
        //                        ^^^^^^^^^ Matches base case alias
    }
}
```

**Denormalized Recursive** (lines 2088-2101):
```rust
// Carry forward start properties
for prop in &self.properties {
    if prop.cypher_alias == self.start_cypher_alias {
        if let Ok(physical_col) = self.map_denormalized_property(&prop.alias, true) {
            select_items.push(format!("vp.{} as {}", physical_col, physical_col));
            //                        ^^^^^^^ Must match base case alias!
        }
    }
}
```

**Issue**: If base case doesn't add properties, recursive case can't carry them forward!

---

## Debug Plan: Systematic Investigation

### Phase 1: Confirm Property Extraction (5 min)

Add debug output in `cte_extraction.rs` (line 1340):

```rust
eprintln!("🔧 CTE: Final all_denorm_properties count: {}", all_denorm_properties.len());
for prop in &all_denorm_properties {
    eprintln!("🔧 CTE:   - {:?}", prop);
}
```

**Expected**: 6 properties (3 from_node, 3 to_node)

### Phase 2: Trace Property Flow in Generator (10 min)

Add debug in `variable_length_cte.rs` constructor (after line 254):

```rust
eprintln!("🔧 GENERATOR: Received {} properties", properties.len());
for prop in &properties {
    eprintln!("🔧 GENERATOR:   - {:?}", prop);
}
```

Add debug in `generate_denormalized_base_case` (line 1972):

```rust
eprintln!("🔧 BASE_CASE: Starting with {} properties", self.properties.len());
eprintln!("🔧 BASE_CASE: start_cypher_alias={}, end_cypher_alias={}", 
          self.start_cypher_alias, self.end_cypher_alias);

for prop in &self.properties {
    eprintln!("🔧 BASE_CASE: Processing prop: {:?}", prop);
    if prop.cypher_alias == self.start_cypher_alias {
        eprintln!("🔧 BASE_CASE:   - Matches start alias");
        match self.map_denormalized_property(&prop.alias, true) {
            Ok(physical_col) => {
                eprintln!("🔧 BASE_CASE:   - Mapped to physical: {}", physical_col);
                // ... add to select_items ...
            },
            Err(e) => {
                eprintln!("❌ BASE_CASE:   - Mapping FAILED: {}", e);
            }
        }
    }
    if prop.cypher_alias == self.end_cypher_alias {
        eprintln!("🔧 BASE_CASE:   - Matches end alias");
        // Similar...
    }
}

eprintln!("🔧 BASE_CASE: Final select_items count: {}", select_items.len());
```

### Phase 3: Test Property Mapping Function (5 min)

Add standalone test in `generate_denormalized_base_case` (before loop):

```rust
// Test map_denormalized_property directly
eprintln!("🔧 TEST: Testing map_denormalized_property");
eprintln!("🔧 TEST: relationship_table={}", self.relationship_table);
let test_result = self.map_denormalized_property("city", true);
eprintln!("🔧 TEST: map('city', true) = {:?}", test_result);
```

### Phase 4: Run Test and Analyze (10 min)

```bash
# Run single denormalized VLP test
pytest tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths::test_variable_path_with_denormalized_properties -v 2>&1 | tee vlp_debug.log

# Extract debug lines
grep "🔧" vlp_debug.log
grep "❌" vlp_debug.log
```

---

## Hypothesis Matrix

| Hypothesis | Test | If True | If False |
|------------|------|---------|----------|
| **H1**: Properties not passed to generator | Check constructor debug | Fix property passing | Next hypothesis |
| **H2**: `map_denormalized_property()` fails | Check mapping debug | Fix schema lookup | Next hypothesis |
| **H3**: Cypher alias mismatch | Check alias comparison | Fix alias extraction | Next hypothesis |
| **H4**: Properties added but lost in SQL | Check `select_items` length | Fix SQL generation | Next hypothesis |

---

## Design Principles for Rock-Solid CTE

### Principle 1: Uniform Property Representation

**Problem**: Different patterns use different internal representations.

**Solution**: Normalize properties at extraction time:

```rust
struct NormalizedProperty {
    cypher_alias: String,        // "origin" - node identifier
    logical_name: String,        // "city" - Cypher property name
    physical_column: String,     // "OriginCityName" - actual ClickHouse column
    base_alias: String,          // "start_city" or "OriginCityName" - CTE column alias
    recursive_alias: String,     // "start_city" or "OriginCityName" - same for carry forward
    source: PropertySource,      // Where to get property
}

enum PropertySource {
    StartNodeTable { table: String, column: String },
    EndNodeTable { table: String, column: String },
    EdgeTable { column: String },
    EdgeTableFromNode { column: String },  // Denormalized from_node
    EdgeTableToNode { column: String },    // Denormalized to_node
}
```

### Principle 2: Explicit Property Aliasing Strategy

Define aliasing strategy per schema pattern:

```rust
trait PropertyAliasStrategy {
    /// Generate alias for property in base case SELECT
    fn base_case_alias(&self, prop: &NormalizedProperty, is_start_node: bool) -> String;
    
    /// Generate alias for carrying forward in recursive case
    fn recursive_alias(&self, prop: &NormalizedProperty, is_start_node: bool) -> String;
    
    /// Generate SQL expression to select property
    fn select_expression(&self, prop: &NormalizedProperty, is_start_node: bool) -> String;
}

struct StandardAliasStrategy;  // start_city, end_city
struct DenormalizedAliasStrategy;  // OriginCityName, DestCityName (physical)
struct FKEdgeAliasStrategy;  // start_city, end_city (like standard)
```

### Principle 3: Fail-Fast Property Validation

Validate properties at extraction time, not generation time:

```rust
fn extract_vlp_properties(
    schema: &GraphSchema,
    graph_rel: &GraphRel,
    schema_type: VlpSchemaType,
) -> Result<Vec<NormalizedProperty>, ExtractionError> {
    let mut properties = Vec::new();
    
    // Extract and validate ALL properties needed
    for prop in &graph_rel.required_properties {
        let normalized = match schema_type {
            VlpSchemaType::Denormalized => {
                // Validate that property exists in from_properties or to_properties
                validate_denormalized_property(schema, &graph_rel, prop)?;
                normalize_denormalized_property(schema, &graph_rel, prop)?
            },
            VlpSchemaType::FKEdge => {
                normalize_fk_edge_property(schema, &graph_rel, prop)?
            },
            VlpSchemaType::Standard => {
                normalize_standard_property(schema, &graph_rel, prop)?
            },
        };
        
        properties.push(normalized);
    }
    
    Ok(properties)  // ✅ All properties validated and normalized
}
```

### Principle 4: Schema-Specific Generators

Don't use flags (`is_denormalized`, `is_fk_edge`, etc.) - use trait polymorphism:

```rust
trait VlpCteGenerator {
    fn generate_base_case(&self, hop_count: u32) -> String;
    fn generate_recursive_case(&self, max_hops: u32, cte_name: &str) -> String;
}

struct StandardVlpCte {
    properties: Vec<NormalizedProperty>,
    alias_strategy: StandardAliasStrategy,
    // ...
}

struct DenormalizedVlpCte {
    properties: Vec<NormalizedProperty>,
    alias_strategy: DenormalizedAliasStrategy,
    // ...
}

struct FKEdgeVlpCte {
    properties: Vec<NormalizedProperty>,
    alias_strategy: FKEdgeAliasStrategy,
    // ...
}
```

---

## Immediate Fix Strategy (Pragmatic)

### Step 1: Fix Property Mapping (30 min)

**Issue**: `prop.alias` might not match schema property keys.

**Fix in `cte_extraction.rs`** (lines 1308-1332):

```rust
// When extracting denormalized properties, use the SCHEMA's property keys
if let Some(ref from_props) = node_schema.from_properties {
    for (logical_prop, physical_col) in from_props {
        all_denorm_properties.push(NodeProperty {
            cypher_alias: graph_rel.left_connection.clone(),
            column_name: logical_prop.clone(),  // ✅ Use schema key
            alias: logical_prop.clone(),        // ✅ Use schema key
        });
    }
}
```

### Step 2: Fix Aliasing Consistency (30 min)

**Issue**: Denormalized uses physical column names as aliases, but recursive case expects them.

**Option A** (Minimal change): Use physical column names consistently:
```rust
// Base case
select_items.push(format!("{}.{} as {}", rel, physical_col, physical_col));

// Recursive case  
select_items.push(format!("vp.{} as {}", physical_col, physical_col));
```

**Option B** (Better): Use prefixed aliases like FK-edge:
```rust
// Base case
select_items.push(format!("{}.{} as start_{}", rel, physical_col, logical_prop));

// Recursive case
select_items.push(format!("vp.start_{} as start_{}", logical_prop, logical_prop));
```

### Step 3: Add Comprehensive Logging (15 min)

Add debug output at every step to catch issues early:

```rust
// At extraction
eprintln!("EXTRACT: {} properties extracted", properties.len());

// At generator construction
eprintln!("GENERATOR: {} properties received", self.properties.len());

// At base case generation
eprintln!("BASE: {} properties to process", self.properties.len());
eprintln!("BASE: {} select items after properties", select_items.len());

// At recursive case generation
eprintln!("RECURSIVE: {} properties to carry forward", self.properties.len());
```

### Step 4: Test with Real Queries (30 min)

```bash
# Remove xfail markers
sed -i 's/@pytest.mark.xfail.*//g' tests/integration/test_denormalized_edges.py

# Run tests
pytest tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths -v

# Check generated SQL
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "USE denormalized_flights_test MATCH (origin:Airport)-[:FLIGHT*1..2]->(dest:Airport) WHERE origin.code = \"LAX\" RETURN origin.city, dest.city",
    "sql_only": true
  }' | jq '.generated_sql'
```

---

## Long-Term Refactoring (2-3 days)

Follow the design principles above:

1. **Day 1**: Create `NormalizedProperty` and aliasing strategies
2. **Day 2**: Refactor extractors to use normalized properties  
3. **Day 3**: Implement schema-specific generators with trait polymorphism

This ensures:
- ✅ Clear separation of concerns
- ✅ Easy to add new schema patterns
- ✅ Fail-fast property validation
- ✅ Consistent property aliasing
- ✅ No fragile flag-based logic

---

## Test Coverage Matrix

| Schema Pattern | Single Hop | VLP *1..3 | VLP * | With Properties | With Filters |
|----------------|------------|-----------|-------|-----------------|--------------|
| Standard | ✅ | ✅ | ✅ | ✅ | ✅ |
| FK-Edge | ✅ | ✅ | ✅ | ✅ | ✅ |
| Denormalized | ✅ | ❌ | ❌ | ❌ | ✅ |
| Partial Denorm | ⚠️ | ❌ | ❌ | ❌ | ⚠️ |
| Polymorphic | ✅ | ⚠️ | ⚠️ | ⚠️ | ✅ |

**Legend**: ✅ Working, ❌ Broken, ⚠️ Untested

---

## Next Steps (This Session)

### Immediate (30 min)
1. ✅ Complete this analysis document
2. 🔄 Add debug logging to trace property flow
3. 🔄 Run test and capture logs
4. 🔄 Identify exact failure point

### Short-term (2 hours)
5. 🔄 Implement pragmatic fix (Steps 1-2 above)
6. 🔄 Verify denormalized VLP tests pass
7. 🔄 Test all other schema patterns (regression check)
8. 🔄 Document findings in STATUS.md

### Medium-term (Future session)
9. ⬜ Design and implement normalized property architecture
10. ⬜ Refactor all VLP generators to use trait-based approach
11. ⬜ Add comprehensive test suite for all schema variations
12. ⬜ Performance benchmarking

---

## References

- **Current Status**: `notes/VLP_DENORMALIZED_PROPERTY_TODO.md`
- **Earlier Analysis**: `notes/vlp-code-analysis.md`
- **Code Locations**: 
  - Generator: `src/clickhouse_query_generator/variable_length_cte.rs`
  - Extraction: `src/render_plan/cte_extraction.rs`
  - Tests: `tests/integration/test_denormalized_edges.py`
- **Schema**: `schemas/test/denormalized_flights.yaml`
