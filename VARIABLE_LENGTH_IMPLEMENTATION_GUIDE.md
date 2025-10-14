# Variable-Length Path Implementation Guide

## For the Next Developer

This guide provides everything you need to complete the variable-length path SQL generation integration.

## Current State (What's Done)

### 1. Parser ✅
- File: `brahmand/src/open_cypher_parser/path_pattern.rs`
- Function: `parse_variable_length_spec()`
- Handles: `*`, `*N`, `*N..M`, `*..M`
- Example: `MATCH (a)-[*1..3]->(b)` correctly parsed

### 2. AST ✅
- File: `brahmand/src/open_cypher_parser/ast.rs`
- Structure: `VariableLengthSpec { min_hops: Option<u32>, max_hops: Option<u32> }`
- Helper methods: `fixed()`, `range()`, `unbounded()`, `effective_min_hops()`, etc.

### 3. Logical Plan ✅
- File: `brahmand/src/query_planner/logical_plan/mod.rs`
- Field: `GraphRel { variable_length: Option<VariableLengthSpec>, ... }`
- Helper: `contains_variable_length_path()` recursively checks plan tree
- Type conversion: `From<ast::VariableLengthSpec> for VariableLengthSpec`

### 4. Analyzer Bypass ✅
Three passes modified to skip variable-length paths:
- `query_validation.rs`: Lines 48-51 (skip schema validation)
- `graph_traversal_planning.rs`: Lines 49-52 (skip traversal planning)
- `graph_join_inference.rs`: Lines 247-250 (skip join inference)

### 5. SQL Generator Class ✅
- File: `brahmand/src/clickhouse_query_generator/variable_length_cte.rs`
- Class: `VariableLengthCteGenerator`
- Methods:
  - `generate_cte()` - Creates Cte structure
  - `generate_recursive_sql()` - Main SQL generation
  - `generate_base_case()` - Single-hop connections
  - `generate_recursive_case()` - Multi-hop extension

## What Needs to be Done

### The Integration Point

**File:** `brahmand/src/render_plan/plan_builder.rs`
**Method:** `extract_ctes()` (around line 101)

**Current Code:**
```rust
LogicalPlan::GraphRel(graph_rel) => {
    // Handle variable-length paths differently
    if graph_rel.variable_length.is_some() {
        // TODO: Generate recursive CTE for variable-length path
        // For now, just return empty vec to avoid errors
        return Ok(vec![]);
    }

    // Normal CTE extraction continues...
}
```

**You need to replace the TODO with actual implementation.**

## Implementation Options

### Option A: Structured Approach (Recommended)

Extend the `RenderPlan` structure to support recursive CTEs.

**Steps:**

1. **Extend `Cte` structure** in `brahmand/src/render_plan/mod.rs`:
```rust
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Cte {
    pub cte_name: String,
    pub cte_plan: RenderPlan,
    pub is_recursive: bool,  // NEW FIELD
}
```

2. **Add recursive CTE generation** to `RenderPlan`:
```rust
impl RenderPlan {
    pub fn from_variable_length_graph_rel(
        graph_rel: &GraphRel,
        spec: &VariableLengthSpec,
    ) -> RenderPlanBuilderResult<Self> {
        // Use VariableLengthCteGenerator to build structure
        // Return properly structured RenderPlan
    }
}
```

3. **Update `ToSql` trait** in `brahmand/src/clickhouse_query_generator/to_sql.rs`:
```rust
impl ToSql for Cte {
    fn to_sql(&self) -> String {
        if self.is_recursive {
            format!("{} AS RECURSIVE (\n{}\n)", 
                    self.cte_name, 
                    self.cte_plan.to_sql())
        } else {
            // existing logic
        }
    }
}
```

4. **Implement in `extract_ctes()`**:
```rust
if let Some(spec) = &graph_rel.variable_length {
    let var_len_cte = RenderPlan::from_variable_length_graph_rel(graph_rel, spec)?;
    let cte = Cte {
        cte_name: format!("var_path_{}", graph_rel.alias),
        cte_plan: var_len_cte,
        is_recursive: true,
    };
    return Ok(vec![cte]);
}
```

**Pros:**
- Maintains architectural consistency
- Easier to test and debug
- Supports future enhancements

**Cons:**
- More code changes required
- Need to refactor CTE handling

**Estimated Time:** 2-3 days

### Option B: Quick Hack (Not Recommended)

Generate raw SQL and bypass the structured pipeline.

**Steps:**

1. **Create special SQL wrapper**:
```rust
pub enum CteContent {
    Structured(RenderPlan),
    RawSql(String),
}

pub struct Cte {
    pub cte_name: String,
    pub content: CteContent,
}
```

2. **Generate raw SQL directly**:
```rust
if let Some(spec) = &graph_rel.variable_length {
    let generator = VariableLengthCteGenerator::new(
        spec.clone(),
        &left_node,
        &relationship,
        &right_node,
    );
    let raw_sql = generator.generate_recursive_sql();
    let cte = Cte {
        cte_name: format!("var_path_{}", graph_rel.alias),
        content: CteContent::RawSql(raw_sql),
    };
    return Ok(vec![cte]);
}
```

3. **Update `ToSql` to handle both**:
```rust
impl ToSql for Cte {
    fn to_sql(&self) -> String {
        match &self.content {
            CteContent::Structured(plan) => /* existing logic */,
            CteContent::RawSql(sql) => format!("{} AS ({})", self.cte_name, sql),
        }
    }
}
```

**Pros:**
- Faster to implement
- Minimal changes to existing code

**Cons:**
- Creates architectural debt
- Harder to maintain long-term
- Inconsistent with rest of codebase

**Estimated Time:** 1 day

## Detailed Implementation Steps (Option A)

### Step 1: Understand Current Architecture

Read these files in order:
1. `brahmand/src/render_plan/mod.rs` - Understand `RenderPlan` and `Cte`
2. `brahmand/src/render_plan/plan_builder.rs` - Understand `extract_ctes()`
3. `brahmand/src/clickhouse_query_generator/to_sql.rs` - Understand SQL generation

### Step 2: Design the Recursive CTE Structure

Current `RenderPlan` has:
```rust
pub struct RenderPlan {
    pub ctes: CteItems,
    pub select: SelectItems,
    pub from: FromTableItem,
    pub joins: JoinItems,
    pub filters: FilterItems,
    pub group_by: GroupByExpressions,
    pub order_by: OrderByItems,
    pub skip: SkipItem,
    pub limit: LimitItem,
    pub union: UnionItems,
}
```

For recursive CTEs, you need:
- **Base case**: Initial SELECT (1-hop connections)
- **Recursive case**: UNION ALL with join to previous results
- **Cycle detection**: WHERE clause with `has()` function
- **Hop counting**: hop_count column

### Step 3: Modify `VariableLengthCteGenerator`

Update the `generate_cte()` method to return a proper `RenderPlan`:

```rust
pub fn generate_cte(&self) -> Cte {
    let spec = &self.spec;
    
    // Build base case as RenderPlan
    let base_case = self.build_base_case_plan();
    
    // Build recursive case as RenderPlan  
    let recursive_case = self.build_recursive_case_plan();
    
    // Combine with UNION
    let combined_plan = RenderPlan {
        union: UnionItems(Some(Union {
            input: vec![base_case, recursive_case],
            union_type: UnionType::All,
        })),
        // ... other fields
    };
    
    Cte {
        cte_name: self.cte_name.clone(),
        cte_plan: combined_plan,
        is_recursive: true,
    }
}
```

### Step 4: Add Helper Methods

You'll need these new methods in `VariableLengthCteGenerator`:

```rust
impl VariableLengthCteGenerator {
    fn build_base_case_plan(&self) -> RenderPlan {
        // Convert self.generate_base_case() SQL into RenderPlan structure
        // Include: start_id, end_id, hop_count=1, path_nodes=[start_id]
    }
    
    fn build_recursive_case_plan(&self) -> RenderPlan {
        // Convert self.generate_recursive_case() SQL into RenderPlan structure
        // Include: path extension, hop_count increment, cycle check
    }
    
    fn create_cycle_detection_filter(&self) -> RenderExpr {
        // WHERE NOT has(path_nodes, next_node_id)
    }
    
    fn create_hop_limit_filter(&self) -> RenderExpr {
        // WHERE hop_count <= max_hops
    }
}
```

### Step 5: Update `extract_ctes()` in `plan_builder.rs`

Replace the TODO:

```rust
LogicalPlan::GraphRel(graph_rel) => {
    if let Some(spec) = &graph_rel.variable_length {
        // Get node/relationship information from graph_rel
        let left_label = get_node_label(&graph_rel.left_connection)?;
        let rel_label = &graph_rel.alias;  
        let right_label = get_node_label(&graph_rel.right_connection)?;
        
        // Create generator
        let generator = VariableLengthCteGenerator::new(
            spec.clone().into(),
            &left_label,
            rel_label,
            &right_label,
        );
        
        // Generate CTE
        let var_len_cte = generator.generate_cte();
        
        // Also extract CTEs from child plans
        let mut child_ctes = graph_rel.right.extract_ctes(last_node_alias)?;
        child_ctes.push(var_len_cte);
        
        return Ok(child_ctes);
    }
    
    // Normal CTE extraction continues...
}
```

### Step 6: Update `ToSql` Implementation

In `brahmand/src/clickhouse_query_generator/to_sql.rs`:

```rust
impl ToSql for Cte {
    fn to_sql(&self) -> String {
        let keyword = if self.is_recursive { "RECURSIVE " } else { "" };
        format!("{}{} AS (\n{}\n)", 
                keyword,
                self.cte_name,
                self.cte_plan.to_sql())
    }
}
```

### Step 7: Test Incrementally

After each step, test with:

```cypher
MATCH (u1:user)-[*1..3]->(u2:user) 
RETURN u1.name, u2.name LIMIT 10
```

Expected SQL structure:
```sql
WITH RECURSIVE var_path_rel AS (
  -- Base case: 1-hop connections
  SELECT 
    u1.user_id as start_id,
    u2.user_id as end_id,
    1 as hop_count,
    [u1.user_id] as path_nodes
  FROM users u1
  JOIN follows ON u1.user_id = follows.from_user
  JOIN users u2 ON follows.to_user = u2.user_id
  
  UNION ALL
  
  -- Recursive case: extend paths
  SELECT
    p.start_id,
    u2.user_id as end_id,
    p.hop_count + 1 as hop_count,
    arrayConcat(p.path_nodes, [u2.user_id]) as path_nodes
  FROM var_path_rel p
  JOIN follows ON p.end_id = follows.from_user
  JOIN users u2 ON follows.to_user = u2.user_id
  WHERE NOT has(p.path_nodes, u2.user_id)  -- Cycle detection
    AND p.hop_count < 3  -- Max depth
)
SELECT 
  u1.name as start_user,
  u2.name as end_user
FROM var_path_rel p
JOIN users u1 ON p.start_id = u1.user_id
JOIN users u2 ON p.end_id = u2.user_id
WHERE p.hop_count >= 1  -- Min depth
LIMIT 10
```

## Testing Strategy

### Unit Tests

Create `brahmand/src/clickhouse_query_generator/tests/variable_length_tests.rs`:

```rust
#[test]
fn test_parse_and_generate_variable_length_1_to_3() {
    let query = "MATCH (u1:user)-[*1..3]->(u2:user) RETURN u1.name, u2.name";
    let plan = parse_and_plan(query).unwrap();
    let sql = plan.to_render_plan().unwrap().to_sql();
    
    assert!(sql.contains("WITH RECURSIVE"));
    assert!(sql.contains("UNION ALL"));
    assert!(sql.contains("hop_count"));
    assert!(sql.contains("has("));  // Cycle detection
}

#[test]
fn test_unbounded_has_default_limit() {
    let query = "MATCH (u1:user)-[*]->(u2:user) RETURN u1.name";
    let plan = parse_and_plan(query).unwrap();
    let sql = plan.to_render_plan().unwrap().to_sql();
    
    assert!(sql.contains("hop_count <= 10") || sql.contains("hop_count < 11"));
}
```

### Integration Tests

Use the existing `test_relationships.ipynb`:
- Test 6: Range (1-3 hops)
- Test 7: Fixed length (*2)
- Test 8: Upper bounded (*..5)
- Test 9: Unbounded (*)
- Test 10: Typed variable-length
- Test 11: Edge cases

### SQL Validation

For each test, verify the generated SQL contains:
- ✅ `WITH RECURSIVE` keyword
- ✅ Base case SELECT
- ✅ `UNION ALL`
- ✅ Recursive case SELECT
- ✅ `hop_count` column
- ✅ `path_nodes` array
- ✅ `has(path_nodes, node_id)` for cycle detection
- ✅ `hop_count >= min_hops` WHERE clause
- ✅ `hop_count <= max_hops` WHERE clause

## Common Pitfalls

### 1. Infinite Recursion
**Problem:** Unbounded queries without cycle detection loop forever.
**Solution:** Always include `has(path_nodes, next_node)` check.

### 2. Performance
**Problem:** Large graphs cause exponential query time.
**Solution:** Set reasonable default max depth (10 hops).

### 3. Schema Mismatch
**Problem:** Variable-length relationships don't have schema.
**Solution:** Already handled by analyzer bypass.

### 4. Column Naming
**Problem:** Ambiguous column names in recursive CTEs.
**Solution:** Use explicit aliases: `p.end_id`, not just `end_id`.

### 5. ClickHouse Compatibility
**Problem:** ClickHouse recursive CTE syntax differs from PostgreSQL.
**Solution:** Use ClickHouse array functions: `arrayConcat()`, `has()`.

## Debugging Tips

### 1. Print SQL at Each Stage
Add debug logging:
```rust
println!("Generated SQL: {}", sql);
```

### 2. Test with Simple Queries First
Start with `*1` (single hop), then `*1..2`, then `*1..3`.

### 3. Use ClickHouse Client
Test generated SQL directly in ClickHouse:
```bash
clickhouse-client --query "WITH RECURSIVE ..."
```

### 4. Check AST Structure
Print the parsed AST:
```rust
println!("AST: {:#?}", parsed_query);
```

### 5. Verify Plan Structure
Print the logical plan:
```rust
println!("Plan: {:#?}", logical_plan);
```

## Resources

### ClickHouse Documentation
- [WITH clause](https://clickhouse.com/docs/en/sql-reference/statements/select/with)
- [Array functions](https://clickhouse.com/docs/en/sql-reference/functions/array-functions)
- [has() function](https://clickhouse.com/docs/en/sql-reference/functions/array-functions#has)

### OpenCypher Specification
- [Variable-length patterns](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf) (Section 10.7)

### Existing Code References
- `brahmand/src/render_plan/plan_builder.rs` - How CTEs are built
- `brahmand/src/clickhouse_query_generator/to_sql.rs` - How SQL is generated
- `brahmand/src/query_planner/logical_plan/mod.rs` - How plans are structured

## Questions? Issues?

Common questions and answers:

**Q: Should I use Option A or Option B?**
A: Option A (structured approach) is recommended for maintainability.

**Q: How do I get node/relationship labels in extract_ctes()?**
A: They're available in `graph_rel.left_connection`, `graph_rel.alias`, `graph_rel.right_connection`.

**Q: What if I need to access the graph schema?**
A: Pass `graph_schema` as parameter to `extract_ctes()` method.

**Q: How do I handle relationship properties?**
A: Start without them, add support later as enhancement.

**Q: What about bidirectional relationships (incoming)?**
A: Handle `graph_rel.direction` to swap source/target columns.

## Success Criteria

You'll know you're done when:
- ✅ `cargo build` succeeds with no errors
- ✅ Test 6 returns SQL (not error message)
- ✅ Generated SQL contains `WITH RECURSIVE`
- ✅ SQL has base case and recursive case
- ✅ Tests 7-11 pass with different patterns
- ✅ No regressions in Tests 1-5

Good luck! The hard parts are already done. You're 70% there!
