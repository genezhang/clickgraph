# Cross-Table Branching Pattern - Implementation Plan

## Phase 1: Add Node Tracking to collect_graph_joins

### Step 1: Add tracking HashMap parameter

Add new parameter to `collect_graph_joins`:
```rust
node_appearances: &mut HashMap<String, Vec<NodeAppearance>>,
```

Where `NodeAppearance` is:
```rust
struct NodeAppearance {
    rel_alias: String,      // Which GraphRel owns this node
    node_label: String,     // Node label (e.g., "IP")
    table_name: String,     // Table where node data lives
    column_name: String,    // Column name for node ID
}
```

### Step 2: Initialize at entry point (line 52)

```rust
let mut node_appearances: HashMap<String, Vec<NodeAppearance>> = HashMap::new();
self.collect_graph_joins(
    logical_plan.clone(),
    logical_plan.clone(),
    plan_ctx,
    graph_schema,
    &mut collected_graph_joins,
    &mut joined_entities,
    &cte_scope_aliases,
    &mut node_appearances,  // NEW
)?;
```

### Step 3: Track nodes in GraphRel case

In `LogicalPlan::GraphRel(graph_rel)` case (around line 1606):

```rust
LogicalPlan::GraphRel(graph_rel) => {
    // ... existing processing of branches ...
    
    // AFTER processing branches, BEFORE processing current relationship:
    // Check and record node appearances for cross-branch JOIN generation
    
    self.check_and_generate_cross_branch_joins(
        graph_rel,
        plan_ctx,
        graph_schema,
        node_appearances,
        collected_graph_joins,
    )?;
    
    // Continue with normal relationship processing...
    self.infer_graph_join(...)?;
    
    // ... rest of processing ...
}
```

### Step 4: Implement check_and_generate_cross_branch_joins

```rust
fn check_and_generate_cross_branch_joins(
    &self,
    graph_rel: &GraphRel,
    plan_ctx: &PlanCtx,
    graph_schema: &GraphSchema,
    node_appearances: &mut HashMap<String, Vec<NodeAppearance>>,
    collected_graph_joins: &mut Vec<Join>,
) -> AnalyzerResult<()> {
    // 1. Extract node info for left_connection
    let left_node_alias = &graph_rel.left_connection;
    let left_appearance = self.extract_node_appearance(
        left_node_alias,
        &graph_rel.alias,
        plan_ctx,
        graph_schema,
    )?;
    
    // 2. Check if left_connection was already seen in different GraphRel
    if let Some(prev_appearances) = node_appearances.get(left_node_alias) {
        for prev in prev_appearances {
            if prev.rel_alias != graph_rel.alias {
                // Found cross-branch sharing! Generate JOIN
                self.generate_cross_branch_join(
                    left_node_alias,
                    prev,
                    &left_appearance,
                    collected_graph_joins,
                )?;
            }
        }
    }
    
    // 3. Record this appearance
    node_appearances
        .entry(left_node_alias.clone())
        .or_insert_with(Vec::new)
        .push(left_appearance);
    
    // 4. Repeat for right_connection if needed
    // (Similar logic for right node)
    
    Ok(())
}
```

### Step 5: Implement extract_node_appearance

```rust
fn extract_node_appearance(
    &self,
    node_alias: &str,
    rel_alias: &str,
    plan_ctx: &PlanCtx,
    graph_schema: &GraphSchema,
) -> AnalyzerResult<NodeAppearance> {
    // 1. Get node label from plan_ctx
    let table_ctx = plan_ctx
        .get_table_ctx_from_alias_opt(&Some(node_alias.to_string()))
        .map_err(|_| AnalyzerError::schema_error(
            format!("Cannot find TableCtx for node alias '{}'", node_alias),
            Pass::GraphJoinInference,
        ))?;
    
    let node_label = table_ctx.get_label_str().map_err(|_| 
        AnalyzerError::schema_error(
            format!("Cannot get label for node '{}'", node_alias),
            Pass::GraphJoinInference,
        )
    )?;
    
    // 2. Get relationship schema to determine table name
    let rel_schema = self.get_rel_schema_for_alias(rel_alias, plan_ctx, graph_schema)?;
    
    // 3. Build composite key and get node schema
    let composite_key = format!(
        "{}::{}::{}",
        rel_schema.database,
        rel_schema.table_name,
        node_label
    );
    
    let node_schema = graph_schema
        .get_node_schema_opt(&composite_key)
        .or_else(|| graph_schema.get_node_schema_opt(&node_label))
        .ok_or_else(|| AnalyzerError::schema_error(
            format!("Cannot find node schema for '{}'", composite_key),
            Pass::GraphJoinInference,
        ))?;
    
    // 4. Determine which column to use (from_column or to_column)
    // This depends on whether this node is on the left or right of the relationship
    // For now, assume it's a left node (from_column)
    let column_name = node_schema.from_column.clone();
    
    Ok(NodeAppearance {
        rel_alias: rel_alias.to_string(),
        node_label: node_label.to_string(),
        table_name: rel_schema.table_name.clone(),
        column_name,
    })
}
```

### Step 6: Implement generate_cross_branch_join

```rust
fn generate_cross_branch_join(
    &self,
    node_alias: &str,
    prev_appearance: &NodeAppearance,
    current_appearance: &NodeAppearance,
    collected_graph_joins: &mut Vec<Join>,
) -> AnalyzerResult<()> {
    crate::debug_print!(
        "🔗 Cross-branch JOIN detected for node '{}': {} ({}.{}) ↔ {} ({}.{})",
        node_alias,
        prev_appearance.rel_alias,
        prev_appearance.table_name,
        prev_appearance.column_name,
        current_appearance.rel_alias,
        current_appearance.table_name,
        current_appearance.column_name,
    );
    
    let join = Join {
        left_table: prev_appearance.rel_alias.clone(),
        left_column: prev_appearance.column_name.clone(),
        right_table: current_appearance.rel_alias.clone(),
        right_column: current_appearance.column_name.clone(),
        join_type: JoinType::Inner,  // Cross-branch is always INNER (required match)
    };
    
    collected_graph_joins.push(join);
    
    crate::debug_print!(
        "   ✅ Generated: {} JOIN {} ON {}.{} = {}.{}",
        prev_appearance.rel_alias,
        current_appearance.rel_alias,
        prev_appearance.rel_alias,
        prev_appearance.column_name,
        current_appearance.rel_alias,
        current_appearance.column_name,
    );
    
    Ok(())
}
```

## Issues to Handle

### Issue 1: from_column vs to_column

The `extract_node_appearance` needs to know whether this node is on the left or right of the relationship to choose the correct column.

**Solution**: Pass additional context about whether we're extracting left or right node.

### Issue 2: Getting rel_schema from rel_alias

Need a helper to get relationship schema from alias.

**Solution**: Add helper method `get_rel_schema_for_alias` or extract from existing code.

### Issue 3: Nested vs Sibling Nodes

A node appearing in nested GraphRels (linear pattern a→b→c) shouldn't generate JOINs because they're the same physical node.

**Current approach**: Track by `rel_alias` - different GraphRels have different aliases, so this naturally handles it.
- Linear pattern: a(t1) → b(t2) → c(t3) - three different relationships
- Branching pattern: a(t3)-→d, a(t4)→e - a appears in both t3 and t4

**Verification needed**: Check if this handles all cases correctly.

## Testing Strategy

### Test 1: Two-Branch Pattern
```cypher
MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), (srcip)-[:ACCESSED]->(dest:IP)
WHERE srcip.ip = '192.168.1.10'
RETURN srcip.ip, d.name, dest.ip
```

**Expected**:
- Track srcip in GraphRel t3 (dns_log)
- Track srcip in GraphRel t4 (conn_log)
- Generate JOIN: t3 JOIN t4 ON t3.orig_h = t4.orig_h

### Test 2: Three-Branch Pattern
```cypher
MATCH (a)-[:R1]->(b), (a)-[:R2]->(c), (a)-[:R3]->(d)
RETURN a, b, c, d
```

**Expected**:
- t1 stores a-R1-b
- t2 stores a-R2-c, generates JOIN with t1 on 'a'
- t3 stores a-R3-d, generates JOIN with t1 on 'a' (or t2, doesn't matter)

### Test 3: Linear Pattern (should NOT generate cross-branch JOIN)
```cypher
MATCH (a)-[:R1]->(b)-[:R2]->(c)
RETURN a, b, c
```

**Expected**:
- t1 stores a-R1-b
- t2 stores b-R2-c
- b appears in both, but they're connected linearly via normal JOIN logic
- Should NOT generate cross-branch JOIN (handled by normal `infer_graph_join`)

## Next Steps

1. ✅ Design complete
2. ⏳ Define NodeAppearance struct
3. ⏳ Update collect_graph_joins signature
4. ⏳ Implement check_and_generate_cross_branch_joins
5. ⏳ Implement extract_node_appearance
6. ⏳ Implement generate_cross_branch_join
7. ⏳ Test with two-branch pattern
8. ⏳ Debug and iterate
9. ⏳ Run full test suite
10. ⏳ Update documentation
