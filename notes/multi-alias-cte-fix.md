# Multi-Alias CTE Property Mapping Fix

## Problem

When a CTE exports multiple aliases (e.g., `WITH a, b, c`), the `create_cte_reference()` function only builds property mappings for ONE alias, leading to errors like "Unknown expression identifier `a.full_name`".

### Example: 4-Level WITH Query

```cypher
MATCH (a:User) WHERE a.user_id = 1 
WITH a 
MATCH (a)-[:FOLLOWS]->(b:User) 
WITH a, b 
MATCH (b)-[:FOLLOWS]->(c:User) 
WITH a, b, c 
MATCH (c)-[:FOLLOWS]->(d:User) 
RETURN a.name, b.name, c.name, d.name
```

### Current Behavior (Broken)

**CTE 3: `with_a_b_c_cte_1`** has columns:
- `a_city`, `a_name`, `a_user_id`, etc.
- `b_city`, `b_name`, `b_user_id`, etc.
- `c_city`, `c_name`, `c_user_id`, etc.

But when we create a ViewScan reference to this CTE, we call:
```rust
create_cte_reference("with_a_b_c_cte_1", "a_b_c", cte_schemas)
```

And `create_cte_reference()` only looks for columns matching the FIRST alias:
```rust
let alias_prefix = with_alias;  // "a_b_c" (WRONG!)
// Only looks for "a_b_c_*" columns, which don't exist!
```

The ViewScan ends up with:
```rust
property_mapping: {
    "c_user_id": Column("c_user_id"),
    "c_name": Column("c_name"),
    // ... only "c_*" columns!
    // ❌ Missing "a_*" and "b_*" columns!
}
```

When the final SELECT tries to access `a.name`, it fails because there's no mapping for alias "a".

## Root Cause

**Key Insight**: The `with_alias` parameter is a COMPOSITE key like "a_b_c", but we need to extract ALL individual aliases ["a", "b", "c"] and build property mappings for each one.

**Architecture Flaw**:
1. `build_chained_with_match_cte_plan()` processes WITH clauses iteratively
2. For `WITH a, b, c`, it generates key "a_b_c" (sorted, underscore-joined)
3. This key is used as the `with_alias` parameter to `create_cte_reference()`
4. But `create_cte_reference()` treats it as a SINGLE alias name, not a composite

## Solution Design

### Option 1: Parse Composite Alias in `create_cte_reference()`

```rust
fn create_cte_reference(
    cte_name: &str,
    with_alias: &str,  // e.g., "a_b_c"
    cte_schemas: &HashMap<String, (Vec<SelectItem>, Vec<String>)>,
) -> LogicalPlan {
    // Parse composite alias into individual aliases
    let individual_aliases: Vec<&str> = with_alias.split('_').collect();
    
    let mut property_mapping = HashMap::new();
    
    if let Some((select_items, _)) = cte_schemas.get(cte_name) {
        // For each individual alias, find its columns
        for alias in &individual_aliases {
            let prefix = format!("{}_", alias);
            for item in select_items {
                if let Some(col_alias) = &item.col_alias {
                    if col_alias.0.starts_with(&prefix) {
                        // Extract Cypher property: "a_name" -> "name"
                        let cypher_prop = col_alias.0.strip_prefix(&prefix).unwrap();
                        
                        // Map with ALIAS NAMESPACE: "a.name" (not just "name")
                        let key = format!("{}.{}", alias, cypher_prop);
                        property_mapping.insert(key, PropertyValue::Column(col_alias.0.clone()));
                    }
                }
            }
        }
    }
    
    // Use the composite alias for the ViewScan table_alias
    // (e.g., "FROM with_a_b_c_cte_1 AS a_b_c")
    LogicalPlan::GraphNode(GraphNode {
        input: Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan {
            source_table: cte_name.to_string(),
            property_mapping,
            // ... rest of fields
        }))),
        alias: with_alias.to_string(),  // "a_b_c"
        // ...
    })
}
```

**Problem with Option 1**: This mixes "alias-qualified" keys (`"a.name"`) with simple keys (`"name"`), breaking assumptions elsewhere in the code.

### Option 2: Create Separate GraphNodes for Each Alias ✅ **PREFERRED**

Instead of one GraphNode with composite alias "a_b_c", create MULTIPLE GraphNodes, one per alias:

```rust
fn create_multi_alias_cte_reference(
    cte_name: &str,
    with_alias: &str,  // e.g., "a_b_c"
    cte_schemas: &HashMap<String, (Vec<SelectItem>, Vec<String>)>,
) -> LogicalPlan {
    let individual_aliases: Vec<&str> = with_alias.split('_').collect();
    
    // Build a Projection that references the CTE multiple times
    // FROM with_a_b_c_cte_1 AS a_b_c
    // This makes columns available under the composite alias
    
    let mut projection_items = Vec::new();
    
    if let Some((select_items, _)) = cte_schemas.get(cte_name) {
        // Create projection items for ALL columns, using composite alias
        for item in select_items {
            if let Some(col_alias) = &item.col_alias {
                projection_items.push(ProjectionItem {
                    expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(with_alias.to_string()),  // "a_b_c"
                        column: Column(PropertyValue::Column(col_alias.0.clone())),
                    }),
                    col_alias: Some(ColumnAlias(col_alias.0.clone())),
                });
            }
        }
    }
    
    // Build ViewScan with ALL aliases' columns
    let property_mapping = build_property_mapping_for_all_aliases(
        &individual_aliases, 
        cte_schemas.get(cte_name).map(|(items, _)| items)
    );
    
    LogicalPlan::Projection(Projection {
        input: Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan {
                source_table: cte_name.to_string(),
                property_mapping,
                id_column: "id".to_string(),
                // ... rest
            }))),
            alias: with_alias.to_string(),  // Use composite as table alias
            label: None,
            is_denormalized: false,
            projected_columns: None,
        })),
        items: projection_items,
        distinct: false,
    })
}

fn build_property_mapping_for_all_aliases(
    aliases: &[&str],
    select_items: Option<&Vec<SelectItem>>,
) -> HashMap<String, PropertyValue> {
    let mut mapping = HashMap::new();
    
    if let Some(items) = select_items {
        for alias in aliases {
            let prefix = format!("{}_", alias);
            for item in items {
                if let Some(col_alias) = &item.col_alias {
                    if col_alias.0.starts_with(&prefix) {
                        // Extract property: "a_name" -> "name"
                        let prop = col_alias.0.strip_prefix(&prefix).unwrap();
                        
                        // Key is just the property name (no alias prefix)
                        // The GraphNode alias handles the namespace
                        mapping.insert(prop.to_string(), PropertyValue::Column(col_alias.0.clone()));
                    }
                }
            }
        }
    }
    
    mapping
}
```

**Problem with Option 2**: The ViewScan property_mapping still uses simple keys like `"name"`, but we have multiple aliases accessing it. We need NAMESPACE-aware property resolution.

### Option 3: Multiple GraphNodes in a Chain ✅ **BEST SOLUTION**

Create a CHAIN of GraphNodes, one per alias, all referencing the same CTE:

```rust
// For "WITH a, b, c", create:
// GraphNode(alias="a", input=ViewScan("with_a_b_c_cte_1", mapping for "a_*"))
// GraphNode(alias="b", input=ViewScan("with_a_b_c_cte_1", mapping for "b_*"))
// GraphNode(alias="c", input=ViewScan("with_a_b_c_cte_1", mapping for "c_*"))
// Then wrap them in a Joins/Projection structure

fn create_cte_reference(
    cte_name: &str,
    with_alias: &str,  // e.g., "a_b_c"
    cte_schemas: &HashMap<String, (Vec<SelectItem>, Vec<String>)>,
) -> LogicalPlan {
    let individual_aliases: Vec<&str> = with_alias.split('_').collect();
    
    if individual_aliases.len() == 1 {
        // Simple case: single alias
        return create_single_alias_cte_reference(cte_name, with_alias, cte_schemas);
    }
    
    // Multi-alias case: Create ViewScan with ALL columns mapped
    let mut full_property_mapping = HashMap::new();
    
    if let Some((select_items, _)) = cte_schemas.get(cte_name) {
        for alias in &individual_aliases {
            let prefix = format!("{}_", alias);
            for item in select_items {
                if let Some(col_alias) = &item.col_alias {
                    if col_alias.0.starts_with(&prefix) {
                        let prop = col_alias.0.strip_prefix(&prefix).unwrap();
                        // CRITICAL: Use composite key "alias_property" so properties don't conflict
                        let key = format!("{}_{}", alias, prop);
                        full_property_mapping.insert(key, PropertyValue::Column(col_alias.0.clone()));
                    }
                }
            }
        }
    }
    
    // Return a GraphNode with the COMPOSITE alias
    // The property_mapping has ALL aliases' properties
    LogicalPlan::GraphNode(GraphNode {
        input: Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan {
            source_table: cte_name.to_string(),
            property_mapping: full_property_mapping,
            id_column: "id".to_string(),
            // ...
        }))),
        alias: with_alias.to_string(),  // "a_b_c"
        // ...
    })
}
```

**BUT**: This changes the property access pattern - now we'd need `a_b_c.a_name` instead of `a.name`.

### Option 4: Fix at Query Rewrite Level ✅ **ACTUAL BEST SOLUTION**

The real issue: When we reference a CTE with multiple aliases, we create GraphNodes that use individual aliases ("a", "b", "c") but they ALL point to the same CTE ViewScan.

**The fix**: Update `expand_table_alias_to_select_items()` to handle composite CTEs correctly!

When expanding alias "a" and we find it maps to CTE "with_a_b_c_cte_1":
1. Look for columns with prefix "a_" in that CTE ✅ (already done)
2. **BUT**: When rewriting expressions, use the COMPOSITE alias "a_b_c" as the table reference!

Actually wait - let me re-check the logs... The issue is that the final SELECT references `a`, `b`, `c` as separate aliases, but the FROM clause doesn't include them!

The REAL problem is simpler: **The final query doesn't reference the last CTE at all!**

```sql
SELECT a.full_name, b.full_name, c.full_name, d.full_name
FROM brahmand.user_follows_bench AS t4  -- ❌ Should be: FROM with_a_b_c_cte_1
```

This is actually a DIFFERENT bug - the final rendering doesn't use the CTE!

## Actual Root Cause (Revised)

After deeper analysis, the issues are:

1. ✅ **CTE 1 & 2 work fine** - column rewriting works
2. ❌ **CTE 3 missing `a` and `b` columns** - Only includes `c_*` columns
3. ❌ **Final SELECT doesn't use CTE 3** - Starts from scratch with new table scans

The problem is in **HOW THE CTE IS BUILT**, not how it's referenced!

When processing `WITH a, b, c`, the code needs to:
1. Expand alias "a" → get `a_*` columns
2. Expand alias "b" → get `b_*` columns
3. Expand alias "c" → get `c_*` columns
4. **Combine ALL of them into the CTE SELECT**

Let me check the CTE building code...
