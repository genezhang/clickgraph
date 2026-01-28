# WITH + GROUP BY Fix - Column Resolution Phase

## Current Status

✅ **GROUP BY Fixed**: Using correct ID column (`GROUP BY friend.id`)
✅ **anyLast() Wrapping**: Working correctly  
✅ **CTE Schema Tracking**: Extended with `alias_to_id_column`
🔧 **Column Name Resolution**: In progress

## The Problem

When rendering `RETURN friend.id` after `WITH friend, count(*) AS cnt`:

**Generated SQL** (WRONG):
```sql
SELECT cnt_friend.id AS `friend.id`
FROM with_cnt_friend_cte_1 AS cnt_friend
```

**Should be**:
```sql
SELECT cnt_friend.friend_id AS `friend.id`  
FROM with_cnt_friend_cte_1 AS cnt_friend
```

**Root Cause**: Missing explicit property mapping from Cypher `(alias, property)` to CTE column names.

## The Solution: Explicit Property Mapping

### Current CTE Schema Type
```rust
HashMap<String, (
    Vec<SelectItem>,          // 1. Column definitions
    Vec<String>,              // 2. Property names
    HashMap<String, String>   // 3. alias → ID column
)>
```

### Proposed CTE Schema Type
```rust
HashMap<String, (
    Vec<SelectItem>,                      // 1. Column definitions
    Vec<String>,                          // 2. Property names  
    HashMap<String, String>,              // 3. alias → ID column
    HashMap<(String, String), String>     // 4. (alias, property) → column_name ✨ NEW
)>
```

### Why Explicit Mapping?

**User's Key Insight**: Naming convention-based reverse engineering is fragile:
- ❌ Breaks with nested CTEs
- ❌ Breaks with complex property names containing underscores
- ❌ Requires guessing which separator was used (`.` vs `_`)

**Explicit Mapping Approach**:
- ✅ Track mapping when CTE is **created** (not reconstructed later)
- ✅ Works for any nesting level
- ✅ Works for any property name
- ✅ Single source of truth

## Implementation Steps

### 1. Add Helper Function ✅ DONE

```rust
fn build_property_mapping_from_columns(
    select_items: &[SelectItem],
) -> HashMap<(String, String), String> {
    // Extract (alias, property) → column_name from column names
    // Handles both "alias.property" and "alias_property" patterns
}
```

### 2. Update CTE Schema Storage (3 locations)

**Location 1**: `build_chained_with_match_cte_plan` - Nested CTE extraction (~line 1589)
```rust
let property_mapping = build_property_mapping_from_columns(&select_items);
cte_schemas.insert(
    cte.cte_name.clone(),
    (select_items, property_names, alias_to_id_column, property_mapping),  // Add 4th element
);
```

**Location 2**: VLP UNION schema extraction (~line 1645)
```rust
let property_mapping = build_property_mapping_from_columns(&union_select_items);
cte_schemas.insert(
    "__union_vlp".to_string(),
    (items, names, union_alias_to_id, property_mapping),  // Add 4th element
);
```

**Location 3**: WITH CTE schema storage (~line 2108)
```rust
let property_mapping = build_property_mapping_from_columns(&select_items_for_schema);
cte_schemas.insert(
    cte_name.clone(),
    (select_items_for_schema, property_names_for_schema, alias_to_id_column, property_mapping),
);
```

### 3. Update All Function Signatures

Update ~15+ function signatures that reference `cte_schemas` type:
- `expand_table_alias_to_select_items()`
- `expand_table_alias_to_group_by_id_only()`
- `find_id_column_with_cte_context()`
- All pattern matching: `(items, _, _)` → `(items, _, _, _)`
- All pattern matching: `(items, names, alias_to_id)` → `(items, names, alias_to_id, prop_map)`

### 4. Use Explicit Mapping in Final SELECT Rewriting

Replace the fragile reverse mapping code (~line 2320-2380) with:

```rust
// Build mappings from CTE schema - USE EXPLICIT MAPPING
if let Some((_items, _names, _alias_to_id, property_mapping)) = cte_schemas.get(&from_ref.name) {
    log::info!("🔍 Using explicit property mapping with {} entries", property_mapping.len());
    
    // Direct lookup instead of pattern matching
    let mut reverse_mapping: HashMap<(String, String), String> = property_mapping.clone();
    
    log::info!("🔧 build_chained_with_match_cte_plan: Built reverse mapping with {} entries", reverse_mapping.len());
}
```

### 5. Rewrite PropertyAccessExp in Final SELECT

```rust
// For PropertyAccessExp("friend", "id"), look up:
if let Some(cte_col) = reverse_mapping.get(&(alias, property)) {
    // Rewrite to: PropertyAccessExp("cnt_friend", "friend_id")
    PropertyAccessExp {
        table_alias: from_alias,  // FROM alias like "cnt_friend"
        column: Column(PropertyValue::Column(cte_col.clone())),  // CTE column "friend_id"
    }
}
```

## Testing Plan

1. **Unit Test**: Verify `build_property_mapping_from_columns()` handles both patterns
2. **Integration Test**: IC1 query with `WITH friend, count(*) AS cnt`
3. **Nested Test**: Multiple WITH clauses to verify inheritance
4. **Edge Cases**:
   - Properties with underscores: `first_name`
   - Aggregate columns: `cnt` (no separator)
   - Multiple aliases: `WITH a, b, c`

## Expected Impact

- **Fixes**: 5 LDBC queries (IC1, IC3, IC4, IC7, IC8)
- **Pass rate**: 54% (13/24) → ~75% (18/24)
- **Robustness**: Works with any nesting level, any property names

## Files to Modify

- `src/render_plan/plan_builder.rs`:
  - Add helper function (DONE)
  - Update 3x `cte_schemas.insert()` calls
  - Update ~15 function signatures
  - Replace reverse mapping code with explicit lookup
  - Update final SELECT rewriting logic

## Next Session Actions

1. Build and test with explicit mapping
2. Verify logs show non-zero property_mapping entries
3. Test IC1 query success
4. Run full LDBC audit
5. Document in STATUS.md and CHANGELOG.md
