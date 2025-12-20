# Hardcoded Fallback Audit
**Date**: December 20, 2025  
**Purpose**: Identify and classify all hardcoded column name fallbacks in the codebase

## Executive Summary

Found **3 categories** of hardcoded values:
1. ✅ **Test code** - Acceptable (test schemas)
2. ⚠️ **Error-prone fallbacks** - Should be errors
3. ✅ **Legitimate defaults** - Schema conventions

---

## Critical Issues (Need Fixing)

### 1. `to_sql_query.rs:1077` - NULL check fallback ⚠️ HIGH PRIORITY

**Location**: `src/clickhouse_query_generator/to_sql_query.rs:1077`

```rust
let id_col = RELATIONSHIP_COLUMNS.with(|rc| {
    let map = rc.borrow();
    if let Some((from_id, _to_id)) = map.get(table_alias) {
        from_id.clone()
    } else {
        // Fallback for non-relationship tables (shouldn't happen with r.*)
        "id".to_string()  // ⚠️ HARDCODED FALLBACK
    }
});
```

**Problem**: If `r.*` is used on a relationship but the alias isn't in RELATIONSHIP_COLUMNS, we silently use "id" which is WRONG.

**Impact**: 
- User writes: `OPTIONAL MATCH (a)-[r:REL]->(b) WHERE r IS NULL`
- If `r` not in map: generates `r.id IS NULL` instead of error
- Wrong column = wrong results = silent corruption

**Root Cause**: This fallback fires when:
1. `populate_relationship_columns_from_plan()` failed to populate the map
2. Bug in JOIN creation (didn't set from_id_column)
3. Logic error in plan building

**Solution**: Return error instead of fallback:
```rust
let id_col = RELATIONSHIP_COLUMNS.with(|rc| {
    let map = rc.borrow();
    map.get(table_alias)
        .map(|(from_id, _)| from_id.clone())
        .ok_or_else(|| {
            format!("Internal error: Relationship alias '{}' not found in column mapping. \
                     This indicates a bug in query planning - relationship JOINs should \
                     populate from_id_column during creation.", table_alias)
        })
})?;
```

**Rationale**: 
- This is `r.*` wildcard - ALWAYS a relationship
- If alias not in map = planning bug
- Better to fail loudly than silently use wrong column

---

### 2. `plan_builder.rs:7047, 7099` - Node ID extraction fallbacks

**Location 1**: Line 7047
```rust
let left_id_col = extract_id_column(&graph_rel.left)
    .unwrap_or_else(|| "id".to_string());  // ⚠️ FALLBACK
```

**Location 2**: Line 7099
```rust
let right_id_col = extract_id_column(&right_joins.input)
    .unwrap_or_else(|| "id".to_string());  // ⚠️ FALLBACK
```

**Problem**: When `extract_id_column()` fails to find ID column for a node, we assume "id"

**When This Fires**:
- Node has no ViewScan in its plan tree
- Label not found in schema
- Schema doesn't define id_column for the label

**Impact**: Generates SQL with wrong column name → query fails

**Solution**: Propagate error instead:
```rust
let left_id_col = extract_id_column(&graph_rel.left)
    .ok_or_else(|| RenderBuildError::InvalidRenderPlan(
        format!("Cannot determine ID column for node '{}'. \
                 Node might have invalid schema or missing ViewScan. \
                 Check that label exists in schema YAML.", 
                graph_rel.left_connection)
    ))?;
```

**Why This Is Better**:
- Schema MUST define ID column (it's required in YAML)
- If not found = schema error or planning bug
- User gets clear error instead of cryptic "column 'id' not found"

---

## Acceptable Hardcoded Values

### 1. Test Code - Variable Length CTE Tests ✅

**Locations**: `variable_length_cte.rs:2250, 2290, 3269, etc.`

**Usage**: Test schemas for unit tests
```rust
"from_id",  // test relationship from column
"to_id",    // test relationship to column
```

**Status**: ✅ **OK** - Test data, not production code

---

### 2. Schema Defaults in `graph_catalog/pattern_schema.rs` ✅

**Locations**: Lines 1058, 1063-1064, 1151-1152, etc.

**Usage**: Default column names for relationships when not specified in schema:
```rust
RelationshipSchema {
    from_id: "from_id".to_string(),  // default convention
    to_id: "to_id".to_string(),      // default convention
    column_names: vec!["from_id".to_string(), "to_id".to_string()],
}
```

**Status**: ✅ **OK** - Schema conventions
- Many graph databases use `from_id`/`to_id` as standard
- These are defaults when schema doesn't specify
- Users can override in YAML with `from_id: Person1Id`

---

### 3. Documentation Comments in `render_plan/mod.rs` ✅

**Locations**: Lines 98, 102

```rust
/// For relationship tables: the source node ID column name (e.g., "Person1Id", "from_id")
/// For relationship tables: the target node ID column name (e.g., "Person2Id", "to_id")
```

**Status**: ✅ **OK** - Just examples in documentation

---

## Lower Priority Issues

### 1. `plan_builder_helpers.rs:1262` - extract_id_column helper

**Location**: `src/render_plan/plan_builder_helpers.rs:1262`

```rust
pub fn extract_id_column(plan: &LogicalPlan) -> Option<String> {
    // ... search logic ...
    .unwrap_or_else(|| "id".to_string())  // ⚠️ FALLBACK
}
```

**Problem**: Returns `Some("id")` instead of `None` when column not found

**Impact**: Callers can't distinguish "found id column named 'id'" from "didn't find any column"

**Solution**: Return `None` and let callers handle it:
```rust
pub fn extract_id_column(plan: &LogicalPlan) -> Option<String> {
    // Remove .unwrap_or_else, just return Option
    // Callers already handle None case
}
```

---

### 2. `cte_extraction.rs:592, 608` - CTE generation fallbacks

**Locations**: Lines 592, 608

```rust
// Fallback to "id" if not found
// Fallback to "id" if schema not available or table not found
```

**Context**: Comments near fallback logic in CTE property mapping

**Status**: Need to check if these are legitimate or should error

---

## Recommendations

### Immediate Actions (High Priority)

1. **Fix `to_sql_query.rs:1077`** ⚠️ **CRITICAL**
   - Replace fallback with error
   - Add test for missing relationship column mapping
   - Verify `populate_relationship_columns_from_plan()` coverage

2. **Fix `plan_builder.rs:7047, 7099`** ⚠️ **HIGH**
   - Replace fallbacks with proper errors
   - Add test for missing node ID columns
   - Improve error message to help debugging

3. **Fix `plan_builder_helpers.rs:1262`** 
   - Return `None` instead of `Some("id")`
   - Audit all callers to handle `None` properly

### Testing Strategy

For each fix, add tests:
```rust
#[test]
#[should_panic(expected = "Cannot determine ID column")]
fn test_missing_node_id_column_error() {
    // Relationship with node that has no ID column in schema
    // Should error, not fall back to "id"
}

#[test]
#[should_panic(expected = "Relationship alias.*not found")]
fn test_missing_relationship_column_mapping_error() {
    // r IS NULL but r not in RELATIONSHIP_COLUMNS map
    // Should error, not fall back to "id"
}
```

### Long-term Strategy

**Principle**: **Fail fast, fail loud**
- Hardcoded fallbacks hide bugs
- Schema errors should surface immediately
- Better stack trace > silent wrong results

**Schema Validation**:
- All labels MUST define `id_column` in YAML
- All relationships MUST define `from_id` and `to_id`
- Validation during schema loading (not at query time)

**Error Messages**:
```
❌ BAD:  "Column 'id' not found in table 'Person_knows_Person'"
✅ GOOD: "Cannot determine ID column for relationship 'k:KNOWS'. 
         Check that schema defines from_id/to_id in YAML."
```

---

## Summary

**Found**: 3 critical fallbacks that should be errors  
**Impact**: Silent data corruption when schema/planning has bugs  
**Solution**: Replace fallbacks with descriptive errors  
**Benefit**: Faster debugging, better error messages, no silent failures

**Philosophy**: If we don't know the column name, we should ASK (error), not GUESS (fallback).
