# PropertyValue Migration - Find/Replace Guide

## Overview
We need to wrap all `Column(String)` constructors with `PropertyValue::Column()` because Column now expects `PropertyValue` instead of `String`.

## Step 0: Add Import Statements

For each file you're fixing, add this import at the top (after existing `use` statements):

```rust
use crate::graph_catalog::expression_parser::PropertyValue;
```

**Files that need this import:**
- `src/render_plan/plan_builder.rs` (already has it)
- `src/render_plan/plan_builder_helpers.rs`
- `src/render_plan/filter_pipeline.rs`
- `src/render_plan/cte_extraction.rs` (already has it)
- `src/render_plan/expression_utils.rs`

---

## VS Code Find/Replace Instructions

### Setup
1. Open VS Code
2. Press `Ctrl+Shift+H` to open Find/Replace in Files
3. Enable regex mode (click the `.*` button or press `Alt+R`)
4. Set "Files to include": `src/render_plan/**/*.rs`

---

## Fix Pattern 1: Column with string literal
**Find:** `Column\("([^"]+)"\)`
**Replace:** `Column(PropertyValue::Column("$1".to_string()))`
**Files:** `src/render_plan/*.rs`
**Estimated:** ~15 replacements

---

## Fix Pattern 2: Column with .to_string()
**Find:** `Column\(([a-z_]+)\.to_string\(\)\)`
**Replace:** `Column(PropertyValue::Column($1.to_string()))`
**Files:** `src/render_plan/*.rs`
**Estimated:** ~8 replacements

---

## Fix Pattern 3: Column with .clone()
**Find:** `Column\(([a-z_]+)\.clone\(\)\)`
**Replace:** `Column(PropertyValue::Column($1.clone()))`
**Files:** `src/render_plan/*.rs`
**Estimated:** ~25 replacements

---

## Fix Pattern 4: Column with variable (no method call)
**Find:** `Column\(([a-z_][a-z0-9_]*)\)([^.])`
**Replace:** `Column(PropertyValue::Column($1))$2`
**Files:** `src/render_plan/*.rs`
**Estimated:** ~5 replacements

---

## Fix Pattern 5: Comparisons with PropertyValue (col.0 == "string")
**Find:** `\.column\.0 == "([^"]+)"`
**Replace:** `.column.0.raw() == "$1"`
**Files:** `src/render_plan/*.rs`
**Estimated:** ~8 replacements

---

## Fix Pattern 6: Format with PropertyValue (format!("{}", column))
**Find:** `format!\("([^"]*\{\}[^"]*)", ([^,)]+)\.column\.0\)`
**Replace:** `format!("$1", $2.column.0.raw())`
**Files:** `src/render_plan/*.rs`
**Estimated:** ~5 replacements

---

## Fix Pattern 7: Column field access in render_expr.rs
**Find:** `col\.0`
**Replace:** `col.0.raw()`
**Files:** `src/render_plan/render_expr.rs` only (line 67, 163, 240)
**Manual fix needed:** These need contextual handling

---

## Fix Pattern 8: Direct .column.0 comparisons
**Find:** `prop_access\.column\.0 == "([^"]+)"`
**Replace:** `prop_access.column.0.raw() == "$1"`
**Files:** `src/render_plan/plan_builder.rs`
**Estimated:** ~6 replacements

---

## Manual Fixes Required

### 1. `src/render_plan/render_expr.rs` Line 163
```rust
// OLD:
println!("DEBUG TryFrom: Converting PropertyAccessExp - alias={}, column={}", pa.table_alias.0, pa.column.0);

// NEW:
println!("DEBUG TryFrom: Converting PropertyAccessExp - alias={}, column={}", pa.table_alias.0, pa.column.raw());
```

### 2. `src/render_plan/render_expr.rs` Line 240
```rust
// OLD:
Ok(Column(col.0))

// NEW:
Ok(Column(col.0.clone()))  // col.0 is already PropertyValue
```

### 3. `src/render_plan/plan_builder_helpers.rs` Line 67
```rust
// OLD:
RenderExpr::Column(col) => col.0.clone(),

// NEW:
RenderExpr::Column(col) => col.0.raw().to_string(),
```

### 4. `src/render_plan/filter_pipeline.rs` Lines 309, 315, 432, 438
These involve format! with column variables - use `.raw()`:
```rust
// Line 309:
.unwrap_or_else(|_| column.raw().to_string());

// Line 315:
let result = format!("{}.{}", table_alias, column.raw());

// Lines 432, 438:
Column(PropertyValue::Column(format!("end_{}", column.raw())))
Column(PropertyValue::Column(format!("start_{}", column.raw())))
```

### 5. `src/render_plan/plan_builder_helpers.rs` Lines 625, 627, 630
Format with column variables:
```rust
// Line 625:
format!("start_node.{}", column.raw())

// Line 627:
format!("end_node.{}", column.raw())

// Line 630:
format!("{}.{}", table_alias, column.raw())
```

### 6. `src/render_plan/plan_builder.rs` Line 2356
Wildcard comparison:
```rust
// OLD:
if prop.column.0 == "*" =>

// NEW:
if prop.column.0.raw() == "*" =>
```

### 7. `src/render_plan/plan_builder.rs` Line 2507
println with PropertyValue:
```rust
// OLD:
"DEBUG: Created GroupBy CTE pattern with table_alias={}, key_column={}", table_alias, key_column

// NEW:
"DEBUG: Created GroupBy CTE pattern with table_alias={}, key_column={}", table_alias, key_column.raw()
```

---

## Execution Order

1. **First:** Run Pattern 1-4 replacements (these are safe bulk operations)
2. **Then:** Apply manual fixes for `render_expr.rs` 
3. **Then:** Apply manual fixes for `plan_builder_helpers.rs`
4. **Then:** Apply manual fixes for `filter_pipeline.rs`
5. **Then:** Run Pattern 5-6 for comparison/format fixes
6. **Finally:** Apply remaining manual fixes in `plan_builder.rs`

---

## Verification

After all replacements:

```powershell
# Should compile successfully
cargo build --bin clickgraph

# Check for any remaining Column(String) patterns
rg "Column\([a-z_]+\.(?:clone|to_string)\(\)\)" --type rust

# Check for any remaining .column.0 comparisons without .raw()
rg "\.column\.0 (?:==|!=) \"" --type rust
```

---

## Expected Result

After applying all fixes:
- ✅ All 74 compilation errors resolved
- ✅ ViewScan uses PropertyValue throughout
- ✅ Column wraps PropertyValue correctly
- ✅ SQL generation calls PropertyValue.to_sql()
- ✅ Integration test should work!

---

## If You Get Stuck

**Common issue:** Pattern replacement creates incorrect nesting

**Solution:** Use "Replace All" cautiously. For complex cases, replace one at a time and verify.

**Fallback:** If regex gets too complex, search for each error file individually:
- `src/render_plan/plan_builder.rs`: ~40 errors
- `src/render_plan/plan_builder_helpers.rs`: ~10 errors  
- `src/render_plan/filter_pipeline.rs`: ~6 errors
- `src/render_plan/render_expr.rs`: ~3 errors

And manually apply the PropertyValue::Column() wrapper to each Column constructor.
