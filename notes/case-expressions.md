# CASE Expressions Implementation

## Summary
Complete implementation of CASE WHEN THEN ELSE END conditional expressions in ClickGraph, supporting both simple CASE (`CASE x WHEN val THEN result`) and searched CASE (`CASE WHEN condition THEN result`) syntax.

## How It Works

### Parser Changes
- Extended `open_cypher_parser/expression.rs` with `parse_case_expression()` function
- Added `Case` AST node with `expr` (Option for simple CASE), `when_then` pairs, and `else_expr`
- Fixed parser logic to distinguish simple vs searched CASE using string prefix checking instead of peek combinators

### Logical Planning
- Added `LogicalCase` struct mirroring AST structure
- Integrated into expression conversion pipeline

### Render Planning
- Added `RenderCase` struct for SQL generation phase
- Proper conversion from LogicalCase to RenderCase

### SQL Generation
- **Simple CASE**: Uses ClickHouse `caseWithExpression(expr, val1, res1, val2, res2, ..., default)` for optimal performance
- **Searched CASE**: Uses standard SQL `CASE WHEN condition THEN result ELSE default END` syntax
- **Property Resolution**: Fixed property mapping in expressions (`u.name` → `u.full_name`) during SQL generation

## Key Technical Details

### ClickHouse Optimization
Simple CASE expressions with multiple WHEN/THEN pairs are converted to ClickHouse's efficient `caseWithExpression` function:
```sql
-- Cypher: CASE u.name WHEN 'Alice' THEN 'Admin' WHEN 'Bob' THEN 'Moderator' ELSE 'User' END
-- SQL: caseWithExpression(u.full_name, 'Alice', 'Admin', 'Bob', 'Moderator', 'User')
```

### Property Mapping Fix
Critical fix for property resolution in expressions - logical property names (from Cypher) are mapped to actual column names (from YAML schema) during SQL generation, not during parsing.

## Test Results
- ✅ Simple CASE: `CASE u.name WHEN 'Alice Johnson' THEN 'Admin' WHEN 'Bob Smith' THEN 'Moderator' ELSE 'User' END`
- ✅ Searched CASE: `CASE WHEN u.name = 'Alice Johnson' THEN 'VIP' WHEN u.name = 'Bob Smith' THEN 'Premium' ELSE 'Standard' END`
- ✅ **WHERE clauses**: `WHERE CASE u.name WHEN 'Alice Johnson' THEN true WHEN 'Bob Smith' THEN true ELSE false END`
- ✅ **Function calls**: `length(CASE u.name WHEN 'Alice Johnson' THEN 'Administrator' ELSE 'User' END)`
- ✅ **Complex expressions**: `CASE u.name WHEN 'Alice' THEN 'VIP' ELSE 'Standard' END || ' User'`
- ✅ All return correct results with proper property mapping and boolean literal handling

## Extended Usage Support
CASE expressions now work in all query contexts:
- **RETURN clauses**: Basic conditional logic in result expressions
- **WHERE clauses**: Conditional filtering with boolean results
- **Function arguments**: CASE expressions can be passed to functions like `length()`, `substring()`, etc.
- **Complex expressions**: CASE can be part of string concatenation, arithmetic, etc.

## Critical Bug Fix: Boolean Literals
**Issue**: Boolean literals in CASE expressions were rendered as "FfalseALSE" instead of "false"
**Root Cause**: Incorrect boolean literal rendering in `to_sql_query.rs` - boolean values were being converted to strings incorrectly
**Fix**: Updated boolean literal rendering to properly output "true"/"false" strings for SQL generation
**Impact**: This fix was critical for CASE expressions in WHERE clauses and any boolean-valued CASE expressions

## Files Modified
- `src/open_cypher_parser/expression.rs` - Parser implementation
- `src/query_planner/logical_expr/mod.rs` - Logical expression structures
- `src/render_plan/render_expr.rs` - Render expression structures
- `src/clickhouse_query_generator/to_sql_query.rs` - SQL generation with caseWithExpression and boolean literal fix
- `test_case_expressions.py` - Comprehensive integration tests for all CASE expression contexts

## Gotchas & Limitations
- Property mapping must be handled during SQL generation, not parsing (architectural consideration)
- ClickHouse caseWithExpression requires all arguments to be provided (no optional ELSE)
- Simple CASE with single WHEN/THEN could be optimized further but current implementation handles all cases

## Future Work
- Consider moving property resolution to analyzer phase for better architecture
- Add support for more complex expressions in CASE conditions
- Performance benchmarking against standard SQL CASE


