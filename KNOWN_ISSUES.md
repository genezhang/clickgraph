# Known Issues

**Active Issues**: 0 bugs, 3 feature limitations  
**Last Updated**: January 7, 2026

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Current Bugs

**🎉 No active bugs!**  
*Last bug fixed: Array subscript support (January 7, 2026)*

### ~~Array Subscript Support~~ ✅ **FIXED** - January 7, 2026
**Was**: Array subscript operations on functions and arrays were not implemented

**Now Working**:
```cypher
// ✅ Works on functions
MATCH (u:User) WHERE u.user_id = 1
RETURN labels(u)[1] as first_label  // Returns "User"

// ✅ Works on literal arrays  
RETURN [1, 2, 3][2] as second_element  // Returns 2

// ✅ Works on any expression
MATCH (u)-[:FOLLOWS|AUTHORED*1..2]->(x)
RETURN labels(x)[1] as node_type  // Returns node type
```

**Implementation**:
- Added `ArraySubscript` variant to AST hierarchy (Expression → LogicalExpr → RenderExpr)
- Modified parser to handle `[index]` syntax after any expression
- SQL generation outputs ClickHouse `array[index]` format (1-based indexing)
- Special handling for `labels()` function expansion with subscripts

**Files Modified**: `expression.rs`, `ast.rs`, `logical_expr/mod.rs`, `render_expr.rs`, `projection_tagging.rs`, `to_sql_query.rs`

---

## Current Status

**Bug Status**: ✅ **0 known bugs**  
- Integration test pass rate: **98.5%** (197/200 passing)
- Multi-type VLP test pass rate: **85%** (17/20 passing, 3 have unrelated aggregate query issue)
- All core functionality working correctly
- VLP + WITH clause path functions fixed (Dec 26, 2025)
- VLP cross-functional testing complete (Dec 25, 2025)
- Denormalized VLP fixed (Dec 25, 2025)
- Property pruning complete (Dec 24, 2025)

---

## Recently Fixed

### Path Functions in WITH Clauses (CTEs)
**Status**: ✅ **FIXED** - December 26, 2025

**Problem**: VLP queries with `length(path)` in WITH clauses generated CTEs that used VLP internal aliases (`start_node`/`end_node`) instead of Cypher aliases (`u1`/`u2`) in SELECT items.

**Root Cause**: The `rewrite_vlp_union_branch_aliases` function was incorrectly rewriting WITH CTE bodies. When checking if endpoint aliases had JOINs, it checked the *outer* plan's JOINs, but when rewriting CTE bodies, those nested RenderPlans don't have JOINs yet (they're in the outer plan). This caused it to incorrectly rewrite `u1` → `start_node`.

**Fix**: Modified `rewrite_vlp_union_branch_aliases` to only apply `t` → `vlp_alias` mapping when rewriting CTE bodies, excluding endpoint alias rewrites entirely for CTEs. WITH CTEs have their own JOINs (`JOIN users AS u1`) so SELECT items should use those Cypher aliases.

**Verification**: All VLP + WITH clause tests pass:
- `test_vlp_with_filtering` ✅
- `test_vlp_with_and_aggregation` ✅

---

## Known Limitations

**Documentation**: [docs/development/vlp-cross-functional-testing.md](docs/development/vlp-cross-functional-testing.md)

---

## Feature Limitations

The following Cypher features are **not implemented** (by design - read-only query engine):

### 1. Procedure Calls (APOC/GDS)
**Status**: ⚠️ NOT IMPLEMENTED (out of scope)  
**Example**: `CALL apoc.algo.pageRank(...)`  
**Reason**: ClickGraph is a SQL query translator, not a procedure runtime  
**Impact**: Blocks 4 LDBC BI queries (bi-10, bi-15, bi-19, bi-20)

### 2. Bidirectional Relationship Patterns  
**Status**: ⚠️ NOT IMPLEMENTED (non-standard syntax)  
**Example**: `(a)<-[:TYPE]->(b)` (both arrows on same relationship)  
**Workaround**: Use undirected pattern `(a)-[:TYPE]-(b)` or two MATCH clauses  
**Impact**: Blocks 1 LDBC BI query (bi-17)

### 3. Write Operations
**Status**: ❌ OUT OF SCOPE (read-only by design)  
**Not Supported**: `CREATE`, `SET`, `DELETE`, `MERGE`, `REMOVE`  
**Reason**: ClickGraph is a read-only analytical query engine for ClickHouse  
**Alternative**: Use native ClickHouse INSERT statements for data loading

---

## Test Suite Status

**Integration Tests**: ✅ **100% pass rate** (549 passed, 54 xfailed, 12 skipped)
- Core queries: **549 passed** ✅
- Security graph: **94 passed, 4 xfailed** ✅  
- Variable-length paths: **24 passed, 1 skipped, 2 xfailed** ✅
- VLP cross-functional: **6/6 passing** ✅ (Dec 25, 2025)
- Pattern comprehensions: **5 passed** ✅
- Property expressions: **28 passed, 3 xfailed** ✅
- Node uniqueness: **4 passed** ✅
- Multiple UNWIND: **7 passed** ✅

**LDBC Benchmark**: **29/41 queries passing (70%)**
- All SHORT queries pass ✅
- Remaining 12 blocked by: procedures (4), bidirectional patterns (1), other edge cases (7)

---

## Documentation

For comprehensive feature documentation and examples:
- **User Guide**: [docs/wiki/](docs/wiki/)
- **Getting Started**: [docs/getting-started.md](docs/getting-started.md)
- **Cypher Support**: [docs/features.md](docs/features.md)
- **Schema Configuration**: [docs/schema-reference.md](docs/schema-reference.md)

For developers:
- **Architecture**: [docs/architecture/](docs/architecture/)
- **Development Guide**: [DEVELOPMENT_PROCESS.md](DEVELOPMENT_PROCESS.md)
- **Test Infrastructure**: [tests/README.md](tests/README.md)
- **VLP Cross-Functional Testing**: [docs/development/vlp-cross-functional-testing.md](docs/development/vlp-cross-functional-testing.md) ⭐ NEW
