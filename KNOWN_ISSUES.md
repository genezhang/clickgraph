# Known Issues

**Active Issues**: 0 bugs, 3 feature limitations, 1 known limitation  
**Last Updated**: December 25, 2025

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Current Status

**Bug Status**: ✅ **All known bugs fixed!**
- Integration test pass rate: **100% (549 passed, 54 xfailed)** 
- All core functionality working correctly
- VLP cross-functional testing complete (Dec 25, 2025)
- Denormalized VLP fixed (Dec 25, 2025)
- Property pruning complete (Dec 24, 2025)

---

## Known Limitations

### 1. Path Functions in WITH Clauses (CTEs)
**Status**: ⚠️ KNOWN LIMITATION  
**Discovered**: December 25, 2025  
**Issue**: Using `length(path)` in WITH clauses generates invalid SQL  

**Example (Fails)**:
```cypher
MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*1..2]->(u2:TestUser)
WHERE u1.user_id = 1
WITH u1, u2, length(path) as path_len
WHERE path_len = 2
RETURN u1.name, u2.name, path_len
```

**Error**: `Unknown expression identifier 't.hop_count'`

**Root Cause**: Path functions (`length()`, `nodes()`, `relationships()`) work in RETURN clauses but have issues in WITH clauses (CTEs). The CTE doesn't have access to the path metadata table alias.

**Workaround**: Use fixed-length patterns when exact hop count is needed:
```cypher
MATCH path = (u1:TestUser)-[:TEST_FOLLOWS*2]->(u2:TestUser)
WHERE u1.user_id = 1
RETURN u1.name, u2.name
```

**Scope**: This is a general CTE limitation, not specific to VLP implementation. Path functions work correctly in RETURN clauses.

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
