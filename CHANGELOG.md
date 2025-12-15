## [Unreleased]

### 🚀 Features

- **Cross-Branch Shared Node JOIN Detection** - Automatic JOIN generation for branching patterns (December 15, 2025)
  - **Feature**: When a node appears in multiple relationship branches that use different tables, automatically generate INNER JOIN
  - **Use Case**: Solves GitHub issue #12 - DNS lookup followed by connection correlation
  - **Example**:
    ```cypher
    MATCH (src:IP)-[:REQUESTED]->(d:Domain), (src)-[:ACCESSED]->(dest:IP)
    RETURN src.ip, d.name, dest.ip
    ```
  - **Generated SQL**:
    ```sql
    FROM test_zeek.conn_log AS t3
    INNER JOIN test_zeek.dns_log AS t1 ON t3.orig_h = t1.orig_h
    ```
  - **Impact**: Zeek tests 18→22 passing (91.7%), all 4 comma-pattern cross-table tests pass
  - **Files**: `src/query_planner/analyzer/graph_join_inference.rs`, `tests/integration/test_zeek_merged.py`

- **Predicate-Based Correlation** - Allow disconnected patterns with WHERE clause predicates (December 15, 2025)
  - **Feature**: Support different variable names for same logical node, connected via WHERE clause
  - **Example**:
    ```cypher
    MATCH (srcip1:IP)-[:REQUESTED]->(d:Domain), (srcip2:IP)-[:ACCESSED]->(dest:IP)
    WHERE srcip1.ip = srcip2.ip
    RETURN srcip1.ip, d.name, dest.ip
    ```
  - **Implementation**: Removed DisconnectedPatternFound error, rely on cross-branch JOIN detection
  - **Files**: `src/query_planner/logical_plan/match_clause.rs`

- **Sequential MATCH Clauses** - Multiple MATCH statements in sequence (December 15, 2025)
  - **Feature**: Support `MATCH ... MATCH ... MATCH ...` as per OpenCypher specification
  - **Semantics**: No relationship uniqueness requirement across MATCH boundaries (unlike comma patterns)
  - **Example**:
    ```cypher
    MATCH (srcip:IP)-[:REQUESTED]->(d:Domain)
    MATCH (srcip)-[:ACCESSED]->(dest:IP)
    RETURN srcip.ip, d.name, dest.ip
    ```
  - **Implementation**: Parser AST changed from `Option<MatchClause>` to `Vec<MatchClause>`
  - **Impact**: Zeek tests 22→23 passing (95.8%)
  - **Files**: `src/open_cypher_parser/ast.rs`, `src/open_cypher_parser/mod.rs`, `src/query_planner/logical_plan/plan_builder.rs`

### 🐛 Bug Fixes

- **Coupled Edge Alias Resolution** - Fixed SQL generation for patterns with multiple edges in same table (December 14, 2025)
  - **Problem**: `MATCH (src:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)` failed with SQL error
  - **Error**: "Unknown expression identifier 't1.orig_h' in scope SELECT ... FROM test_zeek.dns_log AS t2. Maybe you meant: ['t2.orig_h']"
  - **Root Cause**:
    - Query has 2 edges (REQUESTED, RESOLVED_TO) using same dns_log table - **coupled edges**
    - Coupled edge detector correctly unified aliases to `t1` and stored in `coupled_edge_aliases` HashMap
    - `AliasResolverContext.transform_plan()` transformed property access expressions in SELECT/WHERE to use `t1`
    - BUT: GraphRel alias itself remained `t2`, so FROM clause generated `AS t2`
    - Result: SELECT/WHERE used `t1`, FROM used `t2` - alias mismatch!
  - **Solution**:
    - Enhanced `transform_plan()` to also transform GraphRel alias when it appears in `coupled_edge_aliases`
    - Now both property expressions AND table alias use unified alias consistently
  - **Impact**:
    - Zeek tests: 16→18 passing (fixed both coupled DNS path tests)
    - All coupled edge patterns now work correctly (multiple edges in same table)
    - Ensures consistent alias usage throughout generated SQL
  - **Files Modified**: `src/render_plan/alias_resolver.rs` (transform_plan GraphRel case, ~lines 150-172)
  - **Before/After SQL**:
    ```sql
    -- Before (❌ broken - t1 vs t2 mismatch):
    SELECT t1.orig_h AS "src.ip", t1.query AS "d.name", t1.answers AS "rip.ip"
    FROM test_zeek.dns_log AS t2
    WHERE t1.orig_h = '192.168.1.10'
    
    -- After (✅ fixed - consistent t1):
    SELECT t1.orig_h AS "src.ip", t1.query AS "d.name", t1.answers AS "rip.ip"
    FROM test_zeek.dns_log AS t1
    WHERE t1.orig_h = '192.168.1.10'
    ```
- **Multi-Table Node Schema Resolution** - Fixed composite key lookup for same label across tables (December 14, 2025)
  - **Problem**: `MATCH (s:IP)-[:REQUESTED]->(d:Domain)` used wrong IP schema (conn_log instead of dns_log)
  - **Symptom**: Unnecessary self-JOINs generated for fully denormalized patterns, or "different tables" errors
  - **Root Cause**: 
    - Zeek schema has TWO `IP` definitions (dns_log table and conn_log table)
    - Schema loader stores with composite keys: `"database::table::label"` (e.g., `"test_zeek::dns_log::IP"`)
    - But `get_node_schema_opt` only used label key (`"IP"`), returned wrong table's schema
    - Pattern classification failed: detected as Mixed instead of FullyDenormalized
  - **Solution**: 
    - In `compute_pattern_context`, construct composite key from edge table: `"database::table::label"`
    - Try composite key first, fallback to label-only for backward compatibility
    - Ensures correct node schema is selected when same label appears in multiple tables
  - **Impact**: 
    - Zeek tests: 17 → 18 passing (eliminated unnecessary self-JOINs)
    - Fully denormalized edge patterns now generate single table scans (no JOINs)
    - **Critical for denormalized schemas**: Same label can appear in multiple edge tables correctly
  - **Files Modified**: `src/query_planner/analyzer/graph_join_inference.rs` (compute_pattern_context)
  - **Example**:
    ```cypher
    -- Before: Generated unnecessary JOIN
    FROM dns_log AS r INNER JOIN dns_log AS s ON s.orig_h = r.orig_h
    
    -- After: Single table scan (no JOIN)
    FROM dns_log AS r
    ```
- **Denormalized Node ID Property Mapping** - Fixed JOIN conditions for composite node IDs in denormalized edges (December 14, 2025)
  - **Problem**: `MATCH (src:IP)-[:REQUESTED]->(d:Domain)` generated invalid SQL: `ON src.ip = r.orig_h`
  - **Error**: "Identifier 'src.ip' cannot be resolved from table src"
  - **Root Cause**: 
    - For denormalized edges, `node_id` uses Cypher property names (e.g., "ip")
    - JOIN conditions need actual DB column names (e.g., "orig_h")
    - Property mappings are in `from_properties`/`to_properties`, not `property_mappings`
    - `resolve_id_column()` only checked `property_mappings`
  - **Solution**: 
    - Updated `resolve_id_column()` to check `from_properties`/`to_properties` first
    - Added `is_from_node` parameter to know which side (from/to) to check
    - Fallback to `property_mappings` for standalone node tables
  - **Impact**: 
    - Zeek merged schema tests: 15 → 17 passing (2 composite ID failures fixed)
    - Generated SQL now correct: `ON src.orig_h = r.orig_h`
  - **Files Modified**: `src/graph_catalog/pattern_schema.rs` (resolve_id_column + 4 call sites)
  - **Example Schema**: 
    ```yaml
    node_id: ip  # Cypher property name
    from_node_properties:
      ip: "id.orig_h"  # DB column mapping
    ```
- **Inline Property Parameters** - Fixed server crash on parameterized inline property patterns (December 14, 2025)
  - **Problem**: `MATCH (n:Person {id: $personId})` caused panic "Property value must be a literal"
  - **Root Cause**: `PropertyKVPair.value` was typed as `Literal`, rejecting parameter expressions
  - **Solution**: Changed `PropertyKVPair.value` from `Literal` to `LogicalExpr` to support all expressions
  - **Impact**: 
    - Official LDBC queries can now use inline property syntax directly
    - Previously required WHERE clause workaround: `MATCH (n) WHERE n.id = $param`
    - No regression: All adapted queries still work, official queries now accessible
  - **Files Modified**: 
    - `src/query_planner/logical_expr/mod.rs` - Changed struct, updated conversion
    - `src/query_planner/logical_plan/match_clause.rs` - Updated usage
  - **Testing**: 647/647 unit tests passing (0 regressions)

### �🚀 Features

- **CTE-Aware Variable Resolution** - Major architectural improvement for WITH clause handling (December 13, 2025)
  - **Problem**: Three-level WITH queries incorrectly used base tables instead of CTEs in final SELECT
  - **Solution**: Move variable resolution from render phase to analyzer phase
  - **Architecture**: 
    - New `GraphJoins.cte_references: HashMap<String, String>` field maps alias → CTE name
    - `register_with_cte_references()` pre-scans plan and updates `TableCtx.cte_reference`
    - `graph_context.rs` checks CTE references before base table lookup during join creation
    - Render phase uses `GraphJoins.cte_references` for anchor table resolution
    - Deterministic CTE naming: `with_{aliases}_cte` (no counter)
  - **Critical Fix**: Correct traversal order (inner WITH before outer) ensures latest definition wins
  - **Testing**: All 642 tests passing (100%), validated edge cases:
    - Three-level nesting: `WITH a → WITH a,b → WITH b,c`
    - OPTIONAL MATCH within WITH clauses
    - Variable scope changes: `WITH a → WITH b`
    - Same alias redefinition: `WITH a → WITH a,b`
  - **Files Modified**: 12 files across query_planner, render_plan modules
  - **Documentation**: Comprehensive architecture doc with data flow diagrams

- **Composite Node IDs** - Support multi-column `node_id` for nodes with composite primary keys
  - YAML syntax: `node_id: [bank_id, account_number]` for composite, `node_id: user_id` for single
  - Generates ClickHouse tuple equality in JOINs: `(a.c1, a.c2) = (b.c1, b.c2)`
  - Property access `RETURN n.id` returns tuple expression for composite IDs
  - Works with all query features: MATCH, size() patterns, EXISTS, NOT EXISTS
  - PageRank algorithm supports composite node IDs (uses tuple as node identifier)
  - New API methods: `sql_tuple()`, `sql_equality()`, `columns_with_alias()`
  - Test schema example: `schemas/test/composite_node_ids.yaml`
  - Files: 16 files updated across graph_catalog, query_planner, render_plan, clickhouse_query_generator
  - Testing: 5 new unit tests, all 644 tests passing (100%)

---

## [0.5.7] - 2025-12-10

### 🐛 Bug Fixes

- **WITH+MATCH CTE generation** - Fixed critical correctness bug where second MATCH after WITH clause ignored first MATCH context
  - Affected: All WITH+MATCH patterns (e.g., IC-9)
  - Root Cause: GraphRel with Projection(kind=With) in right branch was being flattened instead of creating proper CTE boundary
  - Fix: Added `has_with_clause_in_graph_rel()` detection and `build_with_match_cte_plan()` function
  - CTE for WITH clause output is now properly generated and joined to outer query
  - Files: `plan_builder.rs`, `plan_builder_helpers.rs`, `to_sql_query.rs`, `expression_parser.rs`

- **CTE Union rendering** - Fixed malformed SQL when CTE contains Union plan
  - Previously generated `SELECT *\nSELECT ...` (missing UNION ALL keyword)
  - Now correctly renders nested Union as proper SQL

- **Star column quoting** - Fixed `friend."*"` → `friend.*` in SQL generation
  - Property access with `*` wildcard no longer incorrectly quoted

- **Undirected VLP with WITH clause** - Fixed CTE hoisting for UNION branches with aggregation
  - Affected: LDBC IC-1, IC-9
  - Fix: `plan_builder.rs` now collects and preserves CTEs from all UNION branches when wrapping with GROUP BY
  - Previously CTEs were being lost when bidirectional VLP patterns were combined with WITH clause aggregation

- **LDBC schema column mappings** - Corrected 15+ relationship column names to match actual ClickHouse tables
  - Fixed: IS_LOCATED_IN (Person_id, Place_id), HAS_INTEREST (PersonId, TagId), LIKES_POST (PersonId, PostId), etc.
  - Schema now matches actual LDBC SNB data column naming conventions

- **Added POST_LOCATED_IN relationship** - New relationship type for IC-3 benchmark query
  - Maps to `Post_isLocatedIn_Country` table with correct column names (PostId, CountryId)

### 🟡 Known Limitations

- **WITH+MATCH with 2+ hops after WITH** - IC-3 pattern with nested relationships after WITH clause not yet supported
  - Workaround: Break into multiple simpler queries

### 🧪 Testing

- **LDBC SNB Interactive Benchmark**: 7/8 queries passing (87.5%)
  - IS-1, IS-2, IS-3, IS-5: Short queries passing
  - IC-1, IC-2, IC-9: Complex queries passing  
  - IC-3: Known limitation (nested relationships after WITH)
- All 621 unit tests passing (100%)

---

## [0.5.6] - 2025-12-09

### 🐛 Bug Fixes

- **OPTIONAL MATCH join ordering** - Fixed anchor node detection for patterns where anchor is on right side
  - Affected: LDBC BI-6, BI-9
  - Fix: `graph_join_inference.rs` now correctly identifies anchor node for reverse traversal patterns
  - Generated SQL now uses correct FROM table and JOIN order for `OPTIONAL MATCH (a)-[:REL]->(anchor)` patterns

- **Undirected relationship UNION generation** - Fixed join ordering in second UNION branch
  - Affected: LDBC BI-14
  - Fix: `bidirectional_union.rs` now swaps both `left`/`right` GraphNode plans AND connection strings for Incoming direction
  - Previously only connection strings were swapped, leaving FROM table incorrect in second branch

### 🧪 Testing

- **LDBC SNB BI Benchmark**: 24/26 queries passing (92.3%), up from 21/26 (80.8%)
- All 621 unit tests passing (100%)
- All 7 integration tests passing (100%)

---

## [0.5.5] - 2025-12-07

### 🚀 Features

- *(functions)* Add 48 new Neo4j function mappings (73 total, up from 25)
  - **Vector Similarity (4)**: `gds.similarity.cosine`, `gds.similarity.euclidean`, `gds.similarity.euclideanDistance`, `vector.similarity.cosine`
  - **Temporal Extraction (12)**: `year`, `month`, `day`, `hour`, `minute`, `second`, `dayOfWeek`, `dayOfYear`, `quarter`, `week`, `localdatetime`, `localtime`
  - **List Predicates (5)**: `all`, `any`, `none`, `single`, `isEmpty`
  - **String Functions (5)**: `startsWith`, `endsWith`, `contains`, `normalize`, `valueType`
  - **Core Aggregations (5)**: `avg`, `sum`, `min`, `max`, `count`
  - **Trigonometric (7)**: `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`
  - **Math (6)**: `exp`, `log`, `log10`, `pi`, `e`, `pow`
  - **String (2)**: `lTrim`, `rTrim`
  - **Aggregation (4)**: `stDev`, `stDevP`, `percentileCont`, `percentileDisc`
  - **Predicate (2)**: `coalesce`, `nullIf`
  - **Map (1)**: `keys`

- *(functions)* **ClickHouse function pass-through** via `ch.` prefix
  - Direct access to ANY ClickHouse function: `ch.functionName(args)`
  - Uses dot notation for Neo4j ecosystem compatibility (like `apoc.*`, `gds.*`)
  - Property mapping and parameter substitution work normally
  - **Scalar functions**: Hash (cityHash64, MD5), JSON (JSONExtract*), URL (domain, path), IP (IPv4NumToString), Geo (greatCircleDistance, geoToH3), Date (formatDateTime, toStartOf*)
  - **Aggregate functions** (100+ registered): Automatic GROUP BY generation
    - Unique counting: `ch.uniq`, `ch.uniqExact`, `ch.uniqCombined`, `ch.uniqHLL12`
    - Quantiles: `ch.quantile(p)`, `ch.quantiles`, `ch.quantileExact`, `ch.median`
    - TopK: `ch.topK(n)`, `ch.topKWeighted`
    - ArgMin/Max: `ch.argMin`, `ch.argMax`
    - Arrays: `ch.groupArray`, `ch.groupUniqArray`, `ch.groupArraySample`
    - Funnel: `ch.windowFunnel`, `ch.retention`, `ch.sequenceMatch`
    - Statistics: `ch.varPop`, `ch.stddevSamp`, `ch.corr`, `ch.covarPop`
    - Maps: `ch.sumMap`, `ch.avgMap`, `ch.minMap`, `ch.maxMap`

### 🚜 Refactor

- *(code quality)* Remove 423 lines of dead code (filter_pipeline.rs: 889→413 lines)
- *(code quality)* Fix all compiler warnings (58→0)
- *(code quality)* Convert broken doc examples to `text` or `ignore`
- *(dependencies)* Security updates: dotenvy, validator 0.20, reqwest 0.12

### 🧪 Testing

- Add 27 unit tests for functions (17 translator + 16 registry, some overlap)
- 6 new tests for `ch.` aggregate function detection and classification
- Fix doctest compilation errors
- Fix test_view_parameters.rs to use Identifier::Single type

### 📚 Documentation

- Add comprehensive ClickHouse pass-through guide to Cypher-Functions.md
- Expand vector similarity section with RAG usage, HNSW index requirements
- Document all 73 Neo4j function mappings

---

## [0.5.4] - 2025-12-08

### 🚀 Features

- Add native support for self-referencing FK pattern
- Add relationship uniqueness enforcement for undirected patterns
- *(schema)* Add fixed-endpoint polymorphic edge support
- *(union)* Add UNION and UNION ALL query support
- Multi-table label support and denormalized schema improvements
- *(pattern_schema)* Add unified PatternSchemaContext abstraction - Phase 1
- *(graph_join_inference)* Integrate PatternSchemaContext - Phase 2
- *(graph_join_inference)* Add handle_graph_pattern_v2 - Phase 3
- *(pattern_schema)* Add FkEdgeJoin strategy for FK-edge patterns
- *(graph_join)* Wire up handle_graph_pattern_v2 with USE_PATTERN_SCHEMA_V2 env toggle

### 🐛 Bug Fixes

- GROUP BY expansion and count(DISTINCT r) for denormalized schemas
- Undirected multi-hop patterns generate correct SQL
- Support fixed-endpoint polymorphic edges without type_column
- Correct polymorphic filter condition in graph_join_inference
- Normalize GraphRel left/right semantics for consistent JOIN generation
- Recurse into nested GraphRels for VLP detection
- *(render_plan)* Add WHERE filters for VLP chained pattern endpoints (Issue #5)
- *(parser)* Reject binary operators (AND/OR/XOR) as variable names
- Multi-hop anonymous patterns, OPTIONAL MATCH polymorphic, string operators
- Aggregation and UNWIND bugs
- Denormalized schema query pattern fixes (TODO-1, TODO-2, TODO-4)
- Cross-table WITH correlation now generates proper JOINs (TODO-3)
- WITH clause alias propagation through GraphJoins wrapper (TODO-8)
- Multi-hop denormalized edge JOIN generation
- Update schema files to match test data columns
- *(pattern_schema)* Pass prev_edge_info for multi-hop detection in v2 path
- *(filter_tagging)* Correct owning edge detection for multi-hop intermediate nodes
- FK-edge JOIN direction bug - use join_side instead of fk_on_right
- Add polymorphic label filter generation for edges

### 🚜 Refactor

- Unify FK-edge pattern for self-ref and non-self-ref cases
- Minor code cleanup in bidirectional_union and plan_builder_helpers
- Make PatternSchemaContext (v2) the default join inference path
- Reorganize benchmarks into individual directories
- Replace NodeIdSchema.column with Identifier-based id field
- Change YAML field id_column to node_id for consistency
- Extract predicate analysis helpers to plan_builder_helpers.rs
- Extract JOIN and filter helpers to plan_builder_helpers.rs

### 📚 Documentation

- Update README for v0.5.3 release
- Add fixed-endpoint polymorphic edge documentation
- Add VLP+chained patterns docs and private security tests
- Document Issue #5 (WHERE filter on VLP chained endpoints)
- *(readme)* Minor wording improvements
- Update PLANNING_v0.5.3 and CHANGELOG with bug fix status
- Add unified schema abstraction proposal and test scripts
- Add unified schema abstraction Phase 4 completion to STATUS
- Update unified schema abstraction progress - Phase 4 fully complete
- *(benchmarks)* Add ClickHouse env vars and fix paths in README
- *(benchmarks)* Streamline README to be a concise index
- Archive PLANNING_v0.5.3.md - all bugs resolved

### 🧪 Testing

- Add multi-hop pattern integration tests
- Fix Zeek integration tests - response format and skip cross-table tests
- Add v1 vs v2 comparison test script
- Add unit tests for predicate analysis helpers

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Make test files use CLICKGRAPH_URL env var for port flexibility
- *(benchmarks)* Move social_network-specific files to subdirectory
## [0.5.3] - 2025-12-02

### 🚀 Features

- Add regex match (=~) operator and fix collect() function
- Add EXISTS subquery and WITH+MATCH chaining support
- Add label() function for scalar label return

### 🐛 Bug Fixes

- Remove unused schemas volume from docker-compose
- Parser now rejects invalid syntax with unparsed input
- Column alias for type(), id(), labels() graph introspection functions
- Update release workflow to use clickgraph binary name
- Update release workflow to use clickgraph-client binary name
- Build entire workspace in release workflow

### 📚 Documentation

- Archive wiki for v0.5.2 release
- Fix schema documentation and shorten README
- Fix Quick Start to include required GRAPH_CONFIG_PATH
- Add 3 new known issues from ontime schema testing
- Update KNOWN_ISSUES.md - WHERE AND now caught
- Clean up KNOWN_ISSUES.md - remove resolved issues
- Remove false known limitations - all verified working

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Release v0.5.3
- Update CHANGELOG.md [skip ci]
- Update Cargo.lock for v0.5.3
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
## [0.5.2] - 2025-11-30

### 🚀 Features

- Add docker-compose.dev.yaml for development
- [**breaking**] Phase 1 - Fixed-length paths use inline JOINs instead of CTEs
- Add cycle prevention for fixed-length paths
- Restore PropertyValue and denormalized support from stash, integrate with anchor_table
- Complete denormalized query support with alias remapping and WHERE clause filtering
- Implement denormalized node-only queries with UNION ALL
- Support RETURN DISTINCT for denormalized node-only queries
- Support ORDER BY for denormalized UNION queries
- Fix UNION ALL aggregation semantics for denormalized node queries
- Variable-length paths for denormalized edge tables
- Add schema-level filter field with SQL predicate parsing
- Schema-level filters and OPTIONAL MATCH LEFT JOIN fix
- Add VLP + UNWIND support with ARRAY JOIN generation
- Implement coupled edge alias unification for denormalized patterns
- Implement polymorphic edge query support
- *(polymorphic)* Add VLP polymorphic edge filter support
- *(polymorphic)* Add IN clause support for multiple relationship types in single-hop
- Complete polymorphic edge support for wildcard relationship patterns
- Add edge inline property filter tests and update documentation
- Implement bidirectional pattern UNION ALL transformation

### 🐛 Bug Fixes

- ORDER BY rewrite bug for chained JOIN CTEs
- Zero-hop variable-length path support
- Remove ChainedJoinGenerator CTE for fixed-length paths
- Complete PropertyValue type conversions in plan_builder.rs
- Revert table alias remapping in filter_tagging to preserve filter context
- Eliminate duplicate WHERE filters by optimizing FilterIntoGraphRel
- Correct JOIN order and FROM table selection for mixed property expressions
- Ensure variable-length and shortest path queries use CTE path
- Destination node properties now map to correct columns in denormalized edge tables
- Multi-hop denormalized edge patterns and duplicate WHERE filters
- Variable-length path schema resolution for denormalized edges
- Add edge_id support to RelationshipDefinition for cycle prevention
- Fixed-length VLP (*1, *2, *3) now generates inline JOINs
- Fixed-length VLP (*2, *3) now works correctly
- Denormalized schema VLP property alias resolution
- VLP recursive CTE min_hops filtering and aggregation handling
- OPTIONAL MATCH + VLP returns anchor when no path exists
- RETURN r and graph functions (type, id, labels)
- Support inline property filters with numeric literals
- Push projections into Union branches for bidirectional patterns
- Polymorphic multi-type JOIN filter now uses IN clause

### 💼 Other

- Manual addition of denormalized fields (incomplete)

### 🚜 Refactor

- Simplify ORDER BY logic for inline JOINs
- Simplify GraphJoins FROM clause logic - use relationship table when no joins exist
- Store anchor table in GraphJoins, eliminate redundant find_anchor_node() calls
- Set is_denormalized flag directly in analyzer, remove redundant optimizer pass
- Move helper functions from plan_builder.rs to plan_builder_helpers.rs
- Rename co-located → coupled edges terminology
- Consolidate schema loading with shared helpers
- Consolidated VLP handling with VlpSchemaType

### 📚 Documentation

- Prioritize Docker Hub image in getting-started guide
- Update README with v0.5.1 Docker Hub release
- Add v0.5.2 planning document
- Update wiki Quick Start to use Docker Hub image with credentials
- Add Zeek network log examples and denormalized edge table guide
- Update STATUS.md with denormalized single-hop fix
- Update denormalized blocker notes with current status
- Update denormalized edge status to COMPLETE
- Add graph algorithm support to denormalized edge docs
- Add 0-hop pattern support to denormalized edge docs
- *(wiki)* Update denormalized properties with all supported patterns
- Add coupled edges documentation
- *(wiki)* Add Coupled Edges section to denormalized properties
- Add v0.5.2 TODO list for polymorphic edges and code consolidation
- Mark schema loading consolidation complete in TODO
- Update STATUS.md with polymorphic edge filter completion
- Add Schema-Basics.md and wiki versioning workflow
- Update documentation for v0.5.2 schema variations
- Update KNOWN_ISSUES.md with v0.5.2 status
- Update KNOWN_ISSUES.md with fixed-length VLP resolution
- Update KNOWN_ISSUES with VLP fixes and *0 pattern limitation
- Add Cypher Subgraph Extraction wiki with Nebula GET SUBGRAPH comparison
- Update README with v0.5.2 features

### 🎨 Styling

- Use UNION instead of UNION DISTINCT

### 🧪 Testing

- Add comprehensive Docker image validation suite
- Add comprehensive schema variation test suite (73 tests)

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Clean up root directory - remove temp files and organize Python tests
- Release v0.5.2
- Update CHANGELOG.md [skip ci]
- Update Cargo.lock for v0.5.2
## [0.5.1] - 2025-11-21

### 🚀 Features

- Add SQL Generation API (v0.5.1)
- Implement RETURN DISTINCT for de-duplication
- Add role-based connection pool for ClickHouse RBAC

### 🐛 Bug Fixes

- Eliminate flaky cache LRU eviction test with millisecond timestamps
- Replace docker_publish.yaml with docker-publish.yml
- Add missing distinct field to all Projection initializations

### 📚 Documentation

- Fix getting-started guide issues
- Update STATUS.md with fixed flaky test achievement (423/423 passing)
- Add /query/sql endpoint and RETURN DISTINCT documentation
- Add /query/sql endpoint and RETURN DISTINCT to wiki

### 🧪 Testing

- Add role-based connection pool integration tests

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Release v0.5.1
- Update CHANGELOG.md [skip ci]
## [0.5.0] - 2025-11-19

### 🚀 Features

- *(phase2)* Add tenant_id and view_parameters to request context
- *(phase2)* Thread tenant_id through HTTP/Bolt to query planner
- Implement SET ROLE RBAC support for single-tenant deployments
- *(multi-tenancy)* Add view_parameters field to schema config
- *(multi-tenancy)* Implement parameterized view SQL generation
- *(multi-tenancy)* Add Bolt protocol view_parameters extraction
- *(phase2)* Add engine detection for FINAL keyword support
- *(phase2)* Add use_final field to schema configuration
- *(phase2)* Add FINAL keyword support to SQL generation
- *(phase2)* Auto-schema discovery with column auto-detection
- *(auto-discovery)* Add camelCase naming convention support
- Add PowerShell scripts for wiki validation workflow
- Add Helm chart for Kubernetes deployment

### 🐛 Bug Fixes

- *(phase2)* Correct FINAL keyword placement - after alias
- *(tests)* Add missing engine and use_final fields to test schemas
- Implement property expansion for RETURN whole node queries
- Update clickgraph-client and add documentation

### 🚜 Refactor

- Minor code improvements in parser and planner

### 📚 Documentation

- Phase 2 minimal RBAC - parameterized views with multi-parameter support
- Fix Pattern 2 RBAC examples to use SET ROLE approach
- Add Phase 2 progress to STATUS.md
- Add comprehensive Phase 2 multi-tenancy status report
- *(multi-tenancy)* Complete parameterized views documentation + cleanup
- Update parameterized views note with cache optimization details
- *(phase2)* Complete Phase 2 multi-tenancy documentation and tests
- Correct Phase 2 status - 2/5 complete, not fully done
- Update ROADMAP.md Phase 2 progress - 2/5 complete
- *(phase2)* Update STATUS and CHANGELOG for FINAL syntax fix
- *(phase2)* Update STATUS and CHANGELOG for auto-schema discovery
- Align wiki examples with benchmark schema and add validation
- Add session documentation and planning notes
- Update STATUS, CHANGELOG, and KNOWN_ISSUES
- Update ROADMAP with wiki documentation and bug fix progress
- Mark Phase 2 complete - v0.5.0 release ready!

### ⚡ Performance

- *(cache)* Optimize multi-tenant caching with SQL placeholders

### 🧪 Testing

- Add comprehensive SET ROLE RBAC test suite
- *(multi-tenancy)* Add parameterized views test infrastructure
- *(multi-tenancy)* Add unit tests for view_parameters
- Add integration test utilities and schema

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Clean up temporary test output and debug files
## [0.4.0] - 2025-11-15

### 🚀 Features

- Add parameter support via HTTP API + identity fallback for properties
- Add production-ready query cache with LRU eviction
- Complete Bolt 5.8 protocol implementation with E2E tests passing
- Add Neo4j function support with 25+ function mappings
- Complete E2E testing infrastructure + critical bug fixes
- Unified benchmark architecture with scale factor parameter
- Adjust post ratio to 20 and add 2 post-related benchmark queries
- Add MergeTree engine support for large-scale benchmarks
- *(benchmark)* Complete MergeTree benchmark infrastructure, discover multi-hop query bug
- Add comprehensive regression test suite (799 tests)
- Add pre-flight checks to test runner
- Pre-load test_integration schema at server startup
- Implement undirected relationship support (Direction::Either)

### 🐛 Bug Fixes

- Multi-hop JOINs, SELECT aliases, SQL quoting + improve benchmark display
- Use correct schema and database for integration tests
- Start server without pre-loaded schema for integration tests
- IS NULL operator in CASE expressions (22/25 tests passing)
- Resolve compilation errors from API changes and incomplete cleanup
- Additional GraphSchema::build() signature fixes in test files
- Remove unused variable in view_resolver_tests.rs
- Update error handling tests to match actual ClickGraph behavior

### 🚜 Refactor

- Archive NEXT_STEPS.md in favor of ROADMAP.md
- Remove inherited DDL generation code (~1250 LOC)
- Remove bitmap index infrastructure (~200 LOC)
- Remove use_edge_list flag (~50 LOC)
- Flatten directory structure - remove brahmand/ wrapper
- Remove expression_utils dead code - visitor pattern + utility functions
- Convert CteGenerationContext to immutable builder pattern
- Create plan_builder_helpers module (preparatory step)
- Integrate plan_builder_helpers module
- Add deprecation markers to duplicate helper functions
- Complete deprecation markers for all helper functions (20/20)
- Remove all deprecated helper functions (~736 LOC, 22% reduction)
- Replace file-based debug logging with standard log::debug! macro

### 📚 Documentation

- Update KNOWN_ISSUES and copilot-instructions - all major issues resolved
- Add comprehensive ROADMAP with real-world features and prioritization
- Architecture decision - Use string substitution for parameters (not ClickHouse .bind())
- Update NEXT_STEPS.md roadmap with query cache completion
- Update README and ROADMAP with query cache completion
- Highlight parameter support in README and add usage restrictions
- Update ROADMAP.md with Bolt 5.8 completion
- Clarify anonymous node/edge pattern as TODO feature
- Document flaky cache LRU eviction test
- Document anonymous node SQL generation bug
- Change 'production-ready' to 'development-ready' for v0.4.0

### 🧪 Testing

- *(benchmark)* Add regression test script for CI/CD

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Complete v0.4.0 release preparation - Phase 1 complete
## [0.3.0] - 2025-11-10

### 🚀 Features

- Complete WITH clause with GROUP BY, HAVING, and CTE support
- Enable per-request schema support for thread-safe multi-tenant architecture
- Add schema-aware helper functions in render layer

### 🐛 Bug Fixes

- Multi-hop graph query planning and join generation
- Update path variable tests to match tuple() implementation
- Improve anchor node selection to prefer LEFT nodes first
- Prevent double schema prefix in CTE table names
- Use correct node alias for FROM clause in GraphRel fallback
- Prevent both LEFT and RIGHT nodes from being marked as anchor
- Remove duplicate JOINs for path variable queries
- Detect multiple relationship types in GraphJoins tree
- Update JOINs to use UNION CTE for multiple relationship types
- Correct release date in README (November 9, not 23)

### 💼 Other

- Add schema to PlanCtx (Phases 1-3 complete)

### 🚜 Refactor

- Remove BITMAP traversal code and fix relationship direction handling
- Rename handle_edge_list_traversal to handle_graph_pattern
- Remove redundant GLOBAL_GRAPH_SCHEMA

### 📚 Documentation

- Prepare for next session and organize repository
- Python integration test status report (36.4% passing)
- Update STATUS and KNOWN_ISSUES for GLOBAL_GRAPH_SCHEMA removal
- Clean up outdated KNOWN_ISSUES and update README

### 🧪 Testing

- Add debugging utilities for anchor node and JOIN issues

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Disable automatic docker publish
- Clean up test debris and remove deleted optimizer
- Replace emoji characters with text equivalents in test files
- Organize root directory for public repo
- Bump version to 0.2.0
- Bump version to 0.3.0
## [0.2.0] - 2025-11-06

### 🚀 Features

- Implement dual-key schema registration for startup-loaded schemas
- Add COUNT(DISTINCT node) support and fix integration test infrastructure
- Support edge-driven queries with anonymous node patterns

### 🐛 Bug Fixes

- Simplify schema strategy - use only server's default schema
- Remove ALL hardcoded property mappings - CRITICAL BUG FIX
- Enhance column name helpers to support both prefixed and unprefixed names
- Remove is_simple_relationship logic that skipped node joins
- Configure Docker to use integration test schema
- Only create node JOINs when nodes are referenced in query
- Preserve table aliases in WHERE clause filters
- Extract where_predicate from GraphRel during filter extraction
- Remove direction-based logic from JOIN inference - both directions now work
- GraphNode uses its own alias for PropertyAccessExp, not hardcoded 'u'
- Complete OPTIONAL MATCH with clean SQL generation
- Add user_id and product_id to schema property_mappings
- Add schema prefix to JOIN tables in cte_extraction.rs
- Handle fully qualified table names in table_to_id_column
- Variable-length paths now generate recursive CTEs
- Multiple relationship types now generate UNION CTEs
- Correct edge list test assertions for direction semantics

### 💼 Other

- Document property mapping bug investigation

### 🚜 Refactor

- Remove /api/ prefix from routes for simplicity

### 📚 Documentation

- Final Phase 1 summary with all 12 test suites
- Add schema loading architecture documentation and API test
- Update STATUS with integration test results
- Create action plan for property mapping bug fix
- Update STATUS and CHANGELOG with critical bug fix resolution
- Document WHERE clause gap for simple MATCH queries
- Add schema management endpoints and update API references
- Update STATUS.md with WHERE clause alias fix
- Update STATUS with WHERE predicate extraction fix
- Update STATUS and CHANGELOG with schema fix
- Update STATUS with complete session summary

### 🧪 Testing

- Add comprehensive integration test framework
- Add comprehensive relationship traversal tests
- Add variable-length path and shortest path integration tests
- Add OPTIONAL MATCH and aggregation integration tests
- Complete Phase 1 integration test suite with CASE, paths, and multi-database
- Add comprehensive error handling integration tests
- Add basic performance regression tests
- Initial integration test suite run - 272 tests collected
- Fix schema/database naming separation in integration tests

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
## [0.1.0] - 2025-11-02

### 🚀 Features

- *(parser)* Add shortest path function parsing
- *(planner)* Add ShortestPathMode tracking to GraphRel
- *(planner)* Detect and propagate shortest path mode
- *(sql)* Implement shortest path SQL generation with depth filtering
- Add WHERE clause filtering support for shortest path queries
- Add path variable support to parser (Phase 2.1-2.2)
- Track path variables in logical plan (Phase 2.3)
- Pass path variable to SQL generator (Phase 2.4)
- Phase 2.5 - Generate path object SQL for path variables
- Phase 2.6 - Implement path functions (length, nodes, relationships)
- WHERE clause filters for variable-length paths and shortestPath
- Complete allShortestPaths implementation with WHERE filters
- Implement alternate relationship types [:TYPE1|TYPE2] support
- Implement multiple relationship types with UNION logic
- Support multiple relationship types with labels vector
- Complete Path Variables & Functions implementation
- Complete Path Variables implementation with documentation
- Add PageRank algorithm support with CALL statement
- Complete Query Performance Metrics implementation
- Complete CASE expressions implementation with full context support
- Complete WHERE clause filtering pipeline for variable-length paths
- Implement type-safe configuration management
- Systematic error handling improvements - replace panic-prone unwrap() calls
- Complete codebase health restructuring - eliminate runtime panics
- Rebrand from Brahmand to ClickGraph
- Update benchmark suite for ClickGraph rebrand and improved performance testing
- Complete multiple relationship types feature with schema resolution
- Complete WHERE clause filters with schema-driven resolution
- Add per-table database support in multi-schema architecture
- Complete schema-only architecture migration
- Add medium benchmark (10K users, 50K follows) with performance metrics
- Add large benchmark (5M users, 50M follows) - 90% success at massive scale!
- Add Bolt protocol multi-database support
- Add test convenience wrapper and update TESTING_GUIDE
- Implement USE clause for multi-database selection in Cypher queries

### 🐛 Bug Fixes

- *(tests)* Add exhaustive pattern matching for ShortestPath variants
- *(parser)* Improve shortest path function parsing with case-insensitive matching
- *(parser)* Consume leading whitespace in shortest path functions
- *(sql)* Correct nested CTE structure for shortest path queries
- *(phase2)* Phase 2.7 integration test fixes - path variables working end-to-end
- WHERE clause handling for variable-length path queries
- Enable stable background schema monitoring
- Resolve critical TODO/FIXME items causing runtime panics
- Root cause fix for duplicate JOIN generation in relationship queries
- Three critical bug fixes for graph query execution
- Consolidate benchmark results and add SUT information
- Resolve path variable regressions after schema-only migration
- Use last part of CTE name instead of second part

### 💼 Other

- Prepare v0.1.0 release

### 🚜 Refactor

- *(sql)* Wire shortest_path_mode through CTE generator
- Extract CTE generation logic into dedicated module
- Complete codebase health improvements - modular architecture
- Standardize test organization with unit/integration/e2e structure
- Extract common expression processing utilities
- Organize benchmark suite into dedicated directory
- Clean up and improve CTE handling for JOIN optimization
- Remove GraphViewConfig and rename global variables
- Complete migration from view-based to schema-only configuration
- Organize project root directory structure

### 📚 Documentation

- Add session recap and lessons learned
- Add shortest path implementation session progress
- Comprehensive shortest path implementation documentation
- Add session completion summary
- Update STATUS.md with Phase 2.7 completion - path variables fully working
- Update STATUS.md to reflect current state of multiple relationship types
- Add project documentation and cleanup summaries
- Complete schema validation enhancement documentation
- Update STATUS.md and CHANGELOG.md with completed features
- Update NEXT_STEPS.md with recent completions and current priorities
- Correct ViewScan relationship support - relationships DO use YAML schemas
- Correct ViewScan relationship limitation in STATUS.md
- Remove incorrect OPTIONAL MATCH limitation from STATUS.md and NEXT_STEPS.md
- Document property mapping debug findings and render plan fixes
- Update CHANGELOG with property mapping debug session
- Update CHANGELOG with CASE expressions feature
- Fix numbering inconsistencies and update WHERE clause filtering status
- Update STATUS with type-safe configuration completion
- Update STATUS.md with TODO/FIXME resolution completion
- Clarify DDL parser TODOs are out-of-scope for read-only engine
- Sync documentation with current project status
- Update documentation with bug fixes and benchmark results
- Update README with 100% benchmark success and recent bug fixes
- Update STATUS.md with 100% benchmark success
- Update STATUS and CHANGELOG with enterprise-scale validation
- Add What's New section to README highlighting enterprise-scale validation
- Complete benchmark documentation with all three scales
- Add clear navigation to benchmark results
- Tone down production-ready claims to development build
- Add from_node/to_node fields to all relationship schema examples
- Clarify node label terminology in comments and examples
- Update STATUS.md with November 2nd achievements
- Add multi-database support to README and API docs
- Add PROJECT_STRUCTURE.md guide
- Add comprehensive USE clause documentation

### 🧪 Testing

- *(parser)* Add comprehensive shortest path parser tests
- Add shortest path SQL generation test script
- Add shortest path integration test files
- Improve test infrastructure and schema configuration
- Add end-to-end tests for USE clause functionality

### ⚙️ Miscellaneous Tasks

- Update .gitignore to exclude temporary files
- Disable CI on push to main (requires ClickHouse infrastructure)
## [iewscan-complete] - 2025-10-19

### 🚀 Features

- :sparkles: Added basic schema inferenc
- :sparkles: support for multi node conditions
- Support for multi node conditions
- Query planner rewrite (#11)
- Complete view-based graph infrastructure implementation
- Comprehensive view optimization infrastructure
- Complete ClickGraph production-ready implementation
- Implement relationship traversal support with YAML view integration
- Implement variable-length path traversal for Cypher queries
- Complete end-to-end variable-length path execution
- Add chained JOIN optimization for exact hop count queries
- Add parser-level validation for variable-length paths
- Make max_recursive_cte_evaluation_depth configurable with default of 100
- Add OPTIONAL MATCH AST structures
- Implement OPTIONAL MATCH parser
- Implement OPTIONAL MATCH logical plan integration
- Implement OPTIONAL MATCH with LEFT JOIN semantics
- Implement view-based SQL translation with ViewScan for node queries
- Add debug logging for full SQL queries
- Add schema lookup for relationship types

### 🐛 Bug Fixes

- :bug: relation direction when same node types
- :bug: Property tagging to node name
- :bug: node name in return clause related issues
- Count start issue (#6)
- Schema integration bug - separate column names from node types
- Rewrite GROUP BY and ORDER BY expressions for variable-length CTEs
- Preserve Cypher variable aliases in plan sanitization
- Qualify columns in IN subqueries and use schema columns
- Prevent CTE nesting and add SELECT * default
- Pass labels to generate_scan for ViewScan resolution

### 💼 Other

- Node name in return clause related issues
- Add RECURSIVE keyword to variable_length_demo.ipynb SQL descriptions

### 📚 Documentation

- Add comprehensive changelog for October 15, 2025 session
- Update README to use more appropriate terminology
- Add comprehensive test coverage summary for variable-length paths
- Simplify documentation structure for better maintainability
- Add documentation standards to copilot-instructions.md
- Add ViewScan completion documentation
- Add git workflow guide and update .gitignore

### 🧪 Testing

- Add comprehensive test suite for variable-length paths (30 tests)
- Add comprehensive testing infrastructure

### ⚙️ Miscellaneous Tasks

- Fixed docker pipeline mac issue
- Fixed docker mac issue
- Fixed docker image mac issue
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Update Cargo.lock after axum 0.8.6 upgrade
- Clean up debug logging and add NEXT_STEPS documentation
