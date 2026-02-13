# AGENTS.md — server/bolt_protocol

> Neo4j Bolt Protocol v4.1–5.8 implementation for ClickGraph.
> Enables Neo4j Browser, Neo4j Desktop, and any Bolt-compatible driver to query ClickGraph.

## 1. Module Purpose and Role

This sub-module implements the **Neo4j Bolt binary wire protocol**, allowing ClickGraph to masquerade as a Neo4j server. Clients connect over TCP (default port 7687) or WebSocket, negotiate a protocol version, authenticate, and then send Cypher queries. The module:

1. **Accepts connections** — handshake, version negotiation (Bolt 4.1–5.8)
2. **Authenticates** — Basic, None, or Kerberos (stub) schemes
3. **Executes queries** — delegates to ClickGraph's parser → planner → SQL generator → ClickHouse pipeline
4. **Transforms results** — converts flat ClickHouse JSON rows into packstream-encoded Node, Relationship, and Path graph objects
5. **Manages IDs** — maps ClickGraph's string-based `element_id` (e.g., `"User:42"`) to 53-bit integers that Neo4j Browser expects from `id()`
6. **Rewrites queries** — intercepts Neo4j Browser's `id(n) = N` expand queries and rewrites them to property filters

This module is the **only alternative transport** to the HTTP API. It is critical for Neo4j Browser visualization.

## 2. Architecture Overview

### Connection Lifecycle

```
Client TCP connect
       │
       ▼
┌─────────────────┐
│   Connected     │  (waiting for magic preamble + version proposals)
└────────┬────────┘
         │ perform_handshake()
         ▼
┌─────────────────┐
│   Negotiated    │  (version agreed, waiting for HELLO)
└────────┬────────┘
         │ handle_hello()
         ▼
┌─────────────────┐   Bolt 5.1+: HELLO → Authentication → LOGON → Ready
│ Authentication  │   Bolt 4.x:  HELLO (with auth) → Ready
└────────┬────────┘
         │ handle_logon() or handle_hello() with auth
         ▼
┌─────────────────┐
│     Ready       │ ◄──── RESET resets to here
└────────┬────────┘
         │ handle_run()
         ▼
┌─────────────────┐
│   Streaming     │  (results cached, waiting for PULL/DISCARD)
└────────┬────────┘
         │ handle_pull() / handle_discard()
         ▼
┌─────────────────┐
│     Ready       │  (back to ready for next query)
└─────────────────┘

GOODBYE → Failed (connection closes)
Error   → Failed (connection closes)
LOGOFF  → Authentication (Bolt 5.1+ re-auth)
```

### Message Flow for a Query

```
Client                          BoltHandler                    ClickHouse
  │                                │                              │
  │── RUN "MATCH (n:User)..." ────▶│                              │
  │                                │── parse Cypher               │
  │                                │── rewrite id() predicates    │
  │                                │── plan query                 │
  │                                │── generate SQL               │
  │                                │── execute SQL ──────────────▶│
  │                                │◀──── JSON rows ──────────────│
  │                                │── transform_row() (→ Node/Rel/Path)
  │                                │── cache results              │
  │◀──── SUCCESS {fields} ────────│                              │
  │                                │                              │
  │── PULL {n: -1} ──────────────▶│                              │
  │◀──── RECORD [Node{...}] ─────│                              │
  │◀──── RECORD [Node{...}] ─────│                              │
  │◀──── SUCCESS {has_more:false} │                              │
```

### Key Data Structures

- **`BoltContext`** — per-connection mutable state (current state, user, schema, `IdMapper`)
- **`BoltConfig`** — server-wide immutable config (max message size, timeouts, auth settings)
- **`BoltServer`** — top-level server that spawns `BoltConnection` per TCP accept
- **`BoltHandler`** — stateful message handler holding `BoltContext`, `Authenticator`, cached results
- **`IdMapper`** — session-scoped bidirectional map: `element_id ↔ i64`

## 3. Files

| File | Lines | Responsibility |
|---|---|---|
| `mod.rs` | 384 | Module root. Constants (`BOLT_VERSION_*`, `SUPPORTED_VERSIONS`), `ConnectionState` enum, `BoltContext`, `BoltConfig`, `BoltServer`, version negotiation utils. |
| `handler.rs` | 1826 | **Core orchestrator.** Handles every Bolt message type (HELLO, LOGON, RUN, PULL, BEGIN, COMMIT, ROUTE, etc.). Contains `execute_cypher_query()` which runs the full parse→plan→SQL→execute→transform pipeline. Parameter substitution for `id()` predicates. |
| `result_transformer.rs` | 2146 | **Largest file.** `extract_return_metadata()` analyzes logical plan to classify return items as Node/Relationship/Path/Scalar/IdFunction. `transform_row()` converts flat ClickHouse rows into packstream-encoded graph objects. Handles fixed-hop paths, VLP paths, multi-label scans, UNION path JSON format, polymorphic edges. |
| `graph_objects.rs` | 717 | `Node`, `Relationship`, `Path` structs with `to_packstream()` methods. Custom packstream encoding helpers (`encode_integer`, `encode_string`, `encode_properties_map`, `encode_json_value`). |
| `connection.rs` | 605 | `BoltConnection<S>` — wire-level I/O. Handshake (magic preamble + version negotiation), chunked message reading/writing, packstream message parsing/serialization. Generic over `AsyncRead + AsyncWrite`. |
| `id_mapper.rs` | 571 | Deterministic 53-bit ID mapping. Label-encoded IDs (6 bits label code + 47 bits id value). Session cache with cross-session chaining via `ACTIVE_SESSION_CACHES` global registry. |
| `messages.rs` | 563 | `BoltMessage` and `BoltValue` types. Message constructors (hello, run, pull, success, failure, record). Field extractors for auth tokens, database, query, parameters, tenant_id, role, view_parameters. |
| `id_rewriter.rs` | 412 | Regex-based Cypher query rewriter for `id(alias) = N`, `id(alias) IN [...]`, `NOT id(alias) IN [...]`, `ORDER BY id(alias)`. Translates encoded integer IDs back to `__node_id__` property filters. |
| `auth.rs` | 402 | `AuthScheme`, `AuthToken`, `AuthenticatedUser`, `Authenticator`. SHA-256 password hashing, static user database. Auth disabled by default for development. |
| `errors.rs` | 214 | `BoltError` enum with Neo4j-compatible error codes (`Neo.ClientError.*`, `Neo.TransientError.*`). `is_recoverable()` method. |
| `websocket.rs` | 157 | `WebSocketBoltAdapter` — wraps `WebSocketStream<TcpStream>` to implement `AsyncRead + AsyncWrite`, enabling Bolt-over-WebSocket (for browser-based clients). |

**Total: ~7,997 lines**

## 4. Bolt Protocol State Machine

```rust
pub enum ConnectionState {
    Connected,           // TCP accepted, awaiting handshake
    Negotiated(u32),     // Version agreed (version stored), awaiting HELLO
    Authentication(u32), // HELLO received (Bolt 5.1+), awaiting LOGON
    Ready,               // Authenticated, can accept RUN/BEGIN
    Streaming,           // RUN succeeded, results cached, awaiting PULL/DISCARD
    Failed,              // Terminal state, connection will close
    Interrupted,         // Reserved (not currently used)
}
```

### State Transitions

| Current State | Message | Next State | Notes |
|---|---|---|---|
| `Connected` | handshake OK | `Negotiated(ver)` | Magic preamble + version negotiation |
| `Negotiated` | HELLO (Bolt 5.1+) | `Authentication(ver)` | No auth in HELLO for 5.1+ |
| `Negotiated` | HELLO (Bolt 4.x) | `Ready` | Auth included in HELLO |
| `Authentication` | LOGON | `Ready` | Bolt 5.1+ auth completion |
| `Ready` | RUN (success) | `Streaming` | Results cached |
| `Ready` | RUN (failure) | `Ready` | FAILURE sent, state unchanged |
| `Streaming` | PULL | `Ready` | Records + SUCCESS sent |
| `Streaming` | DISCARD | `Ready` | Results discarded |
| `Ready` | BEGIN | `Ready` | tx_id set in context |
| `Ready` | COMMIT/ROLLBACK | `Ready` | tx_id cleared |
| `Ready` | LOGOFF | `Authentication(ver)` | Bolt 5.1+ re-auth |
| Any | RESET | `Ready` | Clears transaction, keeps auth |
| Any | GOODBYE | `Failed` | No response sent |
| Any | Error | `Failed` | Connection closes |

### Bolt 4.x vs 5.1+ Authentication Flow

**Bolt 4.x**: `HELLO(user_agent, {scheme, principal, credentials})` → `SUCCESS` → `Ready`

**Bolt 5.1+**: `HELLO(user_agent, {})` → `SUCCESS` → `Authentication` → `LOGON({scheme, principal, credentials})` → `SUCCESS` → `Ready`

### Version Negotiation Details

The handshake sends 4 client version proposals (4 bytes each, big-endian). Bolt 5.x swaps major/minor byte order vs 4.x:
- **Bolt 4.x**: `[reserved][range][major][minor]`
- **Bolt 5.x**: `[reserved][range][minor][major]` (SWAPPED)

The `negotiate_version()` function uses a heuristic: if the decoded major byte is 5–8, interpret as Bolt 5.x format; otherwise use 4.x format. Range allows a client to express "I support 5.8 down to 5.0" in a single proposal.

## 5. Result Transformation Pipeline

```
ClickHouse JSON row
    │
    ▼
extract_return_metadata()     ← Analyzes LogicalPlan + PlanCtx
    │                            Classifies each RETURN item:
    │                            - Node { labels }
    │                            - Relationship { rel_types, from/to_label }
    │                            - Path { start/end/rel aliases, labels, types, is_vlp }
    │                            - IdFunction { alias, labels }
    │                            - Scalar
    ▼
transform_row()               ← Per-row transformation
    │
    ├── Node ──────────► transform_to_node()
    │                    1. Extract properties by alias prefix ("n.name" → "name")
    │                    2. Resolve label (per-row __label__, metadata, schema inference)
    │                    3. Look up ID columns from NodeSchema
    │                    4. Generate element_id via generate_node_element_id()
    │                    5. Assign integer id via id_mapper.get_or_assign()
    │                    6. Encode to packstream bytes via node.to_packstream()
    │
    ├── Relationship ──► transform_to_relationship()
    │                    1. Extract properties by alias prefix
    │                    2. Handle multi-type CTE arrays (unwrap single-element arrays)
    │                    3. Resolve from/to labels (including $any polymorphic)
    │                    4. Extract from_id/to_id from schema-defined columns
    │                    5. Generate element_ids for rel, start node, end node
    │                    6. Assign integer ids via id_mapper
    │                    7. Encode to packstream bytes
    │
    ├── Path ──────────► transform_to_path() or transform_vlp_path()
    │                    Fixed-hop: find start/end nodes + relationship in row
    │                    VLP: parse 9-field tuple [start_props, end_props, rel_props, ...]
    │                    JSON UNION: parse _start_properties, _end_properties JSON columns
    │                    → Path::single_hop(start, rel, end).to_packstream()
    │
    ├── IdFunction ────► compute element_id from row data, then
    │                    IdMapper::compute_deterministic_id() → i64
    │
    └── Scalar ────────► return row[field_name] as-is (BoltValue::Json)
```

### Multi-Label Scan Detection

`try_transform_multi_label_row()` checks for `{alias}_label`, `{alias}_id`, `{alias}_properties` columns — a special format used by multi-label node scans where each row carries its own label and JSON property blob.

### Label Resolution Priority

For nodes, label is resolved in this order:
1. Per-row `{alias}.__label__` column (VLP/UNION queries)
2. Metadata labels from query planning
3. Global `__label__` column
4. Schema inference from properties (`infer_node_label_from_properties`)

## 6. ID Mapping Scheme (53-bit, Label-Encoded)

### Problem

Neo4j Browser calls `id(node)` which returns an integer. ClickGraph has string-based `element_id` values like `"User:42"`, `"Post:100"`, `"Airport:LAX"`. The IDs must:
- Fit in JavaScript's `Number.MAX_SAFE_INTEGER` (2^53 - 1)
- Be **unique across labels** (`"User:1"` ≠ `"Post:1"`)
- Be **deterministic** (same element_id always → same integer)
- Support **reverse lookup** (integer → element_id for expand queries)

### Encoding Layout

```
┌──────────────────────────────────────────────────────┐
│  53-bit ID layout (within JS MAX_SAFE_INTEGER)       │
├──────────┬───────────────────────────────────────────┤
│  6 bits  │           47 bits                         │
│  label   │    id_value (raw or hash)                 │
│  code    │    max: 140 trillion                      │
│  (0-63)  │                                           │
└──────────┴───────────────────────────────────────────┘
```

- **Label code** (6 bits): Assigned by `LABEL_CODE_REGISTRY` (shared with `utils::id_encoding`). Up to 64 distinct labels.
- **ID value** (47 bits):
  - Numeric IDs (e.g., `"42"`) → used directly if ≤ 47 bits
  - String IDs (e.g., `"LAX"`) → `DefaultHasher` hash masked to 47 bits
  - Composite IDs (e.g., `"tenant1|user42"`) → hash of full string

### Session Cache Architecture

```
ACTIVE_SESSION_CACHES: RwLock<HashMap<u64, Arc<RwLock<SessionCache>>>>
        │
        ├── Connection 1: { "User:42" ↔ 12345, "Post:1" ↔ 67890 }
        ├── Connection 2: { "User:42" ↔ 12345, "Airport:LAX" ↔ 99999 }
        └── Connection 3: { ... }
```

- Each `IdMapper` instance registers itself on creation, unregisters on drop (when `Arc::strong_count ≤ 2`)
- **Forward lookup** (`element_id → i64`): local cache only
- **Reverse lookup** (`i64 → element_id`): local cache first, then cross-session chaining
- `static_lookup_element_id()`: searches all sessions without needing an instance
- `decode_for_query()`: tries cache lookup first, falls back to bit-pattern extraction for small numeric IDs

## 7. ID Rewriting for Neo4j Browser Expand

When a user double-clicks a node in Neo4j Browser, it sends queries like:

```cypher
MATCH (a) WHERE id(a) = 140737488355370 RETURN a
MATCH (a)--(o) WHERE id(a) = 140737488355370 AND NOT id(o) IN [140737488355371, 140737488355372] RETURN o
```

### Rewrite Pipeline (`id_rewriter.rs`)

1. **`rewrite_id_predicates()`** — entry point, applies all rewrite patterns:
   - `id(alias) = N` → `(alias:Label AND alias.__node_id__ = value)`
   - `id(alias) IN [N1, N2]` → `((alias:Label AND alias.__node_id__ = v1) OR (alias:Label AND alias.__node_id__ = v2))`
   - `NOT id(alias) IN [...]` → `NOT ((alias:Label AND ...) OR ...)`
   - `ORDER BY id(alias)` → `ORDER BY alias.id`
2. ID lookup via `IdMapper::get_element_id()` (session cache + cross-session)
3. Element ID parsed via `parse_node_element_id()` → `(label, id_values)`
4. `__node_id__` is a **marker property** that `FilterTagging` (in query planner) recognizes and transforms to the actual schema ID column

### Missing ID Handling

If an encoded integer ID is not found in any session cache, the predicate is replaced with `1 = 0` (impossible condition), producing an empty result set.

## 8. Critical Invariants

1. **State machine ordering**: Messages MUST be processed in correct state. RUN requires `Ready`, PULL requires `Streaming`, LOGON requires `Authentication`.

2. **Packstream encoding**: Graph objects (Node 0x4E, Relationship 0x52, Path 0x50, UnboundRelationship 0x72) MUST use exact byte signatures and field counts or Neo4j drivers will reject them.

3. **RECORD message structure**: `RECORD` always has exactly 1 field (a LIST). The serializer in `connection.rs` wraps `BoltMessage::record(fields)` into `0xB1 [RECORD_SIG] [LIST_HEADER field1 field2 ...]`.

4. **element_id is source of truth**: Integer `id` is always derived FROM `element_id` via `IdMapper::compute_deterministic_id()` — never the reverse. This ensures consistency.

5. **53-bit safety**: All integer IDs MUST be ≤ `2^53 - 1` (JavaScript's `MAX_SAFE_INTEGER`). The encoding uses 6 + 47 = 53 bits.

6. **Version byte order**: Bolt 5.x swaps major/minor bytes in handshake response. If you send 4.x format to a 5.x client, negotiation silently fails.

7. **Rc<RefCell<>> boundary**: The Cypher AST contains `Rc<RefCell<>>` which is `!Send`. The handler parses twice: once before the async boundary (for metadata extraction), once after (for planning). This is intentional, not a bug.

8. **Mutex locking**: `BoltContext` uses `std::sync::Mutex` (not `tokio::Mutex`) because locks are held briefly. The `lock_context!` macro adds proper error handling for mutex poisoning.

9. **Schema access**: The handler sets the task-local `QueryContext` schema before calling the query pipeline. GLOBAL_SCHEMAS is accessed directly only for schema lookup at connection/RUN scope.

10. **No write operations**: RUN rejects non-Read query types. Transactions (BEGIN/COMMIT/ROLLBACK) are accepted but function as no-ops for compatibility with drivers that auto-wrap queries in transactions.

## 9. Common Bug Patterns

### Pattern 1: Packstream Field Count Mismatch
**Symptom**: Neo4j Browser shows "Cannot read property of undefined" or connection drops.
**Cause**: Node struct must have exactly 4 fields (0xB4), Relationship exactly 8 (0xB8), UnboundRelationship 4 (0xB4). If `to_packstream()` adds/removes fields, the driver's deserializer crashes.
**Fix**: Always verify struct marker matches actual field count.

### Pattern 2: element_id → id Inconsistency
**Symptom**: Neo4j Browser expand (double-click) fails to find nodes.
**Cause**: The `id` in a RECORD response doesn't match what `id(n)` returns because different code paths compute the integer ID differently.
**Fix**: ALL id computation must go through `IdMapper::compute_deterministic_id()`. Both `transform_row()` and `id()` function evaluation must use this single source of truth.

### Pattern 3: Label Code Collision
**Symptom**: Two different node types get the same integer ID.
**Cause**: `LABEL_CODE_REGISTRY` is process-global. If labels are registered in different order across test runs, codes can shift. But within a single process, codes are stable.
**Risk**: Low (only 64 labels supported). Monitor with > 64 node types.

### Pattern 4: Cross-Session ID Lookup Failure
**Symptom**: Browser expand query returns empty when it should find nodes.
**Cause**: The IdMapper session that created the mapping was dropped (connection closed). Cross-session lookup only works while the originating session is alive OR the ID can be reconstructed from bit pattern.
**Mitigation**: `decode_for_query()` falls back to bit-pattern extraction for numeric IDs < 2^31.

### Pattern 5: Bolt 5.x Version Byte Swap
**Symptom**: "No compatible version found" despite client supporting 5.x.
**Cause**: Handshake response sent in wrong byte order for 5.x.
**Fix**: `perform_handshake()` swaps bytes for versions ≥ 0x00000500.

### Pattern 6: Multi-Type CTE Array Unwrapping
**Symptom**: Relationship properties show `["FOLLOWS"]` instead of `"FOLLOWS"`.
**Cause**: Multi-type CTE queries return single-element arrays. `transform_to_relationship()` must detect this pattern and call `extract_first_from_array()`.

### Pattern 7: VLP Path Truncation
**Symptom**: Multi-hop VLP paths only show first hop in Neo4j Browser.
**Cause**: `transform_vlp_path()` currently only produces `Path::single_hop()` — multi-hop path serialization is not yet implemented.
**Status**: Known limitation, logged with warning.

### Pattern 8: Directed Relationship in Expand
**Symptom**: Neo4j Browser crashes with "t.source is undefined" after expand.
**Cause**: Making the expand query undirected causes Relationship objects with start/end IDs referencing nodes not in the browser's graph.
**Fix**: Do NOT rewrite browser expand queries to undirected. There is a comment in `handle_run()` explaining this.

## 10. Public API

### Entry Point

```rust
// Create and start Bolt server
let config = BoltConfig::default();
let server = BoltServer::new(config, clickhouse_client);
server.handle_connection(tcp_stream, peer_addr).await?;
```

### Key Public Types

| Type | Location | Usage |
|---|---|---|
| `BoltServer` | `mod.rs` | Top-level server, cloneable, spawns connections |
| `BoltConfig` | `mod.rs` | Server configuration (ports, auth, timeouts) |
| `BoltContext` | `mod.rs` | Per-connection state (state machine, user, schema, id_mapper) |
| `ConnectionState` | `mod.rs` | State machine enum |
| `BoltMessage` | `messages.rs` | Message type with constructors and extractors |
| `BoltValue` | `messages.rs` | Either `Json(Value)` or `PackstreamBytes(Vec<u8>)` |
| `Node` | `graph_objects.rs` | Packstream-encodable node struct |
| `Relationship` | `graph_objects.rs` | Packstream-encodable relationship struct |
| `Path` | `graph_objects.rs` | Packstream-encodable path struct |
| `IdMapper` | `id_mapper.rs` | Session-scoped ID mapper (53-bit encoding) |
| `Authenticator` | `auth.rs` | Authentication manager |
| `WebSocketBoltAdapter` | `websocket.rs` | WebSocket-to-AsyncRead/Write adapter |
| `BoltError` | `errors.rs` | Error type with Neo4j error codes |

### Key Public Functions

| Function | Location | Purpose |
|---|---|---|
| `extract_return_metadata()` | `result_transformer.rs` | Classify RETURN items as Node/Rel/Path/Scalar |
| `transform_row()` | `result_transformer.rs` | Convert ClickHouse row → packstream graph objects |
| `rewrite_id_predicates()` | `id_rewriter.rs` | Rewrite `id(n) = N` to property filters |
| `IdMapper::compute_deterministic_id()` | `id_mapper.rs` | Static: element_id → 53-bit integer |
| `IdMapper::decode_for_query()` | `id_mapper.rs` | Static: encoded integer → (label, raw_value) |
| `utils::negotiate_version()` | `mod.rs` | Select best protocol version from client proposals |

## 11. Testing Guidance

### Unit Tests (in-file `#[cfg(test)]` modules)

All files have unit tests. Run with:
```bash
cargo test --lib server::bolt_protocol
```

Key test areas:
- **`mod.rs`**: Context creation, state transitions, version negotiation, version string formatting
- **`messages.rs`**: Message construction, field extraction, type identification
- **`auth.rs`**: Scheme parsing, token creation, authenticator (enabled/disabled), password hashing
- **`id_mapper.rs`**: Numeric/string/composite IDs, label collision prevention, session cache chaining, cross-session lookup
- **`id_rewriter.rs`**: id() equals/IN/NOT IN rewriting, ORDER BY rewriting, complex query with multiple patterns
- **`graph_objects.rs`**: Packstream encoding for integers, strings, lists, maps, Node/Relationship structures, composite element IDs
- **`handler.rs`**: HELLO/RESET/GOODBYE/transaction lifecycle (async tests)
- **`result_transformer.rs`**: value_to_string, field name extraction
- **`connection.rs`**: MockStream, chunk creation, magic preamble constant
- **`errors.rs`**: Error creation, error codes, recoverability

### Integration Testing

Requires a running ClickGraph server with Bolt enabled:
```bash
# Start server with Bolt
cargo run --bin clickgraph -- --bolt-port 7687

# Connect with cypher-shell or Neo4j Browser
cypher-shell -a bolt://localhost:7687 -u neo4j -p password
```

Tests in `tests/integration/bolt/` cover end-to-end scenarios.

### What to Test After Changes

| Changed File | Test Focus |
|---|---|
| `connection.rs` | Handshake, chunked message I/O, version byte order |
| `handler.rs` | State transitions, query execution, parameter handling |
| `result_transformer.rs` | Graph object creation with various schemas, edge cases |
| `graph_objects.rs` | Packstream byte-level verification |
| `id_mapper.rs` | ID uniqueness, cross-label collision, session chaining |
| `id_rewriter.rs` | Regex patterns, edge cases in complex queries |
| `messages.rs` | Field extraction for all message types |

## 12. Dangerous Files / High-Risk Areas

### `result_transformer.rs` (2146 lines) — **HIGHEST RISK**
- Most complex file. Handles 6+ result formats (standard, VLP, multi-type CTE, UNION JSON, multi-label scan, polymorphic edges)
- Label resolution has 4-level fallback chain — easy to break
- Property key cleaning (`clean_property_keys`) removes prefixes that may be needed
- Relationship ID extraction tries multiple column name patterns (from_id, start_id, schema columns) — order matters
- Any change here can break Neo4j Browser visualization silently

### `handler.rs` (1826 lines) — **HIGH RISK**
- Contains `execute_cypher_query()` (~500 lines) which orchestrates the entire pipeline
- **Two-pass parsing**: parses query twice (before/after async boundary) due to `Rc<RefCell<>>` in AST
- Parameter substitution (`substitute_cypher_parameters`) has security implications — injection prevention via regex + literal checks
- State transitions on error paths — incorrect state can wedge the connection
- Schema resolution has complex fallback: RUN metadata → HELLO metadata → BEGIN metadata → first loaded schema

### `connection.rs` (605 lines) — **MEDIUM RISK**
- Packstream serialization for RECORD messages has special-case handling (LIST wrapping)
- Message size validation can cause DoS if too permissive
- Version byte swap for Bolt 5.x — subtle encoding bug will silently break negotiation

### `id_mapper.rs` (571 lines) — **MEDIUM RISK**
- Global statics (`ACTIVE_SESSION_CACHES`, `NEXT_CONNECTION_ID`) — thread safety critical
- `Drop` implementation conditionally unregisters based on `Arc::strong_count` — race condition possible under high concurrency
- Hash collisions in 47-bit space are theoretically possible for large string IDs

### `graph_objects.rs` (717 lines) — **MEDIUM RISK**
- Custom packstream implementation — must match Neo4j driver expectations exactly
- UnboundRelationship (0x72) vs Relationship (0x52) confusion can crash Path deserialization
- `encode_json_value` for Array/Object types currently returns empty containers (TODO)
