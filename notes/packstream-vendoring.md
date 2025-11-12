# PackStream Vendoring

**Date**: November 12, 2025  
**Status**: ✅ Complete  
**Files**: `brahmand/src/packstream/` (4 files, ~3,371 lines)

## Summary

Vendored the PackStream serialization module from neo4rs v0.9.0-rc.8 to enable complete Bolt protocol message parsing and serialization. PackStream is Neo4j's binary format for encoding data over the Bolt protocol.

## Why Vendor?

### Problem Space
Bolt protocol requires PackStream binary format for:
- **Incoming messages**: HELLO (auth map), RUN (query + parameters), PULL (fetch metadata)
- **Outgoing messages**: SUCCESS (metadata), FAILURE (errors), **RECORD (graph data)**
- **Complex data types**: Nodes, relationships, paths, nested collections

### Crate Landscape
1. **packs-rs**: Abandoned (5 years, no commits, no maintenance)
2. **neo4rs**: Active but `packstream` module is **private**
   - Attempted: Adding `unstable-serde-packstream-format` feature
   - Result: `error[E0603]: module 'packstream' is private`
   - Reason: neo4rs keeps packstream internal for their driver implementation

### Solution Decision
**Vendor the neo4rs packstream module**:
- ✅ MIT licensed (permits vendoring with attribution)
- ✅ Complete, production-tested implementation (~3,371 lines)
- ✅ Serde-based API (`from_bytes`, `to_bytes`)
- ✅ Handles all PackStream types (primitives, collections, structs)
- ✅ Official Neo4j Labs code
- ✅ Full control over code and dependencies

## How It Works

### File Structure
```
brahmand/src/packstream/
├── mod.rs           (634 lines)  - Main module, exports public API
├── de.rs            (567 lines)  - Deserializer implementation
└── ser/
    ├── mod.rs       (1,138 lines) - Serializer implementation
    └── map.rs       (1,032 lines) - Map/struct serialization helpers
```

### Public API
```rust
use crate::packstream;

// Deserialize PackStream bytes into Rust types
let value: HashMap<String, serde_json::Value> = 
    packstream::from_bytes(bytes)?;

// Serialize Rust types into PackStream bytes
let bytes: Bytes = packstream::to_bytes(&value)?;
```

### PackStream Format Basics
- **Marker byte**: Encodes type and size
  - `0x8X`: Tiny string (X = length 0-15)
  - `0x9X`: Tiny list (X = count 0-15)
  - `0xAX`: Tiny map (X = pairs 0-15)
  - `0xBX`: Struct (X = fields 0-15)
  - `0xCX`: Primitives (null, bool, int, float)
  - `0xDX`: Sized variants (8/16/32-bit length)
- **Variable-length encoding**: Minimizes wire size
- **Struct with tag**: `0xBX [tag] [fields...]` for message types

### Integration Points

**In connection.rs**:
```rust
use crate::packstream;

// Parse HELLO message
let metadata: HashMap<String, Value> = 
    packstream::from_bytes(field_bytes)?;

// Parse RUN message (query + parameters)
let fields: (String, HashMap<String, Value>) = 
    packstream::from_bytes(field_bytes)?;

// Serialize response message
let field_bytes = packstream::to_bytes(&field)?;
```

**Message structure**:
```
Bolt Message = 0xB[field_count] [signature] [field1] [field2] ...
               ↑                ↑           ↑
               Structure marker Message type PackStream fields
```

## Key Files Modified

### 1. Vendored Files (with MIT attribution)
All 4 files have attribution header:
```rust
// PackStream [component description]
// Vendored from neo4rs: https://github.com/neo4j-labs/neo4rs
// Original license: MIT
// Copyright (c) Neo4j Labs
```

### 2. brahmand/src/lib.rs
```rust
pub mod packstream;  // Vendored from neo4rs for Bolt protocol support
```

### 3. brahmand/Cargo.toml
```toml
# Removed: neo4rs dependency
# Kept: bytes = "1.8"  # Required by packstream
```

### 4. brahmand/src/server/bolt_protocol/connection.rs
```rust
use crate::packstream;

// Parse incoming messages
let metadata: HashMap<String, Value> = packstream::from_bytes(bytes)?;

// Serialize outgoing messages
let bytes = packstream::to_bytes(&value)?;
```

### 5. brahmand/src/main.rs
```rust
// Fixed: Use library modules instead of redeclaring
use clickgraph::{
    open_cypher_parser,
    clickhouse_query_generator,
    // ... etc
};
```

## Design Decisions

### Why Not Implement Minimal Parser?
**Option considered**: Implement just enough PackStream for our message types (~200-300 lines)

**Rejected because**:
- Need full data type support for RECORD messages
- Nodes: `{id: Int, labels: [String], properties: {String: Value}}`
- Relationships: `{id: Int, type: String, start: Int, end: Int, properties: {...}}`
- Paths: Alternating sequences of nodes and relationships
- Nested collections: Lists, maps, arbitrary depth
- Would take 1-2 weeks to implement and test correctly
- neo4rs code is already production-tested

### Why Not Use Full neo4rs Driver?
**Strategic decision**: Keep our Bolt protocol architecture

**Reasons**:
- Different API design (our `BoltHandler` pattern)
- More control over error handling and state management
- Cleaner separation of concerns
- Only need PackStream, not connection pooling/transactions/etc.

### Why Copy Manually Instead of Git Subtree?
**Practical**: Only 4 files needed, simple structure

**Benefits**:
- No git complexity
- Easier to maintain
- Clear ownership (our codebase, not submodule)
- Can apply local patches if needed

## Gotchas & Limitations

### Test Code References `bolt` Module
**Issue**: Tests in `mod.rs` reference `crate::packstream::bolt`

**Context**: `bolt()` function is test-only helper defined in `#[cfg(test)] mod value`

**Impact**: None - tests only compile with `#[cfg(test)]`, binary doesn't include them

### Data Type Mapping
**Challenge**: Mapping ClickHouse JSON to Neo4j graph types

**Current**: Returns generic JSON Values in RECORD messages

**TODO**: Implement proper graph data types (nodes, relationships, paths)
- Need to define struct types for Node, Relationship, Path
- Implement Serialize trait for these types
- Use PackStream struct encoding with appropriate tags

### PackStream Version
**Current**: Bolt 4.4 / PackStream 1.0

**Future**: Bolt 5.x may introduce PackStream 2.0 with breaking changes

**Mitigation**: Version negotiation in handshake, can add v2 support later

## Testing Strategy

### Unit Tests (Already Present)
- Packstream module has extensive tests (~40 test cases)
- Coverage: All primitive types, collections, structs, edge cases
- Run with: `cargo test --lib packstream`

### Integration Testing (TODO)
1. **Neo4j Python Driver** (`neo4j` package):
   ```python
   from neo4j import GraphDatabase
   driver = GraphDatabase.driver("bolt://localhost:7687")
   with driver.session() as session:
       result = session.run("MATCH (n:User) RETURN n.name")
       print(list(result))
   ```

2. **Neo4j JavaScript Driver** (`neo4j-driver` npm):
   ```javascript
   const neo4j = require('neo4j-driver');
   const driver = neo4j.driver('bolt://localhost:7687');
   const session = driver.session();
   const result = await session.run('MATCH (n:User) RETURN n.name');
   ```

3. **Manual with Neo4j Browser**:
   - Configure Browser to connect to `bolt://localhost:7687`
   - Execute Cypher queries via UI
   - Verify results render correctly

## Future Work

### Implement Graph Data Types
```rust
#[derive(Serialize)]
struct Node {
    id: i64,
    labels: Vec<String>,
    properties: HashMap<String, serde_json::Value>,
}

#[derive(Serialize)]
struct Relationship {
    id: i64,
    type_: String,
    start_node: i64,
    end_node: i64,
    properties: HashMap<String, serde_json::Value>,
}

#[derive(Serialize)]
struct Path {
    nodes: Vec<Node>,
    relationships: Vec<Relationship>,
}
```

### Optimize for Large Results
- Stream RECORD messages without buffering all results
- Implement backpressure if client slows down
- Add result size limits and pagination

### Add PackStream v2 Support
- Bolt 5.x introduces breaking changes
- New data types (date/time improvements, point types)
- Keep v1 support for backward compatibility

## References

- **Neo4j Labs Repository**: https://github.com/neo4j-labs/neo4rs
- **PackStream Specification**: https://neo4j.com/docs/bolt/current/packstream/
- **Bolt Protocol Specification**: https://neo4j.com/docs/bolt/current/
- **Serde Documentation**: https://serde.rs/
- **MIT License**: https://opensource.org/licenses/MIT

## Lessons Learned

1. **Check private modules**: Even with feature flags, modules can still be private
2. **Vendor when needed**: Sometimes copying code is the pragmatic solution
3. **Block scope for Send**: Drop non-Send types before `.await` points
4. **Main.rs vs lib.rs**: Binary should import from library, not redeclare modules
5. **License compliance**: Always add attribution headers for vendored code
