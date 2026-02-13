# packstream Module — Agent Guide

> **Purpose**: PackStream binary serialization/deserialization for the Neo4j Bolt protocol.
> Vendored from [neo4rs](https://github.com/neo4j-labs/neo4rs) (MIT license).
> This is a **wire format codec** — do not modify unless fixing Bolt protocol bugs.

## Module Architecture

```
packstream/
├── mod.rs        (640 lines) ← Public API: from_bytes(), to_bytes(), Data wrapper,
│                                RawBytes, BoltBytesBuilder (test-only), Dbg (debug-only)
├── de.rs         (573 lines) ← Deserializer: bytes → Rust types via serde
└── ser/
    ├── mod.rs    (1144 lines) ← Serializer: Rust types → bytes via serde
    └── map.rs    (1044 lines) ← Map serialization helpers (AsMap, key validation)
```

**Total**: ~3,400 lines (including ~600 lines of tests)

## Key Files

### mod.rs — Public API & Data Types
- `from_bytes<T>(Bytes) → Result<T, de::Error>` — deserialize PackStream bytes to any serde type
- `to_bytes<T>(&T) → Result<Bytes, ser::Error>` — serialize any serde type to PackStream bytes
- `Data` — wrapper around `Bytes` with reset/keep-alive semantics for multi-pass parsing
- `RawBytes` — deserializes raw byte pointer for zero-copy access
- `BoltBytesBuilder` — test-only fluent builder for constructing PackStream byte sequences
- `Dbg` — debug-only pretty-printer for PackStream byte streams

### de.rs — Deserializer
Implements `serde::Deserializer` for the PackStream binary format.

Key design: uses a `Visitation` enum to control parsing behavior:
- `Default` — standard deserialization
- `BytesAsBytes` — deserialize byte sequences as borrowed bytes
- `RawBytes` — return raw unconsumed bytes (for `RawBytes` newtype)
- `MapAsSeq` — deserialize maps as sequences of key-value pairs
- `SeqAsTuple(n)` — deserialize lists as fixed-size tuples

Internal types:
- `ItemsParser` — shared iterator for list/map/struct field parsing
- `StructParser` — handles Bolt struct (tag + fields) as serde enum
- `SharedBytes` — unsafe shared mutable reference for progressive parsing

### ser/mod.rs — Serializer
Implements `serde::Serializer` for PackStream encoding.

Key encoding rules (PackStream spec):
- Integers: compact encoding (tiny_int -16..127 = 1 byte, up to int64 = 9 bytes)
- Strings: length-prefixed (tiny 0-15, string8/16/32)
- Lists: length-prefixed (tiny 0-15, list8/16/32)
- Maps: length-prefixed with string-only keys
- Structs: tag byte + fields (for Bolt protocol messages)

Notable: `MapSerializer` supports unknown-length maps by backpatching the header.

### ser/map.rs — Map Serialization Helpers
- `AsMap<T>` — wrapper to serialize tuples/sequences as PackStream maps
- `AsMapSerializer` — converts sequences of key-value pairs into map entries
- `InnerMapSerializer` — alternating key/value state machine
- `StringKeySerializer` — enforces string-only map keys
- `SpecialKeySerializer`/`SpecialValueSerializer` — internal protocol for map size hints

## Critical Invariants

### 1. PackStream Marker Bytes
The format uses specific marker byte ranges — these are part of the Neo4j spec:
- `0x00-0x7F`: Tiny positive int
- `0x80-0x8F`: Tiny string (length in low nibble)
- `0x90-0x9F`: Tiny list
- `0xA0-0xAF`: Tiny map
- `0xB0-0xBF`: Struct (length in low nibble, followed by tag byte)
- `0xC0`: Null, `0xC1`: Float64, `0xC2`: False, `0xC3`: True
- `0xC8-0xCB`: Int8/16/32/64
- `0xCC-0xCE`: Bytes8/16/32
- `0xD0-0xD2`: String8/16/32
- `0xD4-0xD6`: List8/16/32
- `0xD8-0xDA`: Map8/16/32

**Do not change marker byte assignments** — they must match the Neo4j PackStream specification.

### 2. Unsafe Code
- `SharedBytes::get()` uses `unsafe` for shared mutable reference (progressive byte consumption)
- `parse_string()` and `parse_bytes()` use `unsafe` for zero-copy borrowed slices
- These are vendored patterns from neo4rs — change only with extreme caution

### 3. Roundtrip Guarantee
All PackStream types must roundtrip: `from_bytes(to_bytes(x)) == x`. Tests verify this for
all primitive types, collections, and Bolt structures.

## Dependencies

**What this module uses**:
- `bytes` crate (`Bytes`, `BytesMut`, `BufMut`, `Buf`) — buffer management
- `serde` — serialization framework (Serialize/Deserialize traits)
- `thiserror` — error type definitions

**What uses this module**:
- `server/bolt_protocol/connection.rs` — Bolt message encoding/decoding
  - `packstream::from_bytes()` for deserializing Bolt message fields
  - `packstream::to_bytes()` for serializing response values

## Public API

```rust
// Deserialize PackStream bytes
pub fn from_bytes<T: DeserializeOwned>(bytes: Bytes) -> Result<T, de::Error>;

// Serialize to PackStream bytes
pub fn to_bytes<T: Serialize>(value: &T) -> Result<Bytes, ser::Error>;

// Internal (crate-visible)
pub(crate) fn from_bytes_ref<T>(bytes: &mut Data) -> Result<T, de::Error>;
pub(crate) fn from_bytes_seed<S>(bytes: &mut Data, seed: S) -> Result<S::Value, de::Error>;
```

## Testing Guidance

- Tests are extensive (~200 lines in `mod.rs`, ~160 lines in `ser/map.rs`)
- Use `BoltBytesBuilder` (`bolt()` function) to construct test PackStream bytes
- All tests verify roundtrip: deserialize → re-serialize → compare bytes
- Run with: `cargo test --lib packstream`
- Debug mode enables `Dbg` pretty-printer for inspecting byte streams

## When to Modify

- **Bolt protocol bugs**: If Neo4j clients send/receive unexpected bytes
- **New Bolt message types**: If adding new struct tags to the protocol
- **Never**: Change encoding rules — they're part of the PackStream specification
- **Caution**: This is vendored code — prefer minimal, targeted changes
