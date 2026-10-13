# utils Module — Agent Guide

> **Purpose**: Shared utility functions used across the codebase.
> Small, focused helpers — CTE column naming, ID encoding, SQL literals, serde helpers.
> These are foundational utilities that many modules depend on.

## Module Architecture

```
utils/
├── mod.rs               ← Re-exports submodules
├── cte_column_naming.rs ← `p{N}_{alias}_{property}` CTE column names
├── id_encoding.rs       ← Neo4j-compatible 53-bit ID encoding for Bolt protocol
├── serde_arc.rs         ← Serialize/Deserialize for Arc<T>
└── sql_literal.rs       ← SQL string literals
```

## Key Files

### id_encoding.rs — Neo4j ID Encoding

Encodes label+ID into a single 53-bit integer (fits JavaScript's `MAX_SAFE_INTEGER`).

**53-bit layout**:
```
[6-bit label_code (1-63)][47-bit id_value]
```

**Key design**: Label codes start at 1 (not 0) so ALL encoded IDs are distinguishable
from raw values. Raw `42` stays `42`; encoded User:42 becomes `(1 << 47) | 42`.

**Types**:
- `IdEncoding` — static methods for encode/decode/is_encoded
- `LabelCodeRegistry` — assigns unique 6-bit codes to label names (max 63)
- `LABEL_CODE_REGISTRY` — global `lazy_static` RwLock registry

**Functions**:
- `IdEncoding::encode(label_code, id_value) → i64`
- `IdEncoding::decode(encoded_id) → (u8, i64)`
- `IdEncoding::is_encoded(value) → bool` — checks for non-zero high bits
- `IdEncoding::decode_with_label(encoded_id) → Option<(String, i64)>` — uses global registry
- `IdEncoding::register_label(label) → u8` — thread-safe registration

### serde_arc.rs — Arc Serde Helper
Custom serialize/deserialize for `Arc<T>` — serializes the inner value, deserializes
and wraps in `Arc::new()`. Used as `#[serde(with = "crate::utils::serde_arc")]`.

## Critical Invariants

### 1. ID Encoding Must Be JS-Safe
All encoded IDs must fit within 2^53 - 1 (JavaScript's MAX_SAFE_INTEGER).
Neo4j Browser processes these IDs in JavaScript, so precision loss = broken IDs.

### 2. Label Code 0 is Reserved
Label code 0 means "not encoded" (raw value). This is why `LabelCodeRegistry` starts
at code 1. Never assign code 0 to a label.

### 3. Global Registry is Thread-Safe but Not Reset
`LABEL_CODE_REGISTRY` uses `lazy_static` + `RwLock`. Once a label is assigned a code,
it persists for the process lifetime. This can cause issues in tests if label ordering
matters — tests should not depend on specific code assignments.

## Dependencies

**What this module uses**:
- `lazy_static` — global registry (id_encoding)
- `serde` — serialization traits (serde_arc)

**What uses this module**:

| Utility | Used by |
|---------|---------|
| `cte_column_naming` | `bound_plan/lower/mod.rs` |
| `id_encoding` | `server/bolt_protocol/id_mapper.rs`, `server/graph_catalog.rs` |
| `sql_literal` | `sql_generator/emitters/clickhouse/to_sql_query.rs` |
| `serde_arc` | `query_planner/logical_expr/mod.rs` |

## Public API

```rust
// ID encoding
pub struct IdEncoding;
impl IdEncoding {
    pub fn encode(label_code: u8, id_value: i64) -> i64;
    pub fn decode(encoded_id: i64) -> (u8, i64);
    pub fn is_encoded(value: i64) -> bool;
    pub fn decode_with_label(encoded_id: i64) -> Option<(String, i64)>;
    pub fn register_label(label: &str) -> u8;
    pub fn get_label_code(label: &str) -> Option<u8>;
}

// Serde helpers (used as #[serde(with = "...")])
pub mod serde_arc { pub fn serialize/deserialize ... }
```

## Testing Guidance

- `id_encoding.rs` has encode/decode roundtrip, boundary value, and registry tests
- `serde_arc.rs` has no dedicated tests
- Run with: `cargo test --lib utils`

## When to Modify

- **More labels than 63**: Would require changing the 6-bit label code scheme in id_encoding
- **New shared utilities**: Add here if used by 2+ modules; keep module-specific utils local
