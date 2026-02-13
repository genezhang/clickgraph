# utils Module — Agent Guide

> **Purpose**: Shared utility functions used across the codebase.
> Small, focused helpers — CTE naming, ID encoding, serde helpers.
> These are foundational utilities that many modules depend on.

## Module Architecture

```
utils/
├── mod.rs          (5 lines)   ← Re-exports submodules
├── cte_naming.rs   (230 lines) ← CTE name generation/parsing (single source of truth)
├── id_encoding.rs  (277 lines) ← Neo4j-compatible 53-bit ID encoding for Bolt protocol
├── serde_arc.rs    (20 lines)  ← Serialize/Deserialize for Arc<T>
└── serde_arc_vec.rs (27 lines) ← Serialize/Deserialize for Vec<Arc<T>>
```

**Total**: ~560 lines

## Key Files

### cte_naming.rs — CTE Name Generation (CRITICAL)

**Problem solved**: Previously 7+ locations generated CTE names with subtle differences
(sorting, sequence numbers, empty alias handling), causing bugs where CTEs were created
with one name but referenced with another.

**Naming convention**: `with_{sorted_aliases}_cte_{counter}`
- Aliases are ALWAYS sorted alphabetically
- Examples: `with_friends_p_cte_1`, `with_a_cte_1`, `with_cte_1` (no aliases)

**Functions**:
- `generate_cte_name(&[aliases], counter) → String` — full name with counter
- `generate_cte_base_name(&[aliases]) → String` — name without counter (for lookups)
- `is_generated_cte_name(name) → bool` — checks if name matches `with_*_cte*` pattern
- `extract_cte_base_name(name) → Option<String>` — strips counter from full name
- `extract_aliases_from_cte_name(name) → Option<Vec<String>>` — reverse lookup

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

### serde_arc_vec.rs — Vec<Arc<T>> Serde Helper
Custom serialize/deserialize for `Vec<Arc<T>>` — serializes each element's inner value,
deserializes and wraps each in `Arc::new()`. Used as `#[serde(with = "crate::utils::serde_arc_vec")]`.

## Critical Invariants

### 1. CTE Naming is the Single Source of Truth
**ALL** CTE name generation MUST use `cte_naming.rs` functions. Generating CTE names
inline in other modules will cause name mismatch bugs. This was a historical pain point.

### 2. ID Encoding Must Be JS-Safe
All encoded IDs must fit within 2^53 - 1 (JavaScript's MAX_SAFE_INTEGER).
Neo4j Browser processes these IDs in JavaScript, so precision loss = broken IDs.

### 3. Label Code 0 is Reserved
Label code 0 means "not encoded" (raw value). This is why `LabelCodeRegistry` starts
at code 1. Never assign code 0 to a label.

### 4. Global Registry is Thread-Safe but Not Reset
`LABEL_CODE_REGISTRY` uses `lazy_static` + `RwLock`. Once a label is assigned a code,
it persists for the process lifetime. This can cause issues in tests if label ordering
matters — tests should not depend on specific code assignments.

## Dependencies

**What this module uses**:
- `lazy_static` — global registry (id_encoding)
- `serde` — serialization traits (serde_arc, serde_arc_vec)

**What uses this module**:

| Utility | Used by |
|---------|---------|
| `cte_naming` | `render_plan/plan_builder.rs` |
| `id_encoding` | `server/bolt_protocol/id_mapper.rs`, `server/graph_catalog.rs`, `query_planner/optimizer/union_pruning.rs` |
| `serde_arc` | `query_planner/logical_plan/mod.rs`, `query_planner/logical_expr/mod.rs` |
| `serde_arc_vec` | `query_planner/logical_plan/mod.rs` |

## Public API

```rust
// CTE naming
pub fn generate_cte_name(aliases: &[impl AsRef<str>], counter: usize) -> String;
pub fn generate_cte_base_name(aliases: &[impl AsRef<str>]) -> String;
pub fn is_generated_cte_name(name: &str) -> bool;
pub fn extract_cte_base_name(name: &str) -> Option<String>;
pub fn extract_aliases_from_cte_name(cte_name: &str) -> Option<Vec<String>>;

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
pub mod serde_arc_vec { pub fn serialize/deserialize ... }
```

## Testing Guidance

- `cte_naming.rs` has comprehensive tests including roundtrip alias extraction
- `id_encoding.rs` has encode/decode roundtrip, boundary value, and registry tests
- `serde_arc*.rs` have no dedicated tests (tested implicitly through LogicalPlan serialization)
- Run with: `cargo test --lib utils`
- Doc tests: `generate_cte_name`, `generate_cte_base_name`, etc. have runnable doc examples

## When to Modify

- **CTE naming bugs**: Always fix in `cte_naming.rs`, never add inline name generation elsewhere
- **More labels than 63**: Would require changing the 6-bit label code scheme in id_encoding
- **New shared utilities**: Add here if used by 2+ modules; keep module-specific utils local
