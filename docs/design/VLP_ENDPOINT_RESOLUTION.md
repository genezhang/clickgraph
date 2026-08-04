# VLP endpoint & property resolution — unification design

Tracking issue: **#989**. Sibling of #887 (`VLP_EDGE_IDENTITY_UNIFICATION.md`),
which owns edge **identity/uniqueness**. This doc owns the *other* systemic VLP
cluster: **resolving a variable-length path's endpoint nodes and their
properties** across the denormalized / composite-id / mixed-access / polymorphic
schema axes.

## 0. Why this doc exists — the bug-driven-refactoring case

The VLP endpoint-resolution family keeps spawning residuals, one schema-axis
combination at a time. Verified instance chain (all filed *from the review of the
previous fix*, not independently discovered):

```
#897 (flat exact-bound poly discriminator)
  → #924  denorm-polymorphic: classifier collapses denorm-before-poly
#908 (mixed-access endpoint property resolver)
  → #927  mixed-access composite endpoint bails (projects nothing)
  → #934  mixed-access denorm start non-id prop → rel.<col> (Code 47)
#922 (closed OPTIONAL VLP anchor)
  → #979  composite-id anchor join keys off first id-col vs composite concat
#1003 (mixed-access end-filter placement)
  → #1006 flat mixed *1..1 standard-side RETURN → t1.<col> (Code 47)
  → #1007 mixed-access end-filter Code 47 on *N..N and *0..N
```

Each fix is correct and complete for the arm it touches, and each surfaces the
identical endpoint-resolution gap in the next arm. **This is the #887 pattern on
the endpoint/property side.** Per PRIORITIES §1.6 and the #887 precedent, the
convergent move is to collapse the duplicated resolution to one canonical
component — not to keep patching cells, because each patch's review spawns the
next cell (measured: 28 issues closed in 24h, open count flat at 22).

## 1. The rule (semantics — the invariant every arm must satisfy)

For a VLP `(s)-[:R*..]->(e)`, at every site that needs an endpoint's **id
column(s)** (for a join/anchor/correlation) or a **non-id property** (for a
projection/filter), the resolution is a pure function of:

- the endpoint's **role** (start vs end — they can differ, e.g. mixed-access),
- the endpoint's **storage** (own node table, or embedded in the edge table when
  denormalized),
- the endpoint's **key arity** (single-column vs composite `node_id`),
- the edge's **polymorphism** (a `type_column` discriminator, orthogonal to the
  above — a denorm edge can ALSO be polymorphic, which #924 proves the current
  classifier cannot represent).

The invariant: **the same (role, storage, key-arity, polymorphism) tuple must
resolve identically at every site** — flat and recursive, render and analyzer,
anchor-join and projection and WHERE. Today it does not, because each site
recomputes the resolution from raw flags and the axes are entangled in a
single-value classifier.

## 2. The duplication, grounded (verified inventory)

### 2.1 The whole-VLP single-value classifier collapses orthogonal axes

`detect_vlp_schema_type` (`render_plan/cte_extraction.rs:7508`) returns ONE
`VlpSchemaType ∈ {Normal, Polymorphic, Denormalized, FkEdge}` for the whole VLP,
via priority-ordered early-returns:

```rust
if left_is_denorm && right_is_denorm { return Denormalized; }   // shadows Polymorphic (#924) & composite (#927)
if rt == st && rt == et            { return FkEdge; }
if scan.type_column.is_some()      { return Polymorphic; }
Normal
```

Three structural defects, each a filed bug:
- **denorm shadows polymorphic** (#924): a denorm edge that also has a
  `type_column` classifies `Denormalized`, dropping the per-hop discriminator.
- **denorm shadows composite** (#927): key-arity is not part of the type, so the
  composite case is invisible until a downstream resolver bails.
- **whole-VLP, not per-endpoint** (#1006/#934/#1003): mixed-access = start and
  end have *different* storage, which a single value cannot represent. The
  mixed-access path works around this with a *separate* pair of booleans
  (`start_is_denormalized`/`end_is_denormalized`, `variable_length_cte.rs:247`)
  threaded through ~90 functions — a second, parallel classification that must be
  kept consistent with the enum by hand.

### 2.2 The resolution is recomputed per-site from raw flags

`variable_length_cte.rs` (4,942 lines) branches on the parallel per-endpoint
booleans `start_is_denormalized`/`end_is_denormalized` at **25** sites across 9
functions (`new_mixed`, `generate_mixed_base_case`, `generate_mixed_recursive_case`,
`mixed_denorm_endpoint_property_items`, `emit_mixed_property_items`, the start/end
join builders at :3449/:3480, the id-expr selection at :3650/:3661, …), plus a
further ~48 raw axis-flag tokens (`is_denormalized`, `to_label_column`,
`from_label_column`, `type_column`) — **73 raw axis branches total in this file**.
The flat expander `expand_fixed_length_joins_with_context`
(`render_plan/cte_extraction.rs:8240`) dispatches on the whole-VLP
`ctx.schema_type` at :8282, and its `Denormalized` arm skips
`vlp_polymorphic_hop_conditions` (:8353) — this `match` is the literal #924/#1006
flat-path root. The analyzer sibling
(`query_planner/analyzer/graph_join/inference.rs`) recomputes an endpoint's id
resolution too — the #979 anchor-join keyed off `anchor_schema.node_id.columns()
.first()` (:3399) instead of the composite `concat` lives here. None of these
consult a shared component; each re-derives storage + key-arity locally, so a fix
to one (e.g. #979 teaching the anchor join about composite keys) does not teach
the others.

### 2.3 The live arms and their current resolution (the matrix)

| storage × arity           | flat *1..1 | recursive *n..m | anchor join | property proj | WHERE |
|---------------------------|-----------|-----------------|-------------|---------------|-------|
| own-table, single         | ok        | ok              | ok          | ok            | ok    |
| own-table, composite      | #627      | #627            | #979        | —             | —     |
| denorm (both), single     | ok        | ok              | ok (#983/#992) | ok         | ok    |
| denorm, polymorphic       | **#924**  | —               | —           | —             | —     |
| mixed (start≠end), single | **#1006** | ok (post-#1003) | —           | ok (#908)     | **#934/#1007** |
| mixed, composite          | —         | —               | —           | **#927**      | —     |

Cells marked with an issue number are *known* gaps; blanks are untested
combinations that the current per-site model will re-open one review at a time.

## 3. Target shape (the canonical component)

One `VlpEndpointResolution`, constructed **once per endpoint** from
`PatternSchemaContext` (satisfying axis-dispatch rule #7 — no raw
`is_denormalized` / `.columns().first()` branching at consumer sites), consumed by
every endpoint/property site. It reads directly off the existing per-endpoint
`NodeAccessStrategy` (`pattern_schema.rs:80` — `OwnTable{id_column, properties}` vs
`EmbeddedInEdge{edge_alias, properties}`), so storage and key-arity are already
first-class:

```rust
/// How to reach a VLP endpoint's id and non-id properties, for ONE endpoint
/// (start or end — resolved independently so mixed-access is representable).
/// Constructed from PatternSchemaContext's per-endpoint NodeAccessStrategy; the
/// axes are ORTHOGONAL data, not a collapsed single-value enum (that collapse is
/// #924/#927's root cause).
struct VlpEndpointResolution {
    /// Where the endpoint's columns live (OwnTable vs EmbeddedInEdge).
    storage: EndpointStorage,
    /// The id column(s) — a Vec so composite keys are first-class (#627/#979).
    id_cols: Vec<Column>,
    /// SQL to reach a non-id property (own-table join vs edge-column) — closes
    /// #908/#927/#934/#1006 by making the property path a property of the
    /// endpoint, not a per-site re-derivation.
    fn property_sql(&self, prop: &PropName) -> Result<PropertyAccess, Loud>;
    /// The composite/single key equality for an anchor/self-join (#979).
    fn key_equality(&self, other_alias: &str) -> String;
}

impl VlpEndpointResolution {
    fn from_pattern(ctx: &PatternSchemaContext, role: EndpointRole) -> Self { ... }
}
```

**Polymorphism is an EDGE property, not an endpoint one** — in the schema it lives
on `EdgeAccessStrategy::Polymorphic{type_column, from_label_column,
to_label_column}` (`pattern_schema.rs:187`), and the #924 per-hop discriminator
(`vlp_polymorphic_hop_conditions`, `cte_extraction.rs:8197`) is an *edge*-level
predicate combining the edge-type equality with BOTH endpoint labels. It therefore
belongs on a small edge-level companion, mirroring #887's `EdgeUniquenessPolicy`
(constructed from the same `PatternSchemaContext`). The clean split: this
`VlpEndpointResolution` owns storage / arity / property-SQL / key-equality (per
endpoint); the edge companion owns the poly discriminator (per edge). denorm+poly
(#924) is then expressible as `EmbeddedInEdge` storage **and** a present edge-level
discriminator — the two axes no longer shadow each other.

The consumer sites stop branching on flags and call
`resolution.property_sql(prop)` / `.key_equality(..)`, reading the discriminator
from the edge companion. Composite and polymorphism become *data*, so a new axis
combination is a new constructor input, not a new `if` at dozens of sites.

## 4. Phased migration (one slice per PR, byte-identity spike per #887 §2.5)

The #887 precedent is binding here: introduce the type with `debug_assert_eq!`
against every inline site FIRST (proving byte-identity), and only then delete the
inline copies. No behavior change until an explicit fix phase.

- **Phase 0 — inventory tripwire + freeze (pure hygiene, 0 behavior change).**
  Land this doc. Add a `#[test]` that asserts the raw-flag branch count in
  `variable_length_cte.rs` + `inference.rs` does not grow (a ratchet specific to
  the endpoint-resolution axis, so no new per-site branch can be added while the
  policy is being built). Convert #924/#927/#934/#979/#1006/#1007/#627/#643/#683
  to checklist rows on #989; relabel any standalone as `design-cycle`. **This is
  the shippable-today slice.**

- **Phase 1 — introduce `VlpEndpointResolution` + `from_pattern`, `debug_assert_eq!`
  it against every current resolution site (no switch yet — the byte-identity
  spike).** Goldens byte-identical.

- **Phase 2 — switch the RENDER consumer sites to the policy.** This is BOTH
  `variable_length_cte.rs` (the recursive/mixed CTE generators — the
  `start_is_denormalized`/`end_is_denormalized` sites) AND
  `cte_extraction.rs`'s flat expander `expand_fixed_length_joins_with_context`
  (the `match ctx.schema_type` at :8282 that strands #924/#1006 — the flat-path
  root MUST migrate here or those cells have no policy to lean on in Phase 4).
  Byte-identical goldens (Phase 1 asserts guarantee it). The raw sites collapse to
  policy calls; the ratchet baseline drops.

- **Phase 3 — switch the ANALYZER sites (`inference.rs`) to the policy.**
  Byte-identical (the #979 anchor-join `.columns().first()` becomes
  `resolution.key_equality(..)`).

- **Phase 4 — fix the open cells as behavior changes, one axis per PR, each with
  a live oracle**: #924 (denorm+poly discriminator — now the flat expander reads
  the edge companion's discriminator regardless of `EmbeddedInEdge` storage),
  #927 (mixed composite endpoint), #934/#1006/#1007 (mixed property/WHERE
  resolution), #979/#627 (composite key equality). Each becomes a small diff
  BECAUSE the resolution is now one place — this is the payoff that makes the
  cluster un-spawnable.

## 5. Success metric (makes "is the loop broken?" measurable)

- Raw endpoint-resolution branch count — the `start_is_denormalized` /
  `end_is_denormalized` booleans (**25** in `variable_length_cte.rs`), the other
  axis-flag tokens there (~48), the flat-expander `match ctx.schema_type` +
  `detect_vlp_schema_type` consumers in `cte_extraction.rs`, and the endpoint-id
  derivations in `inference.rs`: on the order of **~90 raw axis branches today**
  across the three files. Target after Phase 2–3: these route through
  `VlpEndpointResolution` (+ the edge companion), and the count drops toward the
  single constructor.
- **No new endpoint-resolution residual can be filed against an arm the policy
  owns** — because after Phase 2–3 there is one arm. That is the loop-breaking
  condition.

## 6. Explicitly out of scope (do NOT fold in)

- **#887** — edge identity/uniqueness (the sibling; its `EdgeUniquenessPolicy`
  and this `VlpEndpointResolution` are complementary, constructed from the same
  `PatternSchemaContext` but answering different questions).
- **Shared-anchor comma patterns** (#673/#933) — join-builder shape, not VLP
  endpoint resolution.
- The Path-C "`WHERE false` for any unsupported scalar predicate" residual
  (surfaced in #1000) — a render-fallback issue, unrelated.
