# VLP edge-identity & uniqueness unification — design

**Date**: 2026-08-02
**Status**: Design proposal — awaiting review
**Related**: `REFACTORING_SAFETY_PLAN.md` §2.1 (axis-dispatch) / §2.5 (transition-assert),
`CTE_MANAGER_DESIGN.md`, `PRIORITIES.md`,
`src/sql_generator/emitters/clickhouse/AGENTS.md` §uniqueness-axis
**Tracking issue**: #887
**Owned issue cluster**: #606, #628, #710, #806, #808
**Explicitly out of scope** (different subsystems — see §7): #643, #840, #627, #683

---

## 0. Why this doc exists — the bug-driven-refactoring case

The open-issue count is not falling: ~234 issues opened vs ~208 closed in the
last 30 days, and **8 of 26 open issues carry "residual / follow-up / remaining
/ still" in the title.** The VLP uniqueness family (#598 → #606 → #709 → #712 →
#807 → #710 → #806 → #628 → #808) is the clearest instance. Each fix was
*correct and complete for the arm it touched*, and each surfaced the identical
gap in the next arm. That is not thrash — it is a **coverage walk over a
duplication matrix with no shared abstraction.** The issue tracker is
enumerating the duplicate cells one report at a time.

The single rule "Cypher default relationship-uniqueness: a path may revisit a
node but must not reuse an edge" is implemented in **~14 places** across 4
files, with **3 mutually-inconsistent copies** of the edge-vs-node decision and
**6 independent** edge-identity computations. This doc proposes collapsing the
decision and the identity computation to one canonical component each, using the
project's mandated transition-assert method (§2.5) so the collapse is provably
byte-identical.

**This is not the `CTE_MANAGER_DESIGN.md` rewrite.** That doc proposes a
months-long facade over all CTE generation. This is a bounded, single-axis
consolidation (the *uniqueness* axis only) that can land in ~5 small PRs and
retires 5 issues. It is a down-payment on, and compatible with, the larger plan.

---

## 1. The rule (semantics — the invariant every arm must satisfy)

openCypher relationship-uniqueness (the default for variable-length patterns):

- A walk **MAY revisit a node** any number of times.
- A walk **MUST NOT traverse the same relationship twice** (a "trail").
- Two orientations of one physical undirected edge are the **same**
  relationship (trail-uniqueness holds in both directions — #617).
- `shortestPath` legitimately stays **node-unique**: revisiting a node can
  never shorten a path, so the stronger constraint changes no result and is
  cheaper. (Confirmed in #628 for the `*0..` self case.)
- A **zero-hop base** (`*0..N`) has no edge to seed, so its base row is
  node-tracked; edge-uniqueness begins at hop ≥ 1.

"Edge identity" — what makes two traversed edges "the same" — depends only on
the **schema shape**, and this is the axis that is currently duplicated:

| Schema shape | Edge identity | Rationale |
|---|---|---|
| Standard / polymorphic | schema `edge_id` if declared, else `(from,to)` | a real edge table row |
| FK-edge (edge = FK column on node) | ordered node-id pair `(from_id, to_id)` | no edge table; the node pair *is* the edge |
| Denormalized (both nodes virtual) | `(from,to)` tuple | no edge-id column (residual #710: parallel edges) |
| Undirected doubled-edge (#617) | original-orientation `(__cg_orig_from,__cg_orig_to)` | so reverse rows share identity |
| shortestPath / weighted / hetero-poly / zero-hop | none (node-unique) | node-uniqueness by design |

Every one of these is *already computed correctly somewhere*. The bug class is
that each **arm** re-derives it locally, so a new arm (or a flat-vs-recursive
sibling) silently gets it wrong.

---

## 2. The duplication, grounded (verified inventory)

Anchors are `file:line`. VLC = `variable_length_cte.rs`, CM =
`render_plan/cte_manager/mod.rs`, MT = `multi_type_vlp_joins.rs`,
FLAT = `cte_extraction.rs` + `filter_builder.rs`.

### 2.1 Three inconsistent copies of the edge-vs-node decision

1. **`VariableLengthCteGenerator::uses_edge_uniqueness(&self)`** — VLC:987.
   `shortest_path_mode.is_none() && effective_min_hops() >= 1 &&
   !is_heterogeneous_polymorphic_path()`. 8 consult sites (VLC:2444, 2735,
   2805, 3003, 3153, 3183, 3285, 3315).
2. **`DenormalizedCteStrategy::uses_edge_uniqueness(&self, context)`** —
   CM:1379. A **different signature**, and it **omits** the hetero-poly
   exclusion (denorm can't be hetero-poly, so this is safe *today* — but it is a
   second copy that can drift). 3 consult sites (CM:1773, 1960, 2006).
3. **Raw, gate-free node-uniqueness** — the flat FK/multi-type arm emits a
   `start != end` guard (FLAT: cte_extraction.rs:8400) consulting *neither*
   gate; several **dead** CM strategies emit `NOT IN (path_nodes)` directly
   (CM:1164, CM:2796). These are the #606 "projects path_edges but filters
   path_nodes" sites.

The MT flat path uses yet another spelling — `!self.is_shortest_path`
(MT:956) — as its analogue.

### 2.2 Six independent edge-identity computations

- `build_edge_tuple_base` (VLC:1124), `build_edge_tuple_recursive` (VLC:1182),
  `build_fk_edge_tuple` (VLC:1238), `edge_identity_column` (VLC:1110) — four
  in the recursive generator.
- `DenormalizedCteStrategy::edge_tuple` (CM:1387) — a fifth.
- The flat pairwise guard computes identity a **sixth** way, `(from,to)` on
  `r1..rN` aliases only (cte_extraction.rs:8421 / bidirectional_union.rs:1248),
  **ignoring the schema `edge_id`** — this is exactly **#806** (parallel edges
  collapse).

### 2.3 The reachability finding that collapses #606-CM and #808

The live VLP path is `CteManager::generate_vlp_cte` (CM:447) →
`VariableLengthCteStrategy` → the VLC generator arms (via `new_mixed` /
`new_with_fk_edge`, CM:3288–3350). The `CteManager::analyze_pattern` factory
(CM:389) that builds `Traditional / FkEdge / MixedAccess / EdgeToEdge /
Coupled` strategies has **exactly one caller — at CM:3826, inside
`#[cfg(test)] mod tests`.**

**Therefore those five CM strategy classes are corpus-unreachable dead code.**
The #606 "path_edges projected but path_nodes filtered" defect is real in
`TraditionalCteStrategy` (CM:1043 / CM:1164) and `EdgeToEdgeCteStrategy`
(CM:2778 / CM:2796), and *worse* in FkEdge/MixedAccess (project path_edges, **no
cycle predicate at all**) — but **all of it is dead.** The live
`DenormalizedCteStrategy` already does it correctly (edge_tuple → path_edges
CM:1776, filter path_edges CM:2008).

Likewise **#808** already established via `eprintln!` + full corpus sweep that
`generate_mixed_recursive_case`, `generate_heterogeneous_polymorphic_recursive_case`,
and `generate_denormalized_recursive_case` (the VLC copies) get **0 hits**.

**Consequence:** a large share of the "cluster" is not a correctness fix at all
— it is **dead-code deletion** (see Phase 0). Deleting it *first* shrinks the
surface the real unification has to cover, and makes the ratchet debt fall
immediately.

### 2.4 The live arms and their current uniqueness (the matrix)

17 live arms. **Bold = currently NODE-unique where the rule wants EDGE-unique
(latent silent under-count if a cyclic/parallel-edge corpus reaches it).**

| Arm (file:line) | Shape | Uniqueness now | Correct? |
|---|---|---|---|
| VLC:2388 std base / VLC:2697 std recursive | standard/poly/undir | EDGE (gated) | ✅ (#598/#606) |
| VLC:2228 zero-hop | `*0..N` | NODE | ⚠️ #628 (closed `*0..` needs EDGE) |
| VLC:2956 fk base / 3123 append / 3251 prepend | fk-edge | EDGE (gated) | ✅ (#712) |
| **VLC:3388 mixed base / 3533 mixed recursive** | mixed-access | **NODE only** | ⚠️ latent; #808 says corpus-unreachable |
| VLC:2044 hetero-poly | hetero-poly | NODE (global dedup) | ⚠️ latent; #808 corpus-unreachable |
| VLC:1451/1640/2605/2635 BFS+weighted | shortestPath/weighted | NODE | ✅ by design |
| MT:702 multi-type flat | multi-type | EDGE (pairwise) unless sp | ✅ (#807) but identity = `(from,to)` → **#806** |
| cte_extraction:8406 flat normal/poly/denorm | exact-bound | EDGE (pairwise) | ✅ but identity `(from,to)` → **#806** |
| **cte_extraction:8400 flat fk/multi** | exact-bound fk/multi | **NODE (start!=end, weak)** | ⚠️ known #598-residual |

`emit_cycle_check` (VLC:61, node) and `emit_edge_cycle_check` (VLC:72, edge)
are the two shared predicate emitters; the switch between them is at VLC:2805 /
3183 / 3315 (recursive), inline elsewhere.

---

## 3. Target shape (the canonical component)

Introduce **one** edge-identity/uniqueness authority, derived from
`PatternSchemaContext` (satisfying axis-dispatch §2.1 — no raw `is_fk_edge` /
`is_denormalized` / `type_column` branching in the arms), consumed by every
live arm and both flat paths:

```rust
/// The single source of truth for VLP relationship-uniqueness.
/// Constructed from PatternSchemaContext + query shape (min_hops, shortest_path,
/// undirected/doubled). No raw schema-flag branching downstream of this.
struct EdgeUniquenessPolicy {
    kind: UniquenessKind,        // Edge | Node  (Node for shortestPath/weighted/zero-hop base)
    identity: EdgeIdentity,      // how to spell "the same edge" for THIS schema shape
}

enum EdgeIdentity {
    /// schema edge_id (Single or Composite), else the (from,to) node pair
    EdgeIdColumns { cols: Identifier, from_col: String, to_col: String },
    /// FK-edge: ordered node-id pair is the identity
    FkNodePair { from_id: Identifier, to_id: Identifier },
    /// undirected doubled-edge: original-orientation columns (#617)
    OrigOrientation,   // wraps one of the above but reads __cg_orig_from/to
    None,              // node-unique arms
}

impl EdgeUniquenessPolicy {
    fn from_pattern(ctx: &PatternSchemaContext, shape: VlpShape) -> Self { ... }

    /// Recursive-CTE predicate: `NOT array_contains(vp.path_edges, <identity>)`
    /// or the node-unique fallback. Replaces the 3 inline switches + helpers.
    fn recursive_cycle_predicate(&self, rel_alias: &str) -> String { ... }

    /// Flat pairwise guard: `NOT ((r_i.id = r_j.id) AND ...)` over the FULL
    /// identity column set (fixes #806), or the node start!=end fallback.
    fn flat_pairwise_guard(&self, aliases: &[String]) -> Option<String> { ... }

    /// Whether this hop seeds/accumulates a path_edges column.
    fn projects_path_edges(&self) -> bool { matches!(self.kind, Edge) }
}
```

The recursive generator and the flat expander both ask the *same* policy object
for their predicate; `build_edge_tuple_base/recursive`, `build_fk_edge_tuple`,
`edge_identity_column`, `DenormalizedCteStrategy::edge_tuple`, and the flat
pairwise identity all become thin callers of `EdgeIdentity::spell(...)`.

This does **not** merge the arm *bodies* (property projection, join structure
differ legitimately per shape). It merges only the uniqueness *decision + identity
spelling*, which is the axis that keeps regressing.

---

## 4. Phased migration (one slice per PR, §2 protocol)

Every slice: byte-identical goldens + 319-of-1229 VLP corpus-sweep entries
(or a justified regenerated diff), `fmt`/`clippy`/`cargo test` gate,
worktree-isolated adversarial subagent review, `PRIORITIES.md` + this
checklist updated in the same PR.

### Phase 0 — delete the dead code (pure hygiene, 0 behavior change) → **DONE (#890, `1c3bce85`)**
**Shipped.** Deleted the test-only `CteStrategy` dispatch cluster: the enum + its
`generate_sql`/`validate` dispatch, `analyze_pattern` (sole builder of the 5
`Traditional / FkEdge / MixedAccess / EdgeToEdge / Coupled` variants and of
`CteStrategy::Denormalized`, with its only caller inside `#[cfg(test)]`), the
arg-taking `generate_cte`/`validate_strategy` (0 live callers), the 5 dead
strategy structs+impls + their `#[cfg(test)]` tests, `get_fk_edge_node_id_column`,
the orphaned `build_where_clause_from_filters`, the test-only
`CteGenerationContext::with_schema`, and the `render_plan::CteStrategy`
re-export. Deadness was **compiler-verified** (construction statically confined
to the test-only `analyze_pattern`, so the crate compiling after deletion proves
nothing live referenced it) — stronger than the tripwire originally planned.
Kept `DenormalizedCteStrategy` + `VariableLengthCteStrategy` (live via direct
construction) and every `VariableLengthCteGenerator` arm. **−2290 lines.**
Adversarial review APPROVE-0 (multiset line-diff: kept lines byte-identical);
`corpus_sweep` byte-identical with zero golden churn; ratchet `is_denormalized`
in CM 13→12 (baseline locked).

NOTE (corrects an earlier draft): the "3 dead VLC recursive arms" this section
first named were **already deleted in #860** (denorm + heterogeneous-polymorphic
recursive generators); the still-present `generate_mixed_*` arms are
corpus-empty **but reachable** (`cte_manager` `new_mixed`), so they were
deliberately KEPT — deleting them would silently change SQL. Phase 0 was
therefore the CM dispatch cluster only.

### Phase 1 — introduce `EdgeUniquenessPolicy`, transition-assert only (no switch) → §2.5
Land the type + `from_pattern`. At each of the ~14 sites, compute the policy's
predicate **alongside** the existing inline one and `debug_assert_eq!` them
(+ corpus sweep). This proves the canonical API reproduces every arm's current
string **before** anything is deleted. Intentional divergences (if any) surface
here as assert failures, not production bugs.

### Phase 2 — switch the recursive generator to the policy → refactor, byte-identical
Replace the VLC inline switches (2805/3183/3315) and the 4 identity helpers with
policy calls. Goldens byte-identical (asserts from Phase 1 guarantee it). Retires
the second `uses_edge_uniqueness` copy by making CM Denormalized consume the same
policy.

### Phase 3 — fix #806 (flat-path identity) → behavior change, goldens regenerated
Now that the flat pairwise guard reads `EdgeIdentity` (schema `edge_id`), the
`(from,to)`-only collapse is fixed by construction. Regenerate the (small) set
of goldens for parallel-edge schemas; live-verify the count against the trail
oracle (`scratchpad/vlp_oracle.py`, per #606).

### Phase 4 — fix #628 (`*0..N` closed cycle) → behavior change
With the policy owning "zero-hop base is node-tracked, hop≥1 is edge-unique,"
route the closed `*0..N` recursive arm to edge-uniqueness so real cycles survive
(`(a)-[:FOLLOWS*0..2]->(a)` → 14, not 8). Live-verify.

### Phase 5 (optional) — #710 parallel-edge denorm
Only if a real parallel-edge denorm schema is in scope. Needs a synthetic
per-row edge key threaded into `path_edges`. Low priority; documented residual.

**#808's mixed/hetero-poly "fix" is deliberately NOT a phase**: they are
corpus-unreachable (Phase 0 either deletes them or, if a reachable shape later
appears, the policy already has the correct answer to apply).

---

## 5. The byte-identity spike (must precede Phase 2, per §2.5 / PATH_C precedent)

Before deleting any inline derivation:

1. Land Phase 1's `debug_assert_eq!` scaffolding.
2. Run `cargo test` + the corpus sweep (319 VLP entries) in a debug build.
3. Green sweep = the canonical predicate is byte-identical at every live site.
   Only then does Phase 2 delete the inline copies.

A red assert is the *point* — it means an arm encoded a divergence the naive
policy missed; investigate and encode it into `from_pattern` (or prove it a
latent bug and file separately) before switching.

---

## 6. Success metric (makes "is the loop broken?" measurable)

Ratchet baseline today, the 3 VLP files (`tests/rust/ratchet/baseline.txt`):
**148 axis-predicate occurrences** (`is_denormalized`×41 VLC + ×13 CM,
`to_label_column`×17, `from_label_column`×12, `type_column`×11, `is_fk_edge`×7,
…). Target after Phase 2: the uniqueness-axis contribution routes through
`EdgeUniquenessPolicy` (constructed once from `PatternSchemaContext`), and the
baseline **decreases** — the ratchet auto-ratchets down in the same PRs.

Issue metric: #606, #628, #710, #806, #808 all close or convert to a single
documented residual (#710). No *new* uniqueness residual can be filed against an
arm the policy owns, because there is one arm.

---

## 7. Explicitly out of scope (different subsystems — do NOT fold in)

These share the word "VLP" but not the root. Folding them into this cluster
would repeat the mistake this doc exists to fix. Verified subsystems:

- **#643** — chained-OPTIONAL-VLP endpoint→alias resolution.
  `render_plan/vlp_rewrite.rs` (`rewrite_vlp_union_branch_aliases` /
  `vlp_from_alias`). An alias-map bug, not a uniqueness bug.
- **#840** — undirected-VLP + shortestPath reverse-arm join drop.
  `to_sql_query.rs::rewrite_cte_body_vlp_refs` (UNION-arm join clone). A
  join-propagation bug.
- **#627** — composite-id adjacent VLP loud-gate.
  `query_planner/analyzer/graph_join/inference.rs:3540`. Composite-key CTE
  composition. **Adjacent**: the unified `EdgeIdentity` (composite-aware)
  *enables* the eventual fix, so this becomes a natural follow-on **after** this
  cluster, not part of it.
- **#683** residual 1 — `__denorm_scan` anchor projection; residual 2 —
  composite-VLP malformed SQL. Anchor-projection + composite families.

---

## 8. Non-goals

- Not the `CteManager` facade rewrite (`CTE_MANAGER_DESIGN.md`) — this is the
  uniqueness axis only, one bounded consolidation.
- Not merging arm *bodies* (property projection / join structure stay per-shape).
- Not touching shortestPath/weighted node-uniqueness (correct by design).
- Not changing any query's result **except** the three intended behavior slices
  (#806 Phase 3, #628 Phase 4, and optionally #710 Phase 5), each with
  regenerated goldens + live oracle verification.

---

## 9. When to pick this up (trigger — honest scoping)

This is a **design-cycle commitment**, not a one-slice bug fix — Phases 0–2 are
~4 PRs of pure refactor before the first behavior fix. Pick it up when:

- there is appetite for a multi-PR refactor arc (the bounded silent-wrong
  single-slice vein is currently mined out — see 2026-08 loop notes), **or**
- a new corpus/schema makes one of the latent NODE-unique arms (#808 mixed /
  hetero-poly) reachable, converting Phase 2 from refactor to bug fix.

Phase 0 (dead-code deletion) is unblocked and shippable *today* independent of
the rest — it is the recommended first slice.
