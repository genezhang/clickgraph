# WITH export contract: one answer to "which columns does this WITH CTE expose?"

Status: **plan**, with slice S0 (this doc) and S1 (the ratchet) planned next.
Grounded in `main` at `764fcd7f` (2026-10-05). Line numbers drift, so
re-verify each one before editing.

Owner queue entry: `PRIORITIES.md` **P-4b**. Sibling of
`FORWARD_RESOLUTION_PLAN.md` (P-4). That plan covers how `alias.property`
resolves in SELECT, WHERE, GROUP BY and ORDER BY after a barrier. **This one
covers the identity and join-key columns, the part P-4 never reached.**

---

## 0. TL;DR

An audit of the WITH-processing bug family (about 100 closed issues with
"WITH" in the title, 11 open, and 116 merged PRs, 67 of which touched
`with_to_cte/mod.rs` or `plan_builder_utils.rs`) found three structural
roots:

| Root | What goes wrong | Open issues |
|---|---|---|
| **A. No export contract** | The CTE's columns are only known after its body renders. Every later consumer *reconstructs* the identity / join-key column from a naming convention, and the conventions disagree. | #1189, #1160, #1149, #1263, #933, #1106; unfiled: FK-side-owns-FK, `WITH a.user_id AS id, a` key |
| **B. Post-WITH scope repaired after the fact** | The scope after a WITH is joined as if the carried node were its base table. About a dozen render passes then re-derive the FROM and the CTE↔pattern join from surface clues (`with_`/`vlp_` prefixes, WHERE equalities, uniqueness predicates). | #1190, #1089, #1177, #1184.2, #933, #1283 |
| **C. Union-body asymmetry** | A union CTE body is `plan` (arm 0) + `union.input` (the rest), and code treats one part as the whole. | #1149, #1263 residue |

**This doc plans root A.** B depends on A: no repair pass can be deleted
while the join key it would need is still a guess. B gets its own doc once A
has landed. C is independent; see §7.

The fix for A is a single **export contract** per WITH CTE. It records, for
every exported Cypher name, its kind, labels, and the **CTE columns that hold
its identity** (composite-aware). It is derived once, from the CTE's *emitted*
select, at the point the CTE is built. Every reader asks it instead of
guessing. A reader that finds no identity fails loudly; it never falls back
to a convention.

---

## 1. Evidence

### 1.1 Frozen invalid SQL in the corpus

Scope-aware scan of all 1,342 `*.clickhouse.sql` corpus goldens. For each
`with_* AS <alias>` binding, every `<alias>.<col>` reference in the same SQL
scope must name a column that CTE's SELECT exports. **5 goldens violate
this**, so each would fail with ClickHouse Code 47:

| Golden | Reference | Bug |
|---|---|---|
| `denormalized_flights/test_1188__carry_both_then_vlp_den` | `c.code` (CTE has `p1_c_code`) | denorm id spelled as the bare Cypher property |
| `denormalized_flights/test_1182__outside_carried_node_mid_chain_not_repaired_den` | `c.p1_c_start_id` | #1189 |
| `denormalized_flights/test_1182__outside_closed_on_carried_node_not_repaired_den` | `c.p1_c_end_id` | #1189 |
| `denormalized_flights/test_1189__two_carried_path_between_them_not_repaired_den` | `c_z.p1_c_end_id` | #1189 |
| `standard/test_636_shared_anchor_comma_interleaved_stays_loud` | `p.post_id` | #933 |

All five are identity columns that were guessed rather than looked up.

### 1.2 Live probes on the five layouts (2026-10-05)

The shape is "carry a node through WITH, then hop off it".

| Layout | `WITH c MATCH (c)-[:R]->(a) RETURN count(*)` | `... WITH c, a RETURN count(*)` |
|---|---|---|
| standard | 35 = oracle | 35 |
| polymorphic | 3 = oracle | 3 |
| composite id | **Code 47 `c.id`** (single-column guess for a 2-column id) | 8 = oracle |
| FK-edge, CTE side owns the FK | **refused** ("no join key could be resolved"); the property form is Code 47 `d.customer_id` with the edge dropped | 8 = oracle |
| denormalized | 8 = oracle | **36, silent** (#1283: hop tie dropped, `CROSS JOIN`) |

The same identity is spelled differently by different CTE shapes. On
`group_membership_simple.yaml` (Cypher `id` → column `user_id`):
`WITH u` exports `p1_u_user_id`, named after the **DB column**, while
`WITH u, count(g) AS n` exports `p1_u_id`, named after the **Cypher
property**. Both happen to work today only because each reader is paired
with the producer it was written against.

### 1.3 Who decides "the identity column of carried alias `x`" today

Nine places, five conventions:

| # | Site | Convention |
|---|---|---|
| R1 | `PlanCtx::register_cte_columns` / `get_cte_column` (`plan_ctx/mod.rs:883,956`), read by `graph_join/helpers.rs::resolve_column` | `{alias}_{prop}`, which is **not** the real `p{N}_{alias}_{prop}` format. Records only `alias.prop` items (so `WITH a` records nothing) and is keyed by property alone, so two aliases in one CTE collide. |
| R2 | `join_builder.rs` start/end id of a CTE-referenced endpoint (~3655–3700) | the label's table → `table_to_id_column` (**first** id column only, `"id"` on error). Emits the raw column and relies on a later rewrite. |
| R3 | `join_builder.rs::cte_safe_identifier` (730; used at 4459, 4524) | `cte_column_name(alias, <DB column>)` |
| R4 | `cte_rewrite::rewrite_operator_application_for_cte` (join_builder 1652) | `cte_column_name(alias, col)`, unconditionally |
| R5 | `cte_rewrite::rewrite_join_conditions_for_cte_aliases` (398) | `CteSchemaMetadata.property_mapping[(alias, col)]`, mixing DB-column and Cypher-property keys (the "also add DB column" entries) |
| R6 | `plan_builder_utils::compute_cte_id_column_for_alias` (2758), producer of `alias_to_id` | Cypher id property for denorm, DB column otherwise; never checked against the select |
| R7 | `plan_builder_utils::find_id_column_in_cte` (332; callers `with_to_cte` 3463, 3466, 4378, 4385) | `{alias}_user_id` / `{alias}_id`, then any `_id` suffix, then **`format!("{alias}_user_id")`**: the benchmark schema's column name, hard-coded |
| R8 | `generate_vlp_with_cte_join_conditions` / the union-branch block in `resolve_cross_table_with_cte_joins` (3407, ~4350) | `alias_to_id`, else a select item with an `_id` suffix, else R7 |
| R9 | `VariableScope::resolve_generic_id_in_cte` (`variable_scope.rs:299`) | Labels → `node_id.column_or_error()` (single-column only) → Cypher property → `property_mapping`. **The closest to correct**: it looks the column up in data derived from the emitted select. |

`CteSchemaMetadata` (`render_plan/mod.rs:47`) also has **four** construction
sites in `with_to_cte` (the main WITH CTE, nested-CTE extraction, the
`__union_vlp` pseudo-CTE, and the VLP-FROM registration). Each fills
`alias_to_id` differently.

---

## 2. The contract

```rust
/// What one exported Cypher name is, inside one WITH CTE.
pub struct CteExport {
    pub kind: CteExportKind,      // Node | Relationship | Value
    pub labels: Vec<String>,      // node labels / rel types; empty for Value
    /// CTE column(s) holding the identity, in `node_id` column order.
    /// `None` = this CTE does not expose an identity for the name
    /// (a Value, or a node whose id was not projected). Readers that need
    /// one must fail loudly.
    pub identity: Option<Vec<String>>,
}
```

- **Where it lives.** A new `identity` (plus `kind`) on
  `variable_scope::CteVariableInfo`, populated in
  `WithBarrierScope::publish_alias` (`with_to_cte/mod.rs:~2605`). That
  function already receives the labels and the Cypher-property → CTE-column
  map for exactly this alias, after the CTE body is built. Mirrored into
  `CteSchemaMetadata` for the readers that only see `cte_schemas`. Published
  to the task-local `QueryContext` under the CTE's name, for readers that see
  neither (`join_builder`, which renders the *next* scope). Its lifetime is
  scoped by the existing `CteScopeGenerationGuard`.
- **Derivation (ground truth, no conventions).** Take the node schema of the
  label and map each `node_id` DB column to the Cypher property that maps to
  it. For denormalized nodes, the `node_id` names the Cypher property
  directly. Then look that property up in **the alias's
  property→CTE-column map, which is built from the emitted SELECT**. Every
  column must be present, or `identity = None`. Multi-label or unlabeled:
  `None` unless every label agrees.
- **Kind.** `Value` when the per-alias map is empty (the existing
  scalar-vs-node test in `publish_alias`, F1). A `Value` never has an
  identity, which closes the #1263 / #1225 "scalar treated as node" class at
  the source.
- **Pruning.** `plan_optimizer::prune_cte_columns` runs after the contract is
  built. A reader that emits `alias.<identity col>` creates a reference, so
  the pruner keeps the column. The S1 ratchet runs on the final SQL, so a
  violation of this would be caught.
- **Out of scope for v1.** Columns the *next* scope needs that are not the
  identity, such as an FK column for "CTE side owns the FK" (#1279 residue).
  The contract makes that a well-defined extension (`exports_fk: ...`) rather
  than a guess. It is planned as S6.

---

## 3. Slices (one PR each, standard gate + corpus sweep)

| Slice | Content | Expected corpus delta |
|---|---|---|
| **S0** | This doc + `PRIORITIES.md` P-4b. | none (docs) |
| **S1** | **Ratchet.** Test-only `with_cte_column_refs` check over every corpus ClickHouse golden (scope-aware: `with_* AS a` binding → `a.<col>` refs in the same scope must be exported). The violation set must equal an allowlist of the 5 in §1.1, each tagged with its issue. A new violation fails the test; fixing one forces removal from the allowlist. No production change. | none |
| **S2** | **Contract data.** Add `kind`/`identity` and populate them in `publish_alias`. Mirror to `CteSchemaMetadata` and the task-local registry. Transition-assert (debug builds, env-gated so a live debug server is not killed): every legacy answer (R6 `alias_to_id`, R7, R8, R9) vs the contract over the whole corpus, with the disagreements listed in this doc. | none |
| **S3** | **Readers R7/R8 → contract** (`find_id_column_in_cte` deleted, VLP-join branch block). Targets #1189: 4 of the 5 allowlist entries. | denorm goldens for #1189 (intended) |
| **S4** | **Readers R2/R3/R4 → contract**: `join_builder` emits the CTE identity column directly for a CTE-backed endpoint, composite-aware. Targets composite `c.id` and the FK-edge refusals in §1.2, and removes the join-condition dependency on R5's later rewrite. | composite / FK-edge goldens (intended) |
| **S5** | **Producer cleanup**: `alias_to_id` derived from the contract (R6 deleted); R1 deleted or documented as plan-time-only (audit `cte_column_resolver.rs` callers first). | byte-identical |
| **S6** | **Extension**: non-identity join columns (FK side), #1279 residue. | FK-edge goldens (intended) |

Each reader-switch slice must (1) shrink the S1 allowlist or stay
byte-identical, (2) run a live generator sweep over the 5 layouts × {carry
through 1 or 2 WITHs} × {hop out, hop in, VLP, OPTIONAL}, comparing
main-vs-PR-vs-oracle (`WITH count(*)` ≡ plain `count(*)`), and drive
**ERR→WRONG** and **OK→anything** to zero, and (3) mutation-check the
switched reader.

## 4. Non-goals and guard rails

- No change to CTE column *naming* (`p{N}_{alias}_{prop}` stays, and so does
  the DB-vs-Cypher split in §1.2). The contract records what is emitted; it
  does not re-spell it. Unifying the spelling is a later, byte-visible slice,
  and only worth doing if a reader still needs it after S5.
- A reader that gets `identity = None` returns a `RenderBuildError`. It never
  falls back to a convention: those fallbacks are where every R-site bug
  lived (memory: "fallback to a proven-wrong spelling").
- Root B's passes are **not** removed here. S4 only makes the key they need
  correct. The deletion plan is the follow-up doc.

## 5. Checklist

- [ ] S0 doc + P-4b
- [ ] S1 ratchet (allowlist = 5)
- [ ] S2 contract + transition-assert, disagreements recorded here
- [ ] S3 R7/R8 (#1189)
- [ ] S4 R2/R3/R4 (composite `c.id`, FK-edge refusals)
- [ ] S5 R6/R1 cleanup
- [ ] S6 FK-side join columns (#1279 residue)

## 6. Relationship to other plans

- `FORWARD_RESOLUTION_PLAN.md` (P-4): its M1 registry and M3 `VariableScope`
  carry the same `property_mapping` the contract is derived from. The
  contract is built in the same function (`publish_alias`) on purpose, so the
  two cannot diverge.
- `VLP_ENDPOINT_RESOLUTION.md` / #989: VLP CTEs (`vlp_*`, `RawSql`) are not
  WITH CTEs and are not covered. The `__union_vlp` pseudo-CTE stays out of
  the contract.

## 7. Root C note (independent)

A union CTE body keeps arm 0 in the `RenderPlan` itself and the other arms in
`union.input`. There are about 250 `union.0` / `union.input` access sites
across `with_to_cte`, `plan_builder`, `plan_optimizer` and `to_sql_query`.
#1261, #1263, #1267, #1269 and #1281 were each one site treating one part as
the whole. Candidate fix: an `arms()` / `arms_mut()` accessor that yields
every arm, adopted site by site, or making "shell plan + all arms in
`union.input`" an invariant (#1281's fix does this locally in
`build_cypher_union_render`). It is not sequenced here.
