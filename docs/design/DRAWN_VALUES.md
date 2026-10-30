# Spec: Refusing re-reads of CTEs that draw values (#1495)

Status: DRAFT v1 — committed with its implementation (one slice; the spec ships
in the PR that closes it, like every wave-1 slice of OPEN_ISSUE_PLAN.md)
Owner: hari2353
Grounding: codebase at `55112883`; issue #1495; `OPEN_ISSUE_PLAN.md` (wave 1)
and `PRIORITIES.md` §1 (rules) / §P-4d (dispatch); the #1395 stable-lowering
machinery (`bound_plan/lower/unwind.rs`: `rereads`, `changed_ctes`,
`word_count`); `bound_plan/write.rs:178-193` (the write-side refusal of drawn
calls, the AST-level precedent). Facts below locked by empirical probes on
`schemas/test/social_integration.yaml` at `55112883`.

---

## Objective

ClickHouse evaluates a CTE once per read. A value drawn by a nondeterministic
call (`rand()`, `randomUUID()`, the clock) in a CTE read more than once is
drawn again on each read. When an OPTIONAL MATCH's drive reads the value, the
drive exports it and the final SELECT joins back on it —
`ON w1.v1__user_id = o3.v1__user_id AND (w1.v2 = o3.v2 OR ...)` — so the reads
of the two draws never match (`count(b)` collapses to 0; Neo4j 5.26 and Spark
answer the joined value). The same failure reaches the row set when the drawn
call filters the CTE (`WHERE rand() < 0.5`): each read of the CTE keeps a
different half of the rows.

In SQL alone there is no way to make two reads of a drawn value agree, short
of evaluating the CTE once: `MATERIALIZED` CTEs exist only on ClickHouse
26.3+ (`enable_materialized_cte`).

**Decision (issue candidate a): refuse loudly** when a drawn value can decide
the result across two reads. Candidate b — materializing the draw with
supported-server-version gate — stays deferred: refusals must not depend on a
server version, and a wrong answer must never pass silently. Wiki note
follows the slice in the PR that closes the issue.

**Success looks like:** the issue's repro (and its WHERE-filter sibling)
raise a clear `Unsupported` naming the drawing call family and the re-read;
every query that is correct today lowers byte-identically (goldens untouched).

## Facts (locked by probes, dialect ClickHouse; Databricks differs per token)

| # | Shape (on `social_integration.yaml`) | SQL fact | Verdict |
|---|---|---|---|
| 1 | `WITH a, rand() AS r` + OPTIONAL drive reading `r` | drawn column is a final join key: `w1.v2 = o3.v2` | **broken** — refuse |
| 2 | `WITH a` (no draw), same OPTIONAL | identity-only join | correct |
| 3 | draw carried, unread by the drive | drive drops the column; identity-only join | **correct today** — allow |
| 4 | draw read by an inner (non-optional) MATCH once | `with_w1` named once: one external draw | correct — allow |
| 5 | `RETURN sum(r)` over a re-read CTE | `coalesce(sum(w1.v2), 0)`, no `=` | correct — allow |
| 6 | `MATCH (a) WHERE rand() < 0.5` + same OPTIONAL | draw sits in `with_w1`'s WHERE; re-read rows differ | **broken** — refuse |
| 7 | `WITH a WHERE rand() < 0.5` + same OPTIONAL | as #6 | **broken** — refuse |
| 8 | `WITH a, randomUUID() AS u` carried unread | renders **`randomUUID()` verbatim** (unmapped pass-through — Spark has `uuid()`, ClickHouse `generateUUIDv4()`; a separate live-tier gap, filed with the PR) | correct — allow |
| 9 | `WITH a, timestamp() AS t` carried, `count(t)` | renders `toUnixTimestamp(now())` — a nested clock call | correct — allow |
| 10 | `WITH a, date() AS d`, drive filters `d < date(...)` | renders `today()`; same day on both reads → the join agrees | correct — allow (day-granular draws are excluded by design) |

Drawn SQL token spellings verified in rendering: ClickHouse `randCanonical`,
`randomUUID` (verbatim), `now64`, `now` (also nested, as in fact 9),
`today`; Databricks `rand`, `current_timestamp`, `current_date`. Whole-word
matching keeps `rand` from matching `randCanonical` (the ident-boundary `c`).

## Design

One post-pass on the rendered chosen plan (`RenderPlan`), next to the #1395
machinery it reuses. **Single responsibility** (one refusal, one concern —
drawn values deciding across reads), **open for extension** (the token const)
and closed for change (no rewrites of lowering), no new trait to implement.

`reject_reads_of_drawn_values(plan: &RenderPlan) -> Result<(), LowerError>`:

1. Render the plan once (`render_plan_parts_plain`). A CTE *draws* when its
   text contains a drawn call: a drawn token as a whole word followed by `(`,
   outside every quoted string (queries that search for the text `now` / a
   drawn name in a literal must not refuse).
2. Reuse the `rereads` dictate verbatim: each drawn CTE reads the SELECT or
   other CTEs more than once (`those_reread(ctes, select, &{drawn names})`,
   on the parts already rendered in step 1 — the plan renders exactly once).
   With no re-read the recycles stay impossible and we return allow.
3. Refuse, per drawn CTE — each carries its own fate (its drawn items'
   aliases, whether a drawn call decides its rows, its call families),
   never a pool over all CTEs: a read-once CTE's drawn filter must not
   refuse a re-read one, and the blame must be the guilty CTE's own:
   - **joined back** — a drawn item's column alias (the text after the first
     ` AS "` following a drawn call, case-folded like the select text it is
     matched against) is a join key of the final SELECT (`.alias = `
     precedes: the left operand, as the join builder spells keys, literals
     included);
   - **row-set drawing** — a drawn call is not an item's aliased expression
     (no ` AS "` follows its call before the CTE text ends): it filters,
     orders or groups the CTE's rows, so each read keeps a different set;
   - the `Unsupported` names the re-read CTE (`WITH {name}`) and the blamed
     call family, as the criteria's "naming" asks.
4. Tokens (lowercase whole-word): `randcanonical`, `rand`, `randomuuid`,
   `now64`, `now`, `current_timestamp`. Day-granular `today` / `current_date`
   stay out: within one query execution the reads agree, so joins on them are
   correct, and refusing correct queries is a false positive.

The pass is called once in `lower_over` on the plan that is returned (its
re-read selection is the same regardless of which pass lowered it: drawn
spellings do not change with stability). Banned/deferred on purpose: no change
to `write.rs` (that refusal is Cypher-level list: all draws there are
already refused), no legacy-path edits, no SQL-emitter change.

Backward compat: only queries that are wrong today refuse; the corpus is
touched nowhere (asserted byte-identical by the ratchet). Future compat: a
new dialect mapping of a nullary drawn call adds one token to the const —
the spelling-tie guard test resolves the mapping and fails at that change
if the token is missing, so the coupling cannot drift silently; candidate
(b) materialization, when the server floor reaches
ClickHouse 26.3+, replaces refusals with correct answers — the same
tests flip from `.unwrap_err()` to render assertions.

## Testing

- Refusals: the issue's repro; a drawn value set down a WHERE filter of the
  re-read CTE (MATCH-WHERE and WITH-WHERE spellings); the same over Databricks
  (`databricks_sql`, tokens `rand` and the clock drawn and joined back). Each
  refusal's names are pinned: the re-read CTE (`WITH with_w1`) and the blamed
  call family — the clock's repro blames the clock family, not `rand()`.
- Allows: benign (no draw), carried unread (#3), read once (#4), summed only
  (#5), verbatim `randomUUID` carried (#8), clock carried (#9), a day-granular
  draw joined back (#10 — pinned NOT refused, the deliberate exclusion), and
  a quoted literal spelling a drawn name (`'r now() rand()'`) — the whole-word
  scan reads the quotes first.
- Spelling tie: every dialect spelling the registry resolves for a drawn
  Cypher call (`rand`, `randomUUID`) is probed through the scan itself
  (`<spelling>()` outside quoted literals), and an unmapped drawn call is
  probed verbatim — a mapping change without its token fails the guard.
  The clock's spellings are authored inside arg transforms, not mapping
  names, and are pinned behaviorally by the refusals above.
- Goldens: `cargo test --no-default-features --test ratchet` and
  `integration the_corpus_lowers` byte-identical.
- Gate: `cargo fmt --check`, `clippy -D warnings`, `cargo test
  --no-default-features` (lib suite green: 1482 passed / 4 ignored, the
  battery included).
- Live tier (no local ClickHouse; CI's `build` runs the offline battery and
  audit / deny — no live repro check exists in CI): disclosed in the PR body;
  Neo4j oracle corpus unchanged.

## Boundaries

- The join-key check reads rendered text, so an inline subquery spelling an
  ` AS "` between a drawn call and its item's alias would hide a refusal.
  The plain renderer emits none today (CTEs are flat; predicates are flat
  comparisons); if one appears, the missed case stays no worse than today and
  the corpus/oracle should surface it.
- The detector refuses the drawn column as a join key of the *final* SELECT.
  A join-back inside an intermediate CTE's own ON is not seen; the probes
  found the machinery routes every such unification through the final SELECT
  (`optional_d2`/`optional_o3` shape of #1395).
- `ORDER BY rand()` / a drawn GROUP BY are refused via the row-set clause
  (they are draws that decide rows); a drawn ORDER BY without a later LIMIT
  that the machinery numbers is governed by #1395 and the stable pass first.
- Seeded draws are unreachable from Cypher: `rand` is arity 0 (`rand(1)` is
  refused by function arity, #1413), so SQL `rand(<seed>)` cannot appear.

## Success criteria

1. The issue's repro raises `Unsupported` naming the drawn call family and
   the re-read CTE, on ClickHouse and on Databricks, offline.
2. The WHERE-filter siblings raise the refusal naming different rows.
3. Every allow case above lowers green with bytes unchanged.
4. Ratchet + corpus goldens byte-identical; lib suite green; fmt/clippy clean.
5. `PRIORITIES.md` §2/§4 updated in the same PR (§2 stands: the next queue
   rows are other issues' — this slice is a P-1 follow-up, no reorder owed);
   the `OPEN_ISSUE_PLAN.md` wave table is the plan branch's
   (`plan/open-issues-2026-10`, kept outside this tree on purpose) and is
   updated there at merge; the residual `randomUUID` mapping gap filed as
   its own issue (#1540).
6. The registry's mapped spellings and the detector's tokens cannot drift:
   the spelling-tie guard fails on a drawn call's mapping change without
   its token, and the tolerance the clock's transform-authored spellings
   get is stated in the spec itself (behaviorally pinned by the battery).
