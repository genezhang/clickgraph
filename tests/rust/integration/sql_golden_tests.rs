//! Golden SQL snapshots — Phase 0 of the dialect-neutral SQL rendering refactor.
//!
//! These lock the *current* `RenderPlan -> SQL` output for BOTH dialects
//! (ClickHouse + Databricks) over a representative query corpus. They are the
//! regression net for the refactor: every later phase must keep the ClickHouse
//! goldens **byte-identical**, and any intended Databricks change shows up as a
//! reviewable golden diff rather than silently.
//!
//! Goldens live in `tests/rust/integration/golden/sql_ir/{schema}/{name}__{dialect}.sql`,
//! where `{schema}` is the schema-variation subdirectory (`standard`, `fk_edge`,
//! …). The `standard` set (loaded from `social_benchmark.yaml`) is the original
//! 44-case corpus; additional variations lock the SAME feature axes against
//! other schema-pattern shapes so a refactor of the shared render paths proves
//! no-op-ness across variations, not just the standard schema (see
//! `docs/design/REFACTORING_SAFETY_PLAN.md` §3.1). Regenerate after an
//! *intended* change with:
//!
//! ```text
//! UPDATE_GOLDEN=1 cargo test -p clickgraph --test integration sql_golden -- --nocapture
//! ```
//!
//! No ClickHouse connection is needed — this is SQL generation only.

use std::sync::Arc;

use clickgraph::{
    graph_catalog::{config::GraphSchemaConfig, graph_schema::GraphSchema},
    open_cypher_parser::{parse_cypher_statement, strip_comments},
    query_planner::evaluate_read_statement,
    render_plan::{logical_plan_to_render_plan_with_ctx, ToSql},
    server::query_context::{set_current_schema, with_query_context, QueryContext},
    sql_generator::SqlDialect,
};

/// A schema variation. Each variation loads its own YAML and its goldens live
/// under `golden/sql_ir/{dir}/`. The corpus is intentionally NOT portable
/// across variations (labels/properties differ), so each schema carries its own
/// case list mirroring the standard set's feature axes.
#[derive(Clone, Copy)]
enum SchemaId {
    Standard,
    FkEdge,
    Denormalized,
    CompositeId,
    Polymorphic,
}

impl SchemaId {
    /// Subdirectory under `golden/sql_ir/` holding this variation's goldens.
    fn dir(self) -> &'static str {
        match self {
            SchemaId::Standard => "standard",
            SchemaId::FkEdge => "fk_edge",
            SchemaId::Denormalized => "denormalized",
            SchemaId::CompositeId => "composite_id",
            SchemaId::Polymorphic => "polymorphic",
        }
    }

    /// YAML schema file loaded for this variation.
    fn yaml_path(self) -> &'static str {
        match self {
            SchemaId::Standard => "benchmarks/social_network/schemas/social_benchmark.yaml",
            SchemaId::FkEdge => "schemas/test/fk_edge.yaml",
            // Coupled-denormalized single-graph schema: the `flights_denorm` table
            // IS the FLIGHT edge AND the source of Airport node properties (Airport
            // is a virtual node — `node_id: code` maps to origin_code/dest_code via
            // from_node_properties/to_node_properties). Has matching live data in
            // `db_denormalized` (scripts/setup/setup_denormalized_data.sh).
            SchemaId::Denormalized => "schemas/dev/flights_denormalized.yaml",
            SchemaId::CompositeId => "schemas/test/composite_node_ids.yaml",
            SchemaId::Polymorphic => "schemas/test/social_polymorphic.yaml",
        }
    }
}

/// Representative corpus exercising the RenderPlan -> SQL surface. Chosen to
/// render cleanly on BOTH dialects (no UNWIND/arrayJoin or array_count, which
/// hit not-yet-implemented Spark structural gaps). Add cases as coverage grows.
const CORPUS: &[(&str, &str)] = &[
    ("simple_match", "MATCH (u:User) RETURN u.name"),
    ("project_multi", "MATCH (u:User) RETURN u.user_id, u.name, u.country"),
    ("distinct", "MATCH (u:User) RETURN DISTINCT u.country"),
    (
        "where_comparison",
        "MATCH (u:User) WHERE u.country = 'US' RETURN u.name",
    ),
    (
        "where_and",
        "MATCH (u:User) WHERE u.country = 'US' AND u.is_active = true RETURN u.name",
    ),
    (
        "where_contains",
        "MATCH (u:User) WHERE u.name CONTAINS 'a' RETURN u.user_id",
    ),
    (
        "where_in_list",
        "MATCH (u:User) WHERE u.country IN ['US', 'UK'] RETURN u.name",
    ),
    ("in_empty", "MATCH (u:User) WHERE u.country IN [] RETURN u.name"),
    (
        "order_skip_limit",
        "MATCH (u:User) RETURN u.name ORDER BY u.name DESC SKIP 5 LIMIT 10",
    ),
    // SKIP without LIMIT: CH needs a huge upper bound; Spark uses bare OFFSET.
    (
        "skip_only",
        "MATCH (u:User) RETURN u.name ORDER BY u.name SKIP 3",
    ),
    // SKIP/LIMIT inside a WITH -> drives the CTE-body LIMIT emission path.
    (
        "with_skip_limit",
        "MATCH (u:User) WITH u.name AS n ORDER BY n SKIP 2 LIMIT 5 RETURN n",
    ),
    ("aggregate_count", "MATCH (u:User) RETURN count(u)"),
    (
        "aggregate_group_collect",
        "MATCH (u:User) RETURN u.country, collect(u.name) AS names",
    ),
    (
        "string_fns",
        "MATCH (u:User) RETURN toUpper(u.name) AS up, toLower(u.country) AS lo",
    ),
    (
        "case_expr",
        "MATCH (u:User) RETURN CASE WHEN u.is_active = true THEN 'active' ELSE 'inactive' END AS status",
    ),
    (
        "single_hop",
        "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name",
    ),
    (
        "cross_node_hop",
        "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, p.title",
    ),
    // #492 review B2: mixed-TYPE 2-hop with an undirected hop. AUTHORED and
    // LIKED share id column names ([user_id, post_id]) but are DIFFERENT
    // relationship types — the uniqueness guard must NOT pair them (a
    // cross-type guard silently excluded every author-liked-own-post match).
    (
        "mixed_type_2hop_undirected",
        "MATCH (u:User)-[:AUTHORED]-(p:Post)<-[:LIKED]-(v:User) RETURN u.name, p.title, v.name",
    ),
    // #492 review RN4: undirected 2-hop with the MIDDLE node unreferenced.
    // Locks that the Incoming-swapped branches keep valid joins — the parent
    // plan's bridge-node elimination must not clobber branch-defined aliases
    // (tautologies like `ON t1.followed_id = t1.followed_id` inflated results).
    (
        "partial_ref_undirected_2hop",
        "MATCH (a:User)-[:FOLLOWS]-(b)-[:FOLLOWS]-(c:User) RETURN a.name, c.name",
    ),
    // #492 review round 3, finding 2 (B3 scope-tightening): a required
    // undirected multi-hop must split fully (4 branches) even when an
    // UNRELATED undirected OPTIONAL clause sharing the same anchor alias is
    // also present. The OPTIONAL edge's `left` subtree structurally IS the
    // required chain (shared anchor 'a'), but the required chain's own hops
    // are not `is_optional` — the B3 gate must not fire for them.
    (
        "required_split_despite_unrelated_optional",
        "MATCH (a:User)-[:FOLLOWS]-(b)-[:FOLLOWS]-(c:User) \
         OPTIONAL MATCH (a)-[:AUTHORED]-(p:Post) \
         RETURN a.name, c.name, p.title",
    ),
    (
        "with_having",
        "MATCH (u:User) WITH u.country AS c, count(u) AS n WHERE n > 5 RETURN c, n",
    ),
    (
        "vlp_recursive",
        "MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN b.user_id",
    ),
    ("whole_entity", "MATCH (u:User) RETURN u"),
    // List slicing -> arraySlice (CH) / slice (Spark). Both the 3-arg bounded
    // form and the 2-arg open-ended form (Spark needs a computed length).
    // Cypher ranges are HALF-OPEN: [1..3] yields indices 1,2 (2 elements), so
    // the rendered length is `to - from`, not `to - from + 1`.
    (
        "list_slice_bounded",
        "MATCH (u:User) RETURN [10, 20, 30, 40][1..3] AS s",
    ),
    (
        "list_slice_open",
        "MATCH (u:User) RETURN [10, 20, 30, 40][1..] AS s",
    ),
    (
        "list_slice_to",
        "MATCH (u:User) RETURN [10, 20, 30, 40][..2] AS s",
    ),
    // from > to is valid Cypher and must yield []. The length must floor at 0:
    // a negative length is silently wrong on CH arraySlice and errors on Spark slice.
    (
        "list_slice_empty",
        "MATCH (u:User) RETURN [10, 20, 30, 40][3..1] AS s",
    ),
    // tail() -> CH arraySlice(list, 2) / Spark slice(list, 2, greatest(size-1, 0))
    ("list_tail", "MATCH (u:User) RETURN tail([10, 20, 30]) AS t"),
    // Interval arithmetic on an epoch-millis column -> CH
    // `toUnixTimestamp64Milli(fromUnixTimestamp64Milli(x) + toIntervalDay(n))`
    // vs Spark `unix_millis(timestamp_millis(x) + make_dt_interval(n,0,0,0))`.
    // Day-time, year-month, and same-family multi-unit are the validated cases
    // (verified live on Databricks SQL). Mixing year-month with day-time in one
    // duration() is unsupported on Spark and intentionally not in the corpus.
    (
        "interval_add_days",
        "MATCH (u:User) RETURN u.registration_date + duration({days: 7}) AS d",
    ),
    (
        "interval_sub_month",
        "MATCH (u:User) RETURN u.registration_date - duration({months: 1}) AS d",
    ),
    (
        "interval_multi_same_family",
        "MATCH (u:User) RETURN u.registration_date + duration({days: 5, hours: 2}) AS d",
    ),
    // Heterogeneous end type (User|Post) routes through multi_type_vlp_joins,
    // locking the generator output for both dialects (incl. dialect-aware
    // array/string casts: CH `toString(..)`/`['x']` vs Spark `string(..)`/
    // `array('x')`). NOTE: this query enumerates real paths, so it takes the
    // concrete-branch path and does NOT reach `generate_empty_cte_sql` (the
    // empty-placeholder CAST sites migrated in this slice) — those remain
    // covered by the `cast_as`/`sql_type_name` unit tests; an integration
    // golden for the no-path empty branch is a deferred follow-up.
    // Multi-type *variable-length* path. Beyond locking the generator output,
    // this guards a UNION column-count regression: branches span differing hop
    // counts (1-hop and 2-hop), and ALL branches must project the SAME columns.
    // In particular the 1-hop branch must NOT additionally emit the Browser
    // dedup `r_from_id`/`r_to_id` pair that multi-hop branches lack, or
    // ClickHouse rejects the UNION ("different number of columns", Code:53) —
    // the failure seen on LDBC interactive complex-12
    // (`[:HAS_TYPE|IS_SUBCLASS_OF*0..]`). Those columns are only emitted for a
    // pure fixed single hop (`*1..1`), where every branch is a uniform 1-hop.
    (
        "vlp_multi_type",
        "MATCH (a:User)-[:FOLLOWS|AUTHORED*1..2]->(b) RETURN b",
    ),
    // type(r) on a multi-type edge reads the CTE's `path_relationships` array.
    // Regression guard for the array-index fix: the FIRST relationship is index 0
    // (Cypher 0-based) -> renders 1-based as CH `path_relationships[1]` and
    // Databricks `element_at(path_relationships, 1)`. The previous `[2]` was out of
    // bounds on a 1-element array (CH silently returned ""; Databricks errored).
    (
        "multi_type_rel_type_fn",
        "MATCH (a:User)-[r:FOLLOWS|AUTHORED]->(b) RETURN type(r) AS t",
    ),
    // --- #485: single-type VLP relationship-type literals must be the
    // Cypher-visible type name ('FOLLOWS'), never the internal composite
    // schema key ('FOLLOWS::User::User'). Composite keys exist to
    // disambiguate multi-endpoint relationship types in schema lookups; they
    // may not leak into query output. Multi-type VLP already emitted plain
    // names (see vlp_multi_type above); these lock the single-type routes:
    // the recursive CTE's path_relationships arrays and the type(r) literal.
    (
        "vlp_relationships_fn",
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b) RETURN relationships(p)",
    ),
    (
        "vlp_type_fn",
        "MATCH (a:User)-[r:FOLLOWS*1..2]->(b) RETURN type(r) AS t",
    ),
    // --- #488 (standard-schema shape): non-transitive VLP with a bound path
    // variable. AUTHORED (User->Post) cannot chain, so the transitivity pass
    // clamps *1..2 to a single hop; the path variable must then take the
    // fixed-path route (tuple('fixed_path', ...) + component columns).
    // Previously the renderer still emitted tuple(t.path_nodes, ...) against
    // a recursive VLP CTE that was never generated — unbound alias `t`,
    // ClickHouse Code 47.
    (
        "vlp_nontransitive_path_var",
        "MATCH p = (a:User)-[:AUTHORED*1..2]->(b) RETURN p",
    ),
    // Negative list index: Cypher `[-1]` = last element. Both CH arrayElement and
    // Spark element_at already treat -1 as last, so it must render UNCHANGED (not
    // offset by +1). The old +1 shifted -1 -> 0, and CH `arr[0]` silently returned
    // the type default (0) instead of the last element. Guards CH `[-1]` /
    // Databricks `element_at(..., -1)`; a non-negative index (index0 -> [1]) is the
    // control.
    (
        "list_index_negative",
        "MATCH (u:User) RETURN [10, 20, 30][-1] AS last, [10, 20, 30][0] AS first",
    ),
    // Dialect function-name mappings (regression for the Databricks overrides):
    // replace -> CH replaceAll / Spark replace; head/last -> CH arrayElement /
    // Spark element_at; stdev -> CH stddevSamp / Spark stddev_samp. Previously all
    // emitted the CH name on Databricks (unmapped function -> execution error).
    (
        "fn_replace",
        "MATCH (u:User) RETURN replace(u.name, 'a', 'X') AS r",
    ),
    (
        "fn_head_last",
        "MATCH (u:User) WITH collect(u.name) AS ns RETURN head(ns) AS h, last(ns) AS l",
    ),
    (
        "fn_stdev",
        "MATCH (u:User) RETURN stdev(u.user_id) AS s",
    ),
    // toBoolean -> CH toBool / Spark boolean. Both accept string ('true'/'false')
    // and numeric args; the old if(arg,1,0) form emitted invalid SQL for string
    // inputs (CH: "Illegal type String ... of function if").
    (
        "fn_toboolean",
        "MATCH (u:User) RETURN toBoolean('true') AS r",
    ),
    // trim -> bare trim(arg) on both dialects. The old arg_transform emitted
    // `trim(BOTH arg)` (missing `' ' FROM`), invalid SQL that 500'd on CH.
    (
        "fn_trim",
        "MATCH (u:User) RETURN trim(u.name) AS r",
    ),
    // date() -> CH toDate / Spark to_date. Spark has no toDate; the entry
    // previously fell back to the CH name on Databricks (UNRESOLVED_ROUTINE).
    (
        "fn_date",
        "MATCH (u:User) RETURN date('2020-01-15') AS d",
    ),
    // Dialect string-fn mappings: ltrim/rtrim -> CH trimLeft/trimRight but Spark
    // has no trimLeft/trimRight (uses ltrim/rtrim). Previously emitted the CH name
    // on Databricks (UNRESOLVED_ROUTINE `trimLeft`).
    (
        "fn_ltrim_rtrim",
        "MATCH (u:User) RETURN ltrim(u.name) AS l, rtrim(u.name) AS r",
    ),
    // `=~` regex match -> CH match() / Spark rlike(). Spark has no match(); the
    // previous hardcoded match() at every RegexMatch render site errored on
    // Databricks (UNRESOLVED_ROUTINE `match`).
    (
        "fn_regex_match",
        "MATCH (u:User) WHERE u.name =~ '.*a.*' RETURN u.user_id",
    ),
    // reduce -> CH arrayFold((x, acc) -> expr, list, init) / Spark
    // aggregate(list, init, (acc, x) -> expr). Spark has no arrayFold; it
    // previously emitted the CH name on Databricks (UNRESOLVED_ROUTINE).
    (
        "fn_reduce",
        "MATCH (u:User) RETURN reduce(s = 0, x IN [1, 2, 3] | s + x) AS r",
    ),
    // range is INCLUSIVE in Cypher. CH range() is exclusive -> end bumped +1
    // (was silently wrong: range(1,5) gave [1,2,3,4]); Spark has no range() ->
    // sequence() (already inclusive).
    (
        "fn_range",
        "MATCH (u:User) RETURN range(1, 5) AS r",
    ),
    (
        "optional_match",
        "MATCH (u:User) OPTIONAL MATCH (u)-[:AUTHORED]->(p:Post) RETURN u.name, p.title",
    ),
    (
        // Production (#459) renders this as a genuine projection-list UNION
        // DISTINCT (`u.full_name AS "x"` UNION DISTINCT `p.post_title AS "x"`) —
        // the correct Cypher UNION shape (live-verified: 3 user names + 2 post
        // titles = 5 rows). The pre-#459 ctx-less harness path wrongly collapsed
        // it to the `__multi_label_union` whole-node Browser scan (dropping the
        // RETURN projections); switching the net to production fixed the golden.
        "union",
        "MATCH (u:User) RETURN u.name AS x UNION MATCH (p:Post) RETURN p.title AS x",
    ),
    // #487: each Cypher UNION arm is an independent query — its aggregate
    // computes WITHIN the arm and the arms are then unioned. Pre-fix, the
    // internal-union hoisting path wrapped `count(*)` around a UNION DISTINCT
    // of de-aggregated `SELECT 1 AS __dummy` arms → returned 1 instead of
    // {5, 10} (live-verified against social_benchmark).
    (
        "union_agg_per_arm",
        "MATCH ()-[r:AUTHORED]->() RETURN count(r) AS c UNION MATCH ()-[r2:FOLLOWS]->() RETURN count(r2) AS c",
    ),
    (
        "union_all_agg_per_arm",
        "MATCH ()-[r:AUTHORED]->() RETURN count(r) AS c UNION ALL MATCH ()-[r2:FOLLOWS]->() RETURN count(r2) AS c",
    ),
    // #487: per-arm GROUP BY must render inside each arm (live-verified:
    // 4 authored + 6 liked per-user rows, matching per-table ground truth).
    (
        "union_agg_grouped_per_arm",
        "MATCH (u:User)-[:AUTHORED]->() RETURN u.name AS name, count(*) AS c UNION ALL MATCH (u2:User)-[:LIKED]->() RETURN u2.name AS name, count(*) AS c",
    ),
    // #487 review F1: per-arm ORDER BY / LIMIT on GROUPED aggregated arms.
    // Graph-join inference hides them under the arm's outer GraphJoins
    // (GraphJoins(Limit(OrderBy(GraphJoins(GroupBy(...)))))), so they were
    // silently dropped — 10 rows instead of 4 (live-verified: per-arm top-2).
    // The Databricks variant locks review F2: each modifier-carrying arm is
    // parenthesized (a bare per-arm LIMIT is a Spark parse error mid-chain
    // and binds to the WHOLE union as the last arm).
    (
        "union_agg_grouped_ordered_per_arm",
        "MATCH (u:User)-[:AUTHORED]->() RETURN u.name AS name, count(*) AS c ORDER BY c DESC LIMIT 2 UNION ALL MATCH (u2:User)-[:LIKED]->() RETURN u2.name AS name, count(*) AS c ORDER BY c DESC LIMIT 2",
    ),
    // #487: aggregate only in the SECOND arm — the base plan still holds the
    // first (non-aggregated) arm, exercising the base-is-arm render path
    // (live-verified: 8 user ids + one count row = 9 rows).
    (
        "union_agg_second_arm_only",
        "MATCH (u:User) RETURN u.user_id AS v UNION ALL MATCH ()-[r:FOLLOWS]->() RETURN count(r) AS v",
    ),
    (
        "group_two_keys",
        "MATCH (u:User) RETURN u.country, u.city, count(u) AS n",
    ),
    // NOTE: Path D coverage (EXISTS / pattern-predicate, e.g.
    // `WHERE (u)-[:AUTHORED]->(:Post)`) is intentionally absent — that path
    // currently hits `unimplemented!` in render_expr for anonymous pattern
    // nodes in expression context. Add it to this corpus once it renders
    // (it is the path Phase 2 of the IR refactor unifies). Likewise composite
    // node-ID, denormalized, multi-label, and UNWIND/arrayJoin shapes need
    // additional schemas / not-yet-implemented Spark structural support.
];

/// Browser-shaped patterns (Phase 0 slice P0.5): the fully/partially
/// UNLABELED query shapes Neo4j Browser emits by default — node scan,
/// undirected/directed expand, path render, VLP path, and the sidebar
/// count/labels/type probes. These route through the `pattern_union` /
/// `fixed_path` / multi-type-VLP renderers, which had near-zero golden
/// coverage before this slice. See
/// `docs/design/REFACTORING_SAFETY_PLAN.md` §3.1 and the
/// `browser-unlabeled-pattern-bugs` catalog (surfaced 2026-06-30) for
/// background. Standard schema only (`social_benchmark.yaml`, User/Post);
/// `FK_EDGE_BROWSER_CORPUS`/`DENORM_BROWSER_CORPUS` below mirror the
/// schema-sensitive subset (node scan / expand / path / count) on the other
/// two patterns.
///
/// FIXED (#466):
///
///   - `unlabeled_expand` (`MATCH (n)-[r]-(o) RETURN n, r, o`, fully
///     unlabeled + multi-edge-type UNDIRECTED) now renders DIFFERENT SQL from
///     `unlabeled_expand_directed` (the DIRECTED form): the `pattern_union`
///     branch generator emits a REVERSE-direction branch (same edge/join,
///     start and end swapped) for each combination. Confirmed live (local
///     `social` fixture): the undirected form returns 46 rows (23 forward
///     edges — 10 FOLLOWS + 5 AUTHORED + 8 LIKED — each also traversable
///     backward, 0 self-loops), while the directed control is unchanged at 23.
///     Self-loops (from-id == to-id) are excluded from the reverse branch
///     (WHERE guard on the FOLLOWS self-join) so they appear ONCE, per Neo4j.
///     This closed the "GROUP 3b" gap in the browser-unlabeled-pattern-bugs
///     catalog. Contrast: `anchored_unlabeled_expand` (one side labeled, 33
///     rows) and `unlabeled_rel_typed` (single relationship type, 20 rows)
///     route through `vlp_multi_type` / `bidirectional_union` and were already
///     correct.
///
/// Formerly KNOWN BROKEN (locked as characterization in the P0.5 test-only
/// slice; each has since been fixed and the goldens transitioned):
///
///   - `browser_style_count` (`MATCH (n) RETURN count(n)`, heterogeneous
///     unlabeled scan) — FIXED (#467). Previously rendered `count(<id column
///     of ONE arbitrary label>)` (Post's `n.post_id`) over per-label branches
///     where every OTHER branch NULL-pads that column, silently undercounting
///     (5 not 13; FK-edge 4 not 12). Now renders
///     `count(coalesce(n.post_id, n.user_id))` — a discriminator non-NULL on
///     each row's own branch — counting every row (live: Standard 13, FK 12).
///     `count(DISTINCT n)` renders `count(DISTINCT tuple(...))` so cross-label
///     id collisions are not merged (live 13). Denormalized
///     (`dn_browser_style_count`) is one label, unchanged — still
///     `count(a.code)`, already correct (7).
///   - `browser_type_probe` (`MATCH ()-[r]->() RETURN DISTINCT type(r)`) —
///     FIXED (#468). Previously the outer SELECT referenced
///     `t.path_relationships[1]` while the pattern_union CTE was aliased `r`
///     (`FROM pattern_union_r AS r`) — alias `t` unbound, ClickHouse Code 47.
///     The `type(r)` rewrite in projection_tagging now dispatches on the
///     route: pattern_union CTEs (both endpoints unlabeled) are referenced
///     through the relationship alias itself; multi-type VLP-joins CTEs keep
///     `t`. Live: returns AUTHORED/FOLLOWS/LIKED on the `social` fixture.
///   - `path_vlp` (`MATCH p=(a:User)-[:FOLLOWS*1..2]->(b) RETURN p`) — FIXED
///     (#469). Previously the path tuple referenced `t.path_edges`, a column
///     the recursive VLP CTE never defines (it projects `start_id`/`end_id`/
///     `hop_count`/`path_relationships`/`path_nodes`; per-edge arrays were
///     dropped for memory) — ClickHouse Code 47. The tuple now consumes the
///     CTE's actual projection: `tuple(t.path_nodes, t.path_relationships,
///     t.hop_count)`. The denormalized VLP CTE strategy also gained a
///     `path_relationships` column so the same contract holds there
///     (live: `[[LAX, JFK], [FLIGHT], 1]`). FIXED (#485): single-type VLP
///     `path_relationships` values used to leak the composite schema key
///     (`FOLLOWS::User::User`, not `FOLLOWS`) — same leak as
///     `relationships(p)` and single-type `type(r)` literals. Literals now
///     emit only the Cypher-visible type name via
///     `composite_key_utils::extract_type_name`.
///
/// Verified CORRECT (normal locks, not suspicious) — all live-verified
/// against the local `social` fixture (8 Users, 5 Posts, 10 FOLLOWS,
/// 5 AUTHORED, 8 LIKED) set up for this slice:
/// `unlabeled_node_scan` (heterogeneous UNION ALL, deterministic column
/// order, 13 rows); `unlabeled_node_props` (the property `name` is unique to
/// User in this schema, so it optimizes to a plain single-label scan — it
/// does NOT exercise the cross-label property-key-probe UNION path; see
/// `unlabeled_node_props_absent` below for that path, which hits the #417
/// `_empty`-placeholder route); `anchored_unlabeled_expand` (33 rows);
/// `unlabeled_rel_typed` (20 rows); `path_assignment` (clean fixed-path
/// render, 10 rows); `browser_labels_probe` (clean per-label UNION ALL).
///
/// `path_unlabeled` (`MATCH p=()-[]->() RETURN p LIMIT 10`) is NOT a byte
/// golden: it routes through `pattern_union_{alias}` where `alias` is an
/// AUTO-GENERATED anonymous name (e.g. `t3`) drawn from the same
/// process-global counter `normalize()` remaps elsewhere — but here the
/// counter is embedded INSIDE an identifier (`pattern_union_t3`), not as its
/// own token, so `normalize()`'s `\bt\d+\b` (word-boundary) regex does not
/// match it (`_` is a word character, so there is no boundary before the
/// `t`). The CTE name therefore varies run-to-run and cannot be byte-locked
/// without widening `normalize()` (out of scope for this slice — it would
/// touch every existing golden's normalization). Locked instead by the
/// structural test `standard_path_unlabeled_pattern_union_name_is_unstable`
/// below, which also documents the harness gap itself as a follow-up.
const BROWSER_CORPUS: &[(&str, &str)] = &[
    ("unlabeled_node_scan", "MATCH (n) RETURN n LIMIT 25"),
    ("unlabeled_node_props", "MATCH (n) RETURN n.name LIMIT 25"),
    // Property `follow_date` belongs to NO node label (it's a FOLLOWS edge
    // property) — this is the genuine cross-label property-key-probe route:
    // TypeInference finds no valid node type, so the scan collapses to the
    // `_empty` placeholder (the #417 fix domain). Locked as characterization:
    // the placeholder is valid SQL (0 rows) but the declared RETURN alias
    // `n.follow_date` is silently replaced by `_empty` in the result schema.
    (
        "unlabeled_node_props_absent",
        "MATCH (n) RETURN n.follow_date LIMIT 25",
    ),
    (
        "unlabeled_expand",
        "MATCH (n)-[r]-(o) RETURN n, r, o LIMIT 25",
    ),
    (
        "unlabeled_expand_directed",
        "MATCH (n)-[r]->(o) RETURN n, r, o",
    ),
    (
        "anchored_unlabeled_expand",
        "MATCH (a:User)-[r]-(o) RETURN a, r, o",
    ),
    (
        "unlabeled_rel_typed",
        "MATCH (n)-[r:FOLLOWS]-(o) RETURN n, o",
    ),
    (
        "path_assignment",
        "MATCH p=(a:User)-[:FOLLOWS]->(b) RETURN p",
    ),
    // "path_unlabeled" intentionally NOT in this corpus — see the doc comment
    // above; it's locked by `standard_path_unlabeled_pattern_union_name_is_unstable`.
    ("path_vlp", "MATCH p=(a:User)-[:FOLLOWS*1..2]->(b) RETURN p"),
    ("browser_style_count", "MATCH (n) RETURN count(n)"),
    (
        "browser_labels_probe",
        "MATCH (n) RETURN DISTINCT labels(n)",
    ),
    (
        "browser_type_probe",
        "MATCH ()-[r]->() RETURN DISTINCT type(r) LIMIT 25",
    ),
    // #487 issue shape: Cypher UNION over unlabeled patterns — both arms'
    // pattern_union CTEs must be present and joined with UNION DISTINCT
    // (live-verified: 3 deduped type rows).
    (
        "cypher_union_unlabeled_type",
        "MATCH ()-[r]->() RETURN type(r) AS t UNION MATCH ()-[r2]->() RETURN type(r2) AS t",
    ),
    // #487 executable control: count(r) per arm over the pattern_union scan.
    // Pre-fix returned 1 (count of the deduped __dummy row) instead of 23
    // (live-verified: identical arms → UNION DISTINCT → one row of 23).
    (
        "cypher_union_unlabeled_count",
        "MATCH ()-[r]->() RETURN count(r) AS c UNION MATCH ()-[r2]->() RETURN count(r2) AS c",
    ),
];

/// FK-edge variation (`schemas/test/fk_edge.yaml`): Order/Customer where the
/// orders_fk table IS the PLACED_BY edge table (customer_id FK column is the
/// relationship — no separate edge table, not denormalized). Mirrors the
/// standard corpus's feature axes for the FK-edge schema pattern.
///
/// Not expressible in this schema (single edge type, from_node Order != to_node
/// Customer, so an edge cannot chain into itself), intentionally omitted:
///   - recursive VLP — no second hop exists out of Customer. (The
///     `vlp_nontransitive_path_var` entry below deliberately WRITES a VLP
///     `*1..2`, locking the #488 clamp-to-single-hop + fixed-path-route
///     behavior, not a recursive CTE.)
///   - multi-type `[:A|B]` — only one edge type (PLACED_BY).
///   - UNWIND/arrayJoin shapes — same Spark structural gap the standard corpus
///     skips.
///
/// KNOWN-SUSPICIOUS: none currently. `with_match_chain` was confirmed wrong
/// (cartesian) and is FIXED (#451); `optional_match`'s redundant phantom
/// self-join is FIXED (#452); `optional_after_with`'s INNER + mis-anchor is
/// FIXED (#453); `optional_after_with_where`'s silently-dropped optional-side
/// predicate is FIXED (#460 — now a LEFT JOIN pre_filter subquery).
const FK_EDGE_CORPUS: &[(&str, &str)] = &[
    // --- node scans (both node types) ---
    ("node_scan_order", "MATCH (o:Order) RETURN o.order_id"),
    (
        "node_scan_customer",
        "MATCH (c:Customer) RETURN c.customer_id",
    ),
    // --- property projection, incl. the renamed property `amount` (-> column
    // total_amount) ---
    (
        "project_order",
        "MATCH (o:Order) RETURN o.order_id, o.order_date, o.amount",
    ),
    (
        "project_customer",
        "MATCH (c:Customer) RETURN c.customer_id, c.name, c.email",
    ),
    (
        "distinct_customer_name",
        "MATCH (c:Customer) RETURN DISTINCT c.name",
    ),
    // --- WHERE filters on both node types (renamed prop, string, AND, IN) ---
    (
        "where_order_amount",
        "MATCH (o:Order) WHERE o.amount > 100 RETURN o.order_id",
    ),
    (
        "where_customer_name",
        "MATCH (c:Customer) WHERE c.name = 'Alice' RETURN c.email",
    ),
    (
        "where_and",
        "MATCH (o:Order) WHERE o.amount > 50 AND o.order_id < 5 RETURN o.order_id",
    ),
    (
        "where_in_list",
        "MATCH (c:Customer) WHERE c.name IN ['Alice', 'Bob'] RETURN c.email",
    ),
    // --- ordering / paging ---
    (
        "order_skip_limit",
        "MATCH (o:Order) RETURN o.order_id, o.amount ORDER BY o.amount DESC SKIP 1 LIMIT 3",
    ),
    (
        "skip_only",
        "MATCH (o:Order) RETURN o.order_id ORDER BY o.order_id SKIP 2",
    ),
    ("aggregate_count", "MATCH (o:Order) RETURN count(o)"),
    // --- single hop, both directions. FK-edge: the join is node-to-node on
    // orders_fk.customer_id = customers_fk.customer_id, no phantom third table. ---
    (
        "single_hop",
        "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) RETURN o.order_id, c.name",
    ),
    // Reverse pattern (same directed edge, written right-to-left).
    (
        "single_hop_reverse",
        "MATCH (c:Customer)<-[:PLACED_BY]-(o:Order) RETURN c.name, o.amount",
    ),
    // UNDIRECTED single hop — the ERR-G bug class (fixed in PR #432): undirected
    // forms must read the edge id columns from the correct alias.
    (
        "undirected_hop",
        "MATCH (o:Order)-[:PLACED_BY]-(c:Customer) RETURN o.order_id, c.name",
    ),
    // #492 review RN5: undirected 2-hop through a shared Customer. FK-edge
    // relationships have NO edge alias in the SQL (the rel row IS the Order
    // row), so the uniqueness guard must compare the materialized node
    // aliases (NOT a.order_id = b.order_id) — a guard over the rel aliases
    // (t1/t2) referenced never-materialized identifiers.
    (
        "undirected_2hop_shared_customer",
        "MATCH (a:Order)-[:PLACED_BY]-(c:Customer)-[:PLACED_BY]-(b:Order) RETURN a.order_id, c.customer_id, b.order_id",
    ),
    // Filter on BOTH node types across the hop.
    (
        "hop_filter_both",
        "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) WHERE o.amount > 100 AND c.name = 'Alice' RETURN o.order_id",
    ),
    // Whole-edge RETURN r on an FK-edge relationship.
    (
        "whole_edge_r",
        "MATCH (o:Order)-[r:PLACED_BY]->(c:Customer) RETURN r",
    ),
    // --- OPTIONAL MATCH (anchored on Customer, optional incoming order) ---
    (
        "optional_match",
        "MATCH (c:Customer) OPTIONAL MATCH (c)<-[:PLACED_BY]-(o:Order) RETURN c.name, o.order_id",
    ),
    // OPTIONAL MATCH *after* a WITH barrier: the required Customer arrives as a
    // CTE (with_c_cte_0) and the fresh Order pattern is optional. The CTE is the
    // anchor, so it must be the FROM driver and the Order pattern LEFT-joined to
    // it, preserving customers with no orders. #453.
    (
        "optional_after_with",
        "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) RETURN c.name, o.order_id",
    ),
    // Same shape with a WHERE on the OPTIONAL pattern's optional side. The
    // predicate must filter the optional matches INSIDE the LEFT JOIN (pre_filter
    // subquery), keeping customers with no qualifying order NULL-extended — never
    // in the outer WHERE (which would drop the NULL rows). #460.
    (
        "optional_after_with_where",
        "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) WHERE o.amount > 100 RETURN c.name, o.order_id",
    ),
    // #462 GAP 1 (cross) — FIXED. A WHERE spanning BOTH the optional side (o) and
    // the anchor CTE (c) now lands in the LEFT JOIN ON condition
    // (`... ON o.customer_id = c.p1_c_customer_id AND o.total_amount >
    // c.p1_c_customer_id`), so no-match customers stay NULL-extended (was the
    // outer WHERE, which dropped them). This is correct dialect-neutral SQL and
    // executes on Spark/Databricks; NOTE ClickHouse rejects a DIRECT cross-table
    // comparison in a NULL-preserving LEFT JOIN ON (join_use_nulls), so this exact
    // shape surfaces a clean ClickHouse engine error rather than silently wrong
    // rows — an engine limitation, not a translation bug.
    (
        "optional_after_with_where_cross",
        "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) WHERE o.total_amount > c.customer_id RETURN c.customer_id, o.order_id",
    ),
    // #462 GAP 1 (unsplittable OR) — FIXED. The whole OR now sits in the LEFT JOIN
    // ON condition (parenthesized so `key AND (a OR b)` parses correctly), never
    // the outer WHERE. Each OR leaf compares to a literal (no single cross-table
    // comparison), so this executes on ClickHouse too. Live: 6 rows, all customers
    // preserved.
    (
        "optional_after_with_where_or",
        "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) WHERE o.total_amount > 100 OR c.customer_id > 100 RETURN c.customer_id, o.order_id",
    ),
    // #462 GAP 2 (rel) — FIXED. A predicate on the relationship alias (r) was
    // SILENTLY DROPPED (rendered byte-identically to optional_after_with). On
    // FK-edge r and o share the orders_fk table, so r.order_id remaps to the
    // shared table's column and now sits in the LEFT JOIN pre_filter
    // (`LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE order_id > 3) AS o`).
    // Live: 5 rows (was an unfiltered 8).
    (
        "optional_after_with_where_rel",
        "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[r:PLACED_BY]->(c) WHERE r.order_id > 3 RETURN c.customer_id, o.order_id",
    ),
    // #462 GAP 2 (mixed) — FIXED. rel-alias conjunct AND optional-node conjunct.
    // The r-conjunct was SILENTLY DROPPED while the o-conjunct was recovered (#460)
    // — partial filter application. BOTH now sit in the LEFT JOIN pre_filter.
    // Live: 4 rows, c100 correctly NULL-extended (its only order fails order_id>3).
    (
        "optional_after_with_where_rel_and_node",
        "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[r:PLACED_BY]->(c) WHERE r.order_id > 3 AND o.total_amount > 100 RETURN c.customer_id, o.order_id",
    ),
    // KNOWN LIMITATIONS adjacent to the #462 shapes above (all PRE-EXISTING,
    // found by the #462 adversarial review; none regressed by the fix):
    //   #472 — a PURE-ANCHOR conjunct (e.g. `WHERE c.customer_id > 101`) stays in
    //          the outer WHERE and drops NULL-extended anchor rows (should move
    //          into the LEFT JOIN ON — always safe for a LEFT JOIN).
    //   #473 — cross-WITH-barrier conversion corrupts `IS NULL` (operator
    //          vanishes: `(o.total_amount OR ...)`) and `NOT(..) OR ..`
    //          (the OR becomes an AND split). Plain non-WITH forms are fine.
    //   #474 — FIXED. Plain OPTIONAL MATCH *without* WITH (reversed-anchor
    //          FK-edge shape) silently dropped its optional-node WHERE entirely
    //          (a separate code path from #460/#462). Now recovered into the LEFT
    //          JOIN pre_filter for node-is-edge shapes (see `optional_where_no_with*`
    //          below). The standard separate-edge shape is out of scope and keeps
    //          its pre-existing placement (see the #474 report).
    // Also: ClickHouse rejects cross-table comparisons in a NULL-preserving
    // LEFT JOIN ON (join_use_nulls, error 386) — the `_cross` golden above is
    // correct SQL that executes on Databricks; on ClickHouse it errors cleanly.
    // --- #474: plain OPTIONAL MATCH (NO WITH barrier), reversed anchor ---
    // The anchor Customer arrives from the first MATCH and is the right connection
    // of the optional pattern `(o:Order)-[:PLACED_BY]->(c)`; the Order node is the
    // OPTIONAL (left) connection. A WHERE on the optional Order node must filter
    // the LEFT JOIN subquery (pre_filter) so customers with no qualifying order
    // stay NULL-extended — NOT the outer WHERE (drops them) and NOT dropped
    // entirely. FK-edge: the Order node IS the orders_fk edge table, so the whole
    // optional pattern is a single LEFT JOIN and the pre_filter gates it correctly.
    // FIXED (#474): was silently dropped (unfiltered 8 rows on live db_fk_edge);
    // now renders `LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE total_amount
    // > 100) AS o`. Live: 4 rows, each customer keeps its single order with
    // total_amount>100; with `> 130` c102 is correctly NULL-extended.
    (
        "optional_where_no_with",
        "MATCH (c:Customer) OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) WHERE o.total_amount > 100 RETURN c.customer_id, o.order_id",
    ),
    // FIXED (#477): the pre_filter renderer stripped the node alias from bare
    // columns (`o.total_amount` -> `total_amount`) but NOT when the same
    // column appeared inside a function argument (`toFloat(o.total_amount)`
    // kept the `o.` prefix), producing `LEFT JOIN (SELECT * FROM orders_fk
    // WHERE toFloat64(o.total_amount) > 100) AS o` — invalid SQL, ClickHouse
    // error 47 UNKNOWN_IDENTIFIER (`o` is not in scope inside the subquery).
    // Root cause: `RenderExpr::to_sql_without_table_alias` special-cased
    // `PropertyAccessExp` and `OperatorApplicationExp` but fell through to the
    // ordinary alias-preserving `to_sql()` for `ScalarFnCall` args (and other
    // composite variants). Fixed by a full AST rewrite
    // (`strip_table_alias_everywhere`) that recurses into function args, list
    // items, CASE branches, and array subscript/slicing before delegating to
    // `to_sql()`. Live (db_fk_edge): now renders `toFloat64(total_amount)`
    // (no dangling alias) and executes, returning 4 rows.
    (
        "optional_where_no_with_fn_arg",
        "MATCH (c:Customer) OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) WHERE toFloat(o.total_amount) > 100 RETURN c.customer_id, o.order_id",
    ),
    // FIXED (#535): same pre_filter-subquery shape as `optional_where_no_with_fn_arg`
    // above, but for STRING `+` (Cypher concatenation) instead of a function-arg
    // property access. `RenderExpr::to_sql_without_table_alias`'s OperatorApplicationExp
    // arm never called `render_string_addition` (unlike the two other operator-
    // rendering paths), so `o.status + 'X'` fell through to the generic operator-join
    // formatting — ClickHouse has no `+` for strings, so this would emit invalid SQL
    // (or, if alias-stripping partially applied, a dangling `+`-joined non-function
    // expression) inside the LEFT JOIN pre_filter subquery. Now renders
    // `concat(status, 'X')` (alias-stripped AND converted to the dialect's string-concat
    // function). Same bug class as #477 (missing arm in this hand-rolled walker).
    (
        "optional_where_no_with_string_concat",
        "MATCH (c:Customer) OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) WHERE o.status + 'X' = 'shippedX' RETURN c.customer_id, o.order_id",
    ),
    // Same shape, WHERE on the relationship alias r (order_date). Already correct
    // before #474 (rel-alias pre_filter recovery): r and o share orders_fk, so the
    // predicate sits in the LEFT JOIN pre_filter. Locked to prove #474 did not
    // disturb it.
    (
        "optional_where_no_with_rel",
        "MATCH (c:Customer) OPTIONAL MATCH (o:Order)-[r:PLACED_BY]->(c) WHERE r.order_date > '2024-01-01' RETURN c.customer_id, o.order_id",
    ),
    // Mixed conjunction: optional-node predicate AND pure-anchor predicate. The
    // optional-node conjunct (o.total_amount) is recovered into the LEFT JOIN
    // pre_filter (#474); the pure-anchor conjunct (c.customer_id) is folded into
    // the LEFT JOIN ON (#597 — a false ON just NULL-extends, preserving the
    // anchor row, instead of the old outer-WHERE placement that dropped it).
    (
        "optional_where_no_with_mixed",
        "MATCH (c:Customer) OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) WHERE o.total_amount > 100 AND c.customer_id > 101 RETURN c.customer_id, o.order_id",
    ),
    // --- WITH + aggregation (count per customer), and its HAVING form ---
    (
        "with_agg_count",
        "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) WITH c.name AS name, count(o) AS orders RETURN name, orders",
    ),
    (
        "with_having",
        "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) WITH c.name AS name, count(o) AS n WHERE n > 1 RETURN name, n",
    ),
    // --- WITH -> MATCH chain (filter customers, then match their orders) ---
    (
        "with_match_chain",
        "MATCH (c:Customer) WITH c WHERE c.customer_id > 100 MATCH (c)<-[:PLACED_BY]-(o:Order) RETURN c.name, o.order_id",
    ),
    // SKIP/LIMIT inside a WITH -> CTE-body LIMIT emission path.
    (
        "with_skip_limit",
        "MATCH (o:Order) WITH o.amount AS a ORDER BY a SKIP 1 LIMIT 2 RETURN a",
    ),
    // Group by two keys across the hop.
    (
        "group_two_keys",
        "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) RETURN c.name, c.email, count(o) AS n",
    ),
    // --- whole-entity RETURN n (both node types) ---
    ("whole_entity_order", "MATCH (o:Order) RETURN o"),
    ("whole_entity_customer", "MATCH (c:Customer) RETURN c"),
    // DISTINCT over a hop projection.
    (
        "distinct_hop",
        "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) RETURN DISTINCT c.name",
    ),
    // --- #488: non-transitive VLP with a bound path variable ---
    // PLACED_BY cannot chain (Order->Customer; Customer never re-enters as a
    // FROM node), so the transitivity pass clamps *1..2 to a single hop. The
    // bound path variable must then take the FIXED-path route
    // (tuple('fixed_path', ...) + component columns) exactly like the plain
    // single-hop `MATCH p = (o)-[:PLACED_BY]->(c)`. Previously the renderer
    // still emitted tuple(t.path_nodes, ...) against a recursive VLP CTE that
    // was never generated — unbound alias `t`, ClickHouse Code 47. Live:
    // executes on db_fk_edge (8 rows, one per order).
    (
        "vlp_nontransitive_path_var",
        "MATCH p = (o:Order)-[:PLACED_BY*1..2]->(c) RETURN p",
    ),
    // #487: Cypher UNION arm whose count(o) spans the FK-edge scan — the
    // aggregate must compute WITHIN each arm, never over the combined arms.
    (
        "union_agg_per_arm",
        "MATCH ()-[r:PLACED_BY]->() RETURN count(r) AS c UNION ALL MATCH (c2:Customer) RETURN count(c2) AS c",
    ),
];

/// Denormalized variation (`schemas/dev/flights_denormalized.yaml`): a single
/// `flights_denorm` table that IS the `FLIGHT` edge AND embeds the `Airport`
/// node properties. Airport is a *virtual* node — `node_id: code` maps to
/// `origin_code` (from-side) / `dest_code` (to-side) via
/// from_node_properties/to_node_properties; `city`/`state` map to
/// origin_/dest_ columns likewise. FLIGHT carries a composite `edge_id`
/// `[flight_id, flight_number]` and a renamed edge property `flight_num` ->
/// physical `flight_number`.
///
/// This is the schema pattern with the heaviest documented bug history, so the
/// axes below deliberately exercise the repaired paths:
///   - denorm node materialization (bug class B / #429/#423): a labeled node
///     scan `MATCH (a:Airport)` has no physical Airport table, so it must
///     UNION the origin- and dest-side projections of `flights_denorm`.
///   - whole-node `RETURN a` (bug class A / #427): must project the virtual
///     node_id `code` (resolved to a physical column), never a bare `code`.
///   - from-side vs to-side property sourcing across a directed hop.
///   - fixed-path / pattern-union renderers (#419/#420/#421/#425 family):
///     no spurious self-join of the single edge table; virtual ids resolved to
///     physical columns.
///   - VLP `*1..2` routes through `DenormalizedCteStrategy`.
///
/// Intentionally omitted (document skips): multi-type `[:A|B]` (only one edge
/// type, FLIGHT); UNWIND/arrayJoin shapes (same Spark structural gap the other
/// corpora skip). `from_node == to_node == Airport`, so every node position is
/// the same denormalized table — undirected/reverse hops still exercise the
/// from/to sourcing switch.
///
/// KNOWN-SUSPICIOUS: see the comment block above `sql_golden_snapshots`.
const DENORM_CORPUS: &[(&str, &str)] = &[
    // --- node scan: denorm node materialization (bug class B / #429). No
    // Airport table exists; must UNION origin/dest projections of flights_denorm. ---
    ("node_scan", "MATCH (a:Airport) RETURN a.code"),
    // whole-node RETURN a: must project the virtual node_id `code` (bug class A / #427).
    ("whole_node", "MATCH (a:Airport) RETURN a"),
    // property projection incl. the virtual-id property `code` + denorm props.
    (
        "project_node_props",
        "MATCH (a:Airport) RETURN a.code, a.city, a.state",
    ),
    ("distinct_node_state", "MATCH (a:Airport) RETURN DISTINCT a.state"),
    ("aggregate_count_node", "MATCH (a:Airport) RETURN count(a)"),
    // --- WHERE on denorm node props (state) and on the virtual id (code) ---
    (
        "where_denorm_prop",
        "MATCH (a:Airport) WHERE a.state = 'CA' RETURN a.code",
    ),
    (
        "where_virtual_id",
        "MATCH (a:Airport) WHERE a.code = 'LAX' RETURN a.city",
    ),
    // --- directed hop (from-side + to-side). Both endpoints are the SAME
    // denormalized table; sourcing must switch origin_ (a) vs dest_ (b). ---
    (
        "directed_hop_ids",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code",
    ),
    (
        "directed_hop_props",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.city, a.state, b.city, b.state",
    ),
    // Reverse-written directed hop (right-to-left).
    (
        "reverse_hop",
        "MATCH (b:Airport)<-[:FLIGHT]-(a:Airport) RETURN a.code, b.code",
    ),
    // Undirected hop — must read edge id columns from the correct alias.
    (
        "undirected_hop",
        "MATCH (a:Airport)-[:FLIGHT]-(b:Airport) RETURN a.code, b.code",
    ),
    // Undirected 2-hop (#492): must UNION all four direction assignments
    // (2 per undirected hop) with a relationship-uniqueness guard per branch,
    // NOT collapse to a single directed join chain.
    (
        "undirected_2hop",
        "MATCH (a:Airport)-[:FLIGHT]-(b:Airport)-[:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code",
    ),
    // Mixed-direction 2-hop (#492): the trailing undirected hop alone must
    // fan out into forward + reverse branches.
    (
        "mixed_direction_2hop",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport)-[:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code",
    ),
    // #492 review B1: WHERE on the SHARED MIDDLE node of an undirected 2-hop.
    // Each branch must filter on the same physical column it projects for b
    // (the all-forward branch used to filter on c's column, t2.Dest).
    (
        "where_middle_node_undirected_2hop",
        "MATCH (a:Airport)-[:FLIGHT]-(b:Airport)-[:FLIGHT]-(c:Airport) WHERE b.code = 'LAX' RETURN a.code, b.code, c.code",
    ),
    // #492 review B3 CHARACTERIZATION: OPTIONAL + nested-undirected multi-hop
    // is GATED to the pre-#492 shape (single directed LEFT chain, no direction
    // union): per-orientation LEFT-JOIN branches under UNION ALL cannot
    // express OPTIONAL semantics (NULL-anchor rows dropped by the guard,
    // duplicated across branches when NULL-safe, partial-pattern rows, and
    // swapped branches anchoring FROM on the optional node). This byte-lock is
    // a KNOWN-INCOMPLETE shape (directed-only matches), not semantic coverage;
    // fixing it needs an anchor-LEFT-JOIN-onto-match-union renderer structure.
    //
    // #505 transitioned this golden: the anchor `a` (bare `MATCH (a:Airport)`,
    // no required binding) now correctly gets its own `__denorm_scan_a` CTE +
    // LEFT JOIN instead of silently using the first hop's edge table as FROM
    // (which dropped anchor rows with no match, e.g. an airport with no
    // flights at all). The directed-only-match limitation described above is
    // UNCHANGED and still tracked separately — this fix only restores anchor
    // preservation for the (already directed-only) shape this golden locks.
    (
        "optional_undirected_2hop",
        "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]-(b:Airport)-[:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code",
    ),
    // hop projecting edge properties incl. the renamed `flight_num` -> flight_number.
    (
        "hop_edge_props",
        "MATCH (a:Airport)-[r:FLIGHT]->(b:Airport) RETURN a.code, r.carrier, r.flight_num, r.distance, b.code",
    ),
    // WHERE on an edge property across the hop.
    (
        "where_edge_prop",
        "MATCH (a:Airport)-[r:FLIGHT]->(b:Airport) WHERE r.distance > 1000 RETURN a.code, b.code",
    ),
    // Filter on BOTH endpoints across the hop (from-side + to-side denorm props).
    (
        "hop_filter_both",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) WHERE a.state = 'CA' AND b.state = 'NY' RETURN a.code, b.code",
    ),
    // Whole-edge RETURN r on the FLIGHT relationship (composite edge_id).
    (
        "whole_edge_r",
        "MATCH (a:Airport)-[r:FLIGHT]->(b:Airport) RETURN r",
    ),
    // RESTORED (#464): `optional_match` and `path_return` are byte-goldens again.
    // They were removed in #459 because production emitted their denorm
    // node/edge property columns in nondeterministic HashMap order — for
    // `optional_match` the `__denorm_scan_a` from/to-UNION branches, whose
    // positional misalignment additionally SCRAMBLED values (live: 14 rows with
    // `a.code` holding STATE strings instead of 9 correct rows). #464 sorts both
    // the denorm node-scan branch projection (select_builder ViewScan
    // property_mapping fallback) and the path edge-property expansion
    // (get_relationship_properties call sites) by cypher property name, so the
    // UNION branches derive their column order from a single canonical ordering
    // and align by position. Byte-stable across 15+ process runs; live on
    // `db_denormalized`: optional_match = 9 rows (8 flights + PHX, the dest-only
    // airport, with NULL b), path_return materializes every a.*/b.*/t1.* column.
    // The dedicated structural tests (`denorm_optional_match_resolves_to_node_onto_edge_456`,
    // `denorm_path_return_materializes_node_edge_props_459`) still lock the
    // semantic shape.
    (
        "optional_match",
        "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code",
    ),
    ("path_return", "MATCH p = (a:Airport)-[:FLIGHT]->(b:Airport) RETURN p"),
    // --- WITH + aggregation (out-degree per origin airport), and its HAVING form ---
    (
        "with_agg_count",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) WITH a.code AS origin, count(b) AS flights RETURN origin, flights",
    ),
    (
        "with_having",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) WITH a.code AS origin, count(b) AS n WHERE n > 1 RETURN origin, n",
    ),
    // --- WITH -> MATCH chain (filter CA airports, then their outgoing flights) ---
    (
        "with_match_chain",
        "MATCH (a:Airport) WITH a WHERE a.state = 'CA' MATCH (a)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code",
    ),
    // SKIP/LIMIT inside a WITH -> CTE-body LIMIT emission path.
    (
        "with_skip_limit",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) WITH a.code AS c ORDER BY c SKIP 1 LIMIT 3 RETURN c",
    ),
    // --- ordering / paging over a hop ---
    (
        "order_skip_limit",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code ORDER BY a.code DESC SKIP 1 LIMIT 3",
    ),
    (
        "skip_only",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.code ORDER BY a.code SKIP 2",
    ),
    // Group by two denorm keys across the hop.
    (
        "group_two_keys",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.state, b.state, count(*) AS n",
    ),
    // DISTINCT over a hop projection.
    (
        "distinct_hop",
        "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN DISTINCT a.state",
    ),
    // Variable-length path *1..2 -> DenormalizedCteStrategy (recursive CTE).
    (
        "vlp_recursive",
        "MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport) RETURN b.code",
    ),
    // #569: a fixed hop immediately followed by a VLP hop, where the VLP's
    // OWN start endpoint (`a`) is ALSO the fixed hop's end endpoint — the
    // SINGLE-TABLE denorm sibling of #559's multi-table-label repro (#559
    // itself was about a node label mapped to MULTIPLE physical tables;
    // `Airport` here is always the same `flights_denorm` table, so this
    // exercises the same "VLP start endpoint resolves its OWN role, not the
    // fixed hop's" fix on the OTHER axis — a single denormalized table with
    // separate from-side/to-side column mappings). Confirmed correct: not a
    // bug, just previously untested. Live-verified against
    // `db_denormalized.flights_denorm` (8 seeded flights): 19 rows, matching
    // hand-enumeration of every (x, a, b) combination over the 8 edges.
    (
        "fixed_hop_then_vlp_hop_559",
        "MATCH (x:Airport)-[:FLIGHT]->(a:Airport)-[:FLIGHT*1..2]->(b:Airport) \
         RETURN x.code, a.code, b.code",
    ),
    // #487: Cypher UNION whose first arm aggregates over the denormalized
    // virtual-node from/to union. count(a) must hoist over the arm's OWN
    // internal union (origin_code/dest_code, correctly mapped — NOT unmapped
    // `a.code`) while staying WITHIN the arm (live-verified: {7 airports,
    // 8 flights} against db_denormalized).
    (
        "union_agg_per_arm",
        "MATCH (a:Airport) RETURN count(a) AS c UNION ALL MATCH ()-[f:FLIGHT]->() RETURN count(f) AS c",
    ),
    // #555: ORDER BY on a role-ambiguous denorm property that is NOT itself
    // projected in SELECT. `state` maps to a DIFFERENT physical column on
    // the from-role (origin_state) vs to-role (dest_state) branch, so
    // filter_tagging's #471 ambiguity guard leaves it unmapped — this must
    // still resolve to each branch's OWN role-correct column instead of
    // UNKNOWN_IDENTIFIER on the raw, never-projected `n.state`.
    (
        "order_by_unprojected_ambiguous_prop",
        "MATCH (n:Airport) RETURN n.city ORDER BY n.state",
    ),
    // #563: whole-node non-WITH GROUP BY over an UNLABELED denorm chain node
    // `b` sitting between two edges — must resolve to `b`'s real physical
    // identity column (origin_code), never the raw unmapped `code` property
    // name (which crashes ClickHouse with UNKNOWN_IDENTIFIER).
    (
        "unlabeled_whole_node_group_by_no_with",
        "MATCH (a:Airport)-[:FLIGHT]->(b)-[:FLIGHT]->(c:Airport) RETURN b, count(*) AS n",
    ),
];

/// Composite-node-ID variation (`schemas/test/composite_node_ids.yaml`, extracted
/// from `schemas/examples/composite_node_id_test.yaml` so it loads through
/// `GraphSchemaConfig::from_yaml_file`): Account is identified by a TWO-column
/// composite key `(bank_id, account_number)`; Customer keeps a single-column key
/// `customer_id` (mixed in the same graph, matching real multi-bank schemas).
/// `OWNS` (Customer -> Account) exercises a single-to-composite join;
/// `TRANSFERRED` (Account -> Account) exercises composite-to-composite — BOTH
/// sides of that join must carry ALL id components, e.g.
/// `(a1.bank_id, a1.account_number) = (t.from_bank_id, t.from_account_number)`;
/// a join on only `bank_id` would silently fan out across every account at the
/// same bank. Mirrors the standard/FK-edge corpora's feature axes.
///
/// Not expressible / intentionally omitted (same reasons as the other corpora):
///   - multi-type `[:A|B]` — only one edge type between any given node pair.
///   - UNWIND/arrayJoin shapes — same Spark structural gap the other corpora skip.
///
/// NOTE: the `group_by_whole_node` case previously locked a composite-id GROUP BY
/// correctness bug (grouping collapsed to the FIRST id column only); FIXED in
/// issue #457 — it now keys on the full `node_id.columns()` set. See the comment
/// block above `sql_golden_snapshots` for the resolution.
const COMPOSITE_ID_CORPUS: &[(&str, &str)] = &[
    // --- node scans (both node types) ---
    ("node_scan_account", "MATCH (a:Account) RETURN a.account_number"),
    ("node_scan_customer", "MATCH (c:Customer) RETURN c.customer_id"),
    // --- property projection, incl. all composite-id columns ---
    (
        "project_account",
        "MATCH (a:Account) RETURN a.bank_id, a.account_number, a.balance",
    ),
    (
        "project_customer",
        "MATCH (c:Customer) RETURN c.customer_id, c.name, c.email",
    ),
    (
        "distinct_account_type",
        "MATCH (a:Account) RETURN DISTINCT a.account_type",
    ),
    // --- WHERE on ONE id component vs. ALL id components ---
    (
        "where_one_id_component",
        "MATCH (a:Account) WHERE a.bank_id = 'CHASE' RETURN a.account_number",
    ),
    (
        "where_all_id_components",
        "MATCH (a:Account) WHERE a.bank_id = 'CHASE' AND a.account_number = 'CHK-001' RETURN a.balance",
    ),
    (
        "where_and",
        "MATCH (a:Account) WHERE a.balance > 1000 AND a.account_type = 'Savings' RETURN a.account_number",
    ),
    (
        "where_in_list",
        "MATCH (c:Customer) WHERE c.city IN ['New York', 'Chicago'] RETURN c.name",
    ),
    // --- ordering / paging ---
    (
        "order_skip_limit",
        "MATCH (a:Account) RETURN a.account_number, a.balance ORDER BY a.balance DESC SKIP 1 LIMIT 3",
    ),
    (
        "skip_only",
        "MATCH (a:Account) RETURN a.account_number ORDER BY a.account_number SKIP 2",
    ),
    ("aggregate_count", "MATCH (a:Account) RETURN count(a)"),
    // --- single hop: single-to-composite (OWNS), both directions + undirected ---
    (
        "single_hop",
        "MATCH (c:Customer)-[:OWNS]->(a:Account) RETURN c.name, a.account_number",
    ),
    (
        "single_hop_reverse",
        "MATCH (a:Account)<-[:OWNS]-(c:Customer) RETURN a.account_number, c.name",
    ),
    (
        "undirected_hop",
        "MATCH (c:Customer)-[:OWNS]-(a:Account) RETURN c.name, a.account_number",
    ),
    // --- single hop: composite-to-composite (TRANSFERRED) — the interesting case ---
    (
        "composite_to_composite_hop",
        "MATCH (a1:Account)-[:TRANSFERRED]->(a2:Account) RETURN a1.account_number, a2.account_number",
    ),
    (
        "composite_to_composite_undirected",
        "MATCH (a1:Account)-[:TRANSFERRED]-(a2:Account) RETURN a1.account_number, a2.account_number",
    ),
    // Filter on BOTH node types across a single-to-composite hop.
    (
        "hop_filter_both",
        "MATCH (c:Customer)-[:OWNS]->(a:Account) WHERE c.city = 'New York' AND a.account_type = 'Checking' RETURN c.name, a.account_number",
    ),
    // --- whole-edge RETURN r, both edge shapes ---
    (
        "whole_edge_owns",
        "MATCH (c:Customer)-[r:OWNS]->(a:Account) RETURN r",
    ),
    (
        "whole_edge_transferred",
        "MATCH (a1:Account)-[r:TRANSFERRED]->(a2:Account) RETURN r",
    ),
    // --- OPTIONAL MATCH, single-to-composite and composite-to-composite ---
    (
        "optional_match",
        "MATCH (c:Customer) OPTIONAL MATCH (c)-[:OWNS]->(a:Account) RETURN c.name, a.account_number",
    ),
    (
        "optional_match_composite",
        "MATCH (a1:Account) OPTIONAL MATCH (a1)-[:TRANSFERRED]->(a2:Account) RETURN a1.account_number, a2.account_number",
    ),
    // --- WITH + aggregation (count per customer), and its HAVING form ---
    (
        "with_agg_count",
        "MATCH (c:Customer)-[:OWNS]->(a:Account) WITH c.name AS name, count(a) AS n RETURN name, n",
    ),
    (
        "with_having",
        "MATCH (c:Customer)-[:OWNS]->(a:Account) WITH c.name AS name, count(a) AS n WHERE n > 1 RETURN name, n",
    ),
    // GROUP BY keyed by all explicit composite-id columns (the CORRECT form —
    // contrast with `group_by_whole_node` below).
    (
        "group_by_composite_columns",
        "MATCH (a:Account)-[:TRANSFERRED]->(a2:Account) RETURN a.bank_id, a.account_number, count(a2) AS n",
    ),
    // Grouping by the bare node variable `a` (not its explicit properties) now
    // keys on the FULL composite id (`a.bank_id, a.account_number`), matching
    // `group_by_composite_columns` above. Regression for issue #457 — see the
    // comment block below and `composite_group_by_whole_node_keys_on_all_id_columns_457`.
    (
        "group_by_whole_node",
        "MATCH (a:Account)-[:TRANSFERRED]->(a2:Account) RETURN a, count(a2) AS n",
    ),
    // --- WITH -> MATCH chain (the #451 family): composite correlation across
    // the CTE barrier. Inspected carefully for ON 1=1 / partial-key joins —
    // both cases correctly force-include ALL id components in the CTE body
    // and the rebuilt JOIN condition (see KNOWN-SUSPICIOUS notes: none here).
    (
        "with_match_chain_composite",
        "MATCH (a:Account) WITH a WHERE a.balance > 5000 MATCH (a)-[:TRANSFERRED]->(a2:Account) RETURN a.account_number, a2.account_number",
    ),
    (
        "with_match_chain_single_to_composite",
        "MATCH (c:Customer) WITH c WHERE c.customer_id > 2 MATCH (c)-[:OWNS]->(a:Account) RETURN c.name, a.account_number",
    ),
    // SKIP/LIMIT inside a WITH -> CTE-body LIMIT emission path.
    (
        "with_skip_limit",
        "MATCH (a:Account) WITH a.balance AS b ORDER BY b SKIP 1 LIMIT 2 RETURN b",
    ),
    // Group by two keys across the hop.
    (
        "group_two_keys",
        "MATCH (c:Customer)-[:OWNS]->(a:Account) RETURN c.name, a.account_type, count(a) AS n",
    ),
    // --- whole-entity RETURN n (both node types; Account's composite id columns
    // are already in property_mappings, so they project like any other property —
    // no special "concat" encoding here; that encoding is reserved for id()/join
    // rendering, see `build_id_render_expr`) ---
    ("whole_entity_account", "MATCH (a:Account) RETURN a"),
    ("whole_entity_customer", "MATCH (c:Customer) RETURN c"),
    // DISTINCT over a hop projection.
    (
        "distinct_hop",
        "MATCH (c:Customer)-[:OWNS]->(a:Account) RETURN DISTINCT a.account_type",
    ),
    // Variable-length path over a composite-to-composite edge: exercises
    // to_sql_equality-based composite tuple joins inside the recursive CTE, and
    // the pipe-delimited `concat(toString(c1), '|', toString(c2), ...)` synthetic
    // path-id encoding (both dialects). Caveat (informational, not a bug): this
    // encoding assumes no id-component value literally contains '|' — a general
    // property of the concat-based synthetic id, not specific to this schema.
    (
        "vlp_composite",
        "MATCH (a1:Account)-[:TRANSFERRED*1..2]->(a2:Account) RETURN a2.account_number",
    ),
];

/// Polymorphic variation (`schemas/test/social_polymorphic.yaml`): a SINGLE
/// `brahmand.interactions` edge table holds ALL edge types (FOLLOWS / LIKES /
/// AUTHORED / COMMENTED / SHARED), discriminated by `interaction_type`, with
/// `from_type` / `to_type` label columns resolving the endpoints at query time.
/// FOLLOWS is User->User (self-referential — both endpoints scan `users_bench`,
/// so from/to need distinct aliases); LIKES/AUTHORED/COMMENTED/SHARED are
/// User->Post. Mirrors the standard corpus's feature axes for the polymorphic
/// pattern, and adds the polymorphic-specific axes: the type discriminator must
/// be visible in the SQL, the label columns must be quoted correctly, and the
/// `[:A|B]` multi-type unlabeled-endpoint case (`multi_type_hop`) must expand to
/// a real pattern-union scan, NOT the `SELECT 1 AS "_empty" WHERE false`
/// placeholder (the ERR-E / #428 pruning bug class).
///
/// Intentionally omitted / skipped:
///   - UNWIND/arrayJoin shapes — the same Spark structural gap the standard and
///     FK-edge corpora skip.
///   - Post-anchored VLP — no Post->X edge type exists, so a VLP can only start
///     at User over FOLLOWS (User->User is the only self-chainable edge type).
///   - FULLY-unlabeled single-type / any-type patterns (`(a)-[:SHARED]->(b)`,
///     `p=()-[:SHARED]->()`) — the `pattern_union_*` path emits property blobs in
///     nondeterministic HashMap order, so they cannot be byte-locked; their
///     #428 / ERR-E invariants are locked by dedicated structural tests below
///     (see also `src/render_plan/tests/polymorphic_unlabeled_path_tests.rs`).
const POLYMORPHIC_CORPUS: &[(&str, &str)] = &[
    // --- node scans (both node types) ---
    ("node_scan_user", "MATCH (u:User) RETURN u.user_id"),
    ("node_scan_post", "MATCH (p:Post) RETURN p.post_id"),
    // --- property projection incl. renamed props (name->full_name,
    // email->email_address, title/content->content, created->created_at) ---
    (
        "project_user",
        "MATCH (u:User) RETURN u.user_id, u.name, u.email",
    ),
    (
        "project_post",
        "MATCH (p:Post) RETURN p.post_id, p.title, p.content, p.created",
    ),
    ("distinct_user_name", "MATCH (u:User) RETURN DISTINCT u.name"),
    // --- WHERE filters on both node types (renamed prop, string, AND, IN) ---
    (
        "where_user_name",
        "MATCH (u:User) WHERE u.name = 'Alice Smith' RETURN u.email",
    ),
    (
        "where_and",
        "MATCH (u:User) WHERE u.user_id > 2 AND u.name = 'Bob Jones' RETURN u.user_id",
    ),
    (
        "where_in_list",
        "MATCH (u:User) WHERE u.name IN ['Alice Smith', 'Bob Jones'] RETURN u.email",
    ),
    // --- ordering / paging ---
    (
        "order_skip_limit",
        "MATCH (u:User) RETURN u.name ORDER BY u.name DESC SKIP 1 LIMIT 3",
    ),
    (
        "skip_only",
        "MATCH (u:User) RETURN u.name ORDER BY u.name SKIP 2",
    ),
    ("aggregate_count", "MATCH (u:User) RETURN count(u)"),
    // --- single edge-type hop, labeled BOTH ends. FOLLOWS is User->User, so
    // this is the self-referential case: both endpoints scan users_bench and
    // MUST get distinct aliases. The interaction_type='FOLLOWS' discriminator
    // AND the from_type='User'/to_type='User' label filters must be visible. ---
    (
        "follows_hop",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name",
    ),
    // Cross-type hop (User->Post): AUTHORED. The discriminator + differing
    // from_type='User'/to_type='Post' label filters must both appear.
    (
        "authored_hop",
        "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, p.title",
    ),
    // Reverse-written directed hop (same edge, right-to-left).
    (
        "single_hop_reverse",
        "MATCH (p:Post)<-[:AUTHORED]-(u:User) RETURN p.title, u.name",
    ),
    // UNDIRECTED hop over the polymorphic edge (ERR-G class): the edge id
    // columns must be read from the correct alias for both directions.
    (
        "undirected_hop",
        "MATCH (a:User)-[:FOLLOWS]-(b:User) RETURN a.name, b.name",
    ),
    // Filter on BOTH node types across the hop.
    (
        "hop_filter_both",
        "MATCH (u:User)-[:LIKES]->(p:Post) WHERE u.name = 'Alice Smith' AND p.title = 'Hello world!' RETURN u.user_id",
    ),
    // Edge property projection (weight->interaction_weight) read off the
    // polymorphic edge table.
    (
        "edge_property",
        "MATCH (u:User)-[r:LIKES]->(p:Post) RETURN u.name, r.weight",
    ),
    // Whole-edge RETURN r on a polymorphic relationship.
    (
        "whole_edge_r",
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN r",
    ),
    // type(r) projection over a MULTI-type edge — reads the discriminator/
    // path_relationships back out.
    (
        "rel_type_fn",
        "MATCH (a:User)-[r:FOLLOWS|LIKES]->(b) RETURN type(r) AS t",
    ),
    // --- multi-type hop [:A|B]. FOLLOWS (User->User) | LIKES (User->Post):
    // endpoint `b` is unlabeled and the two branches have DIFFERENT to-labels,
    // so this drives the polymorphic (vlp_)multi-type pattern-union path. This
    // is the byte-lockable unlabeled-endpoint case (the multi-type path sorts
    // its property columns deterministically). ---
    (
        "multi_type_hop",
        "MATCH (u:User)-[:FOLLOWS|LIKES]->(b) RETURN u.name",
    ),
    // NOTE: the FULLY-unlabeled single-type / any-type forms
    // `MATCH (a)-[:SHARED]->(b) RETURN a, b` and `MATCH p=()-[:SHARED]->()
    // RETURN p` (the ERR-E / #428 class) are NOT byte-locked here: they route
    // through the `pattern_union_*` CTE, whose node-property blobs
    // (`formatRowNoNewline(...)` / `to_json(struct(...))`) are emitted in
    // nondeterministic HashMap order (a documented latent defect — see the
    // dedicated tests `polymorphic_unlabeled_endpoints_are_real_scans_not_empty`
    // and `polymorphic_unlabeled_endpoints_current_row_multiplication`, which
    // lock the stable structural invariants instead of the flaky bytes).
    // --- OPTIONAL MATCH (anchored on User, optional AUTHORED->Post) ---
    (
        "optional_match",
        "MATCH (u:User) OPTIONAL MATCH (u)-[:AUTHORED]->(p:Post) RETURN u.name, p.title",
    ),
    // --- WITH + aggregation (followee count per user), and its HAVING form ---
    (
        "with_agg_count",
        "MATCH (u:User)-[:FOLLOWS]->(f:User) WITH u.name AS name, count(f) AS followees RETURN name, followees",
    ),
    (
        "with_having",
        "MATCH (u:User)-[:FOLLOWS]->(f:User) WITH u.name AS name, count(f) AS n WHERE n > 1 RETURN name, n",
    ),
    // --- WITH -> MATCH chain (filter users, then match their follows) ---
    (
        "with_match_chain",
        "MATCH (u:User) WITH u WHERE u.user_id > 2 MATCH (u)-[:FOLLOWS]->(f:User) RETURN u.name, f.name",
    ),
    // SKIP/LIMIT inside a WITH -> CTE-body LIMIT emission path.
    (
        "with_skip_limit",
        "MATCH (u:User) WITH u.name AS n ORDER BY n SKIP 1 LIMIT 2 RETURN n",
    ),
    // Group by two keys.
    (
        "group_two_keys",
        "MATCH (u:User) RETURN u.name, u.email, count(u) AS n",
    ),
    // --- whole-entity RETURN n (both node types) ---
    ("whole_entity_user", "MATCH (u:User) RETURN u"),
    ("whole_entity_post", "MATCH (p:Post) RETURN p"),
    // --- VLP *1..2 over the single self-chainable edge type (FOLLOWS,
    // User->User). Multi-type VLP is intentionally NOT locked: no second
    // User->User edge type exists to chain, so a multi-type VLP over the
    // polymorphic edge has no meaningful corpus query here. ---
    (
        "vlp_follows",
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN b.user_id",
    ),
];

/// Browser-shaped patterns (P0.5), FK-edge variation: the schema-sensitive
/// subset of `BROWSER_CORPUS` (node scan / expand / path / count / labels)
/// re-run on `schemas/test/fk_edge.yaml` (Order/Customer, single PLACED_BY
/// edge type — the edge table IS a node table). Confirms the standard-schema
/// findings above are NOT standard-schema-specific:
///
///   - `fk_unlabeled_expand`: FIXED (#466). The undirected fully-unlabeled
///     expand `(n)-[r]-(o)` now routes through a `pattern_union` CTE (a
///     single NON-self-referential edge type stored as `pattern_combinations`
///     specifically because the pattern is undirected — see
///     `logical_plan::match_clause::traversal`) and emits BOTH the
///     Order->Customer forward branch AND the Customer->Order reverse branch
///     (same edge/join, start and end swapped). Confirmed live: returns 16,
///     not 8 (8 forward × 2 orientations). The DIRECTED form `(n)-[r]->(o)`
///     stays on the plain single-table join path (control unchanged at 8).
///   - `fk_browser_style_count`: FIXED (#467). Previously `count(n.customer_id)`
///     (Customer's id) undercounted (4 not 12) since Order-branch rows NULL-pad
///     that column. Now `count(coalesce(n.customer_id, n.order_id))` — live 12.
///
/// Verified CORRECT: `fk_unlabeled_node_scan` (heterogeneous UNION ALL,
/// deterministic); `fk_path_unlabeled` (fixed_path over the FK-edge join, no
/// separate edge table to join since PLACED_BY IS orders_fk, deterministic);
/// `fk_browser_labels_probe` (clean per-label UNION ALL).
const FK_EDGE_BROWSER_CORPUS: &[(&str, &str)] = &[
    ("fk_unlabeled_node_scan", "MATCH (n) RETURN n LIMIT 25"),
    (
        "fk_unlabeled_expand",
        "MATCH (n)-[r]-(o) RETURN n, r, o LIMIT 25",
    ),
    ("fk_path_unlabeled", "MATCH p=()-[]->() RETURN p LIMIT 10"),
    ("fk_browser_style_count", "MATCH (n) RETURN count(n)"),
    (
        "fk_browser_labels_probe",
        "MATCH (n) RETURN DISTINCT labels(n)",
    ),
];

/// Browser-shaped patterns (P0.5), Denormalized variation: the
/// schema-sensitive subset re-run on `schemas/dev/flights_denormalized.yaml`
/// (single self-referential Airport/FLIGHT coupled-denorm table). A
/// deliberate CONTRAST set: the fully-unlabeled undirected expand and the
/// heterogeneous count were BOTH already correct here before the #466 /
/// #467 fixes — documented so those fixes (and future ones) don't
/// accidentally regress this single-label schema. The `dn_unlabeled_expand`
/// golden is UNCHANGED by #466.
///
///   - `dn_unlabeled_expand` correctly emits BOTH direction branches (the
///     `bidirectional_union` UNION ALL over the single `flights_denorm` table
///     with from/to swapped). FLIGHT is SELF-referential (Airport->Airport),
///     so the reverse branch is schema-valid on the plain bidirectional_union
///     path — it never hit the non-self-referential reverse-branch gap that
///     #466 fixed for Standard/FK-edge, and is deliberately left on that path
///     (only non-self-referential single-type undirected patterns are routed
///     to `pattern_union` by the #466 fix) so its SQL is unchanged.
///   - `dn_browser_style_count`: `count(a)` renders `count(a.code)` (one
///     label, so the #467 fix leaves it single-column), and `code` (the
///     virtual node_id, mapped via from/to_node_properties) is populated on
///     EVERY UNION branch (origin_code / dest_code), so it does NOT undercount.
///
/// `dn_path_unlabeled` was originally NOT a byte-golden: the fixed_path
/// edge-property column order (`t3.distance`/`t3.flight_num`/`t3.carrier`/...)
/// was emitted in nondeterministic HashMap order — the same latent defect
/// documented for `denorm_path_return` in the P0.2/#459 known-suspicious block
/// above. RESTORED as a BYTE-GOLDEN by #480 (`get_node_properties`/
/// `get_relationship_properties` now sort by cypher property name, and
/// `expand_cte_entity` sorts its merged denormalized list); verified
/// byte-identical across 10 fresh-process renders. The structural test
/// `denorm_path_unlabeled_column_set_is_stable` below is retained as a
/// documented invariant lock (origin_code/dest_code role-correct resolution).
const DENORM_BROWSER_CORPUS: &[(&str, &str)] = &[
    (
        "dn_unlabeled_expand",
        "MATCH (a)-[r]-(b) RETURN a, r, b LIMIT 25",
    ),
    ("dn_browser_style_count", "MATCH (a) RETURN count(a)"),
    // VLP path return on the denormalized pattern (#469): the denormalized
    // VLP CTE strategy must project `path_relationships` (populated because a
    // path variable is bound) so the path tuple
    // tuple(t.path_nodes, t.path_relationships, t.hop_count) resolves.
    // Live-verified on db_denormalized: `[[LAX, JFK], [FLIGHT], 1]`.
    (
        "dn_path_vlp",
        "MATCH p=(a:Airport)-[:FLIGHT*1..2]->(b:Airport) RETURN p LIMIT 5",
    ),
    ("dn_path_unlabeled", "MATCH p=()-[]->() RETURN p LIMIT 10"),
];

fn load_schema(yaml_path: &str) -> GraphSchema {
    GraphSchemaConfig::from_yaml_file(yaml_path)
        .unwrap_or_else(|e| panic!("load schema {yaml_path}: {e:?}"))
        .to_graph_schema()
        .unwrap_or_else(|e| panic!("convert {yaml_path} to GraphSchema: {e:?}"))
}

/// Render through the PRODUCTION path: `to_render_plan_with_ctx` with the
/// planner's `PlanCtx`, exactly as `cypher_to_sql` (server / cg / embedded)
/// does — the single, production-faithful render function for the golden net
/// (issue #459). It is step-for-step equivalent to `Connection::query_to_sql`:
/// parse → `evaluate_read_statement` → `logical_plan_to_render_plan_with_ctx`
/// (passing the planner's `PlanCtx`) → `to_sql()`.
///
/// Before #459 the harness rendered via the ctx-less `logical_plan_to_render_plan`
/// wrapper (no `PlanCtx`), which has NO production callers and demonstrably
/// diverges from server output for polymorphic / multi-type expands, denorm
/// node-scan unions, ORDER BY alias resolution, and the OPTIONAL denorm hop
/// (see #454/#455/#456/#458). The net now locks the artifact production actually
/// emits.
async fn render(schema: &GraphSchema, cypher: &str, dialect: SqlDialect) -> String {
    let schema = schema.clone();
    let cypher = cypher.to_string();
    let ctx = QueryContext {
        dialect,
        ..QueryContext::default()
    };
    with_query_context(ctx, async move {
        set_current_schema(Arc::new(schema.clone()));
        let cleaned = strip_comments(&cypher);
        let (_rest, statement) =
            parse_cypher_statement(&cleaned).unwrap_or_else(|e| panic!("parse: {e:?}"));
        let (logical_plan, plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .unwrap_or_else(|e| panic!("plan: {e:?}"));
        let render_plan =
            logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx))
                .unwrap_or_else(|e| panic!("render: {e:?}"));
        render_plan.to_sql()
    })
    .await
}

/// Like [`render`] but surfaces planner/render errors as `Err(message)`
/// instead of panicking — for asserting clean-error behavior (#466).
async fn try_render(
    schema: &GraphSchema,
    cypher: &str,
    dialect: SqlDialect,
) -> Result<String, String> {
    let schema = schema.clone();
    let cypher = cypher.to_string();
    let ctx = QueryContext {
        dialect,
        ..QueryContext::default()
    };
    with_query_context(ctx, async move {
        set_current_schema(Arc::new(schema.clone()));
        let cleaned = strip_comments(&cypher);
        let (_rest, statement) =
            parse_cypher_statement(&cleaned).map_err(|e| format!("parse: {e:?}"))?;
        let (logical_plan, plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .map_err(|e| format!("plan: {e:?}"))?;
        let render_plan =
            logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx))
                .map_err(|e| format!("render: {e:?}"))?;
        Ok(render_plan.to_sql())
    })
    .await
}

/// Anonymize the two process-global counters whose values vary with test
/// ordering/concurrency: `ALIAS_COUNTER` (anonymous rel aliases `t{n}`) and
/// `CTE_COUNTER` (`cte{n}`). Each is remapped by first appearance, so goldens
/// are deterministic while structure (which alias joins where) is preserved.
///
/// CAUTION: this is text-blind — it rewrites any `t<digits>`/`cte<digits>`
/// token, including ones inside string literals or a schema column literally
/// named `t5`. Today the corpus contains no such token (verified), but if you
/// add a query whose SQL contains a non-counter `t<n>`/`cte<n>`, tighten this
/// (e.g. scope to the alias-defining position) so a real regression in that
/// token can't be silently normalized away.
///
/// `pub(crate)` (not private): the P0.6 corpus sweep (`corpus_sweep.rs`, same
/// `integration` test binary) reuses this exact anonymization for its own,
/// much larger corpus — see docs/design/REFACTORING_SAFETY_PLAN.md §3.2.
pub(crate) fn normalize(sql: &str) -> String {
    fn remap(input: &str, pattern: &str, prefix: &str) -> String {
        let re = regex::Regex::new(pattern).unwrap();
        let mut seen: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let mut next = 0usize;
        for m in re.find_iter(input) {
            seen.entry(m.as_str().to_string()).or_insert_with(|| {
                let p = format!("{prefix}{next}");
                next += 1;
                p
            });
        }
        re.replace_all(input, |c: &regex::Captures| seen[&c[0].to_string()].clone())
            .into_owned()
    }
    let s = remap(sql, r"\bt\d+\b", "t");
    remap(&s, r"\bcte\d+\b", "cte")
}

fn golden_path(schema_dir: &str, name: &str, dialect: &str) -> String {
    format!(
        "{}/tests/rust/integration/golden/sql_ir/{}/{}__{}.sql",
        env!("CARGO_MANIFEST_DIR"),
        schema_dir,
        name,
        dialect
    )
}

// KNOWN-SUSPICIOUS FK-edge goldens — locked as *current behavior* (a
// characterization net locks what the engine does today, including any latent
// wrongness, so a refactor's diff is visible). All 26 CH goldens EXECUTE on a
// live db_fk_edge (scripts/setup/setup_fk_edge_data.sh); the notes below are
// from inspecting SQL + comparing result-set row counts to Cypher semantics.
// If you touch FK-edge rendering, inspect these first:
//
//   - fk_edge/with_match_chain: FIXED (#451). The WITH->MATCH chain now emits
//     `INNER JOIN with_c_cte_0 AS c ON c.p1_c_customer_id = o.customer_id` and
//     the CTE force-includes the `customer_id` join key. Previously it dropped
//     the key and cross-joined `ON 1 = 1` (24 rows on live CH); now it returns
//     the correct 5 rows. Root cause: `prune_joins_covered_by_cte` removed the
//     analyzer's FK-edge correlation join and the ON-condition rebuild had no
//     matching entry in `original_correlation_predicates` (those only carry
//     explicit WHERE-style predicates, not graph-pattern edges), so it fell
//     back to a cartesian join. The pruned cross-barrier correlation is now
//     recovered and folded back into the rebuild. Kept as a normal lock.
//
// FIXED (#452) — fk_edge/optional_match: previously emitted a redundant PHANTOM
// self-join `LEFT JOIN orders_fk AS t0 ON t0.order_id = o.order_id` that
// re-materialized the edge table separately from the Order node it IS (1:1 on
// the PK). Now collapsed by `remove_redundant_edge_self_joins` in
// `plan_optimizer.rs`; still 8 rows on live CH, one fewer JOIN. Kept as a normal
// (no longer suspicious) golden lock.
//
//   - fk_edge/optional_after_with: FIXED (#453). `MATCH (c:Customer) WITH c
//     OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c)` renders the required Customer
//     as `with_c_cte_0` and the fresh Order pattern as optional. It previously
//     emitted `FROM orders_fk AS o INNER JOIN with_c_cte_0 AS c` — the optional
//     side drove the query and the required CTE was INNER-joined, dropping every
//     customer with no order (a live no-order customer returned 8 rows instead
//     of 9, wrong OPTIONAL semantics). Now the CTE anchor is the FROM driver and
//     the Order pattern LEFT-joins to it: `FROM with_c_cte_0 AS c LEFT JOIN
//     db_fk_edge.orders_fk AS o ON o.customer_id = c.p1_c_customer_id`. Root
//     cause: `has_optional_match_input` only inspected WITH-clause *bodies*, so a
//     post-WITH OPTIONAL MATCH fell through to the plain hardcoded
//     `JoinType::Inner` CTE-join emission; the fix adds a `is_optional_pattern()`
//     restructure in `build_chained_with_match_cte_plan` that promotes the CTE to
//     FROM and LEFT-joins the optional pattern. Kept as a normal lock.
//
//   - fk_edge/optional_after_with_where: FIXED (#460). The same shape with a
//     `WHERE o.amount > 100` on the optional side previously rendered BYTE-
//     IDENTICALLY to optional_after_with (predicate silently dropped, every order
//     returned). The #453 restructure discarded the join the optional-side
//     predicate was destined for; the fix re-extracts predicates referencing only
//     the optional alias and attaches them to the demoted LEFT JOIN's pre_filter,
//     rendering `LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE
//     total_amount > 100) AS o` — filter before the join, no-match customers stay
//     NULL-extended. Live: 7 rows (was an unfiltered 12). Kept as a normal lock.
//
// Verified CORRECT (kept as normal locks, not suspicious): single_hop /
// single_hop_reverse / undirected_hop all render the node-to-node FK join
// `customers_fk.customer_id = orders_fk.customer_id` with the edge id column
// read from the correct (orders_fk) alias — no ERR-G regression, 8 rows each;
// whole_edge_r projects the FK-edge row (order_id AS from_id, customer_id AS
// to_id), 8 rows.

// KNOWN-SUSPICIOUS/KNOWN-BROKEN denormalized goldens (P0.2). The denorm
// variation has the heaviest documented bug history, so — per the plan's
// characterization-net philosophy (`REFACTORING_SAFETY_PLAN.md` §3.1/§3.2, which
// locks even error-producing translations) — these lock *current* behavior,
// INCLUDING output that is invalid or semantically wrong, so any fix shows up as
// a reviewable golden diff. All 26 cases RENDER (no Rust panic); the notes below
// are from executing every ClickHouse golden against live `db_denormalized`
// (scripts/setup/setup_denormalized_data.sh, 8 flights). If you touch denorm
// rendering, inspect these first.
//
// SCOPE NOTE for Groups A and B (#454/#455, established at review): both defects
// lived ONLY in the ctx-less `to_render_plan` path that this golden harness
// renders through — the production path (`to_render_plan_with_ctx`: server, cg,
// embedded) emitted CORRECT SQL for all these shapes the whole time (verified by
// running the repros through a merge-base `cg` build). Users never saw the broken
// SQL. The fixes converge the harness path onto production output, so these
// goldens now lock the real thing — two more confirmed instances of the
// golden-vs-production divergence tracked as issue #459.
//
// GROUP A — RESOLVED (#454). The 7 node-only cases (node_scan, whole_node,
// project_node_props, where_denorm_prop, where_virtual_id, distinct_node_state,
// aggregate_count_node) previously ALL rendered BYTE-IDENTICAL broken output:
//   `WITH __multi_label_union AS (SELECT 'Airport' as _label,
//    toString(code) as _id, formatRowNoNewline('JSONEachRow',
//    flights_denorm.code AS code) as _properties FROM db_denormalized.flights_denorm)`
// — the virtual node_id `code` (mapped to origin_code/dest_code via
// from/to_node_properties, no standalone physical column) was emitted verbatim as
// the non-existent `flights_denorm.code`, and the RETURN/WHERE/DISTINCT/count were
// all dropped (every shape collapsed to the whole-node Browser format).
// Root cause: the from/to UNION branches are wrapped in `GraphJoins`, but the
// `is_denormalized_union` guard in the Union render handler did not traverse
// `GraphJoins`, so the union was misclassified as a multi-label scan and routed to
// the json_builder whole-node path. Fixed by adding the missing GraphJoins/Limit
// arms (mirroring `is_node_scan_input`). A companion fix in the Projection handler
// moves the base branch into `union.input` for aggregation-over-union so
// `count(a)` counts BOTH from/to branches (was dropping the origin branch). The 7
// goldens now DIVERGE, each projecting/filtering as written, and all execute on
// live `db_denormalized` (7 distinct airports; see
// `denorm_labeled_node_scan_resolves_virtual_id_454`).
//
// GROUP B — RESOLVED (#455). ORDER BY over a denorm hop previously mis-qualified
// the node-id column (order_skip_limit, skip_only): the SELECT was correct
// (`t0.origin_code AS "a.code"` from `flights_denorm AS t0`) but the ORDER BY
// emitted the CYPHER alias — `ORDER BY a.origin_code` — which CH rejects
// (`Unknown expression identifier 'a.origin_code'`). Root cause: the non-ctx
// `to_render_plan` OrderBy handler converted items with a bare `try_into()` and
// skipped the alias→edge-table resolution that SELECT/WHERE (and the server's
// `extract_order_by`) apply; the column was resolved at planning but the alias was
// not. Fixed by running `apply_property_mapping_to_expr` on the order-by items in
// that handler (golden/server parity). Both now emit `ORDER BY t0.origin_code` and
// execute on live CH with correct ordered slices. Paging mechanics (`LIMIT off, n`,
// CH huge-upper-bound for bare SKIP) were already correct.
//
// GROUP C:
//   - with_match_chain: RESOLVED (#456). `MATCH (a:Airport) WITH a WHERE
//     a.state='CA' MATCH (a)-[:FLIGHT]->(b)` yields flights FROM CA airports
//     (4 rows). Previously returned 7: `WITH a` materializes `a` as a from/to
//     UNION, and the post-WITH `WHERE a.state='CA'` — resolved position-blind to
//     the origin column and copied verbatim to every branch — filtered the dest
//     branch on `a.origin_state` instead of `a.dest_state`, polluting the airport
//     set with the destinations of CA-origin flights (LAX,SFO,JFK,ORD,ATL). Fixed
//     by re-pointing the propagated WHERE per UNION branch to that branch's own
//     column for the same exported property (`plan_builder_utils.rs`,
//     build_chained_with_match_cte_plan). Both render paths shared the bug, so the
//     golden updated (dest branch now `WHERE a.dest_state = 'CA'`); live 4 rows.
//     Regression: `denorm_with_match_chain_filters_per_branch_column_456`.
//   - optional_match: `MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b)`.
//     RESTORED as a BYTE-GOLDEN by #464 (back in DENORM_CORPUS; also locked by
//     `denorm_optional_match_resolves_to_node_onto_edge_456`). History: before
//     #459 the harness rendered via the ctx-less path, which emitted a degenerate
//     inner scan (`FROM flights_denorm AS t0`, no LEFT JOIN, 8 rows, dropped PHX —
//     the destination-only airport). #459 re-points the net at production
//     (`to_render_plan_with_ctx`: server/cg/embedded), which materializes `a` as
//     the from/to UNION CTE `__denorm_scan_a` and LEFT-JOINs the edge (the correct
//     OPTIONAL shape — this is where #456's `to_node_properties` reverse-map fix
//     keeps `b.code` resolved to the edge's `t0.dest_code`, not a phantom table).
//     #459 could not byte-lock it for TWO reasons, BOTH fixed by #464:
//       1. NONDETERMINISTIC COLUMN ORDER (fixed #464): the `__denorm_scan_a`
//          from/to-union branches projected their node-property columns in HashMap
//          iteration order (select_builder ViewScan `property_mapping` fallback).
//          #464 sorts that projection by cypher property name, so the order is
//          stable across process runs.
//       2. POSITIONAL-UNION COLUMN SCRAMBLE (semantic bug, fixed #464): because
//          the origin and dest branches received DIFFERENT column orders, and SQL
//          UNION aligns by POSITION, the CTE's `code`/`state` columns got swapped
//          on one branch. Live on `db_denormalized` (2026-07-06) this produced 14
//          rows with `a.code` holding STATE values (NY, IL, CA, …) instead of
//          airport codes — NOT the intended 9 (8 flights + PHX). The #464 sort
//          gives both branches the same canonical order, so alignment-by-position
//          is now structurally correct; live re-verified 9 correct rows.
//
// Verified CORRECT (normal locks, not suspicious) — executed on live CH:
// directed_hop_ids (8), directed_hop_props, reverse_hop (8), undirected_hop (16,
// each edge both directions), hop_edge_props (renamed flight_num -> physical
// flight_number), where_edge_prop (distance>1000 -> 6), hop_filter_both
// (CA->NY -> 1), whole_edge_r (8, composite edge_id + all edge props),
// vlp_recursive *1..2 (DenormalizedCteStrategy, 15),
// with_agg_count (out-degree per origin, 6), with_having (>1 -> 1),
// with_skip_limit (3), group_two_keys (distinct state pairs, 7), distinct_hop
// (5 origin states). All hop cases render from the SINGLE `flights_denorm` table
// with NO spurious edge self-join (the #419 class is clean) and correct
// from-side (origin_*) vs to-side (dest_*) column sourcing.
//
//   - path_return (`MATCH p = (a:Airport)-[:FLIGHT]->(b:Airport) RETURN p`):
//     RESTORED as a BYTE-GOLDEN by #464 (back in DENORM_CORPUS; also locked by
//     `denorm_path_return_materializes_node_edge_props_459`). The ctx-less path
//     emitted ONLY the `tuple('fixed_path', 'a', 'b', 't0')` path marker with no
//     underlying columns — a path with no reconstructable node/edge data.
//     Production correctly materializes every a.*/b.*/t0.* property column of the
//     path off the single `flights_denorm` scan; #459 could not byte-lock it
//     because the edge property columns came out in HashMap order
//     (`get_relationship_properties`, iterated at the denorm-rel path expansion in
//     select_builder). #464 sorts that expansion by cypher property name (the
//     node a.*/b.* columns were already sorted), so the whole path projection is
//     byte-stable.
//
// Databricks goldens for all 26 cases are locked but NOT executed (no live Spark
// here); they render without panic and mirror the CH structure.

// KNOWN-SUSPICIOUS composite-id goldens. All JOIN-shaped goldens in this corpus
// (single_hop* / undirected_hop / composite_to_composite_* / hop_filter_both /
// whole_edge_* / optional_match* / with_match_chain_* / group_by_composite_columns
// / vlp_composite) were inspected and EXECUTE correctly on live `db_composite_id`
// (`scripts/setup/setup_composite_id_data.sh`): every JOIN condition carries ALL
// id components on both sides (e.g. `(a1.bank_id, a1.account_number) = (t.from_bank_id,
// t.from_account_number)`), matching `sql_equality`/`add_identifier_condition`'s
// per-column-pair construction (`src/query_planner/analyzer/graph_join/helpers.rs`).
// The `remove_redundant_edge_self_joins` FK-edge-phantom-join optimizer pass is
// N/A here: this schema's edges are genuinely separate tables (not an FK-edge
// pattern where the edge table IS a node table), so there is no phantom-self-join
// candidate for that pass to ever consider — confirmed by inspecting
// `optional_match`/`optional_match_composite` (a plain LEFT JOIN chain, no
// self-join at all) and by the pass's own single-column-identity guard (composite
// ids can never satisfy `Identifier::Single`, so it is conservatively skipped
// regardless).
//
//   - composite_id/group_by_whole_node: FIXED (issue #457). Now renders
//     `GROUP BY a.bank_id, a.account_number` — the FULL composite key — so each
//     distinct Account is its own bucket (8 buckets on the fixture, not 2), and
//     `account_number` is projected as a bare grouping key rather than
//     `anyLast()`-wrapped. `MATCH (a:Account)-[:TRANSFERRED]->(a2:Account)
//     RETURN a, count(a2) AS n` used to emit `GROUP BY a.bank_id` only, silently
//     merging every Account sharing a bank_id (e.g. CHASE's 4 accounts) into one
//     row. Contrast unchanged: `group_by_composite_columns` (explicit
//     `RETURN a.bank_id, a.account_number, count(a2)`) always keyed correctly.
//     Fix: the whole-node GROUP BY optimization in
//     `handle_table_alias_group_by`/`handle_wildcard_group_by`
//     (`src/render_plan/group_by_builder.rs`) now resolves the node label from
//     the plan and asks the task-local schema for the complete
//     `node_id.columns()` set (via `composite_id_group_by_columns`), pushing one
//     `PropertyAccessExp` per identity column instead of forwarding the single
//     `ViewScan.id_column`. Only composite ids take this path; single-column,
//     denormalized/virtual, VLP and CTE-backed aliases keep the established
//     single-column `find_id_column_for_alias` behavior. The first-column-only
//     value is STILL used (unchanged, harmlessly) for `count(a)`-style aggregates
//     (`aggregate_count`, `group_by_composite_columns`'s own `count(a2)`): COUNT
//     only needs one non-null column to detect row existence, so its argument is
//     deliberately left alone. The SAME collapse also existed on the WITH→CTE
//     render path (near-verbatim copies of the optimization in
//     `plan_builder_utils.rs`: `extract_group_by`'s GroupBy arm and
//     `expand_table_alias_to_group_by_id_only` — the latter fires for
//     `WITH a, count(a2) AS n` shapes and rendered `GROUP BY a.bank_id` inside
//     the CTE body, 2 collapsed bank buckets live instead of 6); all copies now
//     route through the shared `composite_id_group_by_columns` helper (see its
//     §1.4 multiplication note). Regression tests:
//     `composite_group_by_whole_node_keys_on_all_id_columns_457`,
//     `composite_group_by_whole_node_behind_with_barrier_457`.

// KNOWN-SUSPICIOUS Polymorphic goldens — locked as *current behavior* (the net
// characterizes what the engine does today, including latent wrongness, so a
// refactor's diff is visible). All 29 CH goldens EXECUTE on the live
// `clickhouse-test` container with the polymorphic fixture seeded into
// `brahmand.{users_bench,posts_bench,interactions}` (10 FOLLOWS User->User,
// 6 LIKES / 5 AUTHORED / 5 COMMENTED / 3 SHARED, all User->Post; 29 rows total,
// same data as scripts/setup/setup_polymorphic_data.sh). Row counts below are
// from that live run vs. Cypher semantics. If you touch polymorphic rendering,
// inspect these first:
//
//   - polymorphic/whole_edge_r  [FIXED in #458 — LABELED single-type shape only]:
//     `MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN r` now renders
//     `FROM brahmand.interactions AS r WHERE r.interaction_type = 'FOLLOWS' AND
//     r.from_type = 'User' AND r.to_type = 'User'` and returns the 10 FOLLOWS
//     edges (was: all 29 rows, no WHERE). Root cause: projecting only `r` prunes
//     the labeled endpoints, so the analyzer switches to the `SingleTableScan`
//     strategy, which makes the polymorphic edge itself the FROM marker while
//     keeping the type/label discriminators on its `pre_filter`. The render
//     pipeline discarded FROM-marker `pre_filter`s; the fix promotes that
//     pre_filter into the WHERE clause (`from_marker_pre_filter` in
//     plan_builder.rs) so the edge-own filters survive endpoint pruning
//     (whole-edge-projection sibling of the #433 element-id/edge-linkage class).
//     SCOPE: the promotion is gated on the marker's own table being the rendered
//     FROM. When endpoints are UNLABELED (or the rel is multi-type / VLP),
//     extract_from renders a `pattern_union_*`/rel/VLP CTE instead while the
//     marker still carries the FIRST branch's discriminator — promoting there
//     would reference columns the CTE doesn't project and collapse the union
//     (proven live regression, fixed by the gate). For those shapes the
//     per-branch filters inside the CTE do the discriminating and NO outer
//     WHERE is emitted. Regressions:
//     `polymorphic_whole_edge_r_keeps_discriminator` (labeled, both render
//     paths) and `polymorphic_unlabeled_whole_edge_no_outer_discriminator`
//     (unlabeled / multi-type / VLP whole-edge, both render paths).
//
//   - FULLY-unlabeled patterns (`(a)-[:SHARED]->(b)`, `p=()-[:SHARED]->()`) are
//     NOT byte-goldens (their `pattern_union_*` property blobs are emitted in
//     nondeterministic HashMap order — a documented latent defect). Their two
//     findings are locked instead by dedicated tests below:
//       * `polymorphic_unlabeled_endpoints_are_real_scans_not_empty`
//         [GOOD, #428 fixed]: the edge IS scanned — the `pattern_union_*` CTE
//         enumerates all four (from_label, to_label) combinations as real
//         UNION-ALL branches, each with its own interaction_type/from_type/
//         to_type filter, NEVER the `SELECT 1 AS "_empty" WHERE false`
//         placeholder.
//       * `polymorphic_unlabeled_endpoints_single_outer_read`
//         [GOOD — production, #459]: the OUTER query reads the already-complete
//         `pattern_union_*` CTE exactly ONCE, so each enumerated path row is
//         emitted a single time. `render()` IS the production path
//         (`to_render_plan_with_ctx`, used by every server/cg/embedded query)
//         since #459; live CH via cg returns `(a)-[:FOLLOWS]->(b) RETURN a, b`
//         = 10 and `(a)-[:SHARED]->(b) RETURN a, b` = 3 (correct). Historical
//         note: the removed ctx-less render path emitted a 4× outer UNION-ALL
//         over this CTE (row multiplication) — a harness-only artifact with no
//         production callers, gone with that path in #459. (Whole-edge
//         `RETURN r` over these unlabeled shapes is a separate axis — see the
//         whole_edge_r note above.)
//
// Verified CORRECT (kept as normal locks, not suspicious): follows_hop (10,
// self-referential User->User with distinct a/b aliases) / authored_hop (5,
// User->Post) / single_hop_reverse (5) / edge_property (6) / hop_filter_both (1)
// all render `interaction_type` + `from_type`/`to_type` filters on the edge
// scan; multi_type_hop and rel_type_fn (16 = 10 FOLLOWS + 6 LIKES) route the
// `[:A|B]` unlabeled-endpoint pattern through the multi-type pattern-union CTE
// with per-branch discriminators; undirected_hop (20 = 10 edges × both
// directions) is the standard 2-direction union; optional_match (5, LEFT JOIN
// with the discriminator pushed into the joined subquery); with_agg_count /
// with_having (5) / with_match_chain (6, WITH-CTE join on the exported user_id) /
// vlp_follows (24, recursive CTE with the discriminator in both base and step)
// all carry the type filter correctly.

#[tokio::test]
async fn sql_golden_snapshots() {
    let update = std::env::var("UPDATE_GOLDEN").as_deref() == Ok("1");
    let mut mismatches: Vec<String> = Vec::new();

    for (schema_id, corpus) in [
        (SchemaId::Standard, CORPUS),
        (SchemaId::FkEdge, FK_EDGE_CORPUS),
        (SchemaId::Denormalized, DENORM_CORPUS),
        (SchemaId::CompositeId, COMPOSITE_ID_CORPUS),
        (SchemaId::Polymorphic, POLYMORPHIC_CORPUS),
        // P0.5 — Browser-shaped (unlabeled) patterns.
        (SchemaId::Standard, BROWSER_CORPUS),
        (SchemaId::FkEdge, FK_EDGE_BROWSER_CORPUS),
        (SchemaId::Denormalized, DENORM_BROWSER_CORPUS),
    ] {
        let schema = load_schema(schema_id.yaml_path());
        let schema_dir = schema_id.dir();

        for (name, cypher) in corpus {
            for (dialect, dname) in [
                (SqlDialect::ClickHouse, "clickhouse"),
                (SqlDialect::Databricks, "databricks"),
            ] {
                let sql = normalize(&render(&schema, cypher, dialect).await);
                // Guard against a vacuous pass (e.g. a future to_sql() returning "").
                assert!(
                    sql.contains("SELECT"),
                    "{schema_dir}/{name}__{dname} produced SQL without SELECT:\n{sql}"
                );
                let path = golden_path(schema_dir, name, dname);

                if update {
                    if let Some(dir) = std::path::Path::new(&path).parent() {
                        std::fs::create_dir_all(dir).expect("create golden dir");
                    }
                    std::fs::write(&path, &sql).expect("write golden");
                } else {
                    match std::fs::read_to_string(&path) {
                        Ok(expected) if expected == sql => {}
                        Ok(expected) => mismatches.push(format!(
                            "--- {schema_dir}/{name}__{dname} MISMATCH ---\nEXPECTED:\n{expected}\nACTUAL:\n{sql}\n"
                        )),
                        Err(_) => mismatches.push(format!(
                            "--- {schema_dir}/{name}__{dname} MISSING golden (run UPDATE_GOLDEN=1) ---"
                        )),
                    }
                }
            }
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} golden mismatch(es):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

/// Regression: #451 — an FK-edge `WITH c ... MATCH (c)<-[:PLACED_BY]-(o)` chain
/// must join the WITH-CTE on the exported node's id key, NOT cartesian `ON 1 = 1`.
/// Previously `prune_joins_covered_by_cte` dropped the analyzer's FK correlation
/// join and the id column was pruned from the CTE, yielding a cross product
/// (24 rows on live CH instead of 5).
#[tokio::test]
async fn with_cte_join_key_is_correlated_not_cartesian_451() {
    let schema = load_schema("schemas/test/fk_edge.yaml");
    let cypher = "MATCH (c:Customer) WITH c WHERE c.customer_id > 100 \
                  MATCH (c)<-[:PLACED_BY]-(o:Order) RETURN c.name, o.order_id";

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, cypher, dialect).await;

        // The CTE JOIN must carry the real correlation on the customer id key.
        assert!(
            sql.contains("c.p1_c_customer_id = o.customer_id"),
            "expected CTE join on the customer_id key for {dialect:?}, got:\n{sql}"
        );
        // And must NOT degrade to a cartesian product.
        assert!(
            !sql.contains("ON 1 = 1"),
            "CTE join must not be a cartesian `ON 1 = 1` for {dialect:?}, got:\n{sql}"
        );
        // The exported node's id column must be force-included in the CTE body,
        // even though only `c.name` is projected downstream.
        assert!(
            sql.contains("p1_c_customer_id"),
            "expected CTE to project the customer_id join key for {dialect:?}, got:\n{sql}"
        );
    }
}

/// Regression for #457: GROUP BY on a bare composite-id node variable must key
/// on ALL id columns, not just the first. `MATCH (a:Account)-[:TRANSFERRED]->
/// (a2:Account) RETURN a, count(a2)` previously emitted `GROUP BY a.bank_id`
/// only, silently collapsing every Account that shared a bank_id into one bucket
/// (count summed across them, other identity columns returned an arbitrary
/// member's value). The whole-node GROUP BY optimization in `group_by_builder.rs`
/// now expands to the full `node_id.columns()` set via the schema.
#[tokio::test]
async fn composite_group_by_whole_node_keys_on_all_id_columns_457() {
    let schema = load_schema(SchemaId::CompositeId.yaml_path());
    let cypher = "MATCH (a:Account)-[:TRANSFERRED]->(a2:Account) RETURN a, count(a2) AS n";

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, cypher, dialect).await;

        // BOTH composite-id columns must appear as GROUP BY keys.
        assert!(
            sql.contains("GROUP BY a.bank_id, a.account_number"),
            "expected GROUP BY on BOTH composite id columns for {dialect:?}, got:\n{sql}"
        );
        // The old bug emitted a bare `GROUP BY a.bank_id` (no second key).
        assert!(
            !sql.trim_end().ends_with("GROUP BY a.bank_id"),
            "GROUP BY must not collapse to the first id column only for {dialect:?}, got:\n{sql}"
        );
        // account_number is a grouping key now, so it must NOT be wrapped in an
        // aggregate (anyLast/any_value) in the SELECT list.
        assert!(
            !sql.contains("anyLast(a.account_number)")
                && !sql.contains("any_value(a.account_number)"),
            "account_number is a GROUP BY key and must not be aggregate-wrapped for {dialect:?}, got:\n{sql}"
        );
    }
}

/// Regression for #457, WITH-barrier form: the same whole-node GROUP BY
/// collapse existed on the WITH→CTE render path (a separate near-verbatim copy
/// of the id-only optimization: `expand_table_alias_to_group_by_id_only` /
/// `extract_group_by` in `plan_builder_utils.rs` — see the §1.4 triplication
/// note on `composite_id_group_by_columns`). `MATCH (a:Account)-[:TRANSFERRED]->
/// (a2:Account) WITH a, count(a2) AS n RETURN a, n` previously rendered
/// `GROUP BY a.bank_id` inside the CTE body (2 collapsed bank buckets on the
/// live fixture instead of 6 per-account buckets). All copies now share
/// `composite_id_group_by_columns` and emit every id column.
#[tokio::test]
async fn composite_group_by_whole_node_behind_with_barrier_457() {
    let schema = load_schema(SchemaId::CompositeId.yaml_path());
    let cypher =
        "MATCH (a:Account)-[:TRANSFERRED]->(a2:Account) WITH a, count(a2) AS n RETURN a, n";
    // The old bug emitted `GROUP BY a.bank_id` with no second key anywhere.
    let collapsed = regex::Regex::new(r"GROUP BY a\.bank_id\s*(\)|$)").unwrap();

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, cypher, dialect).await;

        // The CTE body's GROUP BY must carry BOTH composite-id columns.
        assert!(
            sql.contains("GROUP BY a.bank_id, a.account_number"),
            "expected the WITH-CTE GROUP BY to key on BOTH composite id columns for {dialect:?}, got:\n{sql}"
        );
        assert!(
            !collapsed.is_match(&sql),
            "WITH-CTE GROUP BY must not collapse to the first id column only for {dialect:?}, got:\n{sql}"
        );
    }
}

/// Regression for #452: on the FK-edge schema the PLACED_BY edge IS the
/// `orders_fk` node table, so `MATCH (c:Customer) OPTIONAL MATCH
/// (c)<-[:PLACED_BY]-(o:Order)` must reach the optional Order with a SINGLE
/// node-to-node FK join — no separate self-join re-materialising the edge table
/// (`LEFT JOIN orders_fk AS t0 ON t0.order_id = o.order_id`). The edge row IS the
/// node row (1:1 on the PK), so that join is pure overhead; the OPTIONAL path
/// used to miss the collapse the required path performs.
#[tokio::test]
async fn fk_edge_optional_match_has_no_phantom_edge_self_join() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());
    let cypher =
        "MATCH (c:Customer) OPTIONAL MATCH (c)<-[:PLACED_BY]-(o:Order) RETURN c.name, o.order_id";

    for (dialect, dname) in [
        (SqlDialect::ClickHouse, "clickhouse"),
        (SqlDialect::Databricks, "databricks"),
    ] {
        let sql = render(&schema, cypher, dialect).await;

        // The orders_fk table must be materialised exactly ONCE (as the Order node
        // `o`). A second occurrence is the phantom edge self-join this fixes.
        let orders_fk_joins = sql.matches("orders_fk AS ").count();
        assert_eq!(
            orders_fk_joins, 1,
            "FK-edge OPTIONAL MATCH ({dname}) must materialise orders_fk exactly once \
             (no phantom edge self-join), got {orders_fk_joins}:\n{sql}"
        );

        // And specifically no `<edge_alias>.order_id = o.order_id` PK identity
        // self-join of the edge table onto the Order node.
        assert!(
            !sql.contains("order_id = o.order_id"),
            "FK-edge OPTIONAL MATCH ({dname}) still emits an edge PK identity \
             self-join:\n{sql}"
        );

        // The genuinely-optional relation is still a LEFT JOIN (NULL-extension
        // rows preserved).
        assert!(
            sql.contains("LEFT JOIN"),
            "FK-edge OPTIONAL MATCH ({dname}) must keep the LEFT JOIN for the \
             optional Order:\n{sql}"
        );
    }
}

/// Regression for #460: a WHERE on a post-WITH OPTIONAL MATCH that references
/// the optional side must FILTER THE OPTIONAL MATCH — the predicate lands inside
/// the LEFT JOIN (pre_filter subquery), keeping customers with no qualifying
/// order NULL-extended. Previously the predicate was SILENTLY DROPPED (the
/// reversed-anchor post-WITH shape routed the anchor-side portion of the WHERE
/// to the outer clause but lost the optional-side portion), so the query
/// returned every order — more rows than asked (ground-rule #1). The fix
/// re-extracts the optional-only predicate in the #453 restructure and attaches
/// it to the demoted LEFT JOIN's pre_filter. Locks BOTH render paths + dialects:
/// predicate present INSIDE the LEFT JOIN, and NOT duplicated into an outer WHERE.
#[tokio::test]
async fn fk_edge_post_with_optional_where_filters_inside_left_join_460() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());
    let cypher = "MATCH (c:Customer) WITH c \
                  OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) WHERE o.amount > 100 \
                  RETURN c.name, o.order_id";

    // The predicate must render exactly once, inside the LEFT JOIN pre_filter
    // subquery over the optional Order table — dialect-neutral (subquery form +
    // physical column name `total_amount` are the same on both dialects).
    let pre_filter = "LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE total_amount > 100) AS o";

    for (dialect, dname) in [
        (SqlDialect::ClickHouse, "clickhouse"),
        (SqlDialect::Databricks, "databricks"),
    ] {
        // Since #459 the harness `render()` IS the production (plan_ctx) path;
        // the drop was observed there (and on the since-deleted ctx-less path).
        {
            let (sql, path) = (render(&schema, cypher, dialect).await, "render");
            // Optional-side predicate is INSIDE the LEFT JOIN (correct place).
            assert!(
                sql.contains(pre_filter),
                "post-WITH OPTIONAL WHERE ({dname}/{path}) must filter inside the \
                 LEFT JOIN pre_filter subquery (predicate was dropped, #460):\n{sql}"
            );
            // The predicate must appear EXACTLY once — never also promoted into an
            // outer WHERE (which would kill the NULL-extended no-match customers).
            assert_eq!(
                sql.matches("total_amount > 100").count(),
                1,
                "post-WITH OPTIONAL WHERE ({dname}/{path}) predicate must appear once \
                 (inside the LEFT JOIN only, not duplicated into an outer WHERE):\n{sql}"
            );
            // The join stays a LEFT JOIN (no-match customers preserved).
            assert!(
                sql.contains("LEFT JOIN (SELECT"),
                "post-WITH OPTIONAL WHERE ({dname}/{path}) must keep the optional \
                 Order as a LEFT JOIN:\n{sql}"
            );
        }
    }
}

/// Regression for #462: two residual predicate shapes on a post-WITH OPTIONAL
/// MATCH WHERE, both pre-existing before #460 (which fixed only the optional-NODE
/// alias shape). All must preserve OPTIONAL MATCH semantics (no dropped
/// NULL-extended rows, no dropped/partial filters):
///
///   GAP 1 — a predicate spanning both the optional side (o) and the anchor CTE
///   (c), including an unsplittable OR, must land in the LEFT JOIN ON condition,
///   never the outer WHERE (which would drop no-match customers). The anchor
///   references resolve to CTE columns (`c.p1_c_customer_id`).
///
///   GAP 2 — a predicate on the relationship alias (r). On FK-edge r and o share
///   the orders_fk table, so it remaps to that table's column and lands in the
///   LEFT JOIN pre_filter (never silently dropped).
#[tokio::test]
async fn fk_edge_post_with_optional_where_462_predicate_placement() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());

    // (cypher, must-contain) — dialect-neutral fragments.
    let cases: &[(&str, &str)] = &[
        // GAP 1 cross: cross-alias comparison in the ON, nothing in outer WHERE.
        (
            "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) \
             WHERE o.total_amount > c.customer_id RETURN c.customer_id, o.order_id",
            "ON o.customer_id = c.p1_c_customer_id AND o.total_amount > c.p1_c_customer_id",
        ),
        // GAP 1 OR: whole (parenthesized) OR in the ON, nothing in outer WHERE.
        (
            "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) \
             WHERE o.total_amount > 100 OR c.customer_id > 100 RETURN c.customer_id, o.order_id",
            "ON o.customer_id = c.p1_c_customer_id AND (o.total_amount > 100 OR c.p1_c_customer_id > 100)",
        ),
        // GAP 2 rel: r.order_id remapped to the shared table column in pre_filter.
        (
            "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[r:PLACED_BY]->(c) \
             WHERE r.order_id > 3 RETURN c.customer_id, o.order_id",
            "LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE order_id > 3) AS o",
        ),
        // GAP 2 mixed: BOTH conjuncts recovered into the pre_filter.
        (
            "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[r:PLACED_BY]->(c) \
             WHERE r.order_id > 3 AND o.total_amount > 100 RETURN c.customer_id, o.order_id",
            "LEFT JOIN (SELECT * FROM db_fk_edge.orders_fk WHERE (total_amount > 100 AND order_id > 3)) AS o",
        ),
    ];

    for (cypher, must_contain) in cases {
        for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
            let sql = render(&schema, cypher, dialect).await;
            assert!(
                sql.contains(must_contain),
                "#462 placement ({dialect:?}) for `{cypher}`\nexpected to contain:\n  {must_contain}\ngot:\n{sql}"
            );
            // No OUTER WHERE: every predicate belongs to the OPTIONAL match (ON
            // condition or LEFT JOIN pre_filter). A top-level WHERE line would mean
            // a predicate was promoted to filter the anchor rows, dropping the
            // NULL-extended no-match customers. (The pre_filter's own `WHERE` is
            // nested inside the `(SELECT … )` subquery, never at line start.)
            let has_outer_where = sql.lines().any(|l| l.trim_start().starts_with("WHERE "));
            assert!(
                !has_outer_where,
                "#462 placement ({dialect:?}) for `{cypher}`\nmust NOT emit an outer WHERE \
                 (predicate must stay in the OPTIONAL match, not filter the anchor rows):\n{sql}"
            );
        }
    }
}

/// Regression for #472: a post-WITH OPTIONAL MATCH WHERE conjunct that
/// references ONLY the anchor CTE alias (not the optional side at all) used to
/// stay in the outer WHERE (the #462 fix only moved conjuncts that referenced
/// the optional alias). An outer WHERE drops the NULL-extended no-match anchor
/// rows OPTIONAL MATCH must preserve. Fix: move EVERY conjunct in this
/// segment's WHERE into the LEFT JOIN ON — always safe for a LEFT JOIN (a false
/// ON condition NULL-extends rather than drops).
///
/// Live (db_fk_edge, `MATCH (c:Customer) WITH c OPTIONAL MATCH
/// (o:Order)-[:PLACED_BY]->(c) WHERE c.customer_id > 101 RETURN
/// c.customer_id, o.order_id`): was 3 rows (100/101 dropped), now the correct
/// 5 rows: (100,∅), (101,∅), (102,5), (102,8), (103,6).
#[tokio::test]
async fn fk_edge_post_with_optional_where_472_pure_anchor_placement() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());
    let cypher = "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) \
                  WHERE c.customer_id > 101 RETURN c.customer_id, o.order_id";

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, cypher, dialect).await;
        assert!(
            sql.contains("ON o.customer_id = c.p1_c_customer_id AND c.p1_c_customer_id > 101"),
            "#472 ({dialect:?}): pure-anchor conjunct must move into the LEFT JOIN ON:\n{sql}"
        );
        let has_outer_where = sql.lines().any(|l| l.trim_start().starts_with("WHERE "));
        assert!(
            !has_outer_where,
            "#472 ({dialect:?}): must NOT emit an outer WHERE (would drop NULL-extended \
             anchor rows):\n{sql}"
        );
    }
}

/// Regression for #473: predicates spanning a WITH barrier were corrupted by a
/// hand-rolled expression walker (`process_expr` in
/// `query_planner::analyzer::filter_tagging`), independent of the join-ON
/// placement fixed by #462/#472:
///
///   A. `IS NULL` silently vanished. The walker's "collapse single-operand
///      operator application" step (meant only for And/Or, which can shrink to
///      one operand after per-alias extraction) also fired for the inherently
///      unary `IsNull`/`IsNotNull`, discarding the operator and leaving just
///      the bare column (`o.total_amount IS NULL` rendered as `o.total_amount`,
///      evaluating a Float column as a boolean).
///
///   B. `NOT(x) OR y` was rewritten as `NOT(x) AND y`. The walker's NOT-operator
///      fast path unconditionally extracted a single-table NOT operand as its
///      own independent filter, without checking whether it was nested inside
///      an OR (`in_or`) — silently splitting the OR into two separately-placed
///      conjuncts.
///
/// Both fixed by making the walker respect `in_or` for the NOT fast path and
/// restricting the single-operand collapse to And/Or only.
///
/// Live (db_fk_edge), both shapes: was 8 bogus rows (A) / 2 wrongly-dropped
/// rows (B); now the correct 5 rows: (100,∅), (101,∅), (102,5), (102,8),
/// (103,6) — identical ground truth to #472 on this fixture (no NULL amounts,
/// no order under 5).
#[tokio::test]
async fn fk_edge_post_with_optional_where_473_is_null_and_not_or() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());

    let cases: &[(&str, &str)] = &[
        (
            "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) \
             WHERE o.amount IS NULL OR c.customer_id > 101 RETURN c.customer_id, o.order_id",
            "ON o.customer_id = c.p1_c_customer_id AND (o.total_amount IS NULL OR c.p1_c_customer_id > 101)",
        ),
        (
            "MATCH (c:Customer) WITH c OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) \
             WHERE NOT(o.amount > 5) OR c.customer_id > 101 RETURN c.customer_id, o.order_id",
            "ON o.customer_id = c.p1_c_customer_id AND (NOT o.total_amount > 5 OR c.p1_c_customer_id > 101)",
        ),
    ];

    for (cypher, must_contain) in cases {
        for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
            let sql = render(&schema, cypher, dialect).await;
            assert!(
                sql.contains(must_contain),
                "#473 ({dialect:?}) for `{cypher}`\nexpected to contain:\n  {must_contain}\ngot:\n{sql}"
            );
            let has_outer_where = sql.lines().any(|l| l.trim_start().starts_with("WHERE "));
            assert!(
                !has_outer_where,
                "#473 ({dialect:?}) for `{cypher}`\nmust NOT emit an outer WHERE:\n{sql}"
            );
        }
    }
}

/// Same #473 corruption (IS NULL vanishing / NOT-OR becoming AND-OR) also
/// reproduces on a PLAIN (non-WITH, non-optional) cross-alias OR — proving the
/// bug lives in the general `filter_tagging` predicate-extraction walker, not
/// specifically in the post-WITH OPTIONAL restructure. A single-table OR
/// (entire OR belongs to one alias) is unaffected — it is extracted whole
/// without recursing, which is why plain single-table forms were reported as
/// "fine" in the original filing.
#[tokio::test]
async fn fk_edge_473_walker_fix_covers_plain_cross_alias_or() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());
    let cypher = "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) \
                  WHERE o.amount IS NULL OR c.customer_id > 101 RETURN c.customer_id, o.order_id";
    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, cypher, dialect).await;
        assert!(
            sql.contains("total_amount IS NULL"),
            "#473 plain cross-alias OR ({dialect:?}): IS NULL must survive:\n{sql}"
        );
    }
}

/// FIXED (#478). Two OPTIONAL MATCH clauses sharing the same anchor (`c`) on
/// an FK-edge schema (`(o:Order)-[:PLACED_BY]->(c)` twice) used to emit a
/// spurious extra INNER JOIN plus a spurious extra LEFT JOIN of `orders_fk`
/// (aliases `t1`/`t2`, the auto-generated aliases for the two unnamed
/// `PLACED_BY` relationship variables), on top of the correct `o`/`o2` node
/// joins. The INNER JOIN dropped order-less customers — a straight OPTIONAL
/// MATCH semantics violation; even flipped to LEFT, `t1`/`t2` would duplicate
/// `o`/`o2`'s own join condition and fan out rows (an #479-style "naive fix
/// is provably worse" trap) — so a superficial join-type flip was never a
/// safe fix here.
///
/// Root cause (traced live, 2026-07): the planner represents "two 1-hop
/// patterns sharing one anchor" as a nested/chained `GraphRel` — outer
/// `GraphRel{left: o2, right: GraphRel{left: o, right: c}}` — reusing the same
/// encoding as a genuine 2-hop chain `(o2)-[t2]->(o)-[t1]->(c)`. There turns
/// out to be NO tree-shape difference between this "star at a shared anchor"
/// and a genuine chain (a real 2-hop pattern fanning through a midpoint is
/// structurally identical to two edges fanning into a shared endpoint) — so
/// `outer.right_connection == inner.right_connection` alone cannot
/// distinguish them, contrary to the original diagnosis. What DOES reliably
/// distinguish this shape on an FK-edge ("node IS the edge") schema: the
/// inner/outer relationship's own table is IDENTICAL to its non-shared node's
/// table (e.g. `orders_fk` for both `o`/`o2` and their `PLACED_BY` edges).
/// When that holds, the non-shared node already fully represents both itself
/// AND its edge to the shared anchor in one row, and the analyzer's
/// `GraphJoins.joins` already carries the single correct JOIN for it —
/// `join_builder.rs`'s `extract_joins` materializing a SEPARATE join keyed by
/// the auto-generated relationship alias (`t1`/`t2`) was always redundant and
/// wrong. Fixed in three symmetric spots in `extract_joins`'s nested-GraphRel
/// handling (the `shared_is_inner_right`/`shared_is_inner_left` branches, and
/// the reversed-anchor `anchor_is_right && right_is_nested` branch) by
/// checking `rel_table == <non-shared node's own table>` and skipping the
/// phantom join when it holds (recursing only for 3+-way sibling nesting).
/// Non-FK-edge (separate edge table) schemas never hit this — the tables
/// differ, so genuine chains still materialize their edge joins normally.
///
/// Live (db_fk_edge, 4 customers / their orders): ground truth (hand-derived
/// LEFT JOIN ... LEFT JOIN ... AND total_amount>100) is 8 rows. Old SQL
/// (INNER JOIN t1 + LEFT JOIN t2 on top of the correct o/o2 joins) executed
/// but returned far more than 8 rows (duplicate fan-out from the phantom
/// joins) — verified live during this fix. New SQL returns exactly 8 rows,
/// matching ground truth.
#[tokio::test]
async fn fk_edge_478_two_optional_matches_no_phantom_edge_joins() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());
    let cypher = "MATCH (c:Customer) OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) \
                  OPTIONAL MATCH (o2:Order)-[:PLACED_BY]->(c) WHERE o2.total_amount > 100 \
                  RETURN c.customer_id, o.order_id, o2.order_id";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // No phantom `t<N>`-aliased relationship join anywhere (INNER or LEFT).
    assert!(
        !sql.contains("JOIN db_fk_edge.orders_fk AS t"),
        "#478 regressed: a phantom `t<N>`-aliased orders_fk edge join is back:\n{sql}"
    );
    // Exactly the two genuine node joins (o, o2), both LEFT (OPTIONAL), on
    // top of the anchor FROM.
    assert!(
        sql.contains("LEFT JOIN db_fk_edge.orders_fk AS o ON o.customer_id = c.customer_id"),
        "#478: expected the plain `o` LEFT JOIN:\n{sql}"
    );
    assert!(
        sql.contains("o2.customer_id = c.customer_id") && sql.contains("total_amount > 100"),
        "#478: expected the `o2` LEFT JOIN pre_filter subquery gated on total_amount:\n{sql}"
    );
}

/// KNOWN BROKEN — deferred. R4 investigation fixed ONE of the three
/// previously-documented downstream symptom bugs, found and precisely pinned
/// a FOURTH (deeper) cause, and ATTEMPTED but had to REVERT a fix for a
/// second — recorded below since the revert itself is a meaningful, tested
/// negative result for the next round.
///
/// FIXED this round (verified safe in isolation — full `cargo test` shows no
/// regressions): `is_optional_pattern()`'s `GraphRel` arm
/// (`query_planner/logical_plan/mod.rs`) only inspected the OUTERMOST
/// `GraphRel`'s own `is_optional` flag, never recursing into nested
/// left/right branches — so a MIXED required+optional chain
/// (analyzer-encoded as `GraphRel{left: o2 (optional, outermost), right:
/// GraphRel{left: o (required), right: c}}`) was wrongly reported as "purely
/// optional", arming `build_chained_with_match_cte_plan`'s single-branch
/// `post_with_optional_restructure` — which blindly demotes every JOIN to
/// LEFT and collapses onto one optional-side FROM table. Fixed via a new
/// `has_required_graph_rel` helper + making the `GraphRel` arm require BOTH
/// nested branches contain no required `GraphRel`, mirroring
/// `CartesianProduct`'s arm (which already correctly required both sides
/// optional). This alone does not fix shape 1's repro (the two other
/// mechanisms below still corrupt it), but is independently correct and
/// kept.
///
/// ATTEMPTED AND REVERTED: `prune_joins_covered_by_cte`'s fixed-point
/// expansion (`render_plan/plan_builder_utils.rs`) treats "my only neighbor
/// is CTE-exported" as sufficient to mark a join removable. For shape 1
/// (`exported_aliases={c}`), both `o`'s and `o2`'s joins have their ONLY
/// neighbor be `c` (seeded removable), so this check sweeps BOTH out of the
/// final query even though neither was ever inside the CTE (only `c` was) —
/// silently discarding the query's own required/optional JOINs entirely.
/// Tightening this to require 2+ neighbors (reasoning: a genuine
/// chain-internal link, the documented `[a→b]` case, is always sandwiched
/// between 2+ removable neighbors; a single fresh join hanging off one
/// anchor is not) DID fix this over-pruning for the star/branch shape here —
/// but broke the single-OPTIONAL-pattern case this same loop is ALSO
/// load-bearing for (`fk_edge_post_with_optional_where_{460,462,472,473}`,
/// `with_cte_join_key_is_correlated_not_cartesian_451`): e.g. `WITH c
/// OPTIONAL MATCH (o)-[:PLACED_BY]->(c) ...` has a SINGLE post-WITH join
/// (`o`) with exactly ONE neighbor (`c`) that genuinely MUST be pruned here
/// — `post_with_optional_restructure` (#453) independently rebuilds that
/// exact JOIN from `render_plan.from`/correlation predicates and expects
/// `prune_joins_covered_by_cte` to have already removed the raw `o` join
/// entry; leaving it in place broke that restructure (`InvalidRenderPlan`
/// "no join key could be resolved" on 5 previously-passing tests, confirmed
/// live via `cargo test --test integration`). Reverted. The real fix needs
/// to distinguish "a single fresh join hanging off the anchor" (prune it —
/// #453 rebuilds it) from "MULTIPLE sibling fresh joins hanging off the SAME
/// anchor" (don't prune any of them — shape 1's actual bug), not a blanket
/// neighbor-count threshold — e.g. only applying the 2+-neighbor
/// requirement when 2+ DISTINCT non-removable joins would otherwise all
/// collapse onto the exact same single anchor. Not re-attempted this round
/// given the remaining time budget; flagging the precise distinguishing
/// condition for whoever picks this up next.
///
/// A FOURTH, deeper cause (not in the original 3-bug characterization) was
/// also found by live `eprintln` tracing the actual `LogicalPlan` reaching
/// `to_render_plan_with_ctx`: `current_plan` for this segment is genuinely
/// `GraphJoins(Projection(GraphRel(Outgoing){left: Node(o2), right:
/// GraphRel(Outgoing){left: Node(o), right: Node(c, ViewScan("with_c_cte_0"))}}))`
/// — confirming the "star mis-encoded as a nested/chained GraphRel"
/// diagnosis exactly. `extract_joins`'s FK-edge nested-GraphRel collapse
/// logic (`join_builder.rs`, the same code #478 already patched with three
/// symmetric guards for the OPTIONAL+OPTIONAL sibling shape) needs a FOURTH
/// guard for this REQUIRED+OPTIONAL mixed case — not attempted, risks
/// regressing #478's own passing tests without dedicated verification.
///
/// Net effect on the characterized SQL this round: unchanged in kind
/// (still a dangling `o.customer_id` reference in `o2`'s combined ON
/// condition, with `o` appearing in no FROM/JOIN), though the exact SQL text
/// shifted slightly (now `o2` alone drives FROM, with a single combined
/// `INNER JOIN with_c_cte_0 AS c ON o.customer_id = ... AND o2.customer_id =
/// ...` folding both correlations into one join) as a side effect of the
/// `is_optional_pattern` fix changing which code path renders it.
///
/// Live (db_fk_edge): current SQL captured verbatim as a characterization
/// lock (ClickHouse would reject this SQL with an unknown identifier error
/// for `o` — not run live here since it cannot execute).
#[tokio::test]
async fn fk_edge_461_mixed_required_optional_post_with_malformed_sql_known_broken() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());
    let cypher = "MATCH (c:Customer) WITH c MATCH (o:Order)-[:PLACED_BY]->(c) \
                  OPTIONAL MATCH (o2:Order)-[:PLACED_BY]->(c) \
                  RETURN c.customer_id, o.order_id, o2.order_id";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // Characterization lock: `o` never appears as its own FROM/JOIN alias, yet
    // an ON condition references `o.customer_id` — a dangling alias. If this
    // starts failing because `o` now HAS its own join (or the query renders
    // differently), that is progress — verify against live row counts before
    // replacing this test.
    let o_has_own_join = sql.contains("AS o ON") || sql.contains("AS o \n");
    assert!(
        !o_has_own_join,
        "#461 KNOWN BROKEN characterization stale — alias 'o' now has its own \
         JOIN; if this is a genuine fix, replace this test with a regression \
         test (verify live row counts / SQL validity first):\n{sql}"
    );
    assert!(
        sql.contains("o.customer_id"),
        "#461 KNOWN BROKEN characterization stale — dangling 'o.customer_id' \
         reference no longer present:\n{sql}"
    );
}

/// KNOWN BROKEN — deferred (#461 shape 2): a multi-hop OPTIONAL pattern after a
/// WITH barrier (`OPTIONAL MATCH (u)-[:AUTHORED]->(p)<-[:LIKED]-(u2)`) emits a
/// disconnected anchor (`u` never joined to the pattern at all) plus a
/// forward reference to an as-yet-undefined alias `t1`, plus a leftover INNER
/// JOIN — multiple compounding structural defects in the post-WITH multi-hop
/// OPTIONAL path (`build_chained_with_match_cte_plan`'s segment handling),
/// distinct from (but likely adjacent to) the #478/#461-shape-1 "star at a
/// shared anchor" bug above. Deferred: needs planner-level work per the
/// original #461 filing, not a small surgical fix.
///
/// Live (social schema): current SQL captured verbatim as a characterization
/// lock.
#[tokio::test]
async fn fk_edge_461_multihop_optional_post_with_disconnected_anchor_known_broken() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (u:User) WITH u \
                  OPTIONAL MATCH (u)-[:AUTHORED]->(p)<-[:LIKED]-(u2) \
                  RETURN u.name, p.id, u2.name";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // Characterization lock: the anchor CTE alias `u` never appears as a JOIN
    // condition operand anywhere in the query — it is disconnected from the
    // rest of the pattern. If this starts failing because `u` IS now
    // referenced in a join, that is progress — verify live row counts before
    // replacing this test.
    let u_referenced_in_join = sql.lines().any(|l| l.contains("JOIN") && l.contains(" u."));
    assert!(
        !u_referenced_in_join,
        "#461 shape-2 KNOWN BROKEN characterization stale — anchor 'u' is now \
         referenced in a JOIN condition; if this is a genuine fix, replace \
         this test with a regression test (verify live row counts first):\n{sql}"
    );
}

/// FIXED (#479, the deepest issue in this family). On separate-edge schemas
/// (standard `social` schema here), a PLAIN (no-WITH) OPTIONAL MATCH node
/// predicate used to land in the outer WHERE, dropping the NULL-extended
/// no-match rows (behaving like an INNER JOIN). Root cause was different from
/// #472/#473 (fixed earlier) and from #478/#461 (the nested-GraphRel
/// chain-vs-star bug): the join structure itself was fine (`u LEFT JOIN
/// follows LEFT JOIN v`, correctly two separate joins for the separate edge
/// table) but no pass relocated `v.city = 'London'` into either join — it
/// stayed in the outer WHERE, evaluated AFTER both LEFT JOINs.
///
/// The #474 fix intentionally did NOT touch this shape: a naive "pre_filter on
/// just the `v` node join" fix was PROVEN WORSE by live ground-truth
/// experiment (8 users, social schema) — it resurrects the unfiltered `follows`
/// edge join as spurious duplicate NULL rows (12 rows instead of the correct
/// 8; see #479's filing for the full experiment). Fixed with the CORRECT
/// shape: a new post-hoc RenderPlan pass
/// (`fold_optional_edge_node_join_with_predicate` in `plan_optimizer.rs`,
/// registered first in `optimize_joins_in_plan`) that folds the edge JOIN +
/// node JOIN + predicate into ONE combined LEFT JOIN subquery gated on the
/// anchor key (`u LEFT JOIN (SELECT f.follower_id AS __cg_combined_anchor_key,
/// v.* FROM follows AS f JOIN users AS v ON v.user_id = f.followed_id WHERE
/// city='London') AS v ON v.__cg_combined_anchor_key = u.user_id`) — narrowly
/// gated (single-column keys, edge alias unreferenced elsewhere, edge
/// connects straight to FROM) so it only fires on exactly this shape; ALL
/// WHERE conjuncts referencing solely the optional node are folded together
/// (never a subset — a partially-folded group would leave the remainder in
/// the outer WHERE to independently reproduce the same drop-NULL-rows bug).
///
/// Live (social, 8 users): ground truth is 8 rows (2 users have a London
/// FOLLOWS target, 6 are NULL-extended). Old SQL (bare outer WHERE) returned
/// 2; the "naive pre_filter on just `v`" alternative would have returned 12
/// (verified live during the #479 investigation). New SQL returns exactly 8,
/// matching every user and correctly NULL-extending the 6 without a London
/// follow.
#[tokio::test]
async fn social_479_plain_optional_where_combined_subquery_preserves_null_extension() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(v:User) \
                  WHERE v.city = 'London' RETURN u.name, v.name";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // No bare outer WHERE on the optional node's predicate.
    assert!(
        !sql.lines()
            .any(|l| l.trim_start().starts_with("WHERE v.city")),
        "#479 regressed: predicate is back in a bare outer WHERE:\n{sql}"
    );
    // The combined subquery form: one LEFT JOIN folding the edge + node +
    // predicate, gated on the anchor key.
    assert!(
        sql.contains("JOIN social.users_bench AS v ON v.user_id ="),
        "#479 regressed: expected the combined subquery's inner node JOIN:\n{sql}"
    );
    // #552: qualified with the LOCAL subquery alias `v.` — not stripped bare
    // (a bare `city` would be unambiguous here, but the general rule must
    // qualify regardless, since some collisions are NOT benign; see the
    // dedicated #552 regression test below).
    assert!(
        sql.contains("WHERE v.city = 'London'"),
        "#479 regressed: expected the predicate inside the combined subquery:\n{sql}"
    );
    assert!(
        sql.contains(".__cg_combined_anchor_key = u.user_id"),
        "#479 regressed: expected the combined JOIN gated on the anchor key:\n{sql}"
    );
}

/// FIXED (#554): an `Incoming`-direction end-bound OPTIONAL MATCH (anchor on
/// the RIGHT connection, e.g. `MATCH (a) OPTIONAL MATCH (b)<-[:R]-(a) WHERE
/// b.prop = X`) rendered with NO WHERE filter applied at ALL — not
/// misplaced, just silently absent.
///
/// Root cause: `collect_graphrel_predicates` (plan_builder_helpers.rs) drops
/// any conjunct referencing ONLY the optional node's alias, trusting
/// `apply_optional_node_pre_filters` (#474, join_builder.rs) to re-attach it
/// as a JOIN `pre_filter` — but that mechanism's own safety gate
/// deliberately declines for the traditional separate-edge shape (folding
/// only the node subquery there would resurrect the edge row as a spurious
/// NULL-extended duplicate). For a separate-edge OPTIONAL MATCH neither
/// mechanism claims the predicate, so it's silently lost. This only
/// surfaces when `GraphRel.anchor_connection` is `Some` (the
/// Incoming-anchor-on-right override, `determine_optional_anchor`); the
/// common Outgoing/left-anchor shape leaves `anchor_connection` `None` by
/// design and hits the "keep everything" conservative branch instead, so
/// Outgoing was never affected — this is why the bug is direction-specific.
///
/// Fixed by only excluding an optional-only conjunct when the optional
/// node's own table is structurally shared with the relationship's scan
/// (`optional_node_shares_table_with_edge`) — exactly when #474 will claim
/// it — otherwise keeping it so `fold_optional_edge_node_join_with_predicate`
/// (#479/#552) can find and fold it, matching Outgoing's existing behavior.
///
/// Live-verified (2026-07-11, social benchmark, 8 users): this Incoming SQL
/// now renders byte-identically (module alias numbering) to the Outgoing
/// form above and returns the same 8-row result set with correct NULL
/// extension for the two users (Grace Hopper, Hank Pym) with no qualifying
/// `country = 'USA'` follow — pre-fix it returned all 8 rows UNFILTERED
/// (the WHERE was silently absent, not just misplaced).
#[tokio::test]
async fn social_554_incoming_direction_optional_where_not_dropped() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (a:User) OPTIONAL MATCH (b:User)<-[:FOLLOWS]-(a) \
                  WHERE b.city = 'London' RETURN a.name, b.name";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // The WHERE must survive SOMEWHERE — either folded into the combined
    // subquery (matching Outgoing) or, at minimum, present in the outer
    // WHERE. The bug was TOTAL disappearance, not misplacement.
    assert!(
        sql.contains("city"),
        "#554 regressed: WHERE clause dropped entirely for the Incoming \
         end-bound OPTIONAL MATCH shape:\n{sql}"
    );
    // Matching Outgoing's existing (#479/#552) correct behavior: folded into
    // the combined LEFT JOIN subquery, qualified with the node's own local
    // alias, gated on the anchor key — NOT a bare post-join outer WHERE.
    assert!(
        sql.contains("WHERE b.city = 'London'"),
        "#554 regressed: expected the predicate folded into the combined \
         subquery (qualified with the node's local alias `b.`), matching \
         the Outgoing-direction shape:\n{sql}"
    );
    assert!(
        !sql.lines()
            .any(|l| l.trim_start().starts_with("WHERE b.city")),
        "#554 regressed: predicate is in a bare outer WHERE post-LEFT-JOIN \
         (drops NULL-extended anchors), not folded into the combined \
         subquery:\n{sql}"
    );
}

// FIXED (#533): was KNOWN BROKEN — the denormalized `__denorm_scan` variant
// of #479/#552's OPTIONAL-node predicate placement is now covered. See
// `denorm_optional_node_predicate_folds_into_pre_filter_533` below (near
// `denorm_optional_where_preserved_both_directions_506_followup`) for the
// live-verified regression test and root-cause writeup.

/// FIXED (#553): zero-match ANCHOR with an inline-map predicate on a
/// denormalized schema returned 0 rows instead of one NULL-extended row.
///
/// `MATCH (a:Airport {code:'PHX'}) OPTIONAL MATCH (a)-[r:FLIGHT]->
/// (b:Airport)` — PHX has zero outgoing FLIGHT edges in the committed
/// `db_denormalized` fixture. Root cause: the anchor's inline-map predicate
/// is independently (a) folded correctly into the `__denorm_scan_a` CTE
/// (`materialize_standalone_denorm_scans`, type_inference.rs — each UNION
/// branch gets its own role-mapped copy: `WHERE a.origin_code = 'PHX'` /
/// `WHERE a.dest_code = 'PHX'`), AND (b) left behind whole in
/// `GraphRel.where_predicate`, which `collect_graphrel_predicates`
/// (plan_builder_helpers.rs) then property-map-resolves through the
/// connected relationship's role (a denormalized node has no
/// `property_mapping` of its own) — landing on `r.origin_code`, the
/// LEFT-JOINed (nullable) alias. That produced a redundant, WRONG outer
/// `WHERE r.origin_code = 'PHX'`: correct when the anchor has >=1 matching
/// edge (coincidentally passed pre-existing test coverage), but for a
/// zero-match anchor the LEFT JOIN NULL-extends `r`, so `NULL = 'PHX'`
/// fails the bare WHERE and drops the row entirely — violating OPTIONAL
/// MATCH semantics.
///
/// Fixed by dropping conjuncts that reference ONLY the anchor's own alias
/// from `GraphRel.where_predicate` before the outer-WHERE rewrite, when the
/// anchor is a denormalized standalone-scan CTE (they're already correctly
/// embedded in the CTE by `materialize_standalone_denorm_scans`).
///
/// Live-verified (2026-07-11, db_denormalized): PHX has 0 outgoing FLIGHT
/// rows (`origin_code = 'PHX'` count 0) and appears once as a dest-only
/// airport (DEN -> PHX). Pre-fix SQL (`WHERE r.origin_code = 'PHX'` after
/// the LEFT JOIN) returns 0 rows against live ClickHouse; post-fix SQL (no
/// outer WHERE — the CTE already filters to just PHX) returns exactly 1 row
/// `{a.code: "PHX", b.code: NULL}`, matching OPTIONAL MATCH ground truth.
/// A non-zero-match anchor (DEN, which has an outgoing flight to PHX)
/// returns `{a.code: "DEN", b.code: "PHX"}` both before and after the fix —
/// unaffected, confirming the fix only removes the SPURIOUS zero-match-only
/// filtering, not the join itself.
#[tokio::test]
async fn denorm_553_zero_match_anchor_inline_map_preserves_null_extended_row() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());
    let cypher = "MATCH (a:Airport {code:'PHX'}) OPTIONAL MATCH (a)-[r:FLIGHT]->(b:Airport) \
                  RETURN a.code, b.code";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // No outer WHERE re-applying the anchor's own predicate against the
    // nullable relationship alias `r` — it's already embedded in the CTE.
    // (The CTE's OWN inner `WHERE a.origin_code = 'PHX'` / `WHERE
    // a.dest_code = 'PHX'` lines are fine and expected — checked below —
    // this only guards against a bare TOP-LEVEL `WHERE r....` line.)
    assert!(
        !sql.lines().any(|l| l.trim_start().starts_with("WHERE r.")),
        "#553 regressed: anchor predicate is back in a bare outer WHERE \
         against the nullable relationship alias — drops the zero-match \
         anchor's NULL-extended row:\n{sql}"
    );
    // The CTE itself must still carry the anchor's filter (both origin and
    // dest role branches), so the anchor scan itself is correctly narrowed.
    assert!(
        sql.contains("WHERE a.origin_code = 'PHX'") && sql.contains("WHERE a.dest_code = 'PHX'"),
        "#553 regressed: expected the anchor's inline-map predicate inside \
         the __denorm_scan CTE (both origin/dest role branches):\n{sql}"
    );
}

/// FIXED (#533, was the third #479 gap): composite-key OPTIONAL MATCH
/// WHERE-on-optional-node. `composite_node_ids.yaml` (Account identified by
/// the TWO-column key `[bank_id, account_number]`) renders the classic
/// separate-edge two-JOIN shape (`c LEFT JOIN account_ownership LEFT JOIN
/// accounts`), but #479's original `fold_optional_edge_node_join_with_predicate`
/// pass (`plan_optimizer.rs`) declined to fold it: its gate hard-required a
/// single-column-key LEFT JOIN (`joining_on.len() == 1`), and a composite-key
/// JOIN's ON is two ANDed equalities (`a.bank_id = t1.bank_id AND
/// a.account_number = t1.account_number`). The predicate therefore stayed in
/// the bare outer WHERE, dropping NULL-extended no-match anchors — the same
/// disease as #479's main case.
///
/// Fixed by generalizing the gate (`equality_join_key_columns`, replacing
/// `single_column_join_key`): every ON condition must be a bare column
/// equality connecting the join's own alias to the SAME single other alias —
/// single- or multi-column — and the combined subquery exports one synthetic
/// `__cg_combined_anchor_key*` per edge→anchor key column (the first keeps
/// the original unsuffixed name, so single-column output is byte-identical
/// to #479's).
///
/// Live-verified (2026-07-11, db_composite_id, 5 customers): pre-fix this
/// returned 4 rows (Charlie dropped — his only account, SAV-004, has balance
/// 8500, below the threshold); post-fix exactly the 5-row ground truth
/// (Alice/SAV-002, Bob/SAV-002 [joint ownership], Charlie/NULL,
/// Diana/WF-1002, Eve/WF-1004).
#[tokio::test]
async fn composite_479_optional_where_folds_into_combined_join_533() {
    let schema = load_schema(SchemaId::CompositeId.yaml_path());
    let cypher = "MATCH (c:Customer) OPTIONAL MATCH (c)-[:OWNS]->(a:Account) \
                  WHERE a.balance > 10000 RETURN c.name, a.account_number";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // No bare outer WHERE on the optional node's predicate.
    assert!(
        !sql.lines()
            .any(|l| l.trim_start().starts_with("WHERE a.balance")),
        "#533 regressed: predicate is back in a bare outer WHERE (drops \
         NULL-extended anchors):\n{sql}"
    );
    // The combined subquery form: the composite-key inner node JOIN plus the
    // predicate inside it, gated on the (single-column, OWNS from_id) anchor
    // key.
    assert!(
        sql.contains("JOIN db_composite_id.accounts AS a ON a.bank_id ="),
        "#533 regressed: expected the combined subquery's inner composite-key \
         node JOIN:\n{sql}"
    );
    // #552: qualified with the LOCAL subquery alias `a.`, not stripped bare.
    assert!(
        sql.contains("WHERE a.balance > 10000"),
        "#533/#552 regressed: expected the alias-qualified predicate inside \
         the combined subquery:\n{sql}"
    );
    assert!(
        sql.contains(".__cg_combined_anchor_key = c.customer_id"),
        "#533 regressed: expected the combined JOIN gated on the anchor \
         key:\n{sql}"
    );
}

/// #533 companion: the FULLY-composite variant — both the node→edge key AND
/// the edge→anchor key are two-column (`TRANSFERRED`: `from_id: [from_bank_id,
/// from_account_number]`, `to_id: [to_bank_id, to_account_number]`). The
/// combined subquery must export one synthetic anchor key per edge→anchor
/// column (`__cg_combined_anchor_key` + `__cg_combined_anchor_key_1`) and the
/// outer combined JOIN must carry both equalities.
///
/// Live-verified (2026-07-11, db_composite_id, 8 accounts): output matches a
/// hand-written combined-subquery ground-truth query row-for-row — 8 rows,
/// every account present, NULL-extended where no qualifying (balance > 10000)
/// transfer target exists (CHK-001→SAV-002, SAV-004→WF-1002, WF-1002→WF-1004,
/// the other five NULL).
#[tokio::test]
async fn composite_479_optional_where_folds_fully_composite_keys_533() {
    let schema = load_schema(SchemaId::CompositeId.yaml_path());
    let cypher = "MATCH (a:Account) OPTIONAL MATCH (a)-[:TRANSFERRED]->(b:Account) \
                  WHERE b.balance > 10000 RETURN a.account_number, b.account_number";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    assert!(
        !sql.lines()
            .any(|l| l.trim_start().starts_with("WHERE b.balance")),
        "#533 (fully-composite) regressed: predicate is back in a bare outer \
         WHERE:\n{sql}"
    );
    // Two synthetic anchor keys exported, one per edge→anchor key column.
    assert!(
        sql.contains("AS __cg_combined_anchor_key,")
            && sql.contains("AS __cg_combined_anchor_key_1,"),
        "#533 (fully-composite) regressed: expected one synthetic anchor key \
         per composite edge→anchor column:\n{sql}"
    );
    // The outer combined JOIN gates on BOTH anchor key columns.
    assert!(
        sql.contains(".__cg_combined_anchor_key = a.bank_id")
            && sql.contains(".__cg_combined_anchor_key_1 = a.account_number"),
        "#533 (fully-composite) regressed: expected the combined JOIN gated \
         on both anchor key columns:\n{sql}"
    );
    // #552: qualified with the LOCAL subquery alias `b.`, not stripped bare.
    assert!(
        sql.contains("WHERE b.balance > 10000"),
        "#533/#552 (fully-composite) regressed: expected the alias-qualified \
         predicate inside the combined subquery:\n{sql}"
    );
}

/// #552: `fold_optional_edge_node_join_with_predicate`'s combined LEFT JOIN
/// subquery has TWO tables in scope (the edge and the node), so a WHERE
/// predicate rendered without its table-alias qualifier is ambiguous
/// whenever the two tables share a column name — ClickHouse silently binds
/// the unqualified reference to whichever table it resolves first. On
/// `composite_node_ids.yaml`, `account_ownership` (the OWNS edge table) and
/// `accounts` (the Account node table) BOTH have a `bank_id` column: the
/// pre-fix SQL was `... FROM db_composite_id.account_ownership AS t1 JOIN
/// db_composite_id.accounts AS a ON a.bank_id = t1.bank_id AND ... WHERE
/// bank_id = 'CHASE'` — bare `bank_id`, ambiguous between `t1` and `a`.
/// Today this specific collision is coincidentally benign (it's the
/// equality-joined key column itself, so both sides hold the same value
/// after the JOIN), but the SAME rendering mechanism would silently bind to
/// the wrong table for any NON-equality-joined colliding column — a live
/// silent-wrong-results trap. Fixed by qualifying with the node's own local
/// alias (`a.bank_id`), which is genuinely in scope inside the subquery.
///
/// Live-verified (2026-07-11, db_composite_id): pre-fix and post-fix SQL
/// both happen to return the same rows here (coincidental benign collision),
/// but the post-fix SQL is unambiguous by construction rather than by luck.
#[tokio::test]
async fn composite_552_combined_subquery_predicate_qualified_not_ambiguous() {
    let schema = load_schema(SchemaId::CompositeId.yaml_path());
    let cypher = "MATCH (c:Customer) OPTIONAL MATCH (c)-[:OWNS]->(a:Account) \
                  WHERE a.bank_id = 'CHASE' RETURN c.name, a.account_number";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // The predicate must be qualified with the node's own local alias `a.` —
    // not a bare column, which is ambiguous against the edge alias `t1`
    // (both `account_ownership` and `accounts` have a `bank_id` column).
    assert!(
        sql.contains("WHERE a.bank_id = 'CHASE'"),
        "#552 regressed: predicate must be qualified with the node's local \
         alias inside the combined subquery (bare `bank_id` is ambiguous \
         against the edge table's own `bank_id` column):\n{sql}"
    );
    assert!(
        !sql.contains("WHERE bank_id = 'CHASE'"),
        "#552 regressed: predicate is back to an unqualified bare column, \
         ambiguous between the edge and node tables:\n{sql}"
    );
}

/// Regression: #477 — `to_sql_without_table_alias` (used to render a LEFT
/// JOIN pre_filter subquery, `LEFT JOIN (SELECT * FROM t WHERE <pred>) AS
/// alias`) stripped the node alias from bare columns but NOT from columns
/// nested inside a function argument: `toFloat(o.total_amount)` kept the `o.`
/// prefix, producing SQL that references an alias not in scope inside the
/// subquery (ClickHouse error 47 UNKNOWN_IDENTIFIER). Verified live
/// (db_fk_edge): the pre-fix SQL shape
/// (`LEFT JOIN (SELECT * FROM orders_fk WHERE toFloat64(o.total_amount) > 100)
/// AS o`) fails with Code 47; the fixed shape
/// (`toFloat64(total_amount) > 100`, no alias) executes and returns 4 rows
/// (customers 100/101/102/103, each keeping its one order with
/// total_amount > 100).
#[tokio::test]
async fn fk_edge_477_pre_filter_strips_alias_inside_function_args() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());
    let cypher = "MATCH (c:Customer) OPTIONAL MATCH (o:Order)-[:PLACED_BY]->(c) \
                  WHERE toFloat(o.total_amount) > 100 RETURN c.customer_id, o.order_id";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // The pre_filter subquery must NOT reference the `o.` alias anywhere —
    // the subquery selects directly from orders_fk with no alias in scope.
    assert!(
        !sql.contains("o.total_amount"),
        "#477 regressed: pre_filter subquery still references the dangling \
         `o.` alias inside a function argument:\n{sql}"
    );
    assert!(
        sql.contains("toFloat64(total_amount) > 100"),
        "#477 regressed: expected the bare (alias-free) function-wrapped \
         predicate inside the LEFT JOIN pre_filter subquery:\n{sql}"
    );
}

/// Regression: #477 adversarial review — `to_sql_without_table_alias`'s
/// original AST-rewrite fix (above) converted every `PropertyAccessExp` into
/// a bare-column `Raw` node BEFORE any type-based special-casing could run,
/// silently breaking the array-membership `IN` rewrite (`x IN node.arrayProp`
/// -> `has(arrayProp, x)`): with the RHS already `Raw`, the
/// `matches!(&op.operands[1], PropertyAccessExp(_))` check in the generic
/// `to_sql()` never fires, degrading to the bare-column default `x IN
/// arrayProp` — a HARD ClickHouse error ("Function 'in' is supported only if
/// second argument is constant or table expression"), reachable via an
/// ordinary `OPTIONAL MATCH ... WHERE 'x' IN o.arrayProp` whenever the schema
/// has an array-typed property. This exercises the SAME mechanism as the
/// #479 combined-subquery fold pass (`plan_optimizer.rs`'s
/// `combined_predicate.to_sql_without_table_alias()`), via
/// `array_property_probe.yaml` (Owner --OWNS--> Item, Item.tags is
/// `Array(String)` on the live ClickHouse dev container's `probe_arr` table).
///
/// Live (default DB, dev container): Alice owns Item 1 (tags=[a,b]) — matches
/// `'a' IN tags`; Bob owns Item 2 (tags=[c,d]) — no match, correctly
/// NULL-extended (not dropped); Carol owns no item — correctly NULL-extended.
/// Ground truth is 3 rows (Alice/Item1, Bob/NULL, Carol/NULL); pre-fix SQL
/// (`has(o.tags, 'a')` degraded to `'a' IN tags`) fails outright with
/// ClickHouse error 1 (UNSUPPORTED_METHOD) — reproduced live during this fix.
#[tokio::test]
async fn array_property_477_pre_filter_preserves_array_membership_in() {
    let schema = load_schema("schemas/test/array_property_probe.yaml");
    let cypher = "MATCH (a:Owner) OPTIONAL MATCH (a)-[:OWNS]->(o:Item) \
                  WHERE 'a' IN o.tags RETURN a.name, o.id";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // #552: qualified with the LOCAL subquery alias `o.`, not stripped bare.
    assert!(
        sql.contains("has(o.tags, 'a')"),
        "#477/#552 (array-membership) regressed: expected `has(o.tags, 'a')` \
         inside the combined LEFT JOIN subquery, got:\n{sql}"
    );
    assert!(
        !sql.contains("'a' IN tags")
            && !sql.contains("'a' in tags")
            && !sql.contains("'a' IN o.tags"),
        "#477 (array-membership) regressed: predicate degraded to a bare \
         scalar IN, which ClickHouse rejects:\n{sql}"
    );
}

/// Guard for the #452 review's blocking finding: an identity self-join of the
/// edge table on a NON-unique column is NOT phantom and must be preserved.
///
/// `MATCH (o:Order) OPTIONAL MATCH (o)-[:PLACED_BY]->(c:Customer)<-[:PLACED_BY]-(o2:Order)`
/// emits `LEFT JOIN orders_fk AS o2 ON o2.customer_id = o.customer_id` — an
/// identity join on the edge's `to_id` FK (`customer_id`), which fans out each
/// order to all sibling orders of the same customer (18 rows on the committed
/// fixture data, not 8). An earlier draft of `remove_redundant_edge_self_joins`
/// accepted `from_id OR to_id` as the identity key and deleted this join; the
/// pass now requires the node PRIMARY KEY (order_id), so `o2` survives.
#[tokio::test]
async fn fk_edge_optional_match_preserves_fanout_self_join() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());
    let cypher = "MATCH (o:Order) OPTIONAL MATCH (o)-[:PLACED_BY]->(c:Customer)<-[:PLACED_BY]-(o2:Order) RETURN o.order_id";

    for (dialect, dname) in [
        (SqlDialect::ClickHouse, "clickhouse"),
        (SqlDialect::Databricks, "databricks"),
    ] {
        let sql = render(&schema, cypher, dialect).await;

        // The fan-out self-join on the non-unique FK must survive.
        assert!(
            sql.contains("LEFT JOIN db_fk_edge.orders_fk AS o2 ON o2.customer_id = o.customer_id"),
            "FK-edge fan-out OPTIONAL MATCH ({dname}) must preserve the o2 \
             self-join on customer_id (non-unique FK — removing it changes the \
             row count from 18 to 8):\n{sql}"
        );
    }
}

/// #458 regression: a whole-edge projection over a polymorphic edge
/// (`MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN r`) prunes the labeled
/// endpoints (only `r` is projected), switching the analyzer to the
/// `SingleTableScan` strategy that makes the edge the FROM marker. The type +
/// from/to label discriminators the analyzer attaches to that marker's
/// `pre_filter` must be PROMOTED into the WHERE clause, not dropped — otherwise
/// the scan returns every interaction type (29 rows) instead of the 10 FOLLOWS
/// edges. Locks the WHERE presence on both dialects.
#[tokio::test]
async fn polymorphic_whole_edge_r_keeps_discriminator() {
    let schema = load_schema(SchemaId::Polymorphic.yaml_path());
    let cypher = "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN r";

    for (dialect, dname) in [
        (SqlDialect::ClickHouse, "clickhouse"),
        (SqlDialect::Databricks, "databricks"),
    ] {
        let sql = render(&schema, cypher, dialect).await;
        // The polymorphic edge is the FROM table (endpoints pruned)…
        assert!(
            sql.contains("brahmand.interactions AS r"),
            "whole_edge_r ({dname}) must scan the edge table as FROM:\n{sql}"
        );
        // …and the discriminator + label filters must survive as a WHERE clause.
        // `render()` is the production (plan_ctx) path post-#459 — the same path
        // server / cg / embedded use.
        assert!(
            sql.contains("r.interaction_type = 'FOLLOWS'")
                && sql.contains("r.from_type = 'User'")
                && sql.contains("r.to_type = 'User'"),
            "whole_edge_r ({dname}) dropped the polymorphic type/label \
             discriminator on the pruned-endpoint edge scan (#458):\n{sql}"
        );
    }
}

/// #458 follow-up regression: the FROM-marker pre_filter promotion (the
/// whole_edge_r fix above) must NOT fire when `extract_from` renders a CTE as
/// the FROM instead of the marker's own edge table. With UNLABELED endpoints
/// (and for multi-type `[:A|B]` and VLP `*1..2` whole-edge projections) the
/// pattern routes through a `pattern_union_*` CTE whose union branches ALREADY
/// carry their per-branch `interaction_type`/`from_type`/`to_type` filters —
/// but the FROM-marker join still sits in `GraphJoins.joins` carrying only the
/// FIRST branch's discriminator. An ungated promotion emitted
/// `FROM pattern_union_r AS r WHERE r.interaction_type = 'SHARED' AND
/// r.from_type = 'Post' AND r.to_type = 'Post'` — columns the CTE never
/// projects (unknown-identifier error on live ClickHouse) and semantically a
/// collapse of the union to one branch. The existing structural tests all use
/// `RETURN a, b` (endpoints referenced ⇒ no SingleTableScan marker), which is
/// how this escaped. `render()` is the PRODUCTION (plan_ctx) render path where
/// the regression was proven (the golden net's only path post-#459); asserts the
/// outer query never references the raw discriminator columns through the CTE
/// alias.
#[tokio::test]
async fn polymorphic_unlabeled_whole_edge_no_outer_discriminator() {
    let schema = load_schema(SchemaId::Polymorphic.yaml_path());
    let cases = [
        "MATCH (a)-[r:SHARED]->(b) RETURN r",
        "MATCH (a)-[r:SHARED]->(b) RETURN r.weight",
        "MATCH (a)-[r:SHARED|FOLLOWS]->(b) RETURN r",
        "MATCH (a)-[r:SHARED*1..2]->(b) RETURN r",
    ];

    for cypher in cases {
        for (dialect, dname) in [
            (SqlDialect::ClickHouse, "clickhouse"),
            (SqlDialect::Databricks, "databricks"),
        ] {
            let sql = render(&schema, cypher, dialect).await;
            // The unlabeled/multi-type/VLP whole-edge shape routes through
            // the pattern-union CTE…
            assert!(
                sql.contains("pattern_union_"),
                "`{cypher}` ({dname}) expected the \
                 pattern_union CTE as FROM:\n{sql}"
            );
            // …and the outer query must NOT reference the raw edge
            // discriminator columns through the CTE alias — the CTE does
            // not project them, and the per-branch filters inside the CTE
            // already discriminate (#458 follow-up).
            for col in ["r.interaction_type", "r.from_type", "r.to_type"] {
                assert!(
                    !sql.contains(col),
                    "`{cypher}` ({dname}) leaked the \
                     FROM-marker pre_filter into the outer WHERE — `{col}` \
                     is not a column of the pattern_union CTE (#458 \
                     follow-up regression):\n{sql}"
                );
            }
        }
    }
}

/// #428 / ERR-E invariant on the POLYMORPHIC schema: a FULLY-unlabeled pattern
/// over the polymorphic edge (`(a)-[:SHARED]->(b)`, or the Browser path form
/// `p=()-[:SHARED]->()`) must expand to a REAL scan of the `interactions` table
/// filtered by the requested type, NEVER the pruned
/// `SELECT 1 AS "_empty" WHERE false` placeholder. These forms route through the
/// `pattern_union_*` CTE whose property blobs are emitted in nondeterministic
/// HashMap order, so they cannot be byte-locked as goldens (see the
/// POLYMORPHIC_CORPUS note); this test locks the stable structural invariants
/// instead. Mirrors `src/render_plan/tests/polymorphic_unlabeled_path_tests.rs`
/// through the golden harness's render path (both dialects).
#[tokio::test]
async fn polymorphic_unlabeled_endpoints_are_real_scans_not_empty() {
    let schema = load_schema(SchemaId::Polymorphic.yaml_path());
    // (cypher, type_value) — one has data (FOLLOWS/SHARED), the placeholder bug
    // was never about missing rows, so a zero-row type must ALSO be a real query.
    let cases = [
        ("MATCH (a)-[:SHARED]->(b) RETURN a, b", "SHARED"),
        ("MATCH (a)-[:FOLLOWS]->(b) RETURN a, b", "FOLLOWS"),
        ("MATCH p=()-[:SHARED]->() RETURN p", "SHARED"),
    ];

    for (cypher, ty) in cases {
        for (dialect, dname) in [
            (SqlDialect::ClickHouse, "clickhouse"),
            (SqlDialect::Databricks, "databricks"),
        ] {
            let sql = render(&schema, cypher, dialect).await;

            // Must NOT prune to the `_empty` placeholder (the #428 bug).
            assert!(
                !(sql.contains("_empty") && sql.contains("WHERE false")),
                "unlabeled polymorphic `{cypher}` ({dname}) pruned to the _empty \
                 placeholder (#428 regression):\n{sql}"
            );
            // Must be a real query over the polymorphic edge table.
            assert!(
                sql.contains("interactions"),
                "unlabeled polymorphic `{cypher}` ({dname}) must scan `interactions`:\n{sql}"
            );
            // Must carry the type discriminator AND the endpoint label columns.
            assert!(
                sql.contains(&format!("interaction_type = '{ty}'")),
                "unlabeled polymorphic `{cypher}` ({dname}) must filter \
                 interaction_type = '{ty}':\n{sql}"
            );
            assert!(
                sql.contains("from_type = ") && sql.contains("to_type = "),
                "unlabeled polymorphic `{cypher}` ({dname}) must filter the \
                 from_type/to_type label columns:\n{sql}"
            );
        }
    }
}

/// Characterization of the ctx-less HARNESS render path for the fully-unlabeled
/// polymorphic pattern. The `pattern_union_*` CTE enumerates all four
/// (from_label, to_label) branches; the outer query then reads from it exactly
/// ONCE — every enumerated path row is emitted a single time.
///
/// #458/#459 finding: the ctx-less render path (`logical_plan_to_render_plan`)
/// used to emit a 4× outer UNION-ALL over `pattern_union_*` (row multiplication)
/// for this shape, but that path had NO production callers and was removed in
/// #459. Post-#459 `render()` IS the production path (`to_render_plan_with_ctx`,
/// used by all server/cg/embedded queries), which collapses the outer union to a
/// single `FROM pattern_union_*` — live CH via cg returns `(a)-[:SHARED]->(b)`
/// = 3 and `(a)-[:FOLLOWS]->(b)` = 10 (correct, NOT 12 / 40). This test now
/// locks that correct single read as the production count.
#[tokio::test]
async fn polymorphic_unlabeled_endpoints_single_outer_read() {
    let schema = load_schema(SchemaId::Polymorphic.yaml_path());

    for (dialect, dname) in [
        (SqlDialect::ClickHouse, "clickhouse"),
        (SqlDialect::Databricks, "databricks"),
    ] {
        let sql = render(&schema, "MATCH (a)-[:SHARED]->(b) RETURN a, b", dialect).await;

        // Sanity: the pattern-union CTE is present.
        assert!(
            sql.contains("pattern_union_t"),
            "expected a pattern_union CTE for the unlabeled polymorphic pattern ({dname}):\n{sql}"
        );
        // PRODUCTION behavior (#459): the outer query reads the already-complete
        // `pattern_union_*` CTE exactly ONCE — no 4× row multiplication.
        let outer_reads = sql.matches("FROM pattern_union_t").count();
        assert_eq!(
            outer_reads, 1,
            "polymorphic unlabeled pattern ({dname}): production path must read \
             pattern_union_* exactly ONCE (no 4× outer-union row multiplication); \
             got {outer_reads} outer reads:\n{sql}"
        );
    }
}

/// #454 regression: labeled node-only queries on a coupled-denormalized node
/// whose node_id is virtual (`code` → origin_code/dest_code via from/to_node_props,
/// `property_mappings: {}`) must materialize a from/to UNION of the edge table with
/// the virtual id/props resolved to PHYSICAL columns — NOT collapse to the
/// multi-label whole-node Browser scan that emitted the non-existent
/// `flights_denorm.code` and dropped the RETURN/WHERE/DISTINCT/aggregation.
///
/// Root cause was a render-side misclassification: the from/to union branches are
/// wrapped in `GraphJoins`, but the `is_denormalized_union` guard in the Union
/// handler did not traverse `GraphJoins`, so the union was routed to the
/// json_builder multi-label path. See `plan_builder.rs`.
#[tokio::test]
async fn denorm_labeled_node_scan_resolves_virtual_id_454() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());

    for (dialect, dname) in [
        (SqlDialect::ClickHouse, "clickhouse"),
        (SqlDialect::Databricks, "databricks"),
    ] {
        // Every node-only shape must (a) never emit the non-existent virtual-id
        // column `flights_denorm.code`, (b) never route through the multi-label
        // whole-node scan, and (c) source BOTH the origin and dest denormalized
        // columns (from/to UNION) so no airport position is dropped.
        for cypher in [
            "MATCH (a:Airport) RETURN a.code",
            "MATCH (a:Airport) RETURN a",
            "MATCH (a:Airport) RETURN a.code, a.city, a.state",
            "MATCH (a:Airport) RETURN DISTINCT a.state",
            "MATCH (a:Airport) RETURN count(a)",
            "MATCH (a:Airport) WHERE a.state = 'CA' RETURN a.code",
            "MATCH (a:Airport) WHERE a.code = 'LAX' RETURN a.city",
        ] {
            let sql = render(&schema, cypher, dialect).await;
            assert!(
                !sql.contains("flights_denorm.code"),
                "#454 ({dname}) [{cypher}]: emitted the non-existent virtual-id \
                 column `flights_denorm.code`:\n{sql}"
            );
            assert!(
                !sql.contains("__multi_label_union"),
                "#454 ({dname}) [{cypher}]: labeled denorm node scan wrongly routed \
                 through the multi-label whole-node path:\n{sql}"
            );
            // Both the from-side (origin_*) and to-side (dest_*) denormalized
            // columns must be sourced — the node set is the from/to union.
            assert!(
                sql.contains("origin_") && sql.contains("dest_"),
                "#454 ({dname}) [{cypher}]: expected a from/to UNION sourcing BOTH \
                 the origin_* and dest_* denormalized columns:\n{sql}"
            );
            assert!(
                sql.contains("UNION DISTINCT"),
                "#454 ({dname}) [{cypher}]: expected a UNION DISTINCT of the \
                 origin/dest branches:\n{sql}"
            );
        }

        // count(a) must count over the FULL union subquery (both branches), not a
        // single dropped branch (regression: only dest-side airports were counted).
        let count_sql = render(&schema, "MATCH (a:Airport) RETURN count(a)", dialect).await;
        assert_eq!(
            count_sql.matches("flights_denorm").count(),
            2,
            "#454 ({dname}): count(a) must aggregate over BOTH from/to branches \
             inside the __union subquery:\n{count_sql}"
        );
        assert!(
            count_sql.contains("__union"),
            "#454 ({dname}): count(a) over the denorm union must wrap the branches \
             in a `__union` subquery:\n{count_sql}"
        );
    }
}

/// #456 regression (WITH→MATCH chain, both render paths): `MATCH (a:Airport)
/// WITH a WHERE a.state = 'CA' MATCH (a)-[:FLIGHT]->(b:Airport) RETURN a.code,
/// b.code` on the coupled-denormalized schema. `WITH a` materializes the Airport
/// node as a from/to UNION CTE (origin branch projects origin_*, dest branch
/// projects dest_*). The post-WITH `WHERE a.state='CA'` was resolved position-blind
/// (the label→column map always yields the from/origin column) and then copied
/// VERBATIM to every UNION branch, so the dest branch filtered `a.origin_state`
/// instead of `a.dest_state`. That polluted the exported airport set with the
/// destinations of CA-origin flights (live: 7 rows instead of 4). The fix re-points
/// the propagated predicate per branch to that branch's own column for the same
/// exported property. Live: correct 4 rows (flights FROM the CA airports LAX/SFO).
/// Both paths were broken identically, so the golden updates as an intended diff.
#[tokio::test]
async fn denorm_with_match_chain_filters_per_branch_column_456() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());
    let cypher = "MATCH (a:Airport) WITH a WHERE a.state = 'CA' \
                  MATCH (a)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code";

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        // `render()` is the production (plan_ctx) path post-#459.
        let sql = render(&schema, cypher, dialect).await;
        // The from/origin branch keeps the origin column…
        assert!(
            sql.contains("a.origin_state = 'CA'"),
            "#456 ({dialect:?}): origin branch must filter origin_state:\n{sql}"
        );
        // …and the dest branch must filter its OWN dest column, not origin.
        assert!(
            sql.contains("a.dest_state = 'CA'"),
            "#456 ({dialect:?}): dest branch must filter dest_state \
             (was origin_state — polluted the exported node set):\n{sql}"
        );
        // The dest branch must NOT carry the from-side column (the bug).
        assert_eq!(
            sql.matches("a.origin_state = 'CA'").count(),
            1,
            "#456 ({dialect:?}): exactly one branch may filter \
             origin_state (the origin branch); the dest branch leaked it:\n{sql}"
        );
    }

    // Follow-up (review finding): the per-branch remap must also descend into
    // expression WRAPPERS. The original remapper hand-rolled its recursion over
    // only PropertyAccess/Operator/Aggregate/ScalarFnCall, so a CASE-wrapped
    // predicate left the dest branch filtering `a.origin_state` INSIDE the CASE
    // (live: 7 rows again). Now implemented on `ExprVisitor::transform_expr`
    // (expression_utils.rs), whose default walk covers Case/List/subscripts/
    // slices/subqueries. Live after: 4 rows.
    let case_cypher = "MATCH (a:Airport) \
                       WITH a WHERE (CASE WHEN a.state = 'CA' THEN 1 ELSE 0 END) = 1 \
                       MATCH (a)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code";

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, case_cypher, dialect).await;
        // The dest branch's CASE must test its OWN column…
        assert!(
            sql.contains("CASE WHEN a.dest_state = 'CA'"),
            "#456 follow-up ({dialect:?}): dest branch must remap the \
             column INSIDE the CASE wrapper to dest_state:\n{sql}"
        );
        // …and the origin column may appear in exactly one branch's CASE.
        assert_eq!(
            sql.matches("CASE WHEN a.origin_state = 'CA'").count(),
            1,
            "#456 follow-up ({dialect:?}): the dest branch leaked \
             origin_state inside the CASE (wrapper not descended):\n{sql}"
        );
    }
}

/// #456 regression (OPTIONAL, production path): `MATCH (a:Airport) OPTIONAL MATCH
/// (a)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code` on the coupled-denormalized
/// schema. The production render path (`to_render_plan_with_ctx`) restructures the
/// OPTIONAL denorm hop into `FROM __denorm_scan_a AS a LEFT JOIN flights_denorm AS
/// t1` (the from-node `a` materialized as a from/to UNION CTE, the edge LEFT-joined
/// for OPTIONAL NULL-extension). The to-node `b`'s properties live on the LEFT-joined
/// edge row (`t1.dest_code`), NOT a table named `b`. A stale rewrite mapped the
/// resolved `t1.dest_code` back to the raw `b.code`, referencing a non-existent
/// table → ClickHouse `UNKNOWN_IDENTIFIER` (HTTP 500 in server/cg). The fix drops
/// `to_node_properties` from the edge→node reverse map so to-node columns stay on
/// the edge alias. Live: correct 9 rows (8 flights + PHX null-extended, the only
/// airport with no outgoing flight). Since #459, `render()` IS the production
/// path; this shape has no byte golden (the from/to-union column order is
/// HashMap-nondeterministic — see the known-suspicious notes), so this
/// structural test is its lock.
#[tokio::test]
async fn denorm_optional_match_resolves_to_node_onto_edge_456() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());
    let cypher =
        "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code";

    for (dialect, dname) in [
        (SqlDialect::ClickHouse, "clickhouse"),
        (SqlDialect::Databricks, "databricks"),
    ] {
        // Production render path — the one server / cg / embedded actually use.
        let sql = render(&schema, cypher, dialect).await;
        // Alias quoting differs per dialect (CH: "…", Databricks: `…`).
        let q = if dname == "databricks" { '`' } else { '"' };

        // The OPTIONAL hop must be a LEFT JOIN (NULL-extends airports with no
        // outgoing flight), not an inner scan that drops them.
        assert!(
            sql.to_uppercase().contains("LEFT JOIN"),
            "#456 ({dname}): OPTIONAL denorm hop must render a LEFT JOIN:\n{sql}"
        );
        // The from-node is materialized as the from/to union CTE.
        assert!(
            sql.contains("__denorm_scan_a"),
            "#456 ({dname}): expected the from-node materialization CTE:\n{sql}"
        );
        // The to-node property must resolve to the edge row's dest column…
        assert!(
            sql.contains(&format!("dest_code AS {q}b.code{q}")),
            "#456 ({dname}): to-node `b.code` must resolve to the edge's \
             dest_code, got:\n{sql}"
        );
        // …and must NOT leak the unresolvable raw `b.code` reference (the bug:
        // a phantom table `b` → ClickHouse UNKNOWN_IDENTIFIER / 500).
        assert!(
            !sql.contains(&format!("      b.code AS {q}b.code{q}")),
            "#456 ({dname}): to-node reference leaked as the unresolvable raw \
             `b.code` (phantom table), got:\n{sql}"
        );
    }
}

/// #459 structural lock for the denorm `path_return` case
/// (`MATCH p = (a:Airport)-[:FLIGHT]->(b:Airport) RETURN p`), which is NOT a
/// byte-golden: production materializes the path's node/edge property columns off
/// the single `flights_denorm` scan in nondeterministic HashMap column order, so
/// the byte layout flips across runs (see the denorm known-suspicious block). The
/// pre-#459 ctx-less path emitted ONLY the `tuple('fixed_path', …)` marker with no
/// underlying columns — a path with no reconstructable data. This locks the stable
/// invariants: the fixed-path tuple is present, the node endpoints resolve to the
/// denorm virtual-id columns (`origin_code AS "a.code"` / `dest_code AS "b.code"`),
/// and the edge properties are sourced — order-independent.
#[tokio::test]
async fn denorm_path_return_materializes_node_edge_props_459() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());
    let cypher = "MATCH p = (a:Airport)-[:FLIGHT]->(b:Airport) RETURN p";

    for (dialect, dname) in [
        (SqlDialect::ClickHouse, "clickhouse"),
        (SqlDialect::Databricks, "databricks"),
    ] {
        let sql = render(&schema, cypher, dialect).await;
        let q = if dname == "databricks" { '`' } else { '"' };
        // CH renders the fixed-path tuple as `tuple(...)`, Databricks as `struct(...)`.
        let marker = if dname == "databricks" {
            "struct('fixed_path', 'a', 'b',"
        } else {
            "tuple('fixed_path', 'a', 'b',"
        };

        // The fixed-path marker tuple must be present.
        assert!(
            sql.contains(marker),
            "#459 ({dname}): path_return must emit the fixed_path marker:\n{sql}"
        );
        // Both node endpoints resolve to the denorm virtual-id physical columns…
        assert!(
            sql.contains(&format!("origin_code AS {q}a.code{q}")),
            "#459 ({dname}): from-node code must resolve to origin_code:\n{sql}"
        );
        assert!(
            sql.contains(&format!("dest_code AS {q}b.code{q}")),
            "#459 ({dname}): to-node code must resolve to dest_code:\n{sql}"
        );
        // …and the edge's own properties are materialized for the path.
        assert!(
            sql.contains("flights_denorm") && sql.contains("carrier"),
            "#459 ({dname}): path edge properties must be sourced from \
             flights_denorm:\n{sql}"
        );
    }
}

/// #455 regression: ORDER BY over a denormalized hop must qualify the sort term
/// with the resolved TABLE alias (`t0.origin_code`), not the raw Cypher node
/// alias (`a.origin_code`, which CH rejects with `Unknown expression identifier`).
/// The column was resolved at planning but the non-ctx OrderBy render handler
/// skipped the alias→edge-table remap that SELECT/WHERE apply.
#[tokio::test]
async fn denorm_order_by_uses_table_alias_not_cypher_alias_455() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());

    for (dialect, dname) in [
        (SqlDialect::ClickHouse, "clickhouse"),
        (SqlDialect::Databricks, "databricks"),
    ] {
        for cypher in [
            "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code ORDER BY a.code DESC SKIP 1 LIMIT 3",
            "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.code ORDER BY a.code SKIP 2",
        ] {
            let sql = render(&schema, cypher, dialect).await;
            // The single denorm table gets an anonymized alias (t{n}); the ORDER BY
            // must reference that alias' physical column, never the Cypher alias `a`.
            let order_line = sql
                .lines()
                .find(|l| l.trim_start().starts_with("ORDER BY"))
                .unwrap_or_else(|| panic!("#455 ({dname}) [{cypher}]: no ORDER BY line:\n{sql}"));
            assert!(
                !order_line.contains("a.origin_code") && !order_line.contains("a.dest_code"),
                "#455 ({dname}) [{cypher}]: ORDER BY still uses the raw Cypher alias \
                 instead of the resolved table alias:\n{sql}"
            );
            assert!(
                order_line.contains("origin_code"),
                "#455 ({dname}) [{cypher}]: ORDER BY must sort by the resolved \
                 physical column origin_code:\n{sql}"
            );
        }
    }
}

/// #470 regression: on a COUPLED-denormalized schema (`zeek_merged_test.yaml`)
/// where a node's `node_id` Cypher name (`id.orig_h`) differs from a property
/// that maps to the SAME db column (`ip: id.orig_h`), the OPTIONAL-MATCH denorm
/// LEFT JOIN key was resolved by iterating a `HashMap` of anchor properties and
/// picking the first whose column matched the edge `from_id`. Both `ip` and the
/// raw self-mapping `id.orig_h` matched, so the pick was NONDETERMINISTIC across
/// fresh processes (~50/50): half the renders keyed on `a.ip` (VALID — the
/// `__denorm_scan_a` CTE exposes `ip`) and half on `a."id.orig_h"` (INVALID —
/// the CTE does NOT expose that column; ClickHouse errors UNKNOWN_IDENTIFIER).
///
/// The fix resolves the join key FORWARD through the CTE's actually-exposed
/// columns (CLAUDE.md rule 2), deterministically. This test locks BOTH the
/// determinism (repeated renders are byte-identical — `HashMap` seeds differ per
/// map, so a nondeterministic site flips within a single process) AND the
/// correctness (the CTE-side join key is the exposed property `a.ip`, never the
/// unexposed raw node_id `a."id.orig_h"`).
#[tokio::test]
async fn denorm_optional_join_key_forward_resolved_and_deterministic_470() {
    let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
    let repro = "MATCH (a:IP) OPTIONAL MATCH (a)-[:REQUESTED]->(d) RETURN a.ip, a.port, d.name";

    // Determinism: many fresh renders in-process must all be byte-identical.
    let first = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
    for _ in 0..30 {
        let again = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#470: OPTIONAL denorm join-key render is nondeterministic:\n\
             FIRST:\n{first}\nAGAIN:\n{again}"
        );
    }

    // Correctness: the LEFT JOIN's CTE-side key must be the CTE-exposed property
    // `a.ip`, NOT the raw node_id `a."id.orig_h"` (which the CTE does not expose).
    let join_line = first
        .lines()
        .find(|l| l.contains("LEFT JOIN"))
        .unwrap_or_else(|| panic!("#470: no LEFT JOIN line:\n{first}"));
    assert!(
        join_line.contains("ON a.ip ="),
        "#470: OPTIONAL denorm join must key on the CTE-exposed column a.ip:\n{first}"
    );
    assert!(
        !join_line.contains(r#"a."id.orig_h""#),
        "#470: OPTIONAL denorm join must NOT key on the unexposed raw node_id \
         a.\"id.orig_h\" (invalid — CTE exposes only ip/port):\n{first}"
    );

    // Sibling coupled shapes from the same table must also render deterministically.
    for cypher in [
        "MATCH (d:Domain) OPTIONAL MATCH (d)-[:RESOLVED_TO]->(r) RETURN d.name, r.ip",
        "MATCH (a:IP) OPTIONAL MATCH (a)-[:ACCESSED]->(b) RETURN a.ip, b.ip",
    ] {
        let base = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
        for _ in 0..10 {
            let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(
                base, again,
                "#470: coupled shape render is nondeterministic [{cypher}]:\n\
                 BASE:\n{base}\nAGAIN:\n{again}"
            );
        }
    }
}

/// #493 regression: `MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b)
/// RETURN a.code, count(b)` on the denormalized flights schema emitted
/// `count(b.code)` with `b` never bound to any table alias — ClickHouse
/// UNKNOWN_IDENTIFIER at execution. The planner correctly rewrites
/// `count(node)` → `count(node.<node_id>)` for NULL-correct OPTIONAL
/// counting, but the SELECT extraction only resolved denormalized (virtual)
/// node references at the TOP level of a projection item, not inside
/// aggregate arguments. The reference must resolve onto the owning edge's
/// embedded column: `count(t1.dest_code)` — NULL-sensitive, so optional-miss
/// rows count 0.
///
/// Live-verified on `db_denormalized` (8 flights): OPTIONAL variant returns 7
/// groups with `PHX -> 0` (the dest-only airport), required variant returns 6
/// groups, both matching hand-written LEFT-JOIN/GROUP-BY ground truth with
/// `join_use_nulls=1` (the setting production applies). Both previously
/// failed with UNKNOWN_IDENTIFIER.
#[tokio::test]
async fn denorm_count_node_resolves_embedded_id_column_493() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());

    // (cypher, the aggregate that must appear after `normalize`'s alias
    // anonymization — the single edge scan is always the first t-alias, t0 —
    // and a context tag)
    let cases = [
        (
            "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b) RETURN a.code, count(b)",
            "count(t0.dest_code)",
            "optional count(b)",
        ),
        (
            "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b) RETURN a.code, count(DISTINCT b)",
            "count(DISTINCT t0.dest_code)",
            "optional count(DISTINCT b)",
        ),
        (
            "MATCH (a:Airport)-[:FLIGHT]->(b) RETURN a.code, count(b)",
            "count(t0.dest_code)",
            "required count(b)",
        ),
        // Aggregates NESTED in wrapper expressions (review coverage gap): the
        // resolver must reach them through operator / scalar-fn wrappers too.
        (
            "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b) RETURN a.code, count(b) + 0 AS c",
            "count(t0.dest_code) + 0",
            "optional count(b) + 0",
        ),
        (
            "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b) RETURN a.code, toFloat(count(b)) AS c",
            "toFloat64(count(t0.dest_code))",
            "optional toFloat(count(b))",
        ),
    ];

    for (cypher, want_agg, tag) in cases {
        let first = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
        for _ in 0..5 {
            let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(
                first, again,
                "#493 [{tag}]: render is nondeterministic:\nFIRST:\n{first}\nAGAIN:\n{again}"
            );
        }
        // The aggregate must reference the owning edge's embedded to-column…
        assert!(
            first.contains(want_agg),
            "#493 [{tag}]: expected `{want_agg}` (owning edge's embedded id \
             column, NULL-sensitive), got:\n{first}"
        );
        // …and the unbound cypher alias must not leak into the SQL.
        assert!(
            !first.contains("count(b.") && !first.contains("count(DISTINCT b."),
            "#493 [{tag}]: unresolved alias `b` leaked into the aggregate \
             (UNKNOWN_IDENTIFIER at execution):\n{first}"
        );
    }
}

/// #502 regression: `count(r)` on an OPTIONAL MATCH relationship must render
/// as a NULL-sensitive count over one of the edge's own (edge_id) columns,
/// not `count(*)`. `count(*)` counts the anchor row itself, which a LEFT
/// JOIN always preserves (NULL-extended) even when the relationship never
/// matched — so a zero-edge anchor silently reported `count(r) == 1`. This
/// is the relationship-count sibling of #493's node-count fix (`count(b)` ->
/// `count(t0.dest_code)`).
#[tokio::test]
async fn denorm_count_relationship_resolves_edge_id_column_502() {
    let denorm_schema = load_schema(SchemaId::Denormalized.yaml_path());
    let coupled_schema = load_schema("schemas/dev/zeek_merged_test.yaml");
    let standard_schema = load_schema(SchemaId::Standard.yaml_path());

    // (schema, cypher, the aggregate that must appear after `normalize`'s
    // alias anonymization, a context tag)
    let cases = [
        (
            &denorm_schema,
            "MATCH (a:Airport) OPTIONAL MATCH (a)-[r:FLIGHT]->(b) RETURN a.code, count(r)",
            "count(r.flight_id)",
            "denorm optional count(r)",
        ),
        (
            &denorm_schema,
            "MATCH (a:Airport)-[r:FLIGHT]->(b) RETURN a.code, count(r)",
            "count(r.flight_id)",
            "denorm required count(r)",
        ),
        (
            &denorm_schema,
            "MATCH (a:Airport) OPTIONAL MATCH (a)-[r:FLIGHT]->(b) RETURN a.code, count(DISTINCT r)",
            "count(DISTINCT tuple(r.flight_id, r.flight_number))",
            "denorm optional count(DISTINCT r)",
        ),
        (
            &coupled_schema,
            "MATCH (a:IP) OPTIONAL MATCH (a)-[r:REQUESTED]->(d) RETURN a.ip, count(r)",
            "count(r.uid)",
            "coupled optional count(r)",
        ),
        (
            &standard_schema,
            "MATCH (a:User) OPTIONAL MATCH (a)-[r:FOLLOWS]->(b) RETURN a.name, count(r)",
            "count(r.follower_id)",
            "standard optional count(r)",
        ),
    ];

    for (schema, cypher, want_agg, tag) in cases {
        let first = normalize(&render(schema, cypher, SqlDialect::ClickHouse).await);
        for _ in 0..5 {
            let again = normalize(&render(schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(
                first, again,
                "#502 [{tag}]: render is nondeterministic:\nFIRST:\n{first}\nAGAIN:\n{again}"
            );
        }
        // The aggregate must be NULL-sensitive (an edge_id column), never
        // count(*), which is always 1 on a LEFT JOIN miss.
        assert!(
            first.contains(want_agg),
            "#502 [{tag}]: expected `{want_agg}` (NULL-sensitive edge_id \
             column), got:\n{first}"
        );
        assert!(
            !first.contains("count(*)"),
            "#502 [{tag}]: count(r) rendered as NULL-insensitive count(*) — \
             zero-edge anchors would report count == 1:\n{first}"
        );
    }
}

/// #506 regression: an INCOMING-direction OPTIONAL MATCH on a denormalized
/// schema (`MATCH (a:Airport) OPTIONAL MATCH (a)<-[:FLIGHT]-(b) ...`) must
/// render the same shape as the OUTGOING direction — an anchor
/// `__denorm_scan_a` CTE + a correctly-keyed LEFT JOIN — not collapse to a
/// standalone Union with an alias (`a`) never introduced in FROM.
///
/// Root cause: `is_optional_denorm_union_graphrel` (the gate for the special
/// CTE + LEFT JOIN rendering) only checked `gr.left` for the anchor's
/// standalone-scan Union. CLAUDE.md rule 4's anchor-aware FROM/JOIN reversal
/// puts the anchor on `gr.right` for incoming-direction OPTIONAL MATCH, so
/// the gate silently never fired, and `UnionDistribution`'s matching
/// right-Union case had no exception to preserve LEFT JOIN semantics either
/// (it unconditionally distributed the OPTIONAL edge into each Union branch).
#[tokio::test]
async fn denorm_incoming_optional_match_preserves_anchor_scan_and_join_506() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());

    let cases = [
        (
            "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b) RETURN a.code, b.code",
            "a.code = t0.origin_code",
            "outgoing",
        ),
        (
            "MATCH (a:Airport) OPTIONAL MATCH (a)<-[:FLIGHT]-(b) RETURN a.code, b.code",
            "a.code = t0.dest_code",
            "incoming",
        ),
    ];

    for (cypher, want_join, tag) in cases {
        let first = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
        for _ in 0..5 {
            let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(
                first, again,
                "#506 [{tag}]: render is nondeterministic:\nFIRST:\n{first}\nAGAIN:\n{again}"
            );
        }
        // The anchor's standalone-scan CTE must always be present — its
        // absence is exactly how #505/#506's silent row loss and invalid SQL
        // happened.
        assert!(
            first.contains("__denorm_scan_a"),
            "#506 [{tag}]: anchor scan CTE __denorm_scan_a missing — anchor \
             rows with no match would be silently dropped:\n{first}"
        );
        // The LEFT JOIN key must reference the correct edge column for this
        // direction (origin for outgoing, dest for incoming) — never a
        // fixed/wrong side, and never an impossible `1 = 0`/`1 = 1` fallback.
        assert!(
            first.contains(want_join),
            "#506 [{tag}]: expected join condition `{want_join}`, got:\n{first}"
        );
        assert!(
            !first.contains("ON 1 = 1") && !first.contains("ON 1 = 0"),
            "#506 [{tag}]: fell back to an impossible/always-true join \
             condition instead of resolving the anchor's join key:\n{first}"
        );
        // Every table alias referenced in SELECT must be introduced by FROM
        // or a JOIN — the original #506 symptom was `a.*` referenced with no
        // `AS a` anywhere in the query.
        assert!(
            first.contains("__denorm_scan_a AS a") || first.contains("AS a\n"),
            "#506 [{tag}]: alias 'a' used in SELECT but never introduced in \
             FROM/JOIN (invalid SQL):\n{first}"
        );
    }
}

/// #505 regression: a chained double-OPTIONAL on a denormalized schema
/// (`MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b) OPTIONAL MATCH
/// (b)-[:FLIGHT]->(c) ...`) must still preserve the anchor's
/// `__denorm_scan_a` CTE — the SAME requirement as the single-hop case
/// (#506's regression test), just with a second OPTIONAL hop chained after
/// it. Dropping the CTE means anchor rows with no outgoing edge at all (e.g.
/// an airport with zero flights) silently vanish.
///
/// Root cause: `find_inner_optional_denorm_graphrel` located the anchor's
/// Union only by walking wrapper nodes (GraphJoins/Projection/Filter/etc.),
/// never into a nested `GraphRel.left`/`.right` — so a SECOND optional hop
/// (which wraps the first hop's GraphRel as its own `.left`) hid the anchor
/// Union from the detector entirely, and rendering fell through to the
/// generic GraphJoins path, which (for this schema pattern) treats the first
/// hop's edge table as a bare FROM marker instead of building the anchor CTE
/// + a real JOIN key.
#[tokio::test]
async fn denorm_chained_optional_preserves_anchor_scan_505() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());
    let cypher = "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b) OPTIONAL MATCH (b)-[:FLIGHT]->(c) RETURN a.code, b.code, c.code";

    let first = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
    for _ in 0..5 {
        let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#505: render is nondeterministic:\nFIRST:\n{first}\nAGAIN:\n{again}"
        );
    }
    assert!(
        first.contains("__denorm_scan_a"),
        "#505: anchor scan CTE __denorm_scan_a missing on a chained double- \
         OPTIONAL — anchor rows with no outgoing edge at all would be \
         silently dropped:\n{first}"
    );
    // Both hops' LEFT JOINs must be present, in dependency order: the first
    // hop keyed off the anchor CTE, the second keyed off the first hop's
    // table (never an impossible/always-true fallback condition).
    assert!(
        first.contains("a.code = t0.origin_code"),
        "#505: first hop's JOIN must key off the anchor CTE alias, got:\n{first}"
    );
    assert!(
        first.contains("t1.origin_code = t0.dest_code"),
        "#505: second hop's JOIN (already correctly computed by the generic \
         pipeline) must be preserved after the anchor CTE stitch, got:\n{first}"
    );
    assert!(
        !first.contains("ON 1 = 1") && !first.contains("ON 1 = 0"),
        "#505: fell back to an impossible/always-true join condition:\n{first}"
    );
}

/// #506 follow-up (adversarial review, post-merge): incoming-direction denorm
/// OPTIONAL MATCH silently dropped a WHERE clause on the matched (non-anchor)
/// node entirely — no error, no WHERE in the generated SQL at all, returning
/// every anchor row unfiltered instead of the correctly-filtered subset.
/// Outgoing direction already rendered the WHERE correctly; the two
/// directions must be consistent.
///
/// Root cause (two layers):
/// 1. The CTE + LEFT JOIN special-case rendering path (`to_render_plan_with_ctx`)
///    re-extracts SELECT/GROUP BY/ORDER BY/SKIP/LIMIT from the outer plan
///    after delegating to `inner.to_render_plan`, but never re-extracted
///    `render.filters` — so any WHERE was silently lost regardless of
///    direction, UNLESS some other code path happened to keep it.
/// 2. `collect_graphrel_predicates` deliberately drops a predicate that
///    references ONLY the non-anchor ("optional") alias whenever
///    `anchor_connection` is set, expecting a downstream JOIN `pre_filter` to
///    absorb it — a mechanism this rendering path doesn't have. Outgoing
///    direction never hit this because its `anchor_connection` is always
///    `None` (CLAUDE.md rule 4), which happens to route through the
///    "no anchor determined — keep all predicates" fallback instead.
///
/// #533 UPDATE: the single-hop cases (both directions) now fold the
/// optional-node predicate into the edge JOIN's `pre_filter` — `LEFT JOIN
/// (SELECT * FROM ... WHERE dest_state = 'CA') AS r ON ...` — instead of a
/// bare post-join outer WHERE, fixing the NULL-extended-row-dropping bug
/// `denorm_479_..._known_broken` characterized (now replaced by
/// `denorm_optional_node_predicate_folds_into_pre_filter_533`). The
/// pre_filter subquery has only ONE table in scope, so the column is
/// unqualified (no `r.`/`t1.` prefix) — expectations updated accordingly.
/// The chained-double-OPTIONAL case's SECOND hop is a genuinely different
/// shape (its anchor `b` is itself a LEFT-JOINed row from the first hop, not
/// a `__denorm_scan` CTE) that #533 does not touch — still a bare outer
/// WHERE, unchanged.
#[tokio::test]
async fn denorm_optional_where_preserved_both_directions_506_followup() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());

    // (cypher, the WHERE condition that must survive, whether it must NOT be
    // a bare post-join outer WHERE (folded into pre_filter instead), a tag)
    let cases = [
        (
            "MATCH (a:Airport) OPTIONAL MATCH (a)-[r:FLIGHT]->(b:Airport) WHERE b.state = 'CA' RETURN a.code, b.code",
            "dest_state = 'CA'",
            true,
            "outgoing, single hop",
        ),
        (
            "MATCH (a:Airport) OPTIONAL MATCH (a)<-[r:FLIGHT]-(b:Airport) WHERE b.state = 'CA' RETURN a.code, b.code",
            "origin_state = 'CA'",
            true,
            "incoming, single hop",
        ),
        (
            "MATCH (a:Airport) OPTIONAL MATCH (a)<-[:FLIGHT]-(b:Airport) OPTIONAL MATCH (b)<-[:FLIGHT]-(c:Airport) WHERE c.state = 'CA' RETURN a.code, b.code, c.code",
            "t1.origin_state = 'CA'",
            false,
            "incoming, chained double-OPTIONAL (#505 shape)",
        ),
    ];

    for (cypher, want_where, folds_into_pre_filter, tag) in cases {
        let first = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
        for _ in 0..5 {
            let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(
                first, again,
                "#506-followup [{tag}]: render is nondeterministic:\nFIRST:\n{first}\nAGAIN:\n{again}"
            );
        }
        assert!(
            first.contains("WHERE"),
            "#506-followup [{tag}]: WHERE clause dropped entirely:\n{first}"
        );
        assert!(
            first.contains(want_where),
            "#506-followup [{tag}]: expected WHERE condition `{want_where}`, got:\n{first}"
        );
        let has_bare_outer_where = first
            .lines()
            .any(|l| l.trim_start().starts_with("WHERE") && l.contains(want_where));
        assert_eq!(
            has_bare_outer_where, !folds_into_pre_filter,
            "#506-followup/#533 [{tag}]: expected folds_into_pre_filter={folds_into_pre_filter} \
             but bare-outer-WHERE presence was {has_bare_outer_where}:\n{first}"
        );
    }
}

/// FIXED (#575): the UNDIRECTED variant of the #566/#533 denorm OPTIONAL
/// MATCH anchor-WHERE family — `MATCH (a:Airport) OPTIONAL MATCH
/// (a)-[:FLIGHT]-(b:Airport) WHERE a.city = 'Chicago'` — silently dropped the
/// `a.city = 'Chicago'` anchor predicate ENTIRELY (no error, no WHERE
/// anywhere in the generated SQL), returning every anchor row unfiltered
/// (live-verified: 18 rows instead of the 3 correct Chicago-anchor rows).
/// The directed forms (`-[:FLIGHT]->`, `<-[:FLIGHT]-`) were already correct
/// (#566/#533).
///
/// Root cause: `BidirectionalUnion` splits an undirected OPTIONAL MATCH into
/// a `Union` of two direction-permutation `GraphRel` branches BEFORE
/// `FilterTagging` runs. `FilterTagging::analyze_union_branch` deliberately
/// does NOT extract a WHERE predicate into `plan_ctx`'s shared per-alias
/// filter map when processing a Union branch (needed to avoid cross-branch
/// contamination for genuine per-label type-union splits) — it leaves the
/// `Filter` node in the tree instead, later folded correctly (and
/// per-branch-role-resolved) into `GraphRel.where_predicate` by
/// `FilterIntoGraphRel`. But `FilterIntoGraphRel`'s OWN separate mechanism
/// for seeding the anchor's standalone-scan Union branches'
/// `ViewScan.view_filter` — what the CTE-building code in `plan_builder.rs`
/// actually renders the CTE's WHERE from — reads that SAME shared
/// `plan_ctx` filter map, left empty by the Union-branch skip. Meanwhile
/// `collect_graphrel_predicates` deliberately drops this same anchor-only
/// conjunct from the outer WHERE, assuming the CTE already has it (#533) —
/// the exact #554 "both mechanisms decline, assuming the other handles it"
/// silent-drop shape, just via a different pair of mechanisms than #554
/// itself. Fixed in `plan_builder.rs`'s OPTIONAL-denorm-Union CTE-building
/// path by recovering the anchor-only conjunct(s) directly from
/// `GraphRel.where_predicate` (reliable for both directed and
/// undirected/split shapes) and injecting them into any Union branch whose
/// `ViewScan` doesn't already carry a `view_filter`, re-targeted to that
/// branch's own role via the same #566 role-aware rewrite. Never touches an
/// already-populated `view_filter`, so the directed shapes' rendering is
/// unchanged (see `denorm_optional_where_preserved_both_directions_506_followup`
/// and `sql_golden_snapshots`, both still green).
#[tokio::test]
async fn undirected_optional_anchor_where_not_dropped_575() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());

    let cypher = "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]-(b:Airport) WHERE a.city = 'Chicago' RETURN a.code, b.code";

    let first = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
    for _ in 0..5 {
        let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#575: render is nondeterministic:\nFIRST:\n{first}\nAGAIN:\n{again}"
        );
    }

    // The anchor-only predicate must survive, remapped through BOTH physical
    // roles of the anchor's own denormalized standalone-scan Union (origin
    // AND dest) — a bare `WHERE a.city = ...` can never appear (that column
    // doesn't exist on the physical table), so its presence in EITHER
    // role-mapped form is the only correct outcome, and #575's bug was that
    // NEITHER appeared anywhere in the SQL at all.
    assert!(
        first.contains("origin_city = 'Chicago'"),
        "#575: anchor predicate missing from the origin-role branch of the \
         __denorm_scan_a CTE:\n{first}"
    );
    assert!(
        first.contains("dest_city = 'Chicago'"),
        "#575: anchor predicate missing from the dest-role branch of the \
         __denorm_scan_a CTE:\n{first}"
    );
    assert!(
        !first.contains("WHERE a.city"),
        "#575: predicate must be resolved to a physical column, not left as \
         the raw (nonexistent) `a.city`:\n{first}"
    );
}

/// FIXED (#582): an OR-mixed WHERE predicate spanning the anchor AND the
/// OPTIONAL side (`MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]-(b:Airport)
/// WHERE a.city = 'Phoenix' OR b.state = 'ZZ'`) silently dropped the
/// NULL-extended anchor row that `a.city = 'Phoenix'` alone correctly keeps.
/// Live-verified on `db_denormalized` (PHX is never itself an `origin_code`,
/// so its only connection is via `DEN -> PHX` plus one NULL-extended row):
/// `WHERE a.city='Phoenix'` alone returns 2 rows (`PHX|DEN`, `PHX|NULL`); with
/// `OR b.state='ZZ'` added (an always-false disjunct on `b` — a no-op WHEN
/// disjunction is correctly evaluated with three-valued SQL logic) the
/// pre-fix renderer dropped to 1 row (only `PHX|DEN`), because `a.city` was
/// resolved to `t1.origin_city` — a column on the NULLABLE LEFT JOIN alias —
/// instead of the anchor's own always-present `__denorm_scan_a` CTE. When
/// `t1` is NULL (the very row an OPTIONAL MATCH exists to keep), `t1.
/// origin_city` is NULL too, so `NULL OR NULL` = NULL, and SQL's WHERE drops
/// the row despite `a.city = 'Phoenix'` being objectively true.
///
/// Root cause: `a.city = 'Chicago' OR b.state = 'CA'` references BOTH
/// aliases, so `references_only_alias_logical` correctly refuses to treat it
/// as anchor-only — splitting an OR across a pre-filter on just one side
/// would itself be unsound (unlike AND, which `split_and_predicates_logical`
/// safely decomposes conjunct-by-conjunct). The predicate therefore stays as
/// ONE unit in the general (non-injected) WHERE clause, which resolves each
/// `PropertyAccess` via `apply_property_mapping_to_expr`
/// (`plan_builder_helpers.rs`). That function's denormalized-node handling
/// (structural property mapping through the CURRENT GraphRel's edge, plus
/// the `denormalized_node_edges` registry's alias remap to the edge/join
/// alias) has no concept of the render-time-only `__denorm_scan_{alias}`
/// anchor CTE — a SEPARATE mechanism (`optional_denorm_union_anchor_is_left`)
/// that exists specifically so the anchor's OWN properties stay
/// always-present, independent of whether the OPTIONAL edge matched. Fixed
/// by `is_denorm_scan_anchor_alias` + `denorm_scan_cte_anchor_reverse_
/// property`: when a `PropertyAccess`'s alias is this CTE's anchor AND the
/// property genuinely belongs to it (already the CTE's own Cypher name, or a
/// physical column that reverse-maps to one), leave it resolved against the
/// anchor alias directly and skip the edge-alias remap — regardless of
/// whether the predicate is a lone conjunct, part of an AND, or (this case)
/// part of an OR.
#[tokio::test]
async fn or_mixed_anchor_optional_predicate_not_dropped_582() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());

    let cypher = "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]-(b:Airport) \
                  WHERE a.city = 'Phoenix' OR b.state = 'ZZ' RETURN a.code, b.code";

    let first = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
    for _ in 0..5 {
        let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#582: render is nondeterministic:\nFIRST:\n{first}\nAGAIN:\n{again}"
        );
    }

    // The anchor's own predicate must resolve directly against the anchor
    // alias (the `__denorm_scan_a` CTE exposes `city` under its plain Cypher
    // name) — NOT through the nullable LEFT JOIN alias.
    assert!(
        first.contains("a.city = 'Phoenix'"),
        "#582: anchor-only disjunct must resolve against the anchor's own \
         always-present `__denorm_scan_a` CTE alias, not the LEFT JOIN's \
         edge alias:\n{first}"
    );
    assert!(
        !first.contains("origin_city = 'Phoenix'") && !first.contains("dest_city = 'Phoenix'"),
        "#582: anchor predicate must NOT be resolved through the edge/join \
         alias's from/to-side column (that's NULL on exactly the \
         OPTIONAL-miss row this predicate must keep):\n{first}"
    );

    // The genuinely optional-side disjunct (`b.state`) is unaffected — it
    // still correctly resolves through the LEFT JOIN's edge alias (NULL on
    // an OPTIONAL-miss row is the correct semantics for `b`'s own property).
    assert!(
        first.contains("origin_state = 'ZZ'") || first.contains("dest_state = 'ZZ'"),
        "#582: optional-side disjunct must still resolve through the LEFT \
         JOIN's edge alias:\n{first}"
    );

    // Control: the already-working single anchor-only conjunct (#575, no OR)
    // must render unaffected — still pushed into the CTE's own inner
    // UNION branches, not left as a bare outer WHERE.
    let single = normalize(
        &render(
            &schema,
            "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]-(b:Airport) \
             WHERE a.city = 'Phoenix' RETURN a.code, b.code",
            SqlDialect::ClickHouse,
        )
        .await,
    );
    assert!(
        single.contains("origin_city = 'Phoenix'") && single.contains("dest_city = 'Phoenix'"),
        "#582: must not regress the #575 anchor-only (no OR) injection path:\n{single}"
    );
}

/// FIXED (#533): the denormalized `__denorm_scan` CTE + LEFT JOIN OPTIONAL
/// MATCH shape's OWN (non-anchor) optional-node predicate now folds into the
/// edge JOIN's `pre_filter` — `LEFT JOIN (SELECT * FROM ... WHERE ...) AS r
/// ON ...` — instead of a bare post-join outer WHERE, which silently dropped
/// every NULL-extended (no-match) anchor row. Replaces the
/// `denorm_479_plain_optional_where_drops_null_extended_rows_known_broken`
/// characterization test — same repro, no longer broken.
///
/// Root cause: this shape's manually-built LEFT JOIN (`plan_builder.rs`'s
/// "OPTIONAL denormalized Union" branch) never goes through the generic
/// JOIN-building code `apply_optional_node_pre_filters` (#474,
/// join_builder.rs) lives in, so nothing folded the optional-node-only
/// predicate into a `pre_filter` for this shape specifically — it fell
/// through to a bare outer WHERE, evaluating false (not "keep, NULL-extend")
/// against every non-matching anchor's NULL-extended row. Fixed by
/// extracting the optional node's own conjunct from `gr.where_predicate`
/// (still carrying its original Cypher alias, e.g. `b`, at this point — only
/// its COLUMN has been role-mapped so far) and rewriting that alias onto the
/// edge JOIN's own alias before installing it as `pre_filter`;
/// `collect_graphrel_predicates` (plan_builder_helpers.rs) drops the SAME
/// conjunct from the outer WHERE so it is embedded exactly once.
///
/// Live-verified (2026-07-11, `db_denormalized`): `MATCH (a:Airport)
/// OPTIONAL MATCH (a)-[:FLIGHT]->(b:Airport) WHERE b.city = 'Chicago' RETURN
/// a.code, b.code` returns exactly 7 rows — LAX->ORD and SFO->ORD (the two
/// Chicago-bound flights) plus 5 NULL-extended airports (ATL, DEN, JFK, ORD,
/// PHX — including dest-only PHX, which a naive `SELECT DISTINCT
/// origin_code` ground-truth check would miss). Was 2 rows pre-fix (dropped
/// all 5 NULL-extended rows).
#[tokio::test]
async fn denorm_optional_node_predicate_folds_into_pre_filter_533() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());
    let cypher = "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b:Airport) \
                  WHERE b.city = 'Chicago' RETURN a.code, b.code";
    let first = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
    for _ in 0..5 {
        let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#533: render is nondeterministic:\nFIRST:\n{first}\nAGAIN:\n{again}"
        );
    }

    assert!(
        first.contains("WHERE dest_city = 'Chicago'"),
        "#533: expected the predicate folded into the edge JOIN's pre_filter \
         subquery (unqualified column — only one table in scope there), \
         got:\n{first}"
    );
    assert!(
        !first
            .lines()
            .any(|l| l.trim_start().starts_with("WHERE") && l.contains("dest_city")),
        "#533 regressed: predicate is back in a bare outer WHERE post-LEFT-JOIN \
         (drops NULL-extended anchors instead of folding into pre_filter):\n{first}"
    );
}

/// #582 review-round regression: the FIRST attempt at the #582 fix (a bare
/// "anchor alias -> always leave PropertyAccess untouched" rule with no
/// concept of WHICH properties the anchor's `__denorm_scan_{alias}` CTE
/// actually exposes) introduced two NEW bugs, both caught by the full test
/// gate rather than the primary live repro:
///
/// 1. GROUP BY on the coupled `zeek_dns` schema, where the OPTIONAL MATCH's
///    relationship type doesn't even connect the anchor's label (`MATCH
///    (a:IP) OPTIONAL MATCH (a)-[:RESOLVED_TO]->(b)` — `RESOLVED_TO` connects
///    Domain/ResolvedIP, not IP, rendering as a `1 = 0` fallback join): the
///    reverse lookup was keyed off the CURRENT (label-mismatched) edge's own
///    property maps, which don't cover the anchor's label at all, so the
///    physical column (`id.orig_h`) leaked through unresolved — invalid SQL
///    (`GROUP BY a."id.orig_h"` against a CTE that only exposes
///    `"ip_address"`). Fixed by resolving against the anchor's OWN scan Union
///    (`denorm_scan_cte_anchor_reverse_property`, independent of which edge
///    the CURRENT OPTIONAL MATCH names) instead of the current GraphRel's
///    edge.
/// 2. A genuinely EDGE-OWNED property requested via `anchor.prop` syntax
///    (`ontime_flights`: `RETURN a.airport, count(r)`, where `airport` is
///    NOT part of Airport's own from/to-node-property definition and the CTE
///    was deliberately built without it) got incorrectly "protected" from the
///    edge-alias remap too, producing `GROUP BY a.airport` — invalid SQL
///    against a CTE that never had an `airport` column. Fixed by checking
///    the property genuinely belongs to the CTE (already its Cypher name, or
///    reverse-maps to one) before skipping the old edge-based resolution;
///    otherwise fall through unchanged (matches the pre-#582 behavior for
///    this shape: `GROUP BY r.airport`).
#[tokio::test]
async fn denorm_group_by_anchor_property_edge_mismatch_and_edge_owned_582() {
    // Case 1: label-mismatched OPTIONAL relationship — anchor's own id
    // property must resolve through the CTE alias, never the raw physical
    // column.
    let zeek_schema = load_schema("schemas/examples/zeek_dns_log.yaml");
    let mismatched = "MATCH (a:IP) OPTIONAL MATCH (a)-[r:RESOLVED_TO]->(b) \
                       RETURN a.ip_address, count(r) AS rel_count";
    let first = normalize(&render(&zeek_schema, mismatched, SqlDialect::ClickHouse).await);
    for _ in 0..5 {
        let again = normalize(&render(&zeek_schema, mismatched, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#582: GROUP BY render is nondeterministic on a label-mismatched \
             OPTIONAL relationship:\nFIRST:\n{first}\nAGAIN:\n{again}"
        );
    }
    assert!(
        first.contains("GROUP BY a.ip_address"),
        "#582: anchor's own id property must GROUP BY through the CTE alias \
         under its Cypher name, even when the OPTIONAL relationship type \
         doesn't connect the anchor's label:\n{first}"
    );
    assert!(
        !first.contains(r#"GROUP BY a."id.orig_h""#),
        "#582: must not GROUP BY the raw physical column — invalid SQL \
         against a CTE that only exposes `ip_address`:\n{first}"
    );

    // Case 2: genuinely edge-owned property requested via anchor syntax —
    // must fall through to the pre-#582 edge-alias resolution unchanged.
    let ontime_schema = load_schema("schemas/examples/ontime_denormalized.yaml");
    let edge_owned =
        "MATCH (a:Airport) OPTIONAL MATCH (a)-[r:FLIGHT]->(b) RETURN a.airport, count(r) AS rel_count";
    let edge_owned_sql =
        normalize(&render(&ontime_schema, edge_owned, SqlDialect::ClickHouse).await);
    assert!(
        edge_owned_sql.contains("GROUP BY r.airport"),
        "#582: a genuinely edge-owned property (not part of the anchor's \
         own CTE) must still GROUP BY through the edge alias, unchanged from \
         pre-#582 behavior:\n{edge_owned_sql}"
    );
    assert!(
        !edge_owned_sql.contains("GROUP BY a.airport"),
        "#582: must not GROUP BY through the anchor CTE alias for a property \
         the CTE never exposes — invalid SQL:\n{edge_owned_sql}"
    );
}

/// #475 regression: on the coupled cross-table denorm `zeek_merged_test`
/// schema, `MATCH (a:IP) OPTIONAL MATCH (a)-[:REQUESTED]->(d:Domain) RETURN
/// a.ip, a.port, d.name` sourced the ANCHOR property `a.port` from the
/// LEFT-JOINed dns_log edge alias (`t1."id.orig_p"`) instead of the anchor's
/// own `__denorm_scan_a` CTE column — NULL on exactly the OPTIONAL-miss rows
/// (live: `93.184.216.34` showed port NULL despite the anchor scan showing
/// 80). The IP label's port property comes from its conn_log node table, so
/// the EDGE's declared from-node property set (dns_log IP: only `ip`) did not
/// cover it and the post-extraction rewrite left it parked on the edge alias.
///
/// The fix extends the delegation-path rewrite map with the anchor Union
/// branch's own property mapping (db column → CTE-exposed property name — the
/// SELECT-list sibling of #470's JOIN-key forward resolution). Locks: every
/// anchor property (`a.ip`, `a.port`) resolves through the CTE alias `a`;
/// the edge-owned `d.name` stays on the edge alias (`query` column); ORDER BY
/// on the anchor property resolves the same way; determinism.
///
/// Live-verified on the zeek fixture (5 conn + 5 dns rows): OPTIONAL-miss
/// rows keep their anchor port (e.g. `93.184.216.34 | 80 | NULL`), matching
/// hand-written LEFT-JOIN ground truth; matched rows unchanged.
#[tokio::test]
async fn denorm_optional_anchor_property_from_scan_cte_475() {
    let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
    let repro = "MATCH (a:IP) OPTIONAL MATCH (a)-[:REQUESTED]->(d:Domain) \
                 RETURN a.ip, a.port, d.name";

    let first = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
    for _ in 0..10 {
        let again = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#475: OPTIONAL denorm anchor-property render is nondeterministic:\n\
             FIRST:\n{first}\nAGAIN:\n{again}"
        );
    }

    // Both anchor properties must be sourced from the anchor CTE alias…
    assert!(
        first.contains(r#"a.ip AS "a.ip""#),
        "#475: a.ip must resolve through the __denorm_scan CTE:\n{first}"
    );
    assert!(
        first.contains(r#"a.port AS "a.port""#),
        "#475: a.port must resolve through the __denorm_scan CTE, not the \
         LEFT-JOINed edge table:\n{first}"
    );
    // …and never from the NULL-extended edge alias.
    assert!(
        !first.contains(r#""id.orig_p" AS "a.port""#),
        "#475: a.port must NOT be sourced from the edge table's id.orig_p \
         (NULL on OPTIONAL-miss rows):\n{first}"
    );
    // The edge-owned property stays on the edge alias (NULL-extension is the
    // CORRECT semantics for d.name).
    assert!(
        first.contains(r#".query AS "d.name""#),
        "#475: d.name must stay sourced from the edge row's query column:\n{first}"
    );

    // ORDER BY on the anchor property must forward-resolve identically.
    let ordered = normalize(
        &render(
            &schema,
            "MATCH (a:IP) OPTIONAL MATCH (a)-[:REQUESTED]->(d:Domain) \
             RETURN a.ip, a.port, d.name ORDER BY a.port",
            SqlDialect::ClickHouse,
        )
        .await,
    );
    assert!(
        ordered.contains("ORDER BY a.port"),
        "#475: ORDER BY on the anchor property must reference the CTE column:\n{ordered}"
    );
}

/// #475 review guard: the anchor-map extension of the OPTIONAL-denorm rewrite
/// is keyed by raw db-column NAME on the edge alias, but the anchor node table
/// and the edge table are DIFFERENT physical tables on a coupled cross-table
/// schema — so a name collision could hijack a legitimate EDGE-OWNED reference
/// onto the anchor CTE. `schemas/test/zeek_merged_collision.yaml` builds
/// exactly that shape: IP@conn_log carries `seen: ts` while REQUESTED@dns_log
/// carries `timestamp: ts` (two different `ts` columns). Without the guard,
/// `r.timestamp` (edge property, correctly `r.ts`) was rewritten to `a.seen` —
/// wrong value on matched rows AND non-NULL on OPTIONAL-miss rows where Cypher
/// requires NULL.
///
/// Locks: the edge property stays on the edge alias; the non-colliding anchor
/// properties (`a.ip`, `a.port`) still forward-resolve through the CTE (#475
/// fix retained); determinism.
///
/// Live-verified on the zeek fixture: 16 rows byte-identical to hand-written
/// ground truth — matched rows carry dns_log's ts (1700000001…), OPTIONAL-miss
/// rows have `r.timestamp` NULL.
///
/// Residual known limitation (documented at the guard): the SHADOWED anchor
/// property itself (`a.seen`) still resolves to the edge column (pre-#475
/// behavior for that column) — disambiguating needs extraction-time
/// provenance.
#[tokio::test]
async fn denorm_optional_edge_column_not_hijacked_by_anchor_475() {
    let schema = load_schema("schemas/test/zeek_merged_collision.yaml");
    let repro = "MATCH (a:IP) OPTIONAL MATCH (a)-[r:REQUESTED]->(d:Domain) \
                 RETURN a.ip, a.port, r.timestamp, d.name";

    let first = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
    for _ in 0..10 {
        let again = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#475 guard: collision-shape render is nondeterministic:\n\
             FIRST:\n{first}\nAGAIN:\n{again}"
        );
    }

    // The EDGE-OWNED property must stay on the LEFT-JOINed edge alias (r) so
    // it is NULL-extended on OPTIONAL-miss rows…
    assert!(
        first.contains(r#"r.ts AS "r.timestamp""#),
        "#475 guard: r.timestamp must stay sourced from the edge row's ts \
         column:\n{first}"
    );
    // …and must NEVER be hijacked onto the anchor CTE.
    assert!(
        !first.contains(r#"a.seen AS "r.timestamp""#),
        "#475 guard: edge-owned r.timestamp was hijacked onto the anchor CTE \
         (wrong value on matched rows, non-NULL on OPTIONAL-miss rows):\n{first}"
    );
    // The #475 fix itself is retained: non-colliding anchor properties still
    // forward-resolve through the __denorm_scan CTE.
    assert!(
        first.contains(r#"a.ip AS "a.ip""#) && first.contains(r#"a.port AS "a.port""#),
        "#475 guard: anchor properties must still resolve through the CTE:\n{first}"
    );
}

/// #475 review round 2: the aggregate-arg resolver
/// (`resolve_denorm_refs_in_expr`, select_builder.rs) rebound the table alias
/// UNCONDITIONALLY whenever the alias resolved to an override binding — even
/// when the referenced property did NOT resolve in that binding's property
/// set. On `zeek_merged_collision.yaml` (IP@conn_log carries `uid: uid`,
/// REQUESTED@dns_log carries the edge property `uid: uid` — same Cypher name
/// AND same physical column, different tables), `MATCH (a:IP) OPTIONAL MATCH
/// (a)-[r:REQUESTED]->(d:Domain) RETURN a.ip, count(a.uid)` turned the ANCHOR
/// reference `a.uid` into `count(r.uid)`: VALID SQL, silently wrong — the
/// edge's uid is NULL-extended on OPTIONAL-miss rows, so those IPs count 0
/// instead of 1. (The `seen: ts` fixture cannot catch this class: `seen` is
/// not a physical dns_log column, so the bad ref would be loud, not silent.)
///
/// The gate: the resolver rebinds the alias ONLY when the reference resolves
/// on the binding — by Cypher property name (mapping the column) or by
/// already-mapped column value (keeping the column). Unresolvable references
/// pass through untouched for the anchor-CTE machinery.
///
/// Live-verified on the zeek fixture: OPTIONAL-miss IPs (1.2.3.4, 10.0.0.99,
/// 142.250.80.46, 93.184.216.34, 93.184.216.35) return count(a.uid) = 1,
/// matching hand-written ground truth.
#[tokio::test]
async fn denorm_optional_anchor_ref_in_aggregate_not_rebound_475() {
    let schema = load_schema("schemas/test/zeek_merged_collision.yaml");
    let repro = "MATCH (a:IP) OPTIONAL MATCH (a)-[r:REQUESTED]->(d:Domain) \
                 RETURN a.ip, count(a.uid)";

    let first = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
    for _ in 0..10 {
        let again = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#475 r2: anchor-ref-in-aggregate render is nondeterministic:\n\
             FIRST:\n{first}\nAGAIN:\n{again}"
        );
    }

    // The anchor reference must stay sourced from the anchor scan CTE (the
    // CTE exposes `uid`), never be rebound to the LEFT-JOINed edge alias.
    assert!(
        first.contains("count(a.uid)"),
        "#475 r2: count(a.uid) must stay anchor-sourced:\n{first}"
    );
    assert!(
        !first.contains("count(r.uid)"),
        "#475 r2: anchor reference a.uid was rebound to the NULL-extended \
         edge alias (silently counts 0 on OPTIONAL-miss rows):\n{first}"
    );

    // Cross-check: a genuinely EDGE-OWNED aggregate reference keeps the edge
    // binding (NULL-extension is correct there).
    let edge_agg = normalize(
        &render(
            &schema,
            "MATCH (a:IP) OPTIONAL MATCH (a)-[r:REQUESTED]->(d:Domain) \
             RETURN a.ip, count(r.timestamp)",
            SqlDialect::ClickHouse,
        )
        .await,
    );
    assert!(
        edge_agg.contains("count(r.ts)"),
        "#475 r2: edge-owned count(r.timestamp) must stay on the edge alias:\n{edge_agg}"
    );
}

/// #481 regression: on the coupled-denormalized `zeek_merged_test` schema, a
/// 2-hop `ACCESSED` chain `(a:IP)->(b:IP)->(c:IP)` resolved the MIDDLE node's
/// property binding by iterating `PlanCtx::pattern_contexts` (a `HashMap`) and
/// taking the FIRST pattern containing `b` — so `b.ip` flapped across fresh
/// processes between the shared endpoint's column (correct) and the OTHER
/// endpoint's column of the same hop (WRONG — identical to `c.ip`, returning
/// wrong data). The fix makes `PlanCtx::get_node_strategy` prefer the node's
/// OWNING edge as recorded by `register_denormalized_aliases` — the same
/// registry the render phase uses for the alias binding — so role and alias
/// always agree, with a sorted fallback for nodes outside the registry.
///
/// Locks BOTH determinism (repeated renders byte-identical — HashMap seeds
/// differ per map instance, so the old bug flips within one process) AND
/// role-correctness REGARDLESS of which hop `b` binds to: given the emitted
/// join `hop2.from_col = hop1.to_col`, `b` must bind to one of those two
/// (equivalent) shared-endpoint columns and NEVER to `a`'s or `c`'s column.
#[tokio::test]
async fn coupled_multihop_middle_node_binds_shared_endpoint_481() {
    let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
    let repro = "MATCH (a:IP)-[:ACCESSED]->(b:IP)-[:ACCESSED]->(c:IP) RETURN a.ip, b.ip, c.ip";

    // Determinism: many in-process renders must be byte-identical.
    let first = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
    for _ in 0..30 {
        let again = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#481: coupled 2-hop middle-node render is nondeterministic:\n\
             FIRST:\n{first}\nAGAIN:\n{again}"
        );
    }

    // Role-correctness, independent of column order and alias numbering.
    let binding = |col_alias: &str| -> (String, String) {
        let re = regex::Regex::new(&format!(
            r#"(t\d+)\."(id\.(?:orig|resp)_h)" AS "{}""#,
            regex::escape(col_alias)
        ))
        .unwrap();
        let caps = re
            .captures(&first)
            .unwrap_or_else(|| panic!("#481: no binding for {col_alias}:\n{first}"));
        (caps[1].to_string(), caps[2].to_string())
    };
    let (a_alias, a_col) = binding("a.ip");
    let (b_alias, b_col) = binding("b.ip");
    let (c_alias, c_col) = binding("c.ip");

    // a is hop1's from-endpoint, c is hop2's to-endpoint, on distinct scans.
    assert_eq!(
        a_col, "id.orig_h",
        "#481: a.ip must be a from-column:\n{first}"
    );
    assert_eq!(
        c_col, "id.resp_h",
        "#481: c.ip must be a to-column:\n{first}"
    );
    assert_ne!(
        a_alias, c_alias,
        "#481: a and c must bind to different hop scans:\n{first}"
    );

    // The join must equate the two representations of the shared node b:
    // hop2.from = hop1.to (either operand order).
    let fwd = format!(r#"{c_alias}."id.orig_h" = {a_alias}."id.resp_h""#);
    let rev = format!(r#"{a_alias}."id.resp_h" = {c_alias}."id.orig_h""#);
    assert!(
        first.contains(&fwd) || first.contains(&rev),
        "#481: join must equate hop2.from with hop1.to:\n{first}"
    );

    // b must bind to ONE of the shared endpoint's two equivalent columns...
    let b_binding = (b_alias.as_str(), b_col.as_str());
    assert!(
        b_binding == (c_alias.as_str(), "id.orig_h")
            || b_binding == (a_alias.as_str(), "id.resp_h"),
        "#481: b.ip must bind to the shared endpoint (hop2 from-column or \
         hop1 to-column), got {b_alias}.\"{b_col}\":\n{first}"
    );
    // ...and NEVER to a's or c's own binding (the wrong-data variant).
    assert_ne!(
        (b_alias.as_str(), b_col.as_str()),
        (a_alias.as_str(), a_col.as_str()),
        "#481: b.ip must not alias a.ip's column:\n{first}"
    );
    assert_ne!(
        (b_alias.as_str(), b_col.as_str()),
        (c_alias.as_str(), c_col.as_str()),
        "#481: b.ip must not alias c.ip's column (wrong-data variant):\n{first}"
    );
}

/// #491 regression: `MATCH (a:Airport)-[:FLIGHT]->(b) OPTIONAL MATCH
/// (b)-[:FLIGHT]->(c)` on the denormalized flights schema bound the REQUIRED
/// node `b` to the OPTIONAL hop's LEFT-JOINed table alias (the
/// `denormalized_node_edges` registry is last-write-wins, and the optional
/// pattern registers after the required one). `b.code` then rendered as
/// `<opt_hop>.origin_code`, which is NULL on exactly the rows the optional hop
/// misses — even though `b` matched in the required MATCH (live: the DEN→PHX
/// row returned b=NULL instead of b=PHX).
///
/// The fix makes an OPTIONAL pattern's registration keep an existing binding:
/// `b.code` must resolve from the REQUIRED hop's row (`dest_code` on the scan
/// that also carries `a.code` as `origin_code`), and the optional hop joins on
/// that same required-side column.
///
/// Live-verified on `db_denormalized` (8 flights): 12 rows, with the
/// optional-miss row `DEN | PHX | NULL` (was `DEN | NULL | NULL`); required
/// 2-hop and single-hop variants byte-unchanged and matching hand-written SQL.
#[tokio::test]
async fn denorm_optional_second_hop_keeps_required_binding_491() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());
    let repro = "MATCH (a:Airport)-[:FLIGHT]->(b) OPTIONAL MATCH (b)-[:FLIGHT]->(c) \
                 RETURN a.code, b.code, c.code";

    // Determinism: repeated in-process renders must be byte-identical.
    let first = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
    for _ in 0..10 {
        let again = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#491: OPTIONAL second-hop render is nondeterministic:\n\
             FIRST:\n{first}\nAGAIN:\n{again}"
        );
    }

    // Identify the required-hop and optional-hop scans structurally: a.code
    // is the required hop's from-column; c.code is the optional hop's
    // to-column; the optional hop is LEFT JOINed.
    let binding = |col_alias: &str| -> (String, String) {
        let re = regex::Regex::new(&format!(
            r#"(t\d+)\.((?:origin|dest)_code) AS "{}""#,
            regex::escape(col_alias)
        ))
        .unwrap();
        let caps = re
            .captures(&first)
            .unwrap_or_else(|| panic!("#491: no binding for {col_alias}:\n{first}"));
        (caps[1].to_string(), caps[2].to_string())
    };
    let (a_alias, a_col) = binding("a.code");
    let (b_alias, b_col) = binding("b.code");
    let (c_alias, c_col) = binding("c.code");

    assert_eq!(
        a_col, "origin_code",
        "#491: a.code must be the required hop's from-column:\n{first}"
    );
    assert_eq!(
        c_col, "dest_code",
        "#491: c.code must be the optional hop's to-column:\n{first}"
    );
    assert_ne!(
        a_alias, c_alias,
        "#491: required and optional hops must be distinct scans:\n{first}"
    );

    // THE FIX: b must bind to the REQUIRED hop's row (its to-column), never to
    // the optional hop's from-column (NULL-extended on optional miss).
    assert_eq!(
        (b_alias.as_str(), b_col.as_str()),
        (a_alias.as_str(), "dest_code"),
        "#491: b.code must resolve from the REQUIRED pattern's binding \
         ({a_alias}.dest_code), not the OPTIONAL hop's:\n{first}"
    );

    // The optional hop must be a LEFT JOIN keyed on the required-side column.
    let join_line = first
        .lines()
        .find(|l| l.contains("LEFT JOIN"))
        .unwrap_or_else(|| panic!("#491: no LEFT JOIN line:\n{first}"));
    assert!(
        join_line.contains(&format!("{c_alias}.origin_code = {a_alias}.dest_code"))
            || join_line.contains(&format!("{a_alias}.dest_code = {c_alias}.origin_code")),
        "#491: optional hop must join its from-column to the required hop's \
         to-column:\n{first}"
    );
}

/// #492 regression: `MATCH (a)-[:FLIGHT]-(b)-[:FLIGHT]-(c)` on the
/// denormalized flights schema rendered a SINGLE directed INNER JOIN chain —
/// the undirectedness was silently dropped. Root causes fixed:
///   1. `BidirectionalUnion`'s Projection arm still carried the
///      nested-undirected-edge skip that the GraphRel arm removed long ago
///      (#147); real queries (RETURN wraps the pattern in a Projection) always
///      hit that arm, so any undirected hop whose left subtree is another
///      GraphRel kept `Direction::Either`, which downstream renders as a
///      plain directed join.
///   2. `collect_relationship_info_inner` only recursed into `left`, so the
///      Incoming-swapped branches (inner GraphRel moved to `right`) lost the
///      relationship-uniqueness guard.
///   3. The SELECT renderer bound the shared middle node's pre-resolved
///      column (schema-mapped via ONE adjacent edge's side, e.g. `b.code` →
///      t1's `Dest`) to the OTHER adjacent edge's alias, reading the wrong
///      endpoint (`t2.Dest` = c's column) in the all-forward branch
///      (`translate_denorm_cross_side_column` now re-maps the column onto the
///      bound edge's side).
///
/// Locks the semantic shape: 4 direction assignments (2 per undirected hop) as
/// UNION ALL branches, one per join-side combination, each carrying the
/// relationship-uniqueness guard, with the middle node projected from the
/// branch's SHARED endpoint column. Live-verified against ClickHouse dev data:
/// 12 rows matching a hand-written 4-branch UNION ground truth (directed = 5).
#[tokio::test]
async fn denorm_undirected_multihop_direction_union_492() {
    let schema = load_schema("schemas/examples/ontime_denormalized.yaml");
    let repro = "MATCH (a)-[:FLIGHT]-(b)-[:FLIGHT]-(c) RETURN a.code, b.code, c.code";
    let sql = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);

    // Four direction assignments -> 4 UNION ALL branches.
    assert_eq!(
        sql.matches("UNION ALL").count(),
        3,
        "#492: undirected 2-hop must expand to 4 direction branches:\n{sql}"
    );

    // Each join-side combination appears exactly once, and its branch projects
    // the middle node from the shared endpoint column of that combination.
    let branches: Vec<&str> = sql.split("UNION ALL").collect();
    for (cond, b_col) in [
        ("t1.Origin = t0.Dest", "t1.Origin"), // fwd/fwd: b = t2 from-side
        ("t1.Origin = t0.Origin", "t1.Origin"), // rev/fwd
        ("t1.Dest = t0.Dest", "t1.Dest"),     // fwd/rev: b = t2 to-side
        ("t1.Dest = t0.Origin", "t1.Dest"),   // rev/rev
    ] {
        let matching: Vec<&&str> = branches.iter().filter(|b| b.contains(cond)).collect();
        assert_eq!(
            matching.len(),
            1,
            "#492: join condition `{cond}` must appear in exactly one branch:\n{sql}"
        );
        assert!(
            matching[0].contains(&format!("{b_col} AS \"b.code\"")),
            "#492: branch `{cond}` must project b.code from its shared \
             endpoint `{b_col}` (cross-side column/alias mismatch reads the \
             WRONG endpoint):\n{sql}"
        );
        // Relationship uniqueness (Cypher: a relationship is traversed once
        // per match) must guard EVERY branch, including the Incoming-swapped
        // ones whose inner GraphRel lives in the right subtree.
        assert!(
            matching[0].contains("NOT (t1.flight_id = t0.flight_id"),
            "#492: branch `{cond}` is missing the relationship-uniqueness \
             guard:\n{sql}"
        );
    }

    // Mixed direction: only the trailing undirected hop fans out (2 branches).
    let mixed = "MATCH (a)-[:FLIGHT]->(b)-[:FLIGHT]-(c) RETURN a.code, b.code, c.code";
    let sql = normalize(&render(&schema, mixed, SqlDialect::ClickHouse).await);
    assert_eq!(
        sql.matches("UNION ALL").count(),
        1,
        "#492: mixed-direction 2-hop must expand the undirected hop into \
         forward + reverse branches:\n{sql}"
    );
    for cond in ["t1.Origin = t0.Dest", "t1.Dest = t0.Dest"] {
        assert!(
            sql.contains(cond),
            "#492: mixed-direction 2-hop is missing the `{cond}` branch:\n{sql}"
        );
    }
}

/// #492 adversarial-review round 2: five structural locks on the newly-enabled
/// undirected multi-hop family.
///
/// B1  WHERE on the shared middle node must filter the SAME physical column
///     each branch projects for it (the all-forward branch used to filter on
///     c's column, returning rows violating the user's WHERE).
/// B2  The relationship-uniqueness guard must pair only same-type/same-table
///     relationships (AUTHORED vs LIKED share [user_id, post_id] column names;
///     a cross-type guard silently excluded author-liked-own-post matches).
/// B3  OPTIONAL + nested-undirected multi-hop is GATED to the pre-#492 single
///     directed LEFT chain: per-orientation LEFT-JOIN UNION ALL branches
///     cannot express OPTIONAL semantics (NULL-anchor rows dropped by the
///     guard / duplicated per branch when NULL-safe / partial-pattern rows /
///     swapped branches anchoring FROM on the optional node). Follow-up needs
///     an anchor-LEFT-JOIN-onto-match-union renderer structure.
/// RN4 Bridge-node elimination must not clobber union branches that DEFINE
///     the alias (tautological joins `ON x.col = x.col` inflated 64 → 147).
/// RN5 FK-edge uniqueness guards must compare materialized NODE aliases (the
///     rel row IS the node row; rel aliases never materialize).
#[tokio::test]
async fn undirected_multihop_review_fixes_492() {
    // B1: per-branch WHERE column == per-branch b.code projection column.
    let denorm = load_schema("schemas/examples/ontime_denormalized.yaml");
    let sql = normalize(
        &render(
            &denorm,
            "MATCH (a:Airport)-[:FLIGHT]-(b:Airport)-[:FLIGHT]-(c:Airport) \
             WHERE b.code = 'JFK' RETURN a.code, b.code, c.code",
            SqlDialect::ClickHouse,
        )
        .await,
    );
    for branch in sql.split("UNION ALL") {
        let b_col = if branch.contains("t1.Origin AS \"b.code\"") {
            "t1.Origin"
        } else if branch.contains("t1.Dest AS \"b.code\"") {
            "t1.Dest"
        } else {
            panic!("#492-B1: branch projects no b.code:\n{branch}");
        };
        assert!(
            branch.contains(&format!("{b_col} = 'JFK'")),
            "#492-B1: branch must filter b on its own column `{b_col}`:\n{branch}"
        );
    }

    // B2: no cross-type uniqueness guard between AUTHORED and LIKED.
    let std_schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = normalize(
        &render(
            &std_schema,
            "MATCH (u:User)-[:AUTHORED]-(p:Post)<-[:LIKED]-(v:User) RETURN u.name, p.title, v.name",
            SqlDialect::ClickHouse,
        )
        .await,
    );
    assert!(
        !sql.contains("NOT ("),
        "#492-B2: cross-type (AUTHORED/LIKED) patterns must not emit a \
         uniqueness guard — different types are never the same relationship:\n{sql}"
    );

    // B3 gate: OPTIONAL nested-undirected keeps the pre-#492 single chain.
    let sql = normalize(
        &render(
            &std_schema,
            "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]-(b:User)-[:FOLLOWS]-(c:User) \
             RETURN a.name, b.name, c.name",
            SqlDialect::ClickHouse,
        )
        .await,
    );
    assert!(
        !sql.contains("UNION ALL"),
        "#492-B3: OPTIONAL nested-undirected must stay gated (single directed \
         LEFT chain) until the renderer can LEFT JOIN an anchor onto a match \
         union:\n{sql}"
    );
    assert!(
        sql.contains("LEFT JOIN"),
        "#492-B3: gated OPTIONAL pattern must still LEFT JOIN:\n{sql}"
    );

    // RN4: no tautological join conditions in any branch (middle node
    // unreferenced; parent bridge elimination must not leak cross-branch).
    let sql = normalize(
        &render(
            &std_schema,
            "MATCH (a:User)-[:FOLLOWS]-(b)-[:FOLLOWS]-(c:User) RETURN a.name, c.name",
            SqlDialect::ClickHouse,
        )
        .await,
    );
    let tautology = regex::Regex::new(r"ON (\w+)\.(\w+) = (\w+)\.(\w+)").unwrap();
    for cap in tautology.captures_iter(&sql) {
        assert!(
            !(cap[1] == cap[3] && cap[2] == cap[4]),
            "#492-RN4: tautological join condition `{}` (bridge elimination \
             clobbered a branch-defined alias):\n{sql}",
            &cap[0]
        );
    }
    assert_eq!(
        sql.matches("UNION ALL").count(),
        3,
        "#492-RN4: partially-referenced undirected 2-hop keeps 4 branches:\n{sql}"
    );

    // RN5: FK-edge guard compares materialized node aliases, never the
    // (unmaterialized) rel aliases.
    let fk = load_schema(SchemaId::FkEdge.yaml_path());
    let sql = normalize(
        &render(
            &fk,
            "MATCH (a:Order)-[:PLACED_BY]-(c:Customer)-[:PLACED_BY]-(b:Order) \
             RETURN a.order_id, c.customer_id, b.order_id",
            SqlDialect::ClickHouse,
        )
        .await,
    );
    assert!(
        sql.contains("NOT b.order_id = a.order_id") || sql.contains("NOT a.order_id = b.order_id"),
        "#492-RN5: FK-edge uniqueness guard must compare the anchor node \
         aliases:\n{sql}"
    );
    assert!(
        !sql.contains("t0.") && !sql.contains("t1."),
        "#492-RN5: FK-edge SQL must not reference unmaterialized rel aliases:\n{sql}"
    );

    // #492 review ROUND 3, finding 1 (MUST-FIX): interaction with #491.
    // `get_properties_with_table_alias` picks a node's property source
    // PURELY STRUCTURALLY (first GraphRel connection match in the tree),
    // while `table_alias_override` comes from the `denormalized_node_edges`
    // registry, which #491 made keep an EARLIER binding for OPTIONAL
    // patterns. For `(a)-[t1]->(b) OPTIONAL (b)-[t2]->(c)`, `b` renders
    // against `t1` (registry, #491-correct) but the structural walk still
    // matches `t2` (the optional GraphRel) first — combining `t2`'s
    // properties with `t1`'s alias silently produced `t1.origin_code` (`a`'s
    // OWN column) instead of `t1.dest_code`. This is #491's OWN exact test
    // query, fully DIRECTED (no undirected edges) — proof the interaction is
    // not scoped to undirected patterns.
    let denorm2 = load_schema(SchemaId::Denormalized.yaml_path());
    let sql = normalize(
        &render(
            &denorm2,
            "MATCH (a:Airport)-[:FLIGHT]->(b) OPTIONAL MATCH (b)-[:FLIGHT]->(c) \
             RETURN a.code, b.code, c.code",
            SqlDialect::ClickHouse,
        )
        .await,
    );
    let binding = |col_alias: &str| -> (String, String) {
        let re = regex::Regex::new(&format!(
            r#"(t\d+)\.((?:origin|dest)_code) AS "{}""#,
            regex::escape(col_alias)
        ))
        .unwrap();
        let caps = re
            .captures(&sql)
            .unwrap_or_else(|| panic!("no binding for {col_alias}:\n{sql}"));
        (caps[1].to_string(), caps[2].to_string())
    };
    let (a_alias, _) = binding("a.code");
    let (b_alias, b_col) = binding("b.code");
    assert_eq!(
        (b_alias.as_str(), b_col.as_str()),
        (a_alias.as_str(), "dest_code"),
        "#492/#491 interaction: b.code must resolve from the REQUIRED \
         pattern's binding ({a_alias}.dest_code) — properties-source and \
         alias-source must come from the SAME edge:\n{sql}"
    );

    // #492 review ROUND 3, finding 2 (SHOULD-FIX): B3 gate must not suppress
    // an UNRELATED required chain's split just because the plan nests an
    // unrelated OPTIONAL undirected edge reachable via shared aliasing.
    // `MATCH (a)-[:R1]-(b)-[:R1]-(c) OPTIONAL MATCH (a)-[:R2]-(p)` — the
    // OPTIONAL R2 edge's `left` subtree IS the required R1 chain (shared
    // anchor 'a'), but neither R1 hop is itself optional. The required
    // portion must still fully split (4 branches), matching the value the
    // original #492 fix delivered before the B3 gate existed.
    let sql = normalize(
        &render(
            &std_schema,
            "MATCH (a:User)-[:FOLLOWS]-(b)-[:FOLLOWS]-(c:User) \
             OPTIONAL MATCH (a)-[:AUTHORED]-(p:Post) \
             RETURN a.name, c.name, p.title",
            SqlDialect::ClickHouse,
        )
        .await,
    );
    assert_eq!(
        sql.matches("UNION ALL").count(),
        3,
        "#492-B3-scope: required multi-hop must split fully (4 branches) \
         despite an unrelated undirected OPTIONAL clause sharing its anchor:\n{sql}"
    );
    assert!(
        sql.contains("LEFT JOIN"),
        "#492-B3-scope: the unrelated OPTIONAL clause must still LEFT JOIN:\n{sql}"
    );
}

/// #480 regression: two rendering sites emitted whole-entity/denorm-VLP
/// columns in raw `HashMap` iteration order, flapping across processes (15
/// corpus entries were excluded for this class in
/// `tests/corpus/nondeterministic.txt`; all now un-excluded):
///   1. `expand_cte_entity` (select_builder.rs) — a bare node/rel variable
///      resolved through a WITH CTE sourced `schema.get_node_properties`/
///      `get_relationship_properties` (unsorted `property_mappings`
///      iteration) plus an unsorted denorm `from_/to_properties` merge.
///   2. The denormalized VLP CTE builder (cte_extraction.rs) iterated
///      `from_/to_properties` straight into the CTE's fixed column order.
/// Fixed by sorting on the cypher property key at the source getters and both
/// sites (the #458/#464 recipe). Each shape below renders many times
/// IN-PROCESS (HashMap seeds are per-map instance, so an unsorted site flips
/// within one process) and must be byte-identical; the first shape's SELECT
/// column aliases must also come out in sorted property order.
#[tokio::test]
async fn whole_entity_and_denorm_vlp_column_order_deterministic_480() {
    // Shape 1: WITH-barrier whole-entity expansion (expand_cte_entity).
    let std_schema = load_schema(SchemaId::Standard.yaml_path());
    let with_entity = "MATCH (u:User) WITH u RETURN u";
    let first = normalize(&render(&std_schema, with_entity, SqlDialect::ClickHouse).await);
    for _ in 0..30 {
        let again = normalize(&render(&std_schema, with_entity, SqlDialect::ClickHouse).await);
        assert_eq!(
            first, again,
            "#480: WITH whole-entity render is nondeterministic:\n\
             FIRST:\n{first}\nAGAIN:\n{again}"
        );
    }
    // The expanded `u.<prop>` aliases must be in sorted property order.
    let alias_re = regex::Regex::new(r#" AS "u\.([A-Za-z0-9_]+)""#).unwrap();
    let props: Vec<String> = alias_re
        .captures_iter(&first)
        .map(|c| c[1].to_string())
        .collect();
    assert!(
        props.len() > 1,
        "#480: expected a multi-property expansion of u:\n{first}"
    );
    let mut sorted = props.clone();
    sorted.sort();
    assert_eq!(
        props, sorted,
        "#480: expand_cte_entity must emit properties in sorted order:\n{first}"
    );

    // Shape 2: denormalized VLP CTE (both endpoints embedded in the edge table).
    let denorm_schema = load_schema(SchemaId::Denormalized.yaml_path());
    let vlp = "MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport) RETURN a, b";
    let base = normalize(&render(&denorm_schema, vlp, SqlDialect::ClickHouse).await);
    for _ in 0..20 {
        let again = normalize(&render(&denorm_schema, vlp, SqlDialect::ClickHouse).await);
        assert_eq!(
            base, again,
            "#480: denormalized VLP CTE render is nondeterministic:\n\
             BASE:\n{base}\nAGAIN:\n{again}"
        );
    }
}

/// #482 regression: sequential/comma MATCH sharing a node variable over
/// coupled cross-table denormalized edges (`zeek_merged_test.yaml`: IP is a
/// VIRTUAL node — its id column lives in each edge table, `id.orig_h` in
/// dns_log AND conn_log) used to DROP the shared-node correlation and render
/// `JOIN ... ON 1 = 1` — a silent cartesian product. The join strategy now
/// emits an edge-to-edge link per shared connection node, equating the two
/// tables' respective embedded id columns.
///
/// Live-verified (zeek fixture, 5 conn + 5 dns rows): sequential/comma
/// shapes return 9 rows (3 domains x 3 dests for 192.168.1.10) where the
/// cartesian returned 15; hand-written ground-truth SQL agrees byte-for-byte.
#[tokio::test]
async fn denorm_shared_node_correlation_not_cartesian_482() {
    let schema = load_schema("schemas/dev/zeek_merged_test.yaml");

    // (cypher, expected shared-node correlation between the two edge tables)
    // Table aliases are process-global counters, so assert on the column
    // pairing only (normalize() remaps t{n} by first appearance).
    let shapes = [
        // Sequential MATCH, shared node is FROM of both edges.
        "MATCH (srcip:IP)-[:REQUESTED]->(d:Domain) MATCH (srcip)-[:ACCESSED]->(dest:IP) \
         WHERE srcip.ip = '192.168.1.10' RETURN DISTINCT srcip.ip, d.name, dest.ip",
        // Comma pattern, same shape.
        "MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), (srcip)-[:ACCESSED]->(dest:IP) \
         WHERE srcip.ip = '192.168.1.10' RETURN DISTINCT srcip.ip, d.name, dest.ip",
        // Full DNS path (coupled REQUESTED+RESOLVED_TO) + cross-table ACCESSED.
        "MATCH (srcip:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP), \
         (srcip)-[:ACCESSED]->(dest:IP) RETURN srcip.ip, d.name, rip.ip, dest.ip",
    ];
    for cypher in shapes {
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
        assert!(
            !sql.contains("ON 1 = 1"),
            "#482 [{cypher}]: shared-node correlation dropped — cartesian ON 1 = 1:\n{sql}"
        );
        assert!(
            sql.contains(r#""id.orig_h" = "#) && sql.contains(r#"."id.orig_h""#),
            "#482 [{cypher}]: JOIN must equate the shared node's embedded id columns:\n{sql}"
        );
        // The virtual node aliases must never leak into SQL as table
        // qualifiers (they are not bound in FROM/JOIN — only edge-table
        // aliases are). A leak renders as `srcip."id.orig_h"` (alias followed
        // by a quoted column); the legit output alias `AS "srcip.ip"` keeps
        // the dot INSIDE the quotes.
        for unbound in ["srcip.\"", "dest.\"", "rip.\"", "rip.answers"] {
            assert!(
                !sql.contains(unbound),
                "#482 [{cypher}]: unbound cypher alias '{unbound}' leaked into SQL:\n{sql}"
            );
        }
    }

    // Shared node as the TO side of the second pattern (right-connection
    // sharing was a second gap: the prev-edge lookup was left-only).
    let to_shared = "MATCH (a:IP)-[:ACCESSED]->(b:IP) MATCH (c:IP)-[:ACCESSED]->(b) \
                     RETURN a.ip, b.ip, c.ip";
    let sql = normalize(&render(&schema, to_shared, SqlDialect::ClickHouse).await);
    assert!(
        !sql.contains("ON 1 = 1"),
        "#482 [{to_shared}]: cartesian ON 1 = 1:\n{sql}"
    );
    assert!(
        sql.contains(r#""id.resp_h" = "#),
        "#482 [{to_shared}]: shared TO-node must correlate on id.resp_h:\n{sql}"
    );

    // BOTH connections shared: needs BOTH conditions (a single one would be
    // a silent under-constraint).
    let both_shared = "MATCH (a:IP)-[:ACCESSED]->(b:IP) MATCH (a)-[:ACCESSED]->(b) \
                       RETURN a.ip, b.ip";
    let sql = normalize(&render(&schema, both_shared, SqlDialect::ClickHouse).await);
    assert!(
        sql.contains(r#""id.orig_h""#) && sql.contains(r#""id.resp_h""#) && sql.contains(" AND "),
        "#482 [{both_shared}]: both shared nodes must be correlated:\n{sql}"
    );

    // Cycle: hop 2 shares BOTH endpoints with hop 1 in crossed roles
    // (b = hop1.to = hop2.from, a = hop1.from = hop2.to). One condition
    // alone is a silent under-constraint (#482 review F4; live-verified:
    // 4 rows vs the single-condition superset).
    let cycle = "MATCH (a:IP)-[:ACCESSED]->(b:IP)-[:ACCESSED]->(a) RETURN a.ip, b.ip";
    let sql = normalize(&render(&schema, cycle, SqlDialect::ClickHouse).await);
    assert!(
        !sql.contains("ON 1 = 1"),
        "#482 [{cycle}]: cartesian ON 1 = 1:\n{sql}"
    );
    assert!(
        sql.contains(r#""id.orig_h""#) && sql.contains(r#""id.resp_h""#) && sql.contains(" AND "),
        "#482 [{cycle}]: cycle needs BOTH crossed-role join conditions:\n{sql}"
    );

    // Triangle: the third edge shares its FROM with edge 1's FROM and its
    // TO with edge 2's TO — both links must be emitted (#482 review F4;
    // live-verified on an injected chain: 1 row vs main's 22).
    let triangle = "MATCH (a:IP)-[:ACCESSED]->(b:IP), (b)-[:ACCESSED]->(c:IP), \
                    (a)-[:ACCESSED]->(c) RETURN a.ip, b.ip, c.ip";
    let sql = normalize(&render(&schema, triangle, SqlDialect::ClickHouse).await);
    assert!(
        !sql.contains("ON 1 = 1"),
        "#482 [{triangle}]: cartesian ON 1 = 1:\n{sql}"
    );
    assert_eq!(
        sql.matches(r#""id.orig_h" = "#).count() + sql.matches(r#""id.resp_h" = "#).count(),
        3,
        "#482 [{triangle}]: expected 3 embedded-id join equalities \
         (edge 2's b-link; edge 3's a-link AND c-link):\n{sql}"
    );
}

/// #482 regression (failure 2): cross-pattern WHERE correlation between two
/// virtual (denormalized) node aliases — `WHERE srcip1.ip = srcip2.ip` over
/// disconnected patterns — used to emit `WHERE srcip1."id.orig_h" =
/// srcip2."id.orig_h"` while FROM/JOIN alias the tables `t0`/`t1`: the cypher
/// aliases were never bound in SQL (ClickHouse UNKNOWN_IDENTIFIER).
/// `CartesianJoinExtraction` now remaps denormalized node aliases to the edge
/// aliases that embed them. The INNER JOIN stays `ON 1 = 1` with the equality
/// in WHERE — semantically an inner equi-join (live-verified: 11 rows,
/// matching hand-written ground truth).
#[tokio::test]
async fn denorm_predicate_correlation_aliases_bound_482() {
    let schema = load_schema("schemas/dev/zeek_merged_test.yaml");

    let cypher =
        "MATCH (srcip1:IP)-[:REQUESTED]->(d:Domain), (srcip2:IP)-[:ACCESSED]->(destip:IP) \
                  WHERE srcip1.ip = srcip2.ip \
                  RETURN DISTINCT srcip1.ip as source, d.name as domain, destip.ip as accessed";
    let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
    for unbound in ["srcip1.\"", "srcip2.\"", "destip.\""] {
        assert!(
            !sql.contains(unbound),
            "#482 [{cypher}]: unbound cypher alias '{unbound}' leaked into SQL:\n{sql}"
        );
    }
    assert!(
        sql.contains(r#""id.orig_h" = "#),
        "#482 [{cypher}]: correlation predicate must survive, bound to edge aliases:\n{sql}"
    );

    // WITH...MATCH variant: `WHERE src2.ip = source_ip` correlates a fresh
    // denormalized node against a CTE-exported scalar. The fresh alias must
    // be remapped to its edge table alias; the CTE alias must NOT be touched.
    let with_match = "MATCH (src:IP)-[dns:REQUESTED]->(d:Domain) \
                      WITH src.ip as source_ip, d.name as domain \
                      MATCH (src2:IP)-[conn:ACCESSED]->(dest:IP) WHERE src2.ip = source_ip \
                      RETURN DISTINCT source_ip, domain, dest.ip as dest_ip";
    let sql = normalize(&render(&schema, with_match, SqlDialect::ClickHouse).await);
    assert!(
        !sql.contains("src2.\""),
        "#482 [{with_match}]: unbound cypher alias 'src2' leaked into SQL:\n{sql}"
    );
    assert!(
        sql.contains(r#"conn."id.orig_h" = "#),
        "#482 [{with_match}]: correlation must bind to the conn_log edge alias:\n{sql}"
    );
}

/// #482 control: the same sequential-MATCH shared-node shape on the STANDARD
/// schema (real node tables) must keep its historic join plan — the shared
/// node's table appears ONCE and both edge tables join to it; no cartesian,
/// no edge-to-edge rewrite.
#[tokio::test]
async fn standard_shared_node_sequential_match_unchanged_482() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) MATCH (a)-[:AUTHORED]->(p:Post) \
                  RETURN a.name, b.name, p.id";
    let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
    assert!(
        !sql.contains("ON 1 = 1"),
        "#482 standard control: cartesian ON 1 = 1:\n{sql}"
    );
    // Anchor on users table `a`; BOTH edge tables key on a.user_id
    // (rel aliases are process-global counters, so match the a-side only).
    assert_eq!(
        sql.matches(" = a.user_id").count(),
        2,
        "#482 standard control: both edges must join on the shared node's id:\n{sql}"
    );
}

/// #466 regression: a FULLY-unlabeled UNDIRECTED expand
/// `MATCH (n)-[r]-(o) RETURN n, r, o` must render DIFFERENT SQL from the
/// DIRECTED form `MATCH (n)-[r]->(o) RETURN n, r, o` on both Standard
/// (multi-edge-type) and FK-edge (single-edge-type) — the `pattern_union`
/// CTE renderer now emits a REVERSE-direction branch (same edge/join, start
/// and end swapped) for each combination when the pattern is undirected.
/// Confirmed live (this slice's `social`/`db_fk_edge` fixtures): the
/// undirected form now returns 46 rows on Standard (23 forward edges — 10
/// FOLLOWS + 5 AUTHORED + 8 LIKED — each also traversable backward, 0
/// self-loops) and 16 on FK-edge (8 forward × 2), while the directed control
/// is unchanged at 23 / 8. Self-loops (from-id == to-id) are excluded from
/// the reverse branch so they appear ONCE, matching Neo4j. Previously both
/// variants were byte-identical (the "GROUP 3b" gap in the
/// `browser-unlabeled-pattern-bugs` catalog, surfaced 2026-06-30); fixed here.
///
/// FK-edge (single-edge-type) now also routes through `pattern_union`: a
/// fully-unlabeled UNDIRECTED expand over a single NON-self-referential edge
/// is stored as `pattern_combinations` (see
/// `logical_plan::match_clause::traversal`) so the same reverse-branch
/// renderer applies. The DIRECTED FK form stays on the plain node-to-node
/// join path (control unchanged).
///
/// Contrast (asserted here too): anchoring ONE side
/// (`anchored_unlabeled_expand`) or fixing the relationship type
/// (`unlabeled_rel_typed`) also produce DIFFERENT SQL for the undirected vs.
/// directed forms (they route through `vlp_multi_type_a_o` /
/// `bidirectional_union`).
#[tokio::test]
async fn browser_unlabeled_undirected_expand_emits_reverse_branches() {
    for (schema_id, undirected, directed) in [
        (
            SchemaId::Standard,
            "MATCH (n)-[r]-(o) RETURN n, r, o LIMIT 25",
            "MATCH (n)-[r]->(o) RETURN n, r, o LIMIT 25",
        ),
        (
            SchemaId::FkEdge,
            "MATCH (n)-[r]-(o) RETURN n, r, o LIMIT 25",
            "MATCH (n)-[r]->(o) RETURN n, r, o LIMIT 25",
        ),
    ] {
        let schema = load_schema(schema_id.yaml_path());
        for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
            let u_sql = render(&schema, undirected, dialect).await;
            let d_sql = render(&schema, directed, dialect).await;
            assert_ne!(
                u_sql,
                d_sql,
                "{:?}/{dialect:?}: undirected expand must render DIFFERENT SQL \
                 from the directed form (reverse-direction pattern_union \
                 branches, #466); the shapes are byte-identical — the \
                 reverse-branch fix regressed:\nundirected:\n{u_sql}\ndirected:\n{d_sql}",
                schema_id.dir()
            );
            // The undirected form carries the reverse orientation: its FIRST
            // branch's start_type reappears as some branch's end_type (and vice
            // versa), which never happens in the forward-only directed SQL for
            // these fixtures' heterogeneous edges.
            assert!(
                u_sql.matches(" AS start_type").count() > d_sql.matches(" AS start_type").count(),
                "{:?}/{dialect:?}: undirected expand must have MORE branches than \
                 directed (forward + reverse):\nundirected:\n{u_sql}",
                schema_id.dir()
            );
        }
    }

    // Contrast: anchoring one side DOES correctly differ between undirected
    // and single-direction forms (no reverse-branch gap here).
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let anchored_undirected = render(
        &schema,
        "MATCH (a:User)-[r]-(o) RETURN a, r, o",
        SqlDialect::ClickHouse,
    )
    .await;
    let anchored_directed = render(
        &schema,
        "MATCH (a:User)-[r]->(o) RETURN a, r, o",
        SqlDialect::ClickHouse,
    )
    .await;
    assert_ne!(
        anchored_undirected, anchored_directed,
        "anchored (labeled-endpoint) unlabeled expand must NOT collapse \
         undirected to the directed-only SQL (the bug is specific to the \
         fully-unlabeled multi-type pattern_union path):\n{anchored_undirected}"
    );
}

/// #466 follow-up regression (adversarial-review finding): a node-property
/// WHERE on a fully-unlabeled pattern that renders through a `pattern_union`
/// CTE must be resolved PER-BRANCH inside the CTE — the CTE projection does
/// not expose node property columns, and which physical table/label an alias
/// binds to differs per combination and (for undirected) per traversal
/// orientation. The old outer-WHERE fallback silently degraded ANY node
/// property to a start_id/end_id comparison (`o.name = 'Alice'` became
/// `r.end_id = 'Alice'` — comparing a customer ID to a name, always false).
///
/// Live-verified (db_fk_edge fixture, Alice = customer 100 with 3 orders):
///   - undirected `WHERE o.name='Alice'` → 3 (was 0 after the first #466
///     commit; 3 on main via the plain-join path)
///   - undirected `WHERE n.name='Alice'` → 3 (reverse orientation binds
///     n=Customer)
///   - undirected `WHERE o.amount > 100` → 4 (renamed property `amount` →
///     `total_amount`, resolved on the Order-bound orientation only)
///   - undirected `WHERE o.name IS NULL` → 8 / `IS NOT NULL` → 8 (a property
///     missing on a branch's bound label is NULL per Cypher, so IS NULL is
///     TRUE for the Order-bound orientation)
/// Standard (multi-type) directed control `WHERE o.name IS NOT NULL` → 10
/// (10 FOLLOWS; was 23 = filter silently dropped — pre-existing looseness
/// fixed by the same per-branch mechanism); undirected → 33 (5 AUTHORED-rev
/// + 10 FOLLOWS-fwd + 10 FOLLOWS-rev + 8 LIKED-rev).
#[tokio::test]
async fn pattern_union_where_resolves_node_properties_per_branch() {
    // FK-edge: single edge type, undirected → pattern_union with 2 branches.
    let fk = load_schema(SchemaId::FkEdge.yaml_path());
    let sql = render(
        &fk,
        "MATCH (n)-[r]-(o) WHERE o.name = 'Alice' RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("db_fk_edge.customers_fk.name = 'Alice'"),
        "forward branch (o=Customer) must filter on the customers table's \
         physical name column:\n{sql}"
    );
    assert!(
        sql.contains("NULL = 'Alice'"),
        "reverse branch (o=Order, no `name` property) must resolve the \
         reference to NULL (Cypher: missing property = NULL → comparison \
         false, branch contributes nothing):\n{sql}"
    );
    assert!(
        !sql.contains("end_id = 'Alice'") && !sql.contains("start_id = 'Alice'"),
        "the node-property predicate must NOT degrade to an id-column \
         comparison in the outer WHERE (the original silent-wrong \
         behavior):\n{sql}"
    );

    // Renamed property on the other side: `o.amount` → physical total_amount,
    // resolved only on the orientation that binds o to Order.
    let sql = render(
        &fk,
        "MATCH (n)-[r]-(o) WHERE o.amount > 100 RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("db_fk_edge.orders_fk.total_amount > 100"),
        "reverse branch (o=Order) must resolve renamed property amount → \
         total_amount:\n{sql}"
    );
    assert!(
        sql.contains("NULL > 100"),
        "forward branch (o=Customer, no `amount`) must resolve to NULL:\n{sql}"
    );

    // Directed FK control stays on the plain node-to-node join path with a
    // normal WHERE on the aliased customers table (main behavior, unchanged).
    let sql = render(
        &fk,
        "MATCH (n)-[r]->(o) WHERE o.name = 'Alice' RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("o.name = 'Alice'") && !sql.contains("pattern_union"),
        "directed FK form must stay on the plain join path with the filter \
         resolved against the o alias:\n{sql}"
    );

    // Standard multi-type: the same per-branch mechanism applies the renamed
    // property (`name` → full_name) on User-bound branches and NULL elsewhere.
    let std_schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &std_schema,
        "MATCH (n)-[r]->(o) WHERE o.name IS NOT NULL RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("full_name IS NOT NULL"),
        "User-bound branches must filter on the physical full_name column:\n{sql}"
    );
    assert!(
        sql.contains("NULL IS NOT NULL"),
        "Post-bound branches must resolve o.name to NULL (filters the branch \
         out for IS NOT NULL):\n{sql}"
    );

    // #466 round 3, blocking finding 1: a renamed property on the ANCHOR
    // (left) alias arrives PRE-RESOLVED to its physical column name
    // (`n.amount` arrives as `n.total_amount`, `n.name` as `n.full_name`)
    // because upstream passes resolve the anchor against its registered
    // table ctx. The per-branch resolver must accept the physical name too —
    // without the fallback the conjunct silently became NULL in EVERY branch
    // (FK `WHERE n.amount > 100` returned 0 instead of 4; live-verified 4
    // after the fix). The conjunct must also NOT leak into the outer WHERE
    // (`n.total_amount` is not a CTE column — double emission was invalid
    // SQL).
    let sql = render(
        &fk,
        "MATCH (n)-[r]-(o) WHERE n.amount > 100 RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("db_fk_edge.orders_fk.total_amount > 100"),
        "forward branch (n=Order) must resolve the pre-resolved physical \
         column total_amount:\n{sql}"
    );
    assert!(
        sql.contains("NULL > 100"),
        "reverse branch (n=Customer, no `amount`) must resolve to NULL:\n{sql}"
    );
    assert!(
        !sql.contains("AS r\nWHERE") && !sql.contains("r\nWHERE n.total_amount"),
        "the anchor-alias conjunct must not ALSO appear in the outer WHERE \
         (double emission references a non-CTE column):\n{sql}"
    );
    // Standard variant of the same finding: `n.name` arrives as `full_name`.
    let sql = render(
        &std_schema,
        "MATCH (n)-[r]-(o) WHERE n.name IS NOT NULL RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("full_name IS NOT NULL"),
        "User-bound branches must resolve the pre-resolved full_name:\n{sql}"
    );

    // #466 round 3, blocking finding 2: the outer-WHERE skip must be coupled
    // to the pattern_union CTE actually being referenced by FROM/JOIN. In
    // this multi-MATCH cartesian shape the GraphRel carries
    // pattern_combinations but the plan renders plain joins (the built CTE
    // is dead-eliminated) — the o-conjunct must STAY in the outer WHERE
    // (previously skipped-but-applied-nowhere: returned 8 instead of 3;
    // live-verified 3 after the fix).
    let sql = render(
        &fk,
        "MATCH (c:Customer) MATCH (n)-[r]-(o) WHERE c.name = 'Alice' AND \
         o.name = 'Alice' RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        !sql.contains("pattern_union"),
        "this multi-MATCH shape renders plain joins (pre-existing cartesian \
         path), not the pattern_union CTE:\n{sql}"
    );
    assert!(
        sql.contains("o.name = 'Alice'"),
        "the o-conjunct must stay in the outer WHERE when no pattern_union \
         CTE absorbs it (never skip-without-apply):\n{sql}"
    );
}

/// #466 round 3 (ride-along): whole-entity conjuncts (`WHERE o = n`) and
/// subquery conjuncts (`EXISTS { ... }`) on a pattern that renders through
/// `pattern_union` cannot be resolved per branch and previously fell through
/// every classifier — SILENTLY unfiltered (returned all 16 rows). They must
/// be a clean UnsupportedFeature error instead.
#[tokio::test]
async fn pattern_union_unresolvable_where_conjuncts_error_cleanly() {
    let fk = load_schema(SchemaId::FkEdge.yaml_path());
    for cypher in [
        "MATCH (n)-[r]-(o) WHERE o = n RETURN count(*) AS c",
        "MATCH (n)-[r]-(o) WHERE EXISTS { MATCH (o)-[:PLACED_BY]->() } RETURN count(*) AS c",
    ] {
        let result = try_render(&fk, cypher, SqlDialect::ClickHouse).await;
        match result {
            Err(msg) => assert!(
                msg.contains("UnsupportedFeature") || msg.contains("Unsupported feature"),
                "[{cypher}] must fail with a clean UnsupportedFeature error, got: {msg}"
            ),
            Ok(sql) => panic!(
                "[{cypher}] must error cleanly instead of silently dropping the \
                 unresolvable conjunct; rendered:\n{sql}"
            ),
        }
    }
}

/// #574: `EXISTS { ... }` conversion (`ast_conversion.rs`'s
/// `TryFrom<ast::ExistsSubquery>`) hand-rolls a single-hop, single-type
/// `GraphRel` and previously hardcoded `variable_length: None` while only
/// ever reading `connected_patterns[0]` — so a multi-type OR'd relationship
/// list (`[:FOLLOWS|AUTHORED|LIKED]`), a variable-length spec (`*1..2`), or a
/// multi-hop chain (`()-[:A]->()-[:B]->()`) inside `EXISTS { }` were all
/// SILENTLY narrowed to "first hop, first type, fixed length" — returning
/// wrong results with no signal (ground rule 1 violation). Until EXISTS
/// routes through the same pattern-conversion pipeline as a top-level MATCH,
/// these shapes must be rejected loudly instead of silently narrowed.
///
/// The plain single-hop, single-type EXISTS shape that the current code
/// handles correctly must keep working (last case below).
#[tokio::test]
async fn exists_subquery_unsupported_shapes_error_cleanly() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let unsupported = [
        // Multi-type OR'd relationship list + variable-length (the exact
        // issue #574 repro).
        "MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS|AUTHORED|LIKED*1..2]->(b) } RETURN a.name",
        // Variable-length only.
        "MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS*1..2]->(b) } RETURN a.name",
        // Multi-type OR'd relationship list only.
        "MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS|AUTHORED]->(b) } RETURN a.name",
        // Multi-hop chain.
        "MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS]->()-[:AUTHORED]->(b) } RETURN a.name",
    ];
    for cypher in unsupported {
        let result = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
        match result {
            Err(msg) => assert!(
                msg.contains("#574") && msg.contains("Unsupported expression"),
                "[{cypher}] must fail with a clean, attributable (#574) \
                 Unsupported expression error, got: {msg}"
            ),
            Ok(sql) => panic!(
                "[{cypher}] must error cleanly instead of silently narrowing the \
                 EXISTS pattern to a single-hop, single-type check; rendered:\n{sql}"
            ),
        }
    }

    // Regression guard: the plain single-hop, single-type shape must still
    // render exactly as before — checking FOLLOWS only, one hop.
    let sql = render(
        &schema,
        "MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS]->(b) } RETURN a.name",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("EXISTS (SELECT 1 FROM social.user_follows_bench"),
        "plain single-hop EXISTS {{ (a)-[:FOLLOWS]->(b) }} must still render \
         a correlated EXISTS over user_follows_bench:\n{sql}"
    );
    assert!(
        !sql.to_uppercase().contains("AUTHORED") && !sql.to_uppercase().contains("LIKED"),
        "plain single-type EXISTS must not reference unrelated relationship \
         tables:\n{sql}"
    );
}

/// #579: the `AstPathPattern::ShortestPath`/`AllShortestPaths` arm in
/// `TryFrom<ast::ExistsSubquery>` (`ast_conversion.rs`) used to rebuild a
/// fresh inner `ExistsSubquery` with `where_clause: None`, discarding the
/// outer EXISTS's WHERE clause entirely and returning early — so
/// `EXISTS { shortestPath((a)-[:FOLLOWS]->(b)) WHERE b.name = 'x' }` rendered
/// successfully but silently dropped the `b.name = 'x'` filter (ground rule 1
/// violation). Fixed by threading `exists.where_clause` through into the
/// recursive call instead of discarding it.
///
/// The render layer's `generate_exists_sql` only supports a bare `GraphRel`
/// subplan (or the complex WithClause/GraphJoins/CartesianProduct pipeline) —
/// a `Filter(GraphRel)` subplan (what any EXISTS-with-WHERE produces) hits
/// "Unsupported feature: EXISTS pattern with non-GraphRel subplan" for the
/// PLAIN (non-shortestPath) shape too, confirmed unchanged on main. So the
/// correct, minimal fix is that the shortestPath-wrapped shape must now fail
/// the exact same loud, clean way as its non-shortestPath sibling — no longer
/// silently succeeding with wrong (WHERE-less) results.
#[tokio::test]
async fn shortest_path_exists_where_clause_errors_cleanly_not_silently_dropped() {
    let schema = load_schema(SchemaId::Standard.yaml_path());

    // The exact #579 repro (shortestPath) and its non-shortestPath sibling
    // (already-existing, unrelated to this fix) must now fail identically —
    // loud and clean, never silently rendering without the WHERE.
    let must_error_not_silently_drop_where = [
        "MATCH (a:User) WHERE EXISTS { MATCH shortestPath((a)-[:FOLLOWS]->(b:User)) \
         WHERE b.name = 'Bob Jones' } RETURN a.name",
        "MATCH (a:User) WHERE EXISTS { MATCH (a)-[:FOLLOWS]->(b:User) \
         WHERE b.name = 'Bob Jones' } RETURN a.name",
    ];
    for cypher in must_error_not_silently_drop_where {
        let result = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
        match result {
            Err(msg) => assert!(
                msg.contains("non-GraphRel subplan"),
                "[{cypher}] must fail with the clean 'non-GraphRel subplan' \
                 error, got: {msg}"
            ),
            Ok(sql) => panic!(
                "[{cypher}] must error cleanly instead of silently dropping \
                 the WHERE clause; rendered:\n{sql}"
            ),
        }
    }

    // Regression guard: shortestPath EXISTS WITHOUT a WHERE clause (the
    // common shape) must still render exactly as before — this fix must not
    // affect the no-WHERE case at all.
    let sql = render(
        &schema,
        "MATCH (a:User) WHERE EXISTS { MATCH shortestPath((a)-[:FOLLOWS]->(b:User)) } \
         RETURN a.name",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("EXISTS (SELECT 1 FROM social.user_follows_bench"),
        "shortestPath EXISTS without WHERE must still render a correlated \
         EXISTS over user_follows_bench:\n{sql}"
    );
}

/// Regression test: `EXISTS { pattern }` whose correlated variable was bound
/// BEFORE a `WITH` barrier — i.e. the variable lives in a CTE by the time the
/// EXISTS subquery is rendered — must correlate against the variable's
/// *actual* in-scope CTE column, not the stale pre-CTE Cypher alias.
///
/// Former root cause (fixed): `generate_exists_sql`'s bare-`GraphRel` branch
/// (render_expr.rs) baked a raw SQL string — `"... = {start_alias}.{id_col}"`
/// — using the pattern's ORIGINAL Cypher alias (e.g. `a`) as a literal table
/// reference, with no awareness that a preceding WITH had already renamed `a`
/// to a CTE-local alias (`a_cnt`) by the time this subquery renders. Fixed by
/// `render_expr::resolve_correlation_id_sql`, which resolves the correlation
/// alias's id through `resolve_correlation_cte_column` — a narrow task-local
/// channel published by `build_chained_with_match_cte_plan` *during* WITH→CTE
/// build (see that function's doc comment), falling back to the direct
/// schema-qualified reference when the alias isn't CTE-scoped. A companion fix
/// in `plan_optimizer.rs`'s CTE column pruning (`collect_columns_for_alias_in_expr`)
/// recognizes `alias.column` references inside `RenderExpr::ExistsSubquery`/
/// `PatternCount`'s pre-rendered SQL text, so the id column this now-correct
/// reference depends on isn't pruned away as "unused" before it's baked.
///
/// Live-verified against ClickHouse (25.8.12): the pre-fix SQL failed with
/// `Code: 47. DB::Exception: Unknown expression or function identifier
/// 'a.user_id' in scope ... (UNKNOWN_IDENTIFIER)`; the fixed SQL below
/// executes successfully and returns the correct rows (spot-checked against
/// raw-table ground truth).
#[tokio::test]
async fn with_barrier_exists_no_where_resolves_correlation_through_cte_scope() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User)-[:FOLLOWS]->(z) WITH a, count(z) AS cnt WHERE cnt > 0 \
         WITH a WHERE EXISTS { (a)-[:FOLLOWS]->(x) } RETURN a.name",
        SqlDialect::ClickHouse,
    )
    .await;

    // The WITH barrier renames the outer scope's alias for `a` to `a_cnt`
    // inside the second CTE's body.
    assert!(
        sql.contains("FROM with_a_cnt_cte_0 AS a_cnt"),
        "expected the first WITH's CTE to be referenced as `a_cnt` in the \
         second CTE's FROM clause:\n{sql}"
    );

    // Fixed: the EXISTS subquery correlates against the in-scope CTE alias
    // and its actual exposed id column, not the dangling pre-WITH `a`.
    assert!(
        sql.contains(
            "EXISTS (SELECT 1 FROM social.user_follows_bench WHERE \
             user_follows_bench.follower_id = a_cnt.p1_a_user_id)"
        ),
        "EXISTS subquery must correlate against the in-scope CTE column \
         `a_cnt.p1_a_user_id`, not the stale pre-CTE alias `a`:\n{sql}"
    );

    // The id column the EXISTS correlation depends on must survive CTE
    // column pruning (it's otherwise unused downstream — only `a.name` is
    // ultimately selected — so without the plan_optimizer.rs fix it gets
    // silently stripped, leaving the reference above dangling).
    assert!(
        sql.contains(r#"a.user_id AS "p1_a_user_id""#),
        "the first CTE must still project the id column the EXISTS \
         correlation needs, even though nothing else references it:\n{sql}"
    );
}

/// Sibling of the EXISTS characterization above, for the SAME WITH-barrier-
/// crossing shape but with an inner `WHERE` clause on the EXISTS pattern —
/// which converts the subplan from a bare `GraphRel` to `Filter(GraphRel)`.
/// `generate_exists_sql` has no arm matching `Filter(GraphRel)` specifically
/// (only bare `LogicalPlan::GraphRel` and the `WithClause|GraphJoins|
/// CartesianProduct` pipeline branch), so it falls into the catch-all and
/// errors — this is the SAME "non-GraphRel subplan" error as #579's
/// no-outer-WITH sibling, confirming the outer WITH barrier does not change
/// this particular failure mode (it fails identically with or without a
/// preceding WITH). Locks in current behavior; not a fix.
///
/// Re-verified unchanged by the `generate_exists_sql` stale-CTE-alias fix
/// (see the sibling test above): that fix only touches the bare-`GraphRel`
/// arm, so this `Filter(GraphRel)` shape still falls into the same catch-all.
#[tokio::test]
async fn with_barrier_exists_with_inner_where_errors_same_as_no_with_barrier() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let result = try_render(
        &schema,
        "MATCH (a:User)-[:FOLLOWS]->(z) WITH a, count(z) AS cnt WHERE cnt > 0 \
         WITH a WHERE EXISTS { (a)-[:FOLLOWS]->(x) WHERE x.name = 'Bob Jones' } \
         RETURN a.name",
        SqlDialect::ClickHouse,
    )
    .await;
    match result {
        Err(msg) => assert!(
            msg.contains("non-GraphRel subplan"),
            "WITH-barrier-crossing EXISTS-with-WHERE must fail with the same \
             clean 'non-GraphRel subplan' error as the no-WITH-barrier shape \
             (#579), got: {msg}"
        ),
        Ok(sql) => panic!(
            "expected a clean 'non-GraphRel subplan' error (matching #579's \
             no-WITH-barrier sibling); instead rendered:\n{sql}"
        ),
    }
}

/// Regression guard for the stale-CTE-alias fix above: a fresh MATCH with NO
/// preceding WITH must resolve its EXISTS correlation exactly as before —
/// `resolve_correlation_id_sql` falls back to the direct schema-qualified
/// reference (`node_schema.node_id.sql_tuple`) whenever the correlation
/// alias isn't CTE-scoped, so this common case is untouched by the fix.
#[tokio::test]
async fn exists_fresh_match_no_with_barrier_unaffected_by_cte_correlation_fix() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS]->(x) } RETURN a.name",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains(
            "EXISTS (SELECT 1 FROM social.user_follows_bench WHERE \
             user_follows_bench.follower_id = a.user_id)"
        ),
        "a fresh (non-WITH-barrier) MATCH's EXISTS correlation must still \
         reference the direct table alias `a.user_id`, unchanged by the \
         CTE-scope resolution fix:\n{sql}"
    );
}

/// #596 (Bug 1, SILENT): the `EXISTS { (a)<-[:FOLLOWS]-(b) }` pattern's arrow
/// points INTO `a` — so `a` is the relationship TARGET and must correlate to
/// `followed_id`, NOT `follower_id`. The old `generate_exists_sql` was
/// direction-blind (always `from_id = left_connection`), so this "users who
/// ARE followed" query silently returned "users who FOLLOW" instead.
/// Live-verified against social_benchmark: correct result is {1,2,3,4,7}.
#[tokio::test]
async fn exists_reverse_direction_correlates_target_column_596() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User) WHERE EXISTS { (a)<-[:FOLLOWS]-(b) } RETURN a.user_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains(
            "EXISTS (SELECT 1 FROM social.user_follows_bench WHERE \
             user_follows_bench.followed_id = a.user_id)"
        ),
        "reverse-direction EXISTS {{ (a)<-[:FOLLOWS]-(b) }} must correlate the \
         anchor `a` (the arrow TARGET) to `followed_id`, not `follower_id`:\n{sql}"
    );
    // NOT EXISTS of the same shape inherits the same (now correct) body.
    let not_sql = render(
        &schema,
        "MATCH (a:User) WHERE NOT EXISTS { (a)<-[:FOLLOWS]-(b) } RETURN a.user_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        not_sql.contains(
            "NOT EXISTS (SELECT 1 FROM social.user_follows_bench WHERE \
             user_follows_bench.followed_id = a.user_id)"
        ),
        "NOT EXISTS of the reverse-direction pattern must also use \
         `followed_id`:\n{not_sql}"
    );
}

/// #596 (Bug 2, SILENT): when BOTH endpoints of an `EXISTS` relationship
/// pattern are outer-bound — `MATCH (a)-[:FOLLOWS]->(b) WHERE EXISTS {
/// (b)-[:FOLLOWS]->(a) }` (mutual follows) — the correlation must pin BOTH
/// edge columns (`follower_id = b AND followed_id = a`). The old code emitted
/// only `follower_id = b` (checking "b follows anyone", not "b follows a"),
/// silently over-returning. Live-verified: 6 mutual-follow rows, not 9.
#[tokio::test]
async fn exists_both_endpoints_bound_correlates_both_columns_596() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE EXISTS { (b)-[:FOLLOWS]->(a) } \
         RETURN a.user_id, b.user_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains(
            "EXISTS (SELECT 1 FROM social.user_follows_bench WHERE \
             user_follows_bench.follower_id = b.user_id AND \
             user_follows_bench.followed_id = a.user_id)"
        ),
        "both-endpoints-bound EXISTS must correlate BOTH edge columns \
         (`follower_id = b AND followed_id = a`):\n{sql}"
    );
}

/// #596 (Bug 4, was LOUD Code 47): when the outer anchor is the RIGHT endpoint
/// and the LEFT is a fresh inner variable — `MATCH (a:User) WHERE EXISTS {
/// (b)-[:FOLLOWS]->(a) }` — the correlation must pin the RIGHT anchor `a` to
/// its edge column (`followed_id = a`), not the out-of-scope fresh `b`. The old
/// code blindly emitted `follower_id = b.user_id`, producing an unknown-identifier
/// error at execution. Live-verified: fixed result is {1,2,3,4,7}.
#[tokio::test]
async fn exists_right_anchor_correlates_right_endpoint_596() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User) WHERE EXISTS { (b)-[:FOLLOWS]->(a) } RETURN a.user_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains(
            "EXISTS (SELECT 1 FROM social.user_follows_bench WHERE \
             user_follows_bench.followed_id = a.user_id)"
        ),
        "right-anchor EXISTS must correlate the RIGHT anchor `a` to \
         `followed_id`:\n{sql}"
    );
    assert!(
        !sql.contains("b.user_id"),
        "right-anchor EXISTS must NOT reference the fresh inner (out-of-scope) \
         `b.user_id`:\n{sql}"
    );

    // Cross-type variant: MATCH (p:Post) WHERE EXISTS { (u:User)-[:LIKED]->(p) }
    // The anchor `p` is the RIGHT/TARGET endpoint → `post_id = p.post_id`.
    let post_sql = render(
        &schema,
        "MATCH (p:Post) WHERE EXISTS { (u:User)-[:LIKED]->(p) } RETURN p.post_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        post_sql.contains(
            "EXISTS (SELECT 1 FROM social.post_likes_bench WHERE \
             post_likes_bench.post_id = p.post_id)"
        ),
        "cross-type right-anchor EXISTS must correlate the TARGET `p` to \
         `post_id`:\n{post_sql}"
    );
    assert!(
        !post_sql.contains("u.user_id"),
        "cross-type right-anchor EXISTS must NOT reference the fresh inner \
         `u.user_id`:\n{post_sql}"
    );
}

/// #596: forward `EXISTS { (a)-[:FOLLOWS]->(x) }` (anchor is the LEFT/SOURCE)
/// must STAY correlated to `follower_id` — the direction-aware rewrite must not
/// regress the case that was already correct.
#[tokio::test]
async fn exists_forward_direction_still_correlates_source_column_596() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS]->(x) } RETURN a.user_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains(
            "EXISTS (SELECT 1 FROM social.user_follows_bench WHERE \
             user_follows_bench.follower_id = a.user_id)"
        ),
        "forward EXISTS {{ (a)-[:FOLLOWS]->(x) }} must still correlate the \
         SOURCE anchor `a` to `follower_id`:\n{sql}"
    );
}

/// #596 (cross-arm isolation regression): the `exists_outer_aliases` task-local
/// that classifies EXISTS anchors must be scoped to the currently-rendering
/// subplan, NOT accumulated across independent UNION arms. An earlier merge-only
/// version leaked arm 1's bound `b` into arm 2's scope, so arm 2's
/// `EXISTS { (u)-[:FOLLOWS]->(b) }` (where `b` is arm 2's FRESH inner var) wrongly
/// gained an `AND followed_id = b.user_id` term correlating an out-of-scope alias
/// → ClickHouse Code 47. This is the same cross-subplan task-local leak class
/// #593/#594 fixed for `cte_scope_for_correlation`; here it's isolated via
/// `CteScopeGenerationGuard` snapshot/restore + a union-boundary-aware collector.
#[tokio::test]
async fn exists_outer_aliases_do_not_leak_across_union_arms_596() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User)-[:FOLLOWS]->(b) RETURN a.user_id AS id \
         UNION \
         MATCH (u:User) WHERE EXISTS { (u)-[:FOLLOWS]->(b) } RETURN u.user_id AS id",
        SqlDialect::ClickHouse,
    )
    .await;
    // Arm 2's EXISTS correlates ONLY its own outer anchor `u` (source →
    // follower_id); the fresh inner `b` must be existentially quantified, NOT
    // pinned to a leaked `followed_id = b.user_id` from arm 1.
    assert!(
        sql.contains(
            "EXISTS (SELECT 1 FROM social.user_follows_bench WHERE \
             user_follows_bench.follower_id = u.user_id)"
        ),
        "arm 2's EXISTS must correlate only its own anchor `u`:\n{sql}"
    );
    assert!(
        !sql.contains("followed_id = b.user_id"),
        "arm 1's bound `b` must NOT leak into arm 2's EXISTS as an outer \
         anchor (cross-arm task-local leak → Code 47):\n{sql}"
    );

    // Sibling variant: an arm binding `x` must not leak into a later arm's
    // right-anchor EXISTS `(x:User)-[:FOLLOWS]->(m)`, which must resolve to the
    // RIGHT anchor `m` (followed_id), never the leaked left `x`.
    let sql2 = render(
        &schema,
        "MATCH (a:User)-[:FOLLOWS]->(x) RETURN a.user_id AS uid \
         UNION \
         MATCH (m:User) WHERE EXISTS { (x:User)-[:FOLLOWS]->(m) } RETURN m.user_id AS uid",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql2.contains(
            "EXISTS (SELECT 1 FROM social.user_follows_bench WHERE \
             user_follows_bench.followed_id = m.user_id)"
        ),
        "arm 2's right-anchor EXISTS must correlate `m` (followed_id), not the \
         leaked sibling alias `x`:\n{sql2}"
    );
    assert!(
        !sql2.contains("follower_id = x.user_id") && !sql2.contains("= x.user_id"),
        "sibling arm's `x` must NOT leak into arm 2's EXISTS correlation:\n{sql2}"
    );
}

/// #596: undirected `EXISTS { (a)-[:FOLLOWS]-(b) }` (single outer anchor `a`)
/// may match the anchor on EITHER edge end, so the correlation is an OR of both
/// columns. Live-verified: returns every user involved in any follow ({1..7}).
#[tokio::test]
async fn exists_undirected_correlates_either_column_596() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS]-(b) } RETURN a.user_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains(
            "EXISTS (SELECT 1 FROM social.user_follows_bench WHERE \
             user_follows_bench.follower_id = a.user_id OR \
             user_follows_bench.followed_id = a.user_id)"
        ),
        "undirected EXISTS must correlate the anchor on EITHER edge column \
         (OR of follower_id / followed_id):\n{sql}"
    );
}

/// Regression test (found by review of the stale-CTE-alias fix above): the
/// `cte_scope_for_correlation` channel `resolve_correlation_id_sql` reads is
/// task-local, so without generation scoping it stayed populated for the
/// rest of a query after the WITH-CTE arm that wrote it finished rendering —
/// a LATER, textually-unrelated UNION arm reusing the SAME Cypher alias name
/// (very natural in Cypher UNION, which requires identical column names —
/// hence often identical alias names — across arms; see #517's sibling tests
/// just above for this project's prior history with exactly this reused-
/// alias-across-arms shape) would then resolve ITS OWN, fresh, non-CTE-scoped
/// EXISTS correlation through the FIRST arm's now-stale CTE column instead of
/// its own real table.
///
/// Fixed by tagging every `cte_scope_for_correlation` entry with a
/// monotonic "generation" (`query_context::enter_cte_scope_generation`),
/// scoped via an RAII guard (`CteScopeGenerationGuard`) around every place an
/// independent sibling subplan gets rendered (each UNION branch — in all
/// three of `to_render_plan`'s and `to_render_plan_with_ctx`'s union-arm
/// code paths — plus each CartesianProduct side): an entry is only trusted
/// while the exact `build_chained_with_match_cte_plan` invocation that wrote
/// it is still the innermost one active, so once that arm's rendering
/// finishes, its entries can never match again, no matter what renders next.
///
/// Live-verified against ClickHouse (25.8.x): before this fix, the second
/// arm below raised `Code: 47. DB::Exception: Unknown expression or function
/// identifier 'a_c.p1_a_user_id'` (the first arm's stale CTE column,
/// nonsensical in the second arm's plain `FROM social.users_bench AS a`
/// scope); after the fix it executes and returns the correct row sets for
/// both arms.
#[tokio::test]
async fn union_arm_exists_does_not_resolve_through_sibling_arms_stale_cte_scope() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User) WITH a, count(*) AS c WHERE c > 0 RETURN a.name AS name \
         UNION MATCH (a:User) WHERE EXISTS { (a)-[:LIKED]->(y) } RETURN 'lit' AS name",
        SqlDialect::ClickHouse,
    )
    .await;

    // First arm: WITH-CTE-scoped `a` renders as before (unaffected).
    assert!(
        sql.contains("FROM with_a_c_cte_0 AS a_c"),
        "first arm's WITH must still render its own CTE as before:\n{sql}"
    );

    // Second arm: a FRESH, non-CTE-scoped `a` — its EXISTS correlation must
    // reference ITS OWN real table alias, never the first arm's CTE column.
    assert!(
        sql.contains(
            "EXISTS (SELECT 1 FROM social.post_likes_bench WHERE \
             post_likes_bench.user_id = a.user_id)"
        ),
        "second UNION arm's EXISTS must correlate against its own \
         `a.user_id` (a plain table reference — it has no preceding WITH), \
         not the first arm's stale CTE column `a_c.p1_a_user_id`:\n{sql}"
    );
    assert!(
        !sql.contains("a_c.p1_a_user_id"),
        "the first arm's CTE column must never leak into the second arm's \
         SQL text:\n{sql}"
    );

    // Determinism (matches the discipline of this file's other UNION tests).
    for _ in 0..5 {
        let again = render(
            &schema,
            "MATCH (a:User) WITH a, count(*) AS c WHERE c > 0 RETURN a.name AS name \
             UNION MATCH (a:User) WHERE EXISTS { (a)-[:LIKED]->(y) } RETURN 'lit' AS name",
            SqlDialect::ClickHouse,
        )
        .await;
        assert_eq!(sql, again, "nondeterministic render across repeated calls");
    }
}

/// #578: `logical_expr::RelationshipPattern` (what `size(...)`/`PatternCount`
/// bottoms out at) has no `variable_length` field at all, so a `*1..2`-style
/// hop bound on a size() pattern was silently dropped at the AST->logical_expr
/// conversion boundary; multi-type OR'd labels (`[:A|B]`) survived that
/// conversion but were then silently narrowed to the FIRST type by the
/// render-layer PatternCount SQL generators. Both shapes must now be
/// rejected loudly (a clean, attributable #578 error) instead of silently
/// narrowing size()'s count to a smaller pattern than the one written.
///
/// The plain single-hop/single-type and legitimate multi-hop-chain shapes
/// that already render correctly must keep working (last two cases below).
#[tokio::test]
async fn size_pattern_count_vlp_and_multi_type_error_cleanly() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let unsupported = [
        // Variable-length hop (the exact #578 repro).
        "MATCH (a:User) RETURN size((a)-[:FOLLOWS*1..2]->()) AS n",
        // Multi-type OR'd relationship list.
        "MATCH (a:User) RETURN size((a)-[:FOLLOWS|LIKED]->()) AS n",
        // Variable-length wrapped in shortestPath() — must be caught via the
        // ShortestPath/AllShortestPaths recursion in the AST walker.
        "MATCH (a:User) RETURN size(shortestPath((a)-[:FOLLOWS*1..3]->())) AS n",
    ];
    for cypher in unsupported {
        let result = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
        match result {
            Err(msg) => assert!(
                msg.contains("#578") && msg.contains("Unsupported expression"),
                "[{cypher}] must fail with a clean, attributable (#578) \
                 Unsupported expression error, got: {msg}"
            ),
            Ok(sql) => panic!(
                "[{cypher}] must error cleanly instead of silently narrowing the \
                 size(...) pattern to a smaller shape; rendered:\n{sql}"
            ),
        }
    }

    // Regression guard: plain single-hop, single-type size() must still work.
    let sql = render(
        &schema,
        "MATCH (a:User) RETURN size((a)-[:FOLLOWS]->()) AS n",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("user_follows_bench"),
        "plain single-hop size() must still render a count over \
         user_follows_bench:\n{sql}"
    );

    // Regression guard: a legitimate multi-HOP chain (each hop single-type,
    // no VLP) is a different shape from a multi-TYPE single hop and must
    // still render via generate_multi_hop_pattern_count_sql.
    let sql = render(
        &schema,
        "MATCH (a:User) RETURN size((a)-[:FOLLOWS]->()-[:LIKED]->()) AS n",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.to_uppercase().contains("JOIN"),
        "legitimate multi-hop size() chain must still render a joined \
         multi-hop count:\n{sql}"
    );
}

/// #609: ClickHouse's analyzer (verified on 25.8) cannot resolve a recursive
/// CTE referenced from the SECOND (or later) arm of a BARE top-level UNION —
/// `WITH RECURSIVE v1 AS (...), v2 AS (...) SELECT ... FROM v1 UNION ALL
/// SELECT ... FROM v2` fails with Code 60 on `v2` (arm-swap moves the error
/// to whichever recursive CTE is second; per-arm wrapping does NOT help).
/// Any top-level UNION emitted alongside a recursive CTE must therefore be
/// wrapped in a subquery. Undirected VLP (BidirectionalUnion → two recursive
/// CTEs + UNION ALL) hit this exactly when the query had no ORDER BY /
/// LIMIT / aggregate (each of which already forced the wrapper — why the
/// bug looked intermittent); a VLP inside a later Cypher-UNION arm hit the
/// same wall via the per-arm render path.
#[tokio::test]
async fn recursive_cte_union_wrapped_in_subquery_609() {
    let schema = load_schema(SchemaId::Standard.yaml_path());

    // Two-arm undirected recursive CTEs + bare top-level UNION, NO ORDER BY.
    // (#609's original repro was an undirected range VLP; #617 turned that
    // shape into a SINGLE walk with no union, so the still-two-arm undirected
    // shortestPath exercises the wrapper instead.)
    let sql = render(
        &schema,
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*1..3]-(b:User)) WHERE a.user_id = 1 \
         RETURN b.user_id, length(p)",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("WITH RECURSIVE"),
        "undirected VLP must render recursive CTEs:\n{sql}"
    );
    let after_ctes = sql
        .rsplit_once("\n)\n")
        .map(|(_, tail)| tail)
        .unwrap_or(&sql);
    assert!(
        !after_ctes.trim_start().starts_with("SELECT \n      t."),
        "the top-level UNION over recursive CTEs must be wrapped in a \
         subquery (bare top-level arms fail CH Code 60):\n{sql}"
    );
    assert!(
        sql.contains("FROM (") && sql.contains(") AS __union"),
        "expected the __union subquery wrapper:\n{sql}"
    );

    // VLP in the SECOND arm of a Cypher UNION (per-arm render path).
    let sql = render(
        &schema,
        "MATCH (a:User) WHERE a.user_id = 6 RETURN a.user_id AS id \
         UNION ALL \
         MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.user_id = 5 RETURN b.user_id AS id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("WITH RECURSIVE") && sql.contains(") AS __cypher_union"),
        "a Cypher UNION whose arm carries a recursive CTE must be wrapped \
         (SELECT * FROM (...) AS __cypher_union):\n{sql}"
    );
}

/// #611/#614: OPTIONAL MATCH WHERE placement — gate conjuncts must never
/// land in the outer WHERE (a bare outer predicate evaluates to false/NULL
/// against NULL-extended anchor rows and silently drops them).
///
/// #611 (chained / sibling clauses): a WHERE on an OPTIONAL clause whose
/// conjunct references only a mandatory variable folds into THAT clause's
/// gating LEFT JOIN ON — including when the clause's entry connection is an
/// optional variable from an earlier clause (chained), and when two sibling
/// clauses share one anchor (each gate goes to its own clause's join).
///
/// #614 (opaque predicates): a WHERE conjunct on the OPTIONAL variable via
/// `size((pattern))` or `EXISTS {}` folds into the #479/#552 combined-anchor
/// LEFT JOIN subquery (structured `correlated_aliases` classification), same
/// as a plain property predicate.
#[tokio::test]
async fn optional_where_gate_placement_611_614() {
    let schema = load_schema(SchemaId::Standard.yaml_path());

    // #611 chained: gate lands on the SECOND clause's edge join.
    let sql = render(
        &schema,
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) \
         OPTIONAL MATCH (b)-[:FOLLOWS]->(c:User) WHERE a.is_active = true \
         RETURN a.user_id, b.user_id, c.user_id",
        SqlDialect::ClickHouse,
    )
    .await;
    // Alias numbers vary with the process-wide counter — match the join-key
    // shape without them: the second clause's edge join keys on the FIRST
    // clause's edge endpoint (".followed_id"), and carries the gate.
    assert!(
        sql.contains(".followed_id AND a.is_active = true"),
        "#611 chained: gate must fold into the second clause's edge join ON:\n{sql}"
    );
    assert!(
        !sql.contains("WHERE a.is_active"),
        "#611 chained: gate must not remain in the outer WHERE:\n{sql}"
    );

    // #611 siblings: each clause's gate on its own edge join.
    let sql = render(
        &schema,
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) WHERE a.is_active = true \
         OPTIONAL MATCH (a)-[:LIKED]->(p:Post) WHERE a.country = 'US' \
         RETURN a.user_id, b.user_id, p.post_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains(".follower_id = a.user_id AND a.is_active = true")
            && sql.contains(".user_id = a.user_id AND a.country = 'US'"),
        "#611 siblings: each gate must fold into its OWN clause's join ON:\n{sql}"
    );
    assert!(
        !sql.contains("WHERE a."),
        "#611 siblings: no gate may remain in the outer WHERE:\n{sql}"
    );

    // #611 guard: an identical conjunct in a pattern-shaped base MATCH stays
    // in the outer WHERE (mandatory row-dropping semantics must survive).
    let sql = render(
        &schema,
        "MATCH (a:User)-[:AUTHORED]->(z:Post) WHERE a.is_active = true \
         OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) WHERE a.is_active = true \
         RETURN a.user_id, z.post_id, b.user_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("WHERE a.is_active = true"),
        "#611 guard: the base MATCH's identical conjunct must stay in the \
         outer WHERE:\n{sql}"
    );

    // #614: PatternCount and EXISTS on the optional variable fold into the
    // combined-anchor subquery, not the outer WHERE.
    for (label, cypher) in [
        (
            "size()",
            "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) \
             WHERE size((b)-[:FOLLOWS]->()) > 0 RETURN a.user_id, b.user_id",
        ),
        (
            "EXISTS",
            "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) \
             WHERE EXISTS { (b)-[:FOLLOWS]->(x:User) } RETURN a.user_id, b.user_id",
        ),
    ] {
        let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
        assert!(
            sql.contains("__cg_combined_anchor_key"),
            "#614 {label}: opaque optional-var predicate must fold into the \
             combined-anchor LEFT JOIN subquery:\n{sql}"
        );
        assert!(
            !sql.to_uppercase().contains("\nWHERE"),
            "#614 {label}: nothing may remain in the outer WHERE:\n{sql}"
        );
    }
}

/// #599: `size((pattern))` renders as a bare correlated `COUNT(*)` scalar
/// subquery; ClickHouse decorrelates that into a LEFT JOIN, so an outer row
/// with ZERO pattern matches yields NULL instead of 0 — silently breaking
/// `size(...) = 0` filters, comparisons, arithmetic, and CASE branches on
/// zero-degree nodes. Every PatternCount rendering (all three direction
/// variants and the multi-hop chain, in RETURN and WHERE positions alike)
/// must come out wrapped in `coalesce(..., 0)`.
#[tokio::test]
async fn size_pattern_count_coalesces_null_to_zero() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cases = [
        // Outgoing, RETURN position (the #599 repro).
        "MATCH (a:User) RETURN a.user_id, size((a)-[:FOLLOWS]->()) AS c",
        // Incoming.
        "MATCH (a:User) RETURN a.user_id, size((a)<-[:FOLLOWS]-()) AS c",
        // Undirected.
        "MATCH (a:User) RETURN a.user_id, size((a)-[:FOLLOWS]-()) AS c",
        // Multi-hop chain (generate_multi_hop_pattern_count_sql path).
        "MATCH (a:User) RETURN a.user_id, size((a)-[:FOLLOWS]->()-[:LIKED]->()) AS c",
        // WHERE position — `NULL = 0` is never true, so without the wrap
        // zero-degree nodes silently vanish from this filter.
        "MATCH (a:User) WHERE size((a)-[:FOLLOWS]->()) = 0 RETURN a.user_id",
        // Post-WITH barrier position.
        "MATCH (a:User) WITH a WHERE size((a)-[:FOLLOWS]->()) = 0 RETURN a.user_id",
    ];
    for cypher in cases {
        let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
        assert!(
            sql.contains("coalesce((SELECT COUNT(*)"),
            "[{cypher}] pattern-count subquery must be wrapped in \
             coalesce(..., 0) so zero matches count as 0, not NULL:\n{sql}"
        );
        assert!(
            !sql.contains(", (SELECT COUNT(*)") && !sql.contains("= (SELECT COUNT(*)"),
            "[{cypher}] found an UNwrapped bare correlated COUNT(*) \
             subquery:\n{sql}"
        );
    }
}

/// #466 round 4 (adversarial-review blocking finding): `id(alias)` on a
/// `pattern_union` endpoint must resolve LABEL-AGNOSTICALLY to the CTE's
/// start_id/end_id — never to ONE label's id column.
///
/// Previously FilterTagging pre-resolved `id(o)` to a single label's id
/// property (`o.post_id`), which the per-branch WHERE resolver then NULLed
/// on every other label's branches: STD directed `WHERE id(o)='1'` returned
/// 6 instead of 10 (regression vs main, which mapped id() to the
/// label-agnostic `r.end_id` in the outer WHERE). FilterTagging now keeps
/// id() unresolved for pattern_union endpoints
/// (`LogicalPlan::pattern_union_endpoint_role`), restoring the outer
/// start_id/end_id rewrite. Live: STD directed `id(o)='1'` → 10; STD
/// undirected `id(n)='1'` → 16 (User#1 AND Post#1 rows — ClickGraph string
/// ids are label-ambiguous, matching main's outer-rewrite semantics); FK
/// undirected `id(o)='5'` → 1 (the reverse-orientation row; main returned 0
/// because it had no reverse rows at all).
///
/// Sibling (same root): `RETURN id(o)` on a pattern_union endpoint used to
/// fall through to the generic function mapping's `toInt64(0)` placeholder
/// (FK: silent 0,0,0,0) or an invalid single-label column (STD: loud DB
/// error). SelectBuilder now maps it to the CTE's start_id/end_id — real
/// ids on both schemas.
///
/// Round 4.5 extends the same treatment to `elementId()`, which previously
/// fell through EVERY handler on pattern_union shapes (silently unfiltered
/// WHERE / invalid SQL in RETURN). elementId in this codebase is the
/// composite `Label:id-` string (`generate_node_element_id`; trailing `-`
/// is a Browser-compat sentinel), so the rewrites rebuild that format from
/// the CTE's type + id columns: `concat(r.end_type, ':', r.end_id, '-')`.
/// A bare-id literal like '5' therefore correctly matches NOTHING —
/// `elementId(o) = 'Order:5-'` is the valid form.
///
/// KNOWN LIMITATIONS (review round-4 catalog; NOT fixed here):
///   - GROUP BY / ORDER BY over `id(o)` still render the generic function
///     mapping's `toInt64(0)` placeholder (pre-existing on main, both
///     schemas) — only SELECT/WHERE positions get the label-agnostic
///     start_id/end_id treatment.
///   - `LogicalPlan::pattern_union_endpoint_role` does not walk
///     Unwind/PageRank wrappers. Currently unreachable: a multi-clause
///     UNWIND+MATCH over a fully-unlabeled pattern prunes to the
///     `WHERE false` placeholder before reaching the walker.
#[tokio::test]
async fn pattern_union_id_function_is_label_agnostic() {
    let std_schema = load_schema(SchemaId::Standard.yaml_path());
    let fk = load_schema(SchemaId::FkEdge.yaml_path());

    // WHERE id(o): outer label-agnostic end_id comparison, no per-branch
    // single-label id column.
    let sql = render(
        &std_schema,
        "MATCH (n)-[r]->(o) WHERE id(o) = '1' RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("r.end_id = '1'"),
        "id(o) must map to the label-agnostic CTE end_id in the outer WHERE:\n{sql}"
    );
    assert!(
        !sql.contains("post_id = '1'") && !sql.contains("user_id = '1'"),
        "id(o) must NOT degrade to a single label's id column (NULL on every \
         other label's branch):\n{sql}"
    );

    // Anchor side: id(n) → start_id.
    let sql = render(
        &std_schema,
        "MATCH (n)-[r]-(o) WHERE id(n) = '1' RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("r.start_id = '1'"),
        "id(n) must map to the label-agnostic CTE start_id:\n{sql}"
    );

    // RETURN id(o): real per-row id from the CTE, not the toInt64(0)
    // placeholder and not a nonexistent single-label column.
    let sql = render(
        &fk,
        "MATCH (n)-[r]-(o) RETURN id(o) AS i LIMIT 4",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("r.end_id AS \"i\""),
        "RETURN id(o) must project the CTE's end_id:\n{sql}"
    );
    assert!(
        !sql.contains("toInt64(0)"),
        "RETURN id(o) must not emit the placeholder zero literal:\n{sql}"
    );

    // Labeled-anchor control: no pattern_union involved — id() resolution
    // unchanged (single-label id column on the plain join path).
    let sql = render(
        &std_schema,
        "MATCH (a:User)-[r:FOLLOWS]->(o) WHERE id(a) = '1' RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        !sql.contains("pattern_union") && sql.contains("user_id"),
        "labeled-anchor id(a) must keep the single-label resolution on the \
         plain join path:\n{sql}"
    );

    // #466 round 4.5: elementId() gets the same label-agnostic treatment,
    // rebuilt in the codebase's composite `Label:id-` format from the CTE's
    // type + id columns (previously fell through every handler: silently
    // unfiltered WHERE on FK, invalid SQL in RETURN).
    let sql = render(
        &fk,
        "MATCH (n)-[r]-(o) WHERE elementId(o) = 'Order:5-' RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("concat(r.end_type, ':', r.end_id, '-') = 'Order:5-'"),
        "elementId(o) must rebuild the composite Label:id- format \
         label-agnostically in the outer WHERE:\n{sql}"
    );
    let sql = render(
        &fk,
        "MATCH (n)-[r]-(o) RETURN elementId(o) AS e LIMIT 3",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        sql.contains("concat(r.end_type, ':', r.end_id, '-') AS \"e\""),
        "RETURN elementId(o) must project the composite Label:id- string:\n{sql}"
    );
}

/// #467 FIX (was P0.5 characterization): `MATCH (n) RETURN count(n)` over a
/// heterogeneous (multi-label) unlabeled node scan used to render
/// `count(<id column of ONE arbitrary label>)` over a UNION of per-label
/// branches — every OTHER branch emits NULL for that specific column, so
/// COUNT silently excluded every row not belonging to that one label
/// (Standard returned 5 not 13; FK-edge returned 4 not 12).
///
/// The fix (`projection_tagging.rs`) compiles whole-node count over a
/// multi-label union to a discriminator over EVERY candidate label's id
/// column:
///   - `count(n)`          -> `count(coalesce(id_a, id_b, ...))` — non-NULL
///     exactly when the node exists on that row (its own branch's id), and
///     still NULL under OPTIONAL NULL-extension so NULL-skipping is preserved.
///   - `count(DISTINCT n)` -> `count(DISTINCT tuple(id_a, id_b, ...))` — a
///     tuple keeps each label's identity separate so ids that collide across
///     labels (e.g. User 3 vs Post 3) are NOT merged.
/// Live after fix: Standard 13, FK-edge 12, count(DISTINCT n) 13.
///
/// Contrast: Denormalized is unchanged (`dn_browser_style_count` golden) — its
/// single virtual id column (`code`) is one label, populated on EVERY UNION
/// branch (both `origin_code` and `dest_code` alias to it), so it still
/// reduces to `count(a.code)` and already counted every row (7).
#[tokio::test]
async fn browser_whole_node_count_covers_heterogeneous_scan() {
    for (schema_id, id_cols) in [
        (SchemaId::Standard, ["n.post_id", "n.user_id"]),
        (SchemaId::FkEdge, ["n.customer_id", "n.order_id"]),
    ] {
        let schema = load_schema(schema_id.yaml_path());

        // count(n): coalesce over every label's id column — non-NULL on the
        // row's own branch, so every row is counted.
        let sql = render(&schema, "MATCH (n) RETURN count(n)", SqlDialect::ClickHouse).await;
        assert!(
            sql.contains(&format!(
                "count(coalesce(`{}`, `{}`))",
                id_cols[0], id_cols[1]
            )),
            "{:?}: expected count(n) to coalesce every label's id column so \
             all rows are counted:\n{sql}",
            schema_id.dir()
        );
        // Each label's id column is projected as an anchor and NULL-padded on
        // the OTHER branch — coalesce recovers the branch's own id.
        for col in id_cols {
            assert!(
                sql.contains(&format!("NULL AS \"{col}\"")),
                "{:?}: expected `{col}` to be NULL-padded in the other UNION \
                 branch (coalesce recovers the present id):\n{sql}",
                schema_id.dir()
            );
        }

        // count(DISTINCT n): tuple over every label's id column so cross-label
        // id collisions are not merged.
        let sql_distinct = render(
            &schema,
            "MATCH (n) RETURN count(DISTINCT n)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql_distinct.contains(&format!(
                "count(DISTINCT tuple(`{}`, `{}`))",
                id_cols[0], id_cols[1]
            )),
            "{:?}: expected count(DISTINCT n) to tuple every label's id column \
             so colliding ids across labels are not merged:\n{sql_distinct}",
            schema_id.dir()
        );
    }

    // Contrast: Denormalized's single virtual id is one label, non-null on
    // every branch, so it still reduces to the single-column form.
    let schema = load_schema(SchemaId::Denormalized.yaml_path());
    let sql = render(&schema, "MATCH (a) RETURN count(a)", SqlDialect::ClickHouse).await;
    assert!(
        sql.contains("count(`a.code`)"),
        "denormalized count(a) should still reduce to the single virtual id \
         column:\n{sql}"
    );
    assert!(
        !sql.contains("NULL AS \"a.code\""),
        "denormalized count(a)'s id column must be non-null on every UNION \
         branch:\n{sql}"
    );
}

/// Regression lock for #468: `MATCH ()-[r]->() RETURN DISTINCT type(r) LIMIT 25`
/// renders `FROM pattern_union_r AS r`, and the outer SELECT must reference
/// the CTE through the SAME alias — `r.path_relationships` — not the VLP
/// alias `t` (which is only bound for multi-type VLP-joins CTEs,
/// `FROM vlp_multi_type_a_b AS t`). Before the fix, the `type(r)` rewrite in
/// projection_tagging hardcoded `t`, producing invalid SQL (ClickHouse
/// `Code: 47 ... Unknown expression or function identifier
/// 't.path_relationships' ... Maybe you meant: ['r.path_relationships']`).
/// Live-verified fixed: returns AUTHORED/FOLLOWS/LIKED on the `social`
/// fixture. Byte-locked by the `browser_type_probe` golden.
#[tokio::test]
async fn browser_type_probe_pattern_union_outer_alias_matches_from() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH ()-[r]->() RETURN DISTINCT type(r) LIMIT 25";

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, cypher, dialect).await;

        assert!(
            sql.contains("pattern_union_r AS ("),
            "expected the pattern_union CTE for {dialect:?}:\n{sql}"
        );
        // The CTE is read under alias `r`…
        assert!(
            sql.contains("FROM pattern_union_r AS r"),
            "expected the outer query to read the CTE as alias `r` for \
             {dialect:?}:\n{sql}"
        );
        // …and the outer SELECT must reference it through that alias (#468).
        assert!(
            sql.contains("r.path_relationships"),
            "expected type(r) to resolve from `r.path_relationships` (the \
             pattern_union CTE alias) for {dialect:?}:\n{sql}"
        );
        assert!(
            !sql.contains("t.path_relationships"),
            "outer SELECT must not reference the unbound VLP alias \
             `t.path_relationships` (#468 regression) for {dialect:?}:\n{sql}"
        );
    }
}

/// Regression lock for #469: `MATCH p=(a:User)-[:FOLLOWS*1..2]->(b) RETURN p`
/// must materialize the path tuple from columns the recursive VLP CTE
/// actually projects — `tuple(t.path_nodes, t.path_relationships,
/// t.hop_count)`.
///
/// #598 (part 2): the directed range VLP CTE now ALSO projects `path_edges`
/// (an internal edge-tracking array for relationship-uniqueness cycle
/// detection, referenced only as `vp.path_edges` inside the recursive arm).
/// `path_edges` is NOT part of the path representation, so the outer `RETURN p`
/// tuple must still consume only `path_nodes`/`path_relationships`/`hop_count`
/// via alias `t` — and must NOT reference `t.path_edges` (which would be the
/// #469 unbound-identifier class of bug, ClickHouse `Code: 47`). Because
/// `path_edges` is now consistently projected through every arm (base,
/// recursive, and the min-hops `_inner` wrapper via `SELECT *`), the recursive
/// `vp.path_edges` reference is bound and the query executes.
/// Live-verified fixed on the `social` fixture. Byte-locked by the
/// `path_vlp` golden.
#[tokio::test]
async fn browser_vlp_path_return_uses_only_cte_defined_columns() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH p=(a:User)-[:FOLLOWS*1..2]->(b) RETURN p";

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, cypher, dialect).await;

        // The outer path tuple must NOT reference the internal edge-tracking
        // column: `path_edges` is projected inside the CTE (as `vp.path_edges`)
        // but is not part of the path representation, so `t.path_edges` must
        // never appear in the outer SELECT (#469 class of unbound reference).
        assert!(
            !sql.contains("t.path_edges"),
            "RETURN p path tuple must not reference `t.path_edges` — it is an \
             internal cycle-tracking column, not part of the path (#469) for \
             {dialect:?}:\n{sql}"
        );
        assert!(
            sql.contains("t.path_nodes")
                && sql.contains("t.path_relationships")
                && sql.contains("t.hop_count"),
            "expected the path tuple to consume the CTE's actual projection \
             (path_nodes, path_relationships, hop_count) for {dialect:?}:\n{sql}"
        );
    }
}

/// #496 UPDATE: `VlpTransitivityCheck`'s clamp-to-single-hop was semantically
/// wrong for two shapes (pre-existing on main, tracked as #496):
///   - `*0..N` — zero-hop paths are real (the start node itself), so a
///     single-hop clamp drops rows;
///   - undirected — reverse-direction chaining can make >1-hop paths
///     possible; the clamp never consulted direction.
/// The #496 fix does NOT attempt to render either shape (both were shown live
/// to require the recursive-VLP-CTE machinery to support heterogeneous
/// start/end node tables across every schema pattern — a real rendering
/// feature, not a clamp fix, out of scope for #496). Instead it converts
/// main's previous SILENT-WRONG-RESULTS failure mode (`RETURN p` rendered
/// `tuple(t.path_nodes, ...)` referencing a NEVER-generated recursive CTE —
/// alias `t` unbound, ClickHouse Code 47 only surfaces at EXECUTION time, and
/// only when a path variable happens to be bound) into a clean, immediate
/// `AnalyzerError::InvalidPlan` at PLAN time, for ANY query using these
/// shapes (path variable or not — the old bug only manifested with a path
/// variable bound; the clamp itself was wrong regardless). This is strictly
/// louder/earlier than before: previously an unbound-path-variable query on
/// these shapes could still silently return the wrong single-hop-clamped
/// rows if `RETURN p` wasn't used (e.g. `RETURN count(*)`); now it errors
/// unconditionally at plan time.
#[tokio::test]
async fn fk_edge_nontransitive_vlp_guarded_shapes_stay_loud_488() {
    let schema = load_schema(SchemaId::FkEdge.yaml_path());
    for cypher in [
        // zero-hop lower bound
        "MATCH p = (o:Order)-[:PLACED_BY*0..2]->(c) RETURN p",
        // undirected
        "MATCH p = (o:Order)-[:PLACED_BY*1..2]-(c) RETURN p",
    ] {
        for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
            let err = try_render(&schema, cypher, dialect)
                .await
                .expect_err(&format!(
                    "guarded shape must fail loudly at plan time (not silently \
                     clamp) for {dialect:?}: {cypher}"
                ));
            assert!(
                err.contains("#496") && err.contains("not yet supported"),
                "expected a clear #496 UnsupportedFeature-style plan error for \
                 {dialect:?}: {cypher}\nGot: {err}"
            );
        }
    }
}

/// P0.5 structural lock for the Denormalized `path_unlabeled` case
/// (`MATCH p=()-[]->() RETURN p LIMIT 10`). Originally this was NOT a
/// byte-golden: the fixed_path edge-property column order (`t3.distance`/
/// `t3.flight_num`/`t3.carrier`/`t3.departure_time`/`t3.arrival_time`) was
/// emitted in nondeterministic HashMap order — verified by 3 independent
/// process invocations producing 3 different orderings; the same latent
/// defect documented for `denorm_path_return` in the P0.2/#459
/// known-suspicious block above. FIXED by #480 (sorted property getters +
/// `expand_cte_entity` sort): the shape is now byte-locked as
/// `dn_path_unlabeled` in `DENORM_BROWSER_CORPUS`. This test is retained as
/// a readable invariant lock: the fixed_path marker, the ROLE-CORRECT
/// virtual-id node endpoints (from → origin_code, to → dest_code), and the
/// presence of every edge property column.
#[tokio::test]
async fn denorm_path_unlabeled_column_set_is_stable() {
    let schema = load_schema(SchemaId::Denormalized.yaml_path());
    let cypher = "MATCH p=()-[]->() RETURN p LIMIT 10";

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, cypher, dialect).await;
        let q = if dialect == SqlDialect::Databricks {
            '`'
        } else {
            '"'
        };
        let marker_fn = if dialect == SqlDialect::Databricks {
            "struct"
        } else {
            "tuple"
        };

        // Anonymous-alias NAMES (the `t<n>` in `t1`/`t2`/`t0` below) are
        // assigned from a process-global counter shared across the whole
        // test binary, so their exact numbers are order-dependent — extract
        // the from/to/rel alias names from the marker itself rather than
        // hardcoding them (unlike `sql_golden_snapshots`'s byte-goldens, this
        // structural test isn't going through `normalize()`).
        let marker_re = regex::Regex::new(&format!(
            r"{marker_fn}\('fixed_path', '(t\d+)', '(t\d+)', '(t\d+)'\)"
        ))
        .unwrap();
        let caps = marker_re
            .captures(&sql)
            .unwrap_or_else(|| panic!("{dialect:?}: expected the fixed_path marker:\n{sql}"));
        let (from_alias, to_alias) = (&caps[1], &caps[2]);

        assert!(
            sql.contains(&format!("origin_code AS {q}{from_alias}.code{q}")),
            "{dialect:?}: from-node code must resolve to origin_code:\n{sql}"
        );
        assert!(
            sql.contains(&format!("dest_code AS {q}{to_alias}.code{q}")),
            "{dialect:?}: to-node code must resolve to dest_code:\n{sql}"
        );
        // Every edge property must be sourced, regardless of column order.
        for col in ["distance", "carrier", "departure_time", "arrival_time"] {
            assert!(
                sql.contains(col),
                "{dialect:?}: path edge properties must include `{col}`:\n{sql}"
            );
        }
    }
}

/// P0.5 structural lock for the Standard `path_unlabeled` case
/// (`MATCH p=()-[]->() RETURN p LIMIT 10`), which is NOT a byte-golden.
/// Unlike the Denormalized case above (nondeterministic COLUMN ORDER), this
/// shape's instability is in the CTE NAME itself: it routes through
/// `pattern_union_{alias}` where `alias` is an anonymous name auto-assigned
/// from the SAME process-global counter that produces the `t<n>` tokens
/// `normalize()` remaps elsewhere in this file (`from_builder.rs`:
/// `format!("pattern_union_{}", graph_rel.alias)`). Because the counter value
/// is embedded INSIDE the identifier (`pattern_union_t3`) rather than as its
/// own token, `normalize()`'s `\bt\d+\b` regex does not match it — `_` is a
/// word character, so there is no boundary before the `t`. Confirmed: two
/// back-to-back `cargo test` runs of the byte-golden suite produced
/// `pattern_union_t173` and `pattern_union_t123` for otherwise byte-identical
/// SQL. This is a harness gap (`normalize()` itself), not a production bug —
/// documented here as a candidate follow-up (widen `normalize()`'s regex to
/// also catch `_t\d+` suffixes) rather than fixed in this test-only slice
/// (widening it would touch every existing golden's normalization, which is
/// its own reviewed slice). Locks the stable invariants: the CTE prefix, and
/// that the outer SELECT reads whatever name the CTE was given.
#[tokio::test]
async fn standard_path_unlabeled_pattern_union_name_is_unstable() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH p=()-[]->() RETURN p LIMIT 10";
    let name_re = regex::Regex::new(r"pattern_union_(t\d+)").unwrap();

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, cypher, dialect).await;

        let caps = name_re
            .captures(&sql)
            .unwrap_or_else(|| panic!("{dialect:?}: expected a `pattern_union_t<n>` CTE:\n{sql}"));
        let cte_name = format!("pattern_union_{}", &caps[1]);

        // The outer query must read the SAME (whatever-numbered) CTE name —
        // this is the actual invariant a refactor must preserve, even though
        // the exact number is unlocked.
        assert!(
            sql.matches(&cte_name).count() >= 2,
            "{dialect:?}: expected the CTE name `{cte_name}` to appear at \
             least twice (definition + outer FROM):\n{sql}"
        );
    }
}

// ---------------------------------------------------------------------------
// #514: mixed UNION / UNION ALL chains must error, not silently coerce to the
// first arm's type.
// ---------------------------------------------------------------------------

/// A chain mixing UNION and UNION ALL previously silently applied the FIRST
/// clause's type to the whole chain (`... UNION ... UNION ALL ...` ran as if
/// every arm were UNION). Neo4j rejects this outright ("Invalid combination
/// of UNION and UNION ALL"); we must too, for every ordering of the mix.
#[tokio::test]
async fn mixed_union_and_union_all_errors_cleanly() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    for cypher in [
        // UNION ... UNION ALL
        "MATCH (u:User) RETURN u.user_id AS id LIMIT 5 \
         UNION MATCH (u2:User) RETURN u2.user_id AS id LIMIT 5 \
         UNION ALL MATCH (u3:User) RETURN u3.user_id AS id LIMIT 5",
        // UNION ALL ... UNION
        "MATCH (u:User) RETURN u.user_id AS id LIMIT 5 \
         UNION ALL MATCH (u2:User) RETURN u2.user_id AS id LIMIT 5 \
         UNION MATCH (u3:User) RETURN u3.user_id AS id LIMIT 5",
    ] {
        let result = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
        match result {
            Err(msg) => assert!(
                msg.contains("MixedUnionTypes") || msg.contains("Invalid combination"),
                "[{cypher}] must fail with a MixedUnionTypes error, got: {msg}"
            ),
            Ok(sql) => panic!(
                "[{cypher}] a UNION chain mixing UNION and UNION ALL must error \
                 cleanly instead of silently coercing to the first arm's type; \
                 rendered:\n{sql}"
            ),
        }
    }
}

/// A uniform chain (all UNION, or all UNION ALL — including chains of 3+
/// arms) must still render successfully; only a genuine mix is rejected.
#[tokio::test]
async fn uniform_union_chains_still_render() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    for cypher in [
        "MATCH (u:User) RETURN u.user_id AS id LIMIT 5 \
         UNION MATCH (u2:User) RETURN u2.user_id AS id LIMIT 5 \
         UNION MATCH (u3:User) RETURN u3.user_id AS id LIMIT 5",
        "MATCH (u:User) RETURN u.user_id AS id LIMIT 5 \
         UNION ALL MATCH (u2:User) RETURN u2.user_id AS id LIMIT 5 \
         UNION ALL MATCH (u3:User) RETURN u3.user_id AS id LIMIT 5",
    ] {
        let result = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
        assert!(
            result.is_ok(),
            "[{cypher}] a uniform UNION chain must still render: {:?}",
            result.err()
        );
    }
}

// ---------------------------------------------------------------------------
// #515: UNION arms are combined positionally with no column-name check.
// Neo4j requires identical column names (as a set) across every arm.
// ---------------------------------------------------------------------------

/// Arms with the same column set but declared in a different order must
/// error rather than silently misaligning values under the wrong column
/// (live-verified pre-fix: a post title landed under column `a`, despite the
/// second arm aliasing it `AS b`).
#[tokio::test]
async fn union_reordered_column_names_errors_cleanly() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (u:User) RETURN u.user_id AS a, u.name AS b \
                  UNION MATCH (p:Post) RETURN p.title AS b, p.post_id AS a";
    let result = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
    match result {
        Err(msg) => assert!(
            msg.contains("UnionColumnMismatch") || msg.contains("same column names"),
            "must fail with a UnionColumnMismatch error, got: {msg}"
        ),
        Ok(sql) => panic!(
            "reordered column names across UNION arms must error cleanly \
             instead of silently misaligning by position; rendered:\n{sql}"
        ),
    }
}

/// Arms with genuinely different column names must error rather than
/// silently NULL-padding to the union of both column sets.
#[tokio::test]
async fn union_mismatched_column_names_errors_cleanly() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (u:User) RETURN u.user_id AS id \
                  UNION MATCH (p:Post) RETURN p.title AS title";
    let result = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
    match result {
        Err(msg) => assert!(
            msg.contains("UnionColumnMismatch") || msg.contains("same column names"),
            "must fail with a UnionColumnMismatch error, got: {msg}"
        ),
        Ok(sql) => panic!(
            "mismatched column names across UNION arms must error cleanly \
             instead of silently NULL-padding; rendered:\n{sql}"
        ),
    }
}

/// Arms with identical column names in the SAME order must still render
/// successfully — the common, correct case.
#[tokio::test]
async fn union_matching_column_names_still_renders() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (u:User) RETURN u.name AS x UNION MATCH (p:Post) RETURN p.title AS x";
    let result = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
    assert!(
        result.is_ok(),
        "matching column names in the same order must still render: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// #512 / #513: non-aggregated Cypher UNION per-arm ORDER BY / SKIP / LIMIT.
//
// #487 fixed per-arm modifier binding + Databricks parenthesization for
// AGGREGATED union arms only (routing them through render_cypher_union_arm),
// deliberately leaving the non-aggregated path on the OLD render_union_
// branch_sql, which hoists the FIRST arm's ORDER BY/SKIP/LIMIT onto the whole
// union (#512) and emits a bare, unparenthesized per-arm ORDER BY/LIMIT
// before `UNION ALL` on Databricks — a Spark parse error mid-chain, and (as
// the last arm) a silent whole-union LIMIT (#513). Both are fixed by routing
// EVERY Cypher union arm through render_cypher_union_arm, unifying the two
// fixes instead of patching render_union_branch_sql a second time (see
// src/sql_generator/emitters/clickhouse/to_sql_query.rs, `cypher_union_per_arm`).
// ---------------------------------------------------------------------------

/// The first arm's ORDER BY / SKIP / LIMIT must bind WITHIN that arm, not to
/// the union as a whole (live-verified against social_benchmark: 2 sorted
/// users (skip 2, limit 2) + all 5 posts = 7 rows, not 2).
#[tokio::test]
async fn union_first_arm_order_by_skip_limit_binds_to_arm_only() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (u:User) RETURN u.user_id AS id ORDER BY id SKIP 2 LIMIT 2 \
                  UNION ALL MATCH (p:Post) RETURN p.post_id AS id";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // The first arm's own ORDER BY/LIMIT/SKIP must appear INSIDE a subselect
    // that wraps only that arm — never as a bare trailing modifier on the
    // whole `... UNION ALL ...` chain (which would bind to the combined
    // result instead of just the first arm).
    assert!(
        sql.contains("ORDER BY id ASC\nLIMIT 2, 2\nUNION ALL"),
        "the first arm's ORDER BY/SKIP/LIMIT must be immediately followed by \
         UNION ALL (i.e. bound inside the arm's own subselect), not trail the \
         whole union:\n{sql}"
    );
    assert!(
        !sql.trim_end().ends_with("LIMIT 2, 2"),
        "the union as a whole must not end with the first arm's LIMIT/SKIP \
         (that would mean it was hoisted to the combined result):\n{sql}"
    );
}

/// Every Cypher UNION arm carrying its own ORDER BY / SKIP / LIMIT must be
/// parenthesized on Databricks (bare per-arm ORDER BY/LIMIT is a Spark parse
/// error mid-chain and silently binds to the whole union as the last arm),
/// matching the treatment `cte_extraction.rs` already applies to
/// pattern_union branches and #487 applies to aggregated Cypher union arms.
#[tokio::test]
async fn union_per_arm_modifiers_parenthesized_for_databricks() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (u:User) RETURN u.user_id AS id ORDER BY id SKIP 2 LIMIT 2 \
                  UNION ALL MATCH (p:Post) RETURN p.post_id AS id";
    let sql = render(&schema, cypher, SqlDialect::Databricks).await;

    assert!(
        sql.trim_start().starts_with('('),
        "the modifier-carrying first arm must be parenthesized before UNION \
         ALL on Databricks:\n{sql}"
    );
    assert!(
        sql.contains(")\nUNION ALL"),
        "the parenthesized arm must close immediately before UNION ALL:\n{sql}"
    );

    // ClickHouse must stay byte-unaffected by the Databricks-only wrap.
    let ch_sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
    assert!(
        !ch_sql.trim_start().starts_with('('),
        "ClickHouse output must NOT be parenthesized (Databricks-only \
         treatment):\n{ch_sql}"
    );
}

/// A per-arm LIMIT on EACH arm of a non-aggregated union must apply
/// independently to every arm, not just the first (#512's exact repro
/// shape, doubled).
#[tokio::test]
async fn union_every_arm_limit_applies_independently() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (u:User) RETURN u.user_id AS id LIMIT 2 \
                  UNION ALL MATCH (p:Post) RETURN p.post_id AS id LIMIT 3";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // Both arms' LIMITs must survive in the rendered SQL — neither hoisted
    // away nor collapsed into a single outer LIMIT.
    let limit_count = sql.matches("LIMIT").count();
    assert_eq!(
        limit_count, 2,
        "expected exactly 2 LIMIT clauses (one bound to each arm), got {limit_count}:\n{sql}"
    );
}

/// #517: a WITH clause inside one arm of a Cypher UNION must not leak its
/// CTE substitution into a sibling arm that never had a WITH clause at all.
/// Root cause was THREE independent render-phase functions
/// (`replace_with_clause_with_cte_reference_v2`, `CteColumnResolver`'s
/// `LogicalPlan::Union` handling, and `update_graph_joins_cte_refs`) that
/// each recursed into every UNION branch unconditionally with the SAME
/// global CTE-reference map/alias-matching logic, with no notion of which
/// branch actually contained the WITH clause. Fixed all three to scope the
/// CTE lookahead to each Cypher-UNION branch's own WITH-exported aliases.
///
/// The two arms here use DIFFERENT variable names (`u` vs `v`) — the common
/// real-world shape (and the shape that's now fully correct end-to-end,
/// live-verified against ClickHouse: 8 users x c=0 from the second arm, 8 x
/// c=1 from the first, 16 rows total).
#[tokio::test]
async fn with_clause_in_union_arm_does_not_leak_into_sibling_arm_517() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (u:User) WITH u, count(*) as c WHERE c > 0 RETURN u.user_id AS uid, c \
         UNION MATCH (v:User) RETURN v.user_id AS uid, 0 as c",
        SqlDialect::ClickHouse,
    )
    .await;

    // The second arm must scan its own table directly — not join or
    // self-alias the first arm's CTE.
    assert!(
        sql.contains("FROM social.users_bench AS v"),
        "second arm must scan users_bench directly as 'v', not reference \
         the first arm's WITH-clause CTE: {sql}"
    );
    assert!(
        !sql.contains("with_c_u_cte_0 AS v") && !sql.contains("AS v ON 1"),
        "second arm must not join or duplicate-alias the first arm's CTE: {sql}"
    );
    // The second arm's own property access must resolve locally (to its own
    // scan alias 'v'), not to any CTE-encoded column name.
    assert!(
        sql.contains("v.user_id AS \"uid\""),
        "second arm's uid must resolve to a plain v.user_id, not a \
         CTE-encoded column: {sql}"
    );
}

/// #593 (was the documented #517 residual gap, now fixed for THIS shape — a
/// `WITH` arm + a plain arm that reuse the EXACT SAME Cypher variable name
/// (`u` in both, independent scopes)): the second (plain) arm's SELECT list
/// previously still leaked —
/// `VariableScope`'s `cte_variables` map (built once, globally, via
/// `build_chained_with_match_cte_plan`'s `final_scope`, with no per-Union-
/// branch scoping) resolved the second arm's `u.user_id` against the FIRST
/// arm's CTE property mapping, emitting `c_u.p1_u_user_id` against a table
/// (`social.users_bench AS u`) that has no such column — a LOUD ClickHouse
/// "unknown identifier" (Code 47).
///
/// #593 threads arm-scoped variable resolution through the Cypher-UNION render
/// (`VariableScope::scoped_to_referenced_ctes` — each arm keeps only the
/// WITH-CTE variables whose CTE its OWN FROM/JOINs reference), applied in the
/// per-arm branch renders, in `rewrite_render_plan_with_scope`'s branch
/// recursion, and in the outer `rewrite_bare_variables_in_plan` /
/// `fix_orphan_table_aliases` passes. Live-verified against ClickHouse: 8
/// users x c=1 (first arm) + 8 x c=0 (second arm) = 16 rows.
///
/// NOT covered here (a SEPARATE, still-open pre-existing bug — a CTE-name
/// collision, distinct from this alias-scope leak): when BOTH arms carry a
/// same-shape `WITH` on the same alias, both collapse onto one shared CTE
/// name and one arm's filter is silently dropped. That shape is byte-identical
/// on main and this branch, so it is out of scope for #593.
#[tokio::test]
async fn with_clause_in_union_arm_same_alias_reused_fully_fixed_593() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (u:User) WITH u, count(*) as c WHERE c > 0 RETURN u.user_id, c \
         UNION MATCH (u:User) RETURN u.user_id, 0 as c",
        SqlDialect::ClickHouse,
    )
    .await;

    // The second arm's FROM must be a direct table scan, not a join to (or
    // duplicate alias of) the first arm's CTE.
    assert!(
        sql.contains("FROM social.users_bench AS u"),
        "second arm must scan users_bench directly (same alias name): {sql}"
    );
    assert!(
        !sql.to_uppercase().contains("JOIN WITH_C_U_CTE"),
        "second arm must never JOIN the first arm's CTE: {sql}"
    );
    // #593: the second (plain) arm's own `u.user_id` must resolve to its OWN
    // scan column `u.user_id`, NOT the first arm's CTE column
    // `c_u.p1_u_user_id`. The plain arm is identified by its `0 AS "c"`
    // literal (the first/WITH arm legitimately DOES read c_u.p1_u_user_id).
    assert!(
        sql.contains("u.user_id AS \"u.user_id\", \n      0 AS \"c\""),
        "second (plain) arm's u.user_id must resolve to its own scan column: {sql}"
    );
    assert!(
        !sql.contains("c_u.p1_u_user_id AS \"u.user_id\", \n      0 AS \"c\""),
        "second (plain) arm must NOT resolve u.user_id through the first arm's \
         CTE (the #593 leak): {sql}"
    );
}

/// #593 (the reported repro): a Cypher `UNION` whose arms reuse the same alias
/// name, where ONE arm has a `WITH ... count(*)` barrier and the OTHER is a
/// plain MATCH. The plain arm was silently corrupted at render time: its FROM
/// was hijacked to the WITH arm's CTE and its projection rewritten to the
/// CTE's column — a SILENT WRONG RESULT (worst class). Both arms must render
/// as fully independent queries. Live-verified against ClickHouse: the whole
/// union returns the 8 distinct users (each arm alone returns all 8).
#[tokio::test]
async fn union_same_alias_plain_arm_not_hijacked_by_with_arm_cte_593() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User) RETURN a.name AS name \
         UNION \
         MATCH (a:User) WITH a, count(*) AS c WHERE c > 0 RETURN a.name AS name",
        SqlDialect::ClickHouse,
    )
    .await;

    // Arm 1 (plain) must scan the base table and project the schema-mapped
    // column — NOT the sibling WITH arm's CTE / CTE column.
    assert!(
        sql.contains("a.full_name AS \"name\"\nFROM social.users_bench AS a"),
        "arm 1 must be `SELECT a.full_name AS \"name\" FROM social.users_bench \
         AS a` (its FROM must not be hijacked to with_a_c_cte_0, nor its \
         projection to the CTE column p1_a_name): {sql}"
    );
    // Arm 2 (WITH) legitimately reads its own CTE.
    assert!(
        sql.contains("a_c.p1_a_name AS \"name\"\nFROM with_a_c_cte_0 AS a_c"),
        "arm 2 (the WITH arm) must read its own CTE with_a_c_cte_0: {sql}"
    );
    // The plain arm must never be joined/cross-joined to the WITH arm's CTE.
    assert!(
        !sql.contains("FROM social.users_bench AS a\nCROSS JOIN with_a_c_cte_0")
            && !sql.contains("FROM social.users_bench AS a\nINNER JOIN with_a_c_cte_0"),
        "arm 1 must not be cross/inner-joined to the sibling arm's CTE: {sql}"
    );
}

/// #593: a THREE-arm Cypher UNION (plain arm, WITH-barrier arm, plain arm)
/// where all arms reuse alias `a`. Locks in that BOTH plain arms (the first —
/// the base_plan/outer arm — and the third — a `union.input` arm) keep their
/// own base-table FROM and schema-mapped projection, and are not hijacked to
/// the middle WITH arm's CTE. (The middle arm's own `EXISTS { }` correlation
/// after the WITH barrier is a SEPARATE, pre-existing mechanism not addressed
/// here — see the report — so this test asserts only the per-arm FROM /
/// projection that #593 fixes.)
#[tokio::test]
async fn union_three_arms_same_alias_per_arm_from_and_projection_593() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User) RETURN a.name AS name \
         UNION \
         MATCH (a:User) WITH a, count(*) AS c WHERE c > 0 RETURN a.name AS name \
         UNION \
         MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS]->(z) } RETURN a.name AS name",
        SqlDialect::ClickHouse,
    )
    .await;

    // Both plain arms must project a.full_name from the base table.
    let plain_arm = "a.full_name AS \"name\"\nFROM social.users_bench AS a";
    assert!(
        sql.matches(plain_arm).count() >= 2,
        "both plain arms (arm 1 and arm 3) must render `a.full_name AS \"name\" \
         FROM social.users_bench AS a` — neither hijacked to the WITH arm's \
         CTE: {sql}"
    );
    // The WITH arm reads its own CTE.
    assert!(
        sql.contains("a_c.p1_a_name AS \"name\"\nFROM with_a_c_cte_0 AS a_c"),
        "the WITH arm must read its own CTE with_a_c_cte_0 as a_c: {sql}"
    );
    // No plain arm should be given the CTE column in its projection.
    assert!(
        !sql.contains("a.p1_a_name"),
        "no plain arm may project the CTE column p1_a_name against its own \
         base-table alias `a`: {sql}"
    );
}

/// #518: a DIRECTED same-type multi-hop pattern (`(a)-[:FOLLOWS]->(b)
/// -[:FOLLOWS]->(c)`) must get the same relationship-uniqueness guard
/// (`r1 <> r2`) that #492 already gives the UNDIRECTED case — Neo4j forbids
/// binding the same relationship twice in one MATCH, regardless of
/// direction. Root cause: `GraphJoinInference` computed the guard correctly
/// (Phase 4, `cross_branch::generate_relationship_uniqueness_constraints`)
/// and attached it to `GraphJoins.correlation_predicates`, but the SHARED
/// `GraphJoins::rebuild_or_clone` helper — used by most optimizer passes
/// whenever a `GraphJoins`' input changes (projection push-down, filter
/// push-down, view optimizer, cartesian join extraction, ...) —
/// unconditionally reconstructed it with `correlation_predicates: vec![]`,
/// silently dropping the guard for every query that touched any of those
/// passes. Live-verified against ClickHouse (19 rows, matching raw-SQL
/// ground truth) and via a synthetic self-loop (the guard correctly
/// excludes a relationship compared against itself: 0 rows).
#[tokio::test]
async fn directed_same_type_multihop_gets_relationship_uniqueness_guard_518() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) \
         RETURN a.user_id, b.user_id, c.user_id",
        SqlDialect::ClickHouse,
    )
    .await;

    assert!(
        sql.to_uppercase().contains("WHERE"),
        "directed same-type 2-hop must emit a WHERE clause with the \
         relationship-uniqueness guard: {sql}"
    );
    assert!(
        sql.contains("follower_id <> ")
            && sql.contains("followed_id <> ")
            && sql.contains(".follower_id")
            && sql.contains(".followed_id"),
        "expected an OR-of-column-inequality uniqueness guard comparing the \
         two FOLLOWS hop aliases' follower_id/followed_id columns: {sql}"
    );
}

/// #518 (correctness guards found via corpus_sweep + live verification
/// while fixing the above): the uniqueness-constraint generator must NOT
/// fire in two cases where it previously would have produced actively
/// WRONG SQL once correlation_predicates started reaching the WHERE clause:
///   1. Two edges of UNRELATED relationship types (can never be the same
///      physical edge) — comparing their id columns is nonsensical and, for
///      some schemas, silently drops legitimate rows.
///   2. A relationship introduced by OPTIONAL MATCH — the guard must not
///      land in a plain WHERE clause ANDed against a LEFT JOIN, which would
///      turn "no optional match" (NULL) into "row excluded", breaking
///      OPTIONAL MATCH semantics.
#[tokio::test]
async fn relationship_uniqueness_guard_skips_unrelated_types_and_optional_518() {
    let schema = load_schema(SchemaId::Standard.yaml_path());

    // Case 1: unrelated relationship types (FOLLOWS vs AUTHORED) — no guard.
    let sql = render(
        &schema,
        "MATCH (u:User)-[:FOLLOWS]->(f:User)-[:AUTHORED]->(p:Post) \
         RETURN u.user_id, f.user_id, p.post_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        !sql.to_uppercase().contains("WHERE") || !sql.contains("<>"),
        "unrelated relationship types (FOLLOWS, AUTHORED) must not get a \
         nonsensical cross-type uniqueness guard: {sql}"
    );

    // Case 2: OPTIONAL MATCH on the second hop — no guard landing in a
    // plain WHERE (would break OPTIONAL semantics via NULL comparisons).
    let sql = render(
        &schema,
        "MATCH (a:User)-[:FOLLOWS]->(b:User) \
         OPTIONAL MATCH (b)-[:FOLLOWS]->(c:User) \
         RETURN a.user_id, b.user_id, c.user_id",
        SqlDialect::ClickHouse,
    )
    .await;
    assert!(
        !sql.contains("<>"),
        "a relationship introduced by OPTIONAL MATCH must not get a \
         uniqueness guard in the outer WHERE clause (breaks OPTIONAL MATCH \
         semantics for non-matching rows): {sql}"
    );
}

/// #511: a hardcoded `LIMIT 1000` "safety cap" on every `pattern_union` CTE
/// branch (unlabeled/multi-type relationship scans, e.g.
/// `MATCH ()-[r]->() RETURN ...`) silently truncated results — with no
/// error, no warning, and no way for the caller to detect it — once a
/// branch's underlying table exceeded 1000 matching rows, even when the
/// user's query had no LIMIT of its own. No design rationale for the value
/// was ever documented (git blame traces it to the original feature commit
/// with no comment). Removed entirely: any limiting the user wants is
/// expressed via an explicit Cypher `LIMIT`, applied normally at the outer
/// query level like any other pattern. Live-verified: `MATCH ()-[r]->()
/// RETURN count(*)` now returns the true total (23, matching a raw-SQL
/// cross-check), not an artificially capped value.
#[tokio::test]
async fn pattern_union_no_hardcoded_limit_cap_511() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let sql = render(
        &schema,
        "MATCH ()-[r]->() RETURN count(*) AS c",
        SqlDialect::ClickHouse,
    )
    .await;

    assert!(
        sql.contains("pattern_union_r"),
        "expected this unlabeled relationship scan to route through a \
         pattern_union CTE: {sql}"
    );
    assert!(
        !sql.to_uppercase().contains("LIMIT 1000"),
        "pattern_union branches must not carry a hardcoded LIMIT 1000 \
         safety cap that silently truncates results: {sql}"
    );
}

/// #542: `get_table_alias_if_single_table_condition` (`filter_tagging.rs`,
/// used to decide whether an OR-branch / condition belongs to a single table
/// so it can be tagged for pushdown) fell through to `_ => None` for
/// `LogicalExpr::List` — an IN-list's elements were invisible to it. For a
/// denormalized cross-alias correlation like `srcip1.ip IN [srcip2.ip]`, the
/// enclosing `OperatorApplicationExp(In, [srcip1.ip, List([srcip2.ip])])` arm
/// only ever saw `srcip1.ip`'s alias (the list operand silently contributed
/// nothing), so the WHOLE condition was misclassified as single-table for
/// `srcip1` — losing track that `srcip2` is a second, real correlated alias.
/// Downstream this leaked `srcip2` as a raw, never-bound SQL identifier
/// (`toString(srcip2."id.orig_h")`) instead of resolving it to its actual
/// joined table alias (`t2`).
///
/// Fix: added a `LogicalExpr::List` arm (mirroring the existing
/// `ScalarFnCall`/`AggregateFnCall` "collect every operand's alias, require
/// they all agree" pattern) so a cross-table list element now surfaces its
/// alias, correctly triggering the existing "operands disagree -> None (not
/// single-table)" rule. This only ADDS visibility into list contents — a
/// same-table or literal-only list (no aliases inside) still resolves
/// exactly as before, so all 8 other call sites of this shared function are
/// unaffected for their existing (non-list, or single-table list) usages.
///
/// Live-verified (2026-07-10, `schemas/dev/zeek_merged_test.yaml`): the
/// #542-reported repro now renders the correlation bound to `conn_log`'s
/// real JOIN alias (e.g. `t2."id.orig_h"`) instead of the dangling
/// `srcip2."id.orig_h"`.
#[tokio::test]
async fn in_list_cross_alias_correlation_resolves_bound_alias_542() {
    let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
    let sql = render(
        &schema,
        "MATCH (srcip1:IP)-[:REQUESTED]->(d:Domain), \
         (srcip2:IP)-[:ACCESSED]->(destip:IP) \
         WHERE srcip1.ip IN [srcip2.ip] \
         RETURN srcip1.ip, d.name, destip.ip",
        SqlDialect::ClickHouse,
    )
    .await;

    // Check the WHERE clause specifically (not the outer SELECT list, whose
    // output column aliases legitimately spell the Cypher alias as a quoted
    // string, e.g. `AS "destip.ip"` — that's a display label, not a SQL
    // identifier reference). The WHERE clause is where the bug's unbound
    // `srcip2."id.orig_h"` reference actually appeared.
    let where_clause = sql
        .rsplit("WHERE")
        .next()
        .expect("query must have a WHERE clause");
    assert!(
        !where_clause.contains("srcip1.")
            && !where_clause.contains("srcip2.")
            && !where_clause.contains("destip."),
        "#542: no raw Cypher alias should leak into the WHERE clause as an \
         unbound SQL identifier — every reference must resolve to its bound \
         JOIN alias (e.g. `t1`/`t2`):\n{sql}"
    );
    // The JOIN alias generator uses a process-wide counter (not test-local),
    // so the exact numeric suffix (`t2`, `t537`, ...) isn't stable across a
    // full-suite run — extract whatever alias `conn_log` (srcip2's table) was
    // actually assigned and confirm the WHERE clause references THAT alias.
    let conn_log_alias = sql
        .split("zeek.conn_log AS ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or_else(|| panic!("expected a `JOIN zeek.conn_log AS <alias>`:\n{sql}"));
    assert!(
        where_clause.contains(&format!("{conn_log_alias}.")),
        "#542: expected the correlated `srcip2` alias to resolve to its real \
         bound JOIN alias `{conn_log_alias}` in the WHERE clause, not vanish \
         or stay unbound:\n{sql}"
    );
}

/// Regression tests for the #484/#490/#495/#476/#483 hand-rolled-walker-drift
/// family (silent wrong results / unbound aliases from expression walkers
/// missing match arms).
mod walker_drift_family_484_490_495_476_483 {
    use super::*;

    /// #484: `id()`/`elementId()` in GROUP BY/ORDER BY position used to fall
    /// through to the generic function-registry placeholder (`id` ->
    /// `toInt64(0)`), which is harmless in SELECT (Bolt/result-transformer
    /// compute the real id from element_id there) but SILENT WRONG RESULTS in
    /// GROUP BY (every row hashes to the same constant key, collapsing all
    /// groups into one) and a no-op in ORDER BY. Both `group_by_builder.rs`
    /// (`resolve_id_function_for_group_order`) and `extract_order_by` in
    /// `plan_builder_utils.rs` now route `id()` through the same
    /// pattern_union-aware resolution the SELECT path already used. Live-
    /// verified (2026-07-10, local `social` fixture): standard schema
    /// GROUP BY id(o) over `MATCH (n)-[r]->(o)` groups correctly (1->10,
    /// 3->5, 2->4, 4->2, 5->1, matching hand-computed ground truth); FK-edge
    /// schema (`schemas/test/fk_edge.yaml`) GROUP BY id(o) returns
    /// 100->3, 101->2, 102->2, 103->1, matching `SELECT customer_id,
    /// count(*) FROM orders_fk GROUP BY customer_id`.
    #[tokio::test]
    async fn group_by_order_by_id_function_pattern_union_484() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let cypher = "MATCH (n)-[r]->(o) RETURN id(o) AS i, count(*)";
        let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
        assert!(
            !sql.contains("GROUP BY toInt64(0)"),
            "id(o) in GROUP BY must not fall back to the toInt64(0) \
             placeholder (every group collapses into one row): {sql}"
        );
        assert!(
            sql.contains("GROUP BY r.end_id"),
            "pattern_union endpoint id(o) must GROUP BY the CTE's \
             label-agnostic end_id column: {sql}"
        );

        let order_cypher = "MATCH (n)-[r]->(o) RETURN id(o) AS i ORDER BY id(o)";
        let order_sql = render(&schema, order_cypher, SqlDialect::ClickHouse).await;
        assert!(
            !order_sql.contains("ORDER BY toInt64(0)"),
            "id(o) in ORDER BY must not fall back to the toInt64(0) \
             placeholder (a no-op sort): {order_sql}"
        );
        assert!(
            order_sql.contains("ORDER BY r.end_id"),
            "pattern_union endpoint id(o) must ORDER BY the CTE's \
             label-agnostic end_id column: {order_sql}"
        );
    }

    /// #484: the plain (non-pattern_union) single-label path must also
    /// resolve id() to the real id column in GROUP BY, not the placeholder.
    #[tokio::test]
    async fn group_by_id_function_plain_single_label_484() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let cypher = "MATCH (u:User) RETURN id(u) AS i, count(*)";
        let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
        assert!(
            sql.contains("GROUP BY u.user_id"),
            "plain single-label id(u) must GROUP BY the real id column, not \
             a placeholder: {sql}"
        );
    }

    /// #484 adversarial-review finding (BLOCKING): a BARE multi-label alias
    /// with NO GraphRel connection (`MATCH (n) RETURN id(n), count(*)`) is a
    /// natural adjacent shape to the pattern_union-endpoint and single-label
    /// cases above, but neither of those detectors fires for it — a bare
    /// `MATCH (n)` scan surfaces as a top-level `LogicalPlan::Union` of full
    /// per-label branches (see `type_inference.rs`'s
    /// `generate_union_for_untyped_nodes`), so `n` only exists INSIDE each
    /// UNION branch subquery, not at the outer `__union`-wrapping scope where
    /// GROUP BY/ORDER BY apply. The initial #484 fix's "plain path" naively
    /// called `find_id_column_for_alias` regardless of this shape, which
    /// returns the FIRST branch's column (`post_id`) and emitted `GROUP BY
    /// n.post_id` in the OUTER scope — turning the PRE-existing
    /// wrong-but-non-fatal `GROUP BY toInt64(0)` bug (all rows silently
    /// collapse into one group) into a HARD ClickHouse Code 47
    /// UNKNOWN_IDENTIFIER failure. A `tuple`/`coalesce`-style discriminator
    /// (à la #467's `count(DISTINCT n)`) would be the semantically complete
    /// fix, but the `alias.<col>`-quoted-column convention that resolution
    /// needs is wired specifically into the aggregate-wrapper render path,
    /// not something a bare GROUP BY/ORDER BY key can reuse without
    /// materially more plumbing (`PropertyRequirementsAnalyzer` timing) than
    /// fits this fix — see `is_bare_multilabel_alias` in
    /// `group_by_builder.rs`. Deferred as a follow-up; for now this locks
    /// the safe minimum: fall through to the PRE-existing (still wrong, but
    /// executable) placeholder path rather than hard-failing. Live-verified
    /// (2026-07-10): pre-fix-regression and post-this-fix both return one
    /// collapsed row (`13`, since all groups still hash to `id(n)`'s
    /// placeholder) — not a hard error either way.
    #[tokio::test]
    async fn group_by_bare_multilabel_id_function_falls_back_not_hard_fail_484() {
        let schema = load_schema(SchemaId::Standard.yaml_path());

        let group_by_cypher = "MATCH (n) RETURN id(n), count(*)";
        let group_by_sql = render(&schema, group_by_cypher, SqlDialect::ClickHouse).await;
        assert!(
            !group_by_sql.contains("GROUP BY n.post_id")
                && !group_by_sql.contains("GROUP BY n.user_id"),
            "#484 BLOCKING: a bare multi-label alias's GROUP BY must not \
             reference a table alias (n) that only exists inside the inner \
             UNION branch subquery, not the outer __union scope — this is a \
             hard ClickHouse UNKNOWN_IDENTIFIER failure, not just wrong \
             results:\n{group_by_sql}"
        );

        let order_by_cypher = "MATCH (n) RETURN id(n), count(*) ORDER BY id(n)";
        let order_by_sql = render(&schema, order_by_cypher, SqlDialect::ClickHouse).await;
        assert!(
            !order_by_sql.contains("GROUP BY n.post_id")
                && !order_by_sql.contains("GROUP BY n.user_id"),
            "#484 BLOCKING: same bare multi-label alias, combined with \
             ORDER BY — the GROUP BY clause must not reference the \
             inner-subquery-only alias either:\n{order_by_sql}"
        );
    }

    /// #484 adversarial-review follow-up (BLOCKING): the bare-multilabel guard
    /// above only fired when the alias had NO GraphRel connection at all —
    /// but a multi-label alias reached through a DIRECTED GraphRel chain
    /// (e.g. `(folder:Folder)-[:CONTAINS]->(item)` with `item` unlabeled)
    /// renders via the exact same raw per-label `UNION ALL` shape
    /// (`generate_union_for_untyped_nodes` clones the WHOLE `GraphRel`
    /// subtree per label, not just the `GraphNode`), so `item` is STILL only
    /// addressable inside each UNION branch, not the outer `__union` scope
    /// — yet the old guard's "has a GraphRel connection" check treated any
    /// GraphRel connection as safe, regardless of direction, and let this
    /// shape fall through to the unsafe "plain path"
    /// (`find_id_column_for_alias`), emitting `GROUP BY item.fs_id` /
    /// `ORDER BY item.fs_id` — a hard ClickHouse Code 47 UNKNOWN_IDENTIFIER
    /// failure. The fix (`renders_via_raw_label_union` in
    /// `group_by_builder.rs`) replaces the "no GraphRel connection" proxy
    /// with the real discriminator already established by #467/#483 in
    /// `projection_tagging.rs`'s `count(DISTINCT alias)` rewrite:
    /// `graph_rel_connection_role` (`Some` only for an UNDIRECTED endpoint,
    /// which collapses into a single-alias `multi_type_vlp_joins`/
    /// `bidirectional_union` CTE — safe). A DIRECTED connection returns
    /// `None` from that check, correctly falling into the same raw-union
    /// bucket as the bare case. The ORDER BY side of this same shape also
    /// needed a companion fix in `FilterTagging::apply_property_mapping`
    /// (`filter_tagging.rs`) and `extract_order_by_columns_for_union`
    /// (`to_sql_query.rs`): ORDER BY's `id()` resolution runs much earlier
    /// (during FilterTagging, which eagerly resolves `id(item)` to a
    /// concrete-but-wrong-scope property access, unlike RETURN-list items
    /// which preserve `id()` for the render-side guard to see).
    #[tokio::test]
    async fn group_by_order_by_directed_multilabel_raw_union_not_hard_fail_484() {
        let schema = load_schema("examples/data_security/data_security.yaml");

        let group_by_cypher = "MATCH (folder:Folder)-[:CONTAINS]->(item) RETURN id(item), count(*)";
        let group_by_sql = render(&schema, group_by_cypher, SqlDialect::ClickHouse).await;
        assert!(
            !group_by_sql.contains("GROUP BY item."),
            "#484 BLOCKING: a multi-label alias reached through a DIRECTED \
             GraphRel must not GROUP BY a table alias (item) that only \
             exists inside the inner UNION branch subquery, not the outer \
             __union scope:\n{group_by_sql}"
        );
        assert!(
            group_by_sql.contains("GROUP BY `id(item)`"),
            "expected the safe fallback: GROUP BY the outer SELECT-list \
             alias, not an inner-scope-only column reference:\n{group_by_sql}"
        );

        let order_by_cypher =
            "MATCH (folder:Folder)-[:CONTAINS]->(item) RETURN id(item), count(*) ORDER BY id(item)";
        let order_by_sql = render(&schema, order_by_cypher, SqlDialect::ClickHouse).await;
        assert!(
            !order_by_sql.contains("GROUP BY item.") && !order_by_sql.contains("ORDER BY item."),
            "#484 BLOCKING: same directed multi-label alias, combined with \
             ORDER BY — neither GROUP BY nor ORDER BY may reference the \
             inner-subquery-only alias:\n{order_by_sql}"
        );
        assert!(
            !order_by_sql.contains("__order_col"),
            "ORDER BY id() on a raw-union multi-label alias must not push a \
             redundant duplicate SELECT item — that collides with the \
             existing id(item) SELECT alias in build_aliased_group_by's \
             expression map and corrupts GROUP BY into referencing a \
             column that doesn't exist in the __union scope:\n{order_by_sql}"
        );
    }

    /// #484 round 3 (adversarial-review follow-up, BLOCKING): round 2's fix
    /// above replaced the "no GraphRel connection" proxy with
    /// `graph_rel_connection_role` (UNDIRECTED-ness), on the assumption that
    /// every undirected multi-label GraphRel endpoint collapses into a single
    /// `multi_type_vlp_joins`/`bidirectional_union` CTE (safe). That
    /// assumption is FALSE for a polymorphic/junction-table schema whose
    /// relationship dispatches through a discriminator column (single
    /// relationship type, ambiguous NODE label): the whole `GraphRel` still
    /// gets cloned once per candidate label into a raw `UNION ALL`, same as
    /// the directed case above — `variable_length` is `None` on every clone,
    /// there is no VLP CTE to collapse into, and `item`/`target` are only
    /// addressable INSIDE each union branch. Both repros below are
    /// live-verified (2026-07-10) to hard-fail with ClickHouse Code 47
    /// UNKNOWN_IDENTIFIER on pre-round-3 code (`GROUP BY item.fs_id` /
    /// `GROUP BY target.post_id`, referencing a table alias that only exists
    /// inside the `__union` branch subqueries).
    ///
    /// The fix (`graph_rel_vlp_endpoint_role` in `logical_plan/mod.rs`,
    /// replacing `graph_rel_connection_role` at both call sites —
    /// `group_by_builder.rs`'s `renders_via_raw_label_union` and
    /// `filter_tagging.rs`'s `is_unsafe_raw_union_multilabel`) checks the
    /// ACTUAL structural marker of the safe collapse
    /// (`variable_length.is_some()` at the connection — the same condition
    /// `find_id_column_for_alias`'s own VLP fallback branch already uses to
    /// resolve such an alias to `start_id`/`end_id`) instead of a proxy for
    /// it. This is sound in both directions: it still says "unsafe" (raw
    /// union) for the junction-table/polymorphic case here, while continuing
    /// to say "safe" for the genuine VLP shapes locked by the test below
    /// (`group_by_order_by_vlp_multilabel_endpoint_resolves_precisely_484`).
    #[tokio::test]
    async fn group_by_order_by_undirected_multilabel_junction_table_raw_union_not_hard_fail_484() {
        // Junction-table dispatch: `ds_fs_objects.fs_type` discriminates which
        // physical table (and join key) each row belongs to.
        let fk_schema = load_schema("examples/data_security/data_security.yaml");

        let fk_group_by_cypher =
            "MATCH (folder:Folder)-[:CONTAINS]-(item) RETURN id(item), count(*)";
        let fk_group_by_sql = render(&fk_schema, fk_group_by_cypher, SqlDialect::ClickHouse).await;
        assert!(
            !fk_group_by_sql.contains("GROUP BY item."),
            "#484 round 3 BLOCKING: an UNDIRECTED multi-label alias reached \
             through a junction-table-dispatch GraphRel must not GROUP BY a \
             table alias (item) that only exists inside the inner UNION \
             branch subquery, not the outer __union scope:\n{fk_group_by_sql}"
        );
        assert!(
            fk_group_by_sql.contains("GROUP BY `id(item)`"),
            "expected the safe fallback: GROUP BY the outer SELECT-list \
             alias, not an inner-scope-only column reference:\n{fk_group_by_sql}"
        );

        let fk_order_by_cypher = "MATCH (folder:Folder)-[:CONTAINS]-(item) RETURN id(item), \
             count(*) ORDER BY id(item)";
        let fk_order_by_sql = render(&fk_schema, fk_order_by_cypher, SqlDialect::ClickHouse).await;
        assert!(
            !fk_order_by_sql.contains("GROUP BY item.")
                && !fk_order_by_sql.contains("ORDER BY item."),
            "#484 round 3 BLOCKING: same undirected junction-table alias, \
             combined with ORDER BY — neither GROUP BY nor ORDER BY may \
             reference the inner-subquery-only alias:\n{fk_order_by_sql}"
        );
        assert!(
            !fk_order_by_sql.contains("__order_col"),
            "ORDER BY id() here must not push a redundant duplicate SELECT \
             item that corrupts GROUP BY into referencing a column that \
             doesn't exist in the __union scope:\n{fk_order_by_sql}"
        );

        // Polymorphic single-interactions-table dispatch: `interaction_type`/
        // `from_type`/`to_type` discriminate the join per candidate label,
        // same raw-union hazard, different schema axis.
        let poly_schema = load_schema(SchemaId::Polymorphic.yaml_path());

        let poly_group_by_cypher = "MATCH (u:User)-[:LIKES]-(target) RETURN id(target), count(*)";
        let poly_group_by_sql =
            render(&poly_schema, poly_group_by_cypher, SqlDialect::ClickHouse).await;
        assert!(
            !poly_group_by_sql.contains("GROUP BY target."),
            "#484 round 3 BLOCKING: an UNDIRECTED multi-label alias reached \
             through a polymorphic-junction-table GraphRel must not GROUP BY \
             a table alias (target) that only exists inside the inner UNION \
             branch subquery:\n{poly_group_by_sql}"
        );
        assert!(
            poly_group_by_sql.contains("GROUP BY `id(target)`"),
            "expected the safe fallback: GROUP BY the outer SELECT-list \
             alias:\n{poly_group_by_sql}"
        );

        let poly_order_by_cypher = "MATCH (u:User)-[:LIKES]-(target) RETURN id(target), \
             count(*) ORDER BY id(target)";
        let poly_order_by_sql =
            render(&poly_schema, poly_order_by_cypher, SqlDialect::ClickHouse).await;
        assert!(
            !poly_order_by_sql.contains("GROUP BY target.")
                && !poly_order_by_sql.contains("ORDER BY target."),
            "#484 round 3 BLOCKING: same undirected polymorphic-junction-table \
             alias, combined with ORDER BY:\n{poly_order_by_sql}"
        );
        assert!(
            !poly_order_by_sql.contains("__order_col"),
            "ORDER BY id() here must not corrupt GROUP BY into referencing an \
             out-of-scope column:\n{poly_order_by_sql}"
        );
    }

    /// #484 round 3: confirms the fix above (`graph_rel_vlp_endpoint_role`
    /// replacing `graph_rel_connection_role`) does NOT regress the genuine
    /// VLP-collapse shapes it must still treat as safe — these must keep
    /// resolving `id()` PRECISELY (the real `start_id`/`end_id` CTE column),
    /// not merely safely (the `toInt64(0)`-via-outer-alias placeholder the
    /// junction-table/polymorphic cases above fall back to).
    ///
    /// `anchored_unlabeled_expand` (`MATCH (a:User)-[r]-(o) RETURN a, r, o`,
    /// from the `BROWSER_CORPUS`) has an unlabeled, multi-edge-type
    /// relationship `r` — this is the degenerate single-hop
    /// `multi_type_vlp_joins` shape (`variable_length.is_some()` even without
    /// `*` syntax), collapsing into one `vlp_multi_type_a_o` CTE that exposes
    /// a real, label-agnostic `end_id` column for every row regardless of
    /// which of `o`'s candidate labels (User/Post) it actually is.
    #[tokio::test]
    async fn group_by_order_by_vlp_multilabel_endpoint_resolves_precisely_484() {
        let schema = load_schema(SchemaId::Standard.yaml_path());

        let cypher = "MATCH (a:User)-[r]-(o) RETURN id(o), count(*)";
        let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
        assert!(
            sql.contains("GROUP BY t.end_id"),
            "#484 round 3: an unlabeled multi-edge-type undirected endpoint \
             (the multi_type_vlp_joins/bidirectional_union shape) must keep \
             resolving id() to the CTE's real end_id column, not fall back to \
             the safe-but-imprecise outer-alias placeholder:\n{sql}"
        );
        assert!(
            !sql.contains("GROUP BY `id(o)`"),
            "must not regress into the raw-union safe fallback for a shape \
             that genuinely collapses into a single-alias CTE:\n{sql}"
        );

        let order_cypher = "MATCH (a:User)-[r]-(o) RETURN id(o), count(*) ORDER BY id(o)";
        let order_sql = render(&schema, order_cypher, SqlDialect::ClickHouse).await;
        assert!(
            order_sql.contains("ORDER BY t.end_id"),
            "ORDER BY must also keep the precise end_id resolution:\n{order_sql}"
        );

        // `unlabeled_rel_typed` (`MATCH (n)-[r:FOLLOWS]-(o) RETURN n, o`):
        // `r`'s type is fixed (FOLLOWS, User->User only on this schema), so
        // `o` is only nominally "multi-label" — BidirectionalUnion's plain
        // (non-VLP) direction-combination split applies instead (2 raw
        // per-direction branches, no VLP CTE: `variable_length` is `None`
        // here too), correctly routing through the SAME raw-union guard as
        // the junction-table cases above. It still resolves PRECISELY,
        // though, because (unlike the junction-table cases, where each
        // candidate label's id column genuinely differs, e.g. fs_id/user_id)
        // FOLLOWS always lands on `o.user_id` in EVERY branch here — so the
        // outer alias the raw-union fallback references (`id(o)`) legitimately
        // carries a real per-row value in the SELECT list already, not the
        // `toInt64(0)` placeholder — GROUP BY-by-alias is honest, not lossy.
        let rel_typed_cypher = "MATCH (n)-[r:FOLLOWS]-(o) RETURN id(o), count(*)";
        let rel_typed_sql = render(&schema, rel_typed_cypher, SqlDialect::ClickHouse).await;
        assert!(
            !rel_typed_sql.contains("toInt64(0)"),
            "#484 round 3: unlabeled_rel_typed's raw-union branches must \
             each still project a REAL o.user_id under the id(o) alias (not \
             the placeholder) — the single relationship type resolves the \
             node label unambiguously in every branch:\n{rel_typed_sql}"
        );
        assert!(
            rel_typed_sql.contains("o.user_id AS \"id(o)\""),
            "expected each UNION branch to project the real id column under \
             the id(o) alias:\n{rel_typed_sql}"
        );
        assert!(
            rel_typed_sql.contains("GROUP BY `id(o)`"),
            "the outer GROUP BY correctly references that real per-row \
             SELECT-list alias (safe AND precise here, unlike the \
             junction-table cases):\n{rel_typed_sql}"
        );
    }

    /// #490: `type(r)` behind a `WITH r` barrier used to hard-code the VLP
    /// alias `t` in the outer SELECT even when the relationship actually
    /// routes through a `pattern_union_r AS r` CTE (both endpoints
    /// unlabeled) — unbound `t.path_relationships`, ClickHouse Code 47. The
    /// route-detection walkers (`rel_uses_pattern_union` in
    /// `projection_tagging.rs` and `has_pattern_combinations_for_alias` in
    /// `select_builder.rs`) had drifted from each other (missing
    /// `WithClause`/`CartesianProduct`/`Union` arms and `GraphRel.center`
    /// recursion between them) and are now unified onto a single canonical
    /// walk, `LogicalPlan::rel_alias_uses_pattern_union`. Live-verified: the
    /// WITH-barrier query below returns AUTHORED/FOLLOWS/LIKED on the
    /// `social` fixture, matching the pre-existing non-WITH
    /// `browser_type_probe_pattern_union_outer_alias_matches_from` lock.
    #[tokio::test]
    async fn type_r_after_with_barrier_pattern_union_490() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let cypher = "MATCH ()-[r]->() WITH r RETURN DISTINCT type(r)";
        let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
        assert!(
            sql.contains("pattern_union_r AS ("),
            "expected the pattern_union CTE: {sql}"
        );
        assert!(
            sql.contains("r.path_relationships"),
            "expected type(r) after WITH r to resolve from \
             r.path_relationships (the pattern_union CTE alias), not the \
             unbound VLP alias t: {sql}"
        );
        assert!(
            !sql.contains("t.path_relationships"),
            "outer SELECT must not reference the unbound VLP alias \
             t.path_relationships (#490 regression): {sql}"
        );
    }

    /// #495: `remap_denormalized_aliases_in_expr` (`graph_join/helpers.rs`)
    /// is the hand-rolled walk that rewrites virtual denormalized node
    /// aliases in a `CartesianProduct.join_condition` onto the edge alias
    /// that actually embeds them. It was missing `List`/`Case`/legacy
    /// `Operator`/`AggregateFnCall` arms, so a CASE-based cross-pattern
    /// correlation (and `IN`-list conjuncts, though that specific shape
    /// turns out to be blocked earlier in the pipeline — see the #495
    /// finding in the fix writeup) left the unbound cypher alias
    /// (`srcip2.ip`) in the rendered WHERE clause instead of the bound edge
    /// alias. This locks the CASE arm: both `srcip1`/`srcip2` must resolve
    /// to their owning edge table aliases (`t1`/`t2`), not leak through
    /// unbound.
    #[tokio::test]
    async fn remap_denormalized_case_expr_alias_bound_495() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let cypher = "MATCH (srcip1:IP)-[:REQUESTED]->(d:Domain), \
                       (srcip2:IP)-[:ACCESSED]->(destip:IP) \
                       WHERE CASE WHEN srcip1.ip = srcip2.ip THEN true ELSE false END \
                       RETURN DISTINCT srcip1.ip";
        let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
        // Quoted-alias form (`srcip1."`) distinguishes an unbound cypher
        // alias reference from the legitimate output column alias
        // (`AS "srcip1.ip"`), which legally contains "srcip1." as a
        // substring — same pattern as the #482 golden tests this mirrors.
        for unbound in ["srcip1.\"", "srcip2.\""] {
            assert!(
                !sql.contains(unbound),
                "#495 [{cypher}]: unbound cypher alias '{unbound}' leaked \
                 into the CASE predicate:\n{sql}"
            );
        }
        // The CASE predicate must bind BOTH sides to the SAME edge alias
        // used by the (already-working) plain-equality case — extract that
        // alias from the WHERE clause rather than hard-coding an
        // auto-generated counter value (t1/t2 vs t5/t6 depending on test
        // ordering within the process).
        assert!(
            sql.contains("CASE WHEN ")
                && sql.contains(".ip = ")
                && !sql.contains("srcip1.ip = srcip2.ip"),
            "#495: CASE predicate must bind to edge aliases, not the raw \
             cypher aliases:\n{sql}"
        );
    }

    /// #483: `count(DISTINCT n)` on an unlabeled endpoint of an UNDIRECTED
    /// GraphRel (routed at render time through a `multi_type_vlp_joins`/
    /// `bidirectional_union` CTE, e.g. `MATCH (a:User)-[r]-(o)`) used to
    /// build its cross-label DISTINCT discriminator tuple from the #467
    /// per-label-raw-column logic (designed for a bare `MATCH (n)`
    /// ViewScan UNION, which has genuinely separate `post_id`/`user_id`
    /// physical columns). That logic doesn't apply to a
    /// `multi_type_vlp_joins` CTE, which exposes exactly one `end_id` (+
    /// `end_type` discriminator) regardless of label count — downstream VLP
    /// alias rewriting collapsed BOTH `o.post_id` and `o.user_id` onto the
    /// SAME `t.end_id` CTE column, emitting `count(DISTINCT
    /// tuple(t.end_id, t.end_id))`: a duplicate tuple member that is NOT
    /// harmless — it silently merges ids that collide across labels
    /// (Post #3 and User #3 count as one). Live-verified (2026-07-10, local
    /// `social` fixture): pre-fix returned 7, post-fix returns 12, matching
    /// `SELECT count(*) FROM (SELECT DISTINCT end_type, end_id FROM
    /// (per-relationship-type UNION))`.
    #[tokio::test]
    async fn count_distinct_bidirectional_endpoint_no_duplicate_tuple_member_483() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let cypher = "MATCH (a:User)-[r]-(o) RETURN count(DISTINCT o)";
        let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
        assert!(
            sql.contains("vlp_multi_type_a_o AS t"),
            "expected this shape to route through the multi_type_vlp_joins \
             CTE aliased t: {sql}"
        );
        assert!(
            !sql.contains("tuple(t.end_id, t.end_id)"),
            "#483: duplicate tuple member silently merges cross-label id \
             collisions: {sql}"
        );
        assert!(
            sql.contains("tuple(t.end_type, t.end_id)"),
            "expected the (type, id) discriminator pair: {sql}"
        );
    }

    /// #483 control: a GraphRel connection through a DIRECTED multi-hop
    /// chain (not `multi_type_vlp_joins`-routed) must keep the #467 raw
    /// per-label-column `coalesce`/tuple behavior — the `graph_rel_connection_role`
    /// detector added for #483 is gated on `was_undirected`/`Direction::Either`
    /// specifically so it does NOT misfire here (an earlier version of the
    /// fix regressed exactly this shape onto a nonexistent `child.end_id`
    /// column; caught by the `test_count_with_alias`/
    /// `test_having_multiple_conditions` golden corpus cases).
    #[tokio::test]
    async fn count_coalesce_directed_multi_hop_unaffected_by_483_fix() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        // Directed 2-hop chain through an unlabeled multi-label endpoint —
        // not undirected, so must NOT route through the #483 tuple path.
        let cypher = "MATCH (a:User)-[:AUTHORED]->(p:Post)<-[:LIKED]-(o) RETURN count(DISTINCT o)";
        let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
        assert!(
            !sql.contains("end_type") && !sql.contains("t.end_id"),
            "a directed multi-hop chain must not be treated as a \
             multi_type_vlp_joins endpoint: {sql}"
        );
    }

    /// #476 (partial fix — see the fix writeup for the remaining gap):
    /// `MATCH (n) WITH count(n) AS c RETURN c` used to render a WITH-CTE
    /// whose FROM was a single label's table (missing the multi-label
    /// UNION scan entirely) alongside a dangling, never-defined
    /// `with_c_cte_1` reference. Three sibling functions in
    /// `type_inference.rs` (`plan_has_aggregation`, `extract_scan_part`,
    /// `rewrap_aggregation`) lacked `WithClause` arms — the exact
    /// "hand-rolled walker missing an arm" class as #490/#495 — causing
    /// `generate_union_for_untyped_nodes` to take the wrong (whole-plan
    /// branch-explosion) path for a WITH-wrapped aggregate instead of
    /// injecting the union below the aggregation the way a bare
    /// `RETURN count(n)` already did. This locks the now-correct CTE
    /// *shape* (single CTE, real union, no dangling reference); live
    /// execution still errors on a separate, downstream per-branch
    /// NULL-padding gap in the CTE-body SQL emitter's shared
    /// `build_union_inner_select` (documented, not fixed, in the #476
    /// section of the fix writeup) — do not extend this test to assert
    /// live execution succeeds until that follow-up lands.
    #[tokio::test]
    async fn with_count_aggregate_multi_label_union_shape_476() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let cypher = "MATCH (n) WITH count(n) AS c RETURN c";
        let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;
        assert!(
            sql.contains("UNION ALL"),
            "#476: the WITH-CTE body must build the multi-label union scan, \
             not collapse to a single label's table:\n{sql}"
        );
        assert!(
            !sql.contains("with_c_cte_1"),
            "#476: must not reference a second CTE that was never actually \
             defined:\n{sql}"
        );
        assert!(
            sql.contains("count(coalesce("),
            "#476: the aggregate must stay the #467 coalesce-over-union \
             form:\n{sql}"
        );
    }
}

/// Regression tests for the #496/#497/#498/#499/#501 VLP/fixed-path family.
mod vlp_fixed_path_family_496_497_498_499_501 {
    use super::*;

    /// #497: `nodes(p)`/`relationships(p)` on a FIXED (non-VLP) path must
    /// expand to real array-construct expressions from the matched node/edge
    /// aliases, not the literal (unbound) function call. Verified against the
    /// standard schema; FK-edge and denormalized are covered by the golden
    /// corpus transitions (test_fixtures/test_path_variables__TestNodesFunction*,
    /// zeek_dns/test_vlp_path_var_*).
    #[tokio::test]
    async fn fixed_path_nodes_and_relationships_expand_497() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH p = (a:User)-[:FOLLOWS]->(b:User) RETURN nodes(p), relationships(p)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !sql.contains("nodes(p) AS") && !sql.contains("relationships(p) AS"),
            "nodes(p)/relationships(p) must not survive as a literal (unbound) \
             function call — only as a SELECT column alias label: {sql}"
        );
        assert!(
            sql.contains("array(a.") && sql.contains("array('FOLLOWS')"),
            "expected array-construct expansion for both functions: {sql}"
        );
    }

    /// #498: `length(p)` on a fixed path must return the real hop count AND
    /// keep the join that provides the end node — on FK-edge specifically,
    /// where the edge has no separate table and the join was previously
    /// silently pruned as "unreferenced" once `length(p)` was rewritten to a
    /// bare literal.
    #[tokio::test]
    async fn fixed_path_length_fk_edge_keeps_join_and_correct_constant_498() {
        let schema = load_schema(SchemaId::FkEdge.yaml_path());
        let sql = render(
            &schema,
            "MATCH p = (o:Order)-[:PLACED_BY]->(c) RETURN length(p)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("1 AS \"length(p)\""),
            "FK-edge length(p) on a 1-hop fixed path must be the literal 1, not 0: {sql}"
        );
        assert!(
            sql.to_uppercase().contains("JOIN"),
            "the customer join must not be silently pruned just because \
             length(p) doesn't literally reference the end-node alias: {sql}"
        );
    }

    /// #496: undirected and `*0..N` non-transitive VLP patterns must fail
    /// LOUDLY at plan time (a clear #496 error) instead of silently returning
    /// clamped-wrong rows. Directed `*1..N` (effective min_hops==1) keeps the
    /// exact single-hop clamp. Self-loop/transitive relationships (e.g.
    /// FOLLOWS on Users) are unaffected by any of this — chaining is real for
    /// them regardless of direction or zero-hop bounds.
    #[tokio::test]
    async fn vlp_clamp_loud_for_undirected_and_zero_hop_496() {
        let schema = load_schema(SchemaId::FkEdge.yaml_path());

        // Zero-hop, directed, non-transitive -> loud #496 error.
        let err = try_render(
            &schema,
            "MATCH (o:Order)-[:PLACED_BY*0..2]->(c) RETURN count(*)",
            SqlDialect::ClickHouse,
        )
        .await
        .expect_err("zero-hop non-transitive VLP must error, not silently clamp");
        assert!(err.contains("#496"), "expected a #496 error: {err}");

        // Undirected, non-transitive -> loud #496 error.
        let err = try_render(
            &schema,
            "MATCH (o:Order)-[:PLACED_BY*1..2]-(c) RETURN count(*)",
            SqlDialect::ClickHouse,
        )
        .await
        .expect_err("undirected non-transitive VLP must error, not silently clamp");
        assert!(err.contains("#496"), "expected a #496 error: {err}");

        // Directed, min_hops==1, non-transitive -> still the exact clamp
        // (fixed single-hop join, no CTE, no error).
        let sql = render(
            &schema,
            "MATCH (o:Order)-[:PLACED_BY*1..2]->(c) RETURN count(*)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !sql.to_uppercase().contains("WITH RECURSIVE"),
            "directed min_hops==1 non-transitive VLP should clamp to a plain \
             join, not a recursive CTE: {sql}"
        );
    }

    /// #499: comma-separated multi-pattern MATCH with two independent path
    /// variables, each over a non-transitive relationship (so both correctly
    /// clamp to the fixed-path route per #496) must render BOTH patterns
    /// fully — not the same broken `tuple(t.path_nodes, ...)` referenced
    /// twice with no CTE (main's pre-fix behavior).
    #[tokio::test]
    async fn multi_pattern_independent_path_vars_fixed_route_499() {
        let schema = load_schema(SchemaId::FkEdge.yaml_path());
        let sql = render(
            &schema,
            "MATCH p1 = (o:Order)-[:PLACED_BY*1..2]->(c), p2 = (o2:Order)-[:PLACED_BY*1..2]->(c2) RETURN p1, p2",
            SqlDialect::ClickHouse,
        )
        .await;
        assert_eq!(
            sql.matches("tuple('fixed_path', 'o', 'c'").count(),
            1,
            "p1 must be expanded exactly once: {sql}"
        );
        assert_eq!(
            sql.matches("tuple('fixed_path', 'o2', 'c2'").count(),
            1,
            "p2 must be expanded exactly once (not a duplicate of p1's CTE \
             reference): {sql}"
        );
        assert!(
            sql.matches("orders_fk").count() >= 2,
            "both independent Order patterns must be scanned: {sql}"
        );
    }

    /// #499 (remaining architectural gap, not silently wrong): two independent
    /// patterns that BOTH need a genuine recursive VLP CTE (transitive
    /// relationships, not clamped by #496) must fail loudly — the CTE-trigger
    /// machinery only supports one recursive VLP CTE per query today.
    #[tokio::test]
    async fn multi_pattern_two_real_vlp_ctes_loud_499() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let err = try_render(
            &schema,
            "MATCH p1 = (a:User)-[:FOLLOWS*1..2]->(b:User), p2 = (a2:User)-[:FOLLOWS*1..2]->(b2:User) RETURN p1, p2",
            SqlDialect::ClickHouse,
        )
        .await
        .expect_err("two independent real VLP CTEs in one MATCH must error, not silently drop one");
        assert!(err.contains("#499"), "expected a #499 error: {err}");
    }

    /// #499 (review follow-up): exactly ONE real VLP branch combined with one
    /// already-fixed sibling branch in a CartesianProduct is just as broken
    /// as two real VLP branches — the VLP branch's entire FROM/JOIN vanishes
    /// from the render (not merely a missing JOIN) while its path variable
    /// still references the never-generated CTE alias `t`. The original
    /// guard only checked `count > 1`; this exercises the `count == 1`
    /// (mixed) case the review found escaping it.
    #[tokio::test]
    async fn multi_pattern_one_vlp_one_fixed_loud_499() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let err = try_render(
            &schema,
            "MATCH p1 = (a:User)-[:FOLLOWS*1..2]->(b:User), p2 = (u2:User)-[:AUTHORED]->(post2) RETURN p1, p2",
            SqlDialect::ClickHouse,
        )
        .await
        .expect_err(
            "one real VLP branch mixed with one fixed sibling branch must \
             error, not silently drop the VLP branch's FROM/JOIN",
        );
        assert!(err.contains("#499"), "expected a #499 error: {err}");
    }

    /// #501: chaining a plain relationship onto a variable-length leg under a
    /// path variable (`MATCH p = (a)-[:A*1..2]->(b)-[:B]->(c) RETURN c`) must
    /// keep the trailing leg's JOIN. Root cause was a stale `is_shortest_path`
    /// check (really just "does any CTE have a path variable") that
    /// unconditionally stripped every non-CTE JOIN whenever ANY path variable
    /// was declared — even when the JOIN was demonstrably load-bearing.
    #[tokio::test]
    async fn vlp_chained_trailing_leg_keeps_join_under_path_variable_501() {
        let schema = load_schema(SchemaId::Standard.yaml_path());

        // RETURN of a node from the trailing (fixed) leg only — no path
        // function at all — is the strongest form of the regression: the old
        // buggy code stripped the JOIN for ANY path-variable query.
        let sql = render(
            &schema,
            "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User)-[:AUTHORED]->(c) RETURN c",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.to_uppercase().contains("JOIN") && sql.contains("posts_bench"),
            "the AUTHORED leg's JOIN to posts must survive: {sql}"
        );
        assert!(
            sql.contains("c.post_id") || sql.contains("c.author_id"),
            "c's properties must resolve against a bound alias, not an \
             unbound one: {sql}"
        );

        // RETURN p: the path tuple's metadata columns must also be backed by
        // a real JOIN.
        let sql = render(
            &schema,
            "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User)-[:AUTHORED]->(c) RETURN p",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("INNER JOIN") && sql.matches("JOIN").count() >= 2,
            "expected the VLP CTE plus at least one real trailing JOIN: {sql}"
        );
    }

    /// #521: a fixed hop adjacent to a VLP hop on a fully DENORMALIZED
    /// (virtual-node/`SingleTableScan`) schema must still get a real,
    /// VLP-correlated JOIN for the trailing leg. Before the fix, the
    /// `SingleTableScan` strategy's join generator only checked whether the
    /// edge's own alias was already available — never whether the LEFT node
    /// (here `b`, a VLP endpoint) was already bound via the VLP CTE — so the
    /// trailing `b->c` edge degenerated into an unconditional FROM marker
    /// with no correlation to the VLP CTE, and got dropped entirely by
    /// downstream anchor-selection logic (`join_builder.rs`'s "Denormalized
    /// VLP is FROM: dropping anchor" branch mistakenly treating the dangling
    /// marker as the redundant original VLP anchor). The generated SQL
    /// referenced `t2.*` columns with no JOIN or FROM binding `t2` at all —
    /// a loud ClickHouse Code 47 UNKNOWN_IDENTIFIER. This is distinct from
    /// #501 above, which covers the same shape on the Traditional (non-
    /// denormalized) strategy — #521 is Denormalized-strategy-specific.
    #[tokio::test]
    async fn vlp_fixed_trailing_leg_denormalized_correlates_to_vlp_cte_521() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = render(
            &schema,
            "MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport)-[:FLIGHT]->(c:Airport) \
             RETURN a.code, b.code, c.code",
            SqlDialect::ClickHouse,
        )
        .await;

        assert!(
            sql.to_uppercase().contains("WITH RECURSIVE"),
            "expected a genuine VLP recursive CTE for a:Airport-[*1..2]->b:Airport: {sql}"
        );
        assert!(
            sql.contains("INNER JOIN") || sql.contains("JOIN"),
            "the trailing b->c FLIGHT hop must emit a real JOIN, not just \
             `FROM vlp_a_b AS t` with a dangling t2 reference: {sql}"
        );
        // The trailing leg's JOIN must correlate to the VLP CTE's end_id
        // column (VLP_CTE_FROM_ALIAS == "t") — proving it's a real
        // correlated join and not an unconditional FROM marker.
        assert!(
            sql.contains("t.end_id"),
            "the trailing leg's JOIN condition must correlate against the \
             VLP CTE's end_id column: {sql}"
        );
        // No dangling alias: whichever alias the trailing leg's edge table
        // uses in the SELECT (e.g. `t2.code`) must also appear as a JOIN
        // table_alias, not just be referenced from an unbound alias.
        let select_and_from_only = sql.split("SELECT").last().expect("SELECT clause present");
        for alias in ["t2"] {
            if select_and_from_only.contains(&format!("{alias}.")) {
                assert!(
                    sql.contains(&format!("AS {alias} ")) || sql.contains(&format!("AS {alias}\n")),
                    "alias '{alias}' is referenced in SELECT but never bound \
                     by a JOIN/FROM: {sql}"
                );
            }
        }
    }

    /// #497 (review follow-up, BLOCKING finding): `nodes(p)` on a fixed path
    /// through a COMPOSITE-key node (`Account`, keyed on `(bank_id,
    /// account_number)` in `schemas/test/composite_node_ids.yaml`) must carry
    /// the node's FULL identity. A prior version of the #497 fix read
    /// `ViewScan.id_column: String` (a single lossy column), which silently
    /// dropped `account_number` entirely — `nodes(p)` would render
    /// `array(c.customer_id, a.bank_id, a2.bank_id)`, structurally valid SQL
    /// that quietly discards half of each Account's identity. Fixed by
    /// routing through `PatternSchemaContext`/`NodeAccessStrategy`, whose
    /// `id_column` is a full composite-aware `Identifier`, and pipe-joining
    /// composite columns the same way the VLP recursive CTE already does
    /// (`emit_id_expr` in variable_length_cte.rs).
    #[tokio::test]
    async fn fixed_path_nodes_composite_id_keeps_all_columns_497() {
        let schema = load_schema(SchemaId::CompositeId.yaml_path());
        let sql = render(
            &schema,
            "MATCH p = (c:Customer)-[:OWNS]->(a:Account)-[:TRANSFERRED]->(a2:Account) RETURN nodes(p)",
            SqlDialect::ClickHouse,
        )
        .await;
        // Both Account occurrences must reference BOTH composite columns.
        for alias in ["a", "a2"] {
            assert!(
                sql.contains(&format!("{alias}.bank_id"))
                    && sql.contains(&format!("{alias}.account_number")),
                "composite Account id for '{alias}' must reference BOTH \
                 bank_id AND account_number, not silently drop one: {sql}"
            );
        }
        // The single-column Customer id must still resolve plainly (or, if
        // cast to string for array-type consistency with the composite
        // entries, via a string cast — either way it must reference
        // customer_id, not be silently dropped either).
        assert!(
            sql.contains("c.customer_id"),
            "single-column Customer id must still be present: {sql}"
        );
        // A composite id renders as a pipe-joined concat — lock that shape so
        // a future refactor can't silently regress back to a single column.
        assert!(
            sql.contains("concat(") && sql.contains("'|'"),
            "expected a pipe-joined composite-id expression: {sql}"
        );
    }

    /// #497 (review follow-up): a path touching ONLY single-column ids must
    /// stay byte-identical to before the composite-ID fix — no spurious
    /// string casts / pipe-joins for the common (non-composite) case.
    #[tokio::test]
    async fn fixed_path_nodes_single_column_unaffected_by_composite_fix_497() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH p = (a:User)-[:FOLLOWS]->(b:User) RETURN nodes(p)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("array(a.user_id, b.user_id)"),
            "single-column ids must render as plain array(...) with no cast \
             or pipe-join scaffolding: {sql}"
        );
    }

    /// #489: denormalized VLP `*0..N` must emit a genuine zero-hop base case
    /// (the start node paired with itself, `hop_count = 0`), matching the
    /// standard (non-denormalized) VLP CTE's behavior. Before the fix, the
    /// `DenormalizedCteStrategy` recursive CTE's base case unconditionally
    /// started at `hop_count = 1` regardless of `min_hops`, so `*0..N`
    /// silently dropped every zero-hop row. Live-verified against
    /// ground-truth ClickHouse data (5 airports -> 5 zero-hop rows, 5 direct
    /// flights -> 5 one-hop rows, 3 real 2-hop chains -> 3 two-hop rows).
    #[tokio::test]
    async fn denorm_vlp_zero_hop_min_bound_emits_self_paired_base_case_489() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = render(
            &schema,
            "MATCH p = (a:Airport)-[:FLIGHT*0..2]->(b:Airport) RETURN p",
            SqlDialect::ClickHouse,
        )
        .await;

        assert!(
            sql.contains("0 as hop_count"),
            "expected a genuine zero-hop (hop_count = 0) base case branch: {sql}"
        );
        assert!(
            sql.contains("node_universe"),
            "expected the zero-hop base case to scan a synthesized node \
             universe (denormalized schemas have no separate node table): {sql}"
        );
        // The zero-hop base case's start_id and end_id must be the SAME
        // column (the node paired with itself) — not two different roles.
        assert!(
            sql.contains("node_universe.__node_id as start_id")
                && sql.contains("node_universe.__node_id as end_id"),
            "zero-hop row must pair the node with itself: {sql}"
        );
        // Empty path_edges/path_relationships in the zero-hop branch must be
        // explicitly typed (Array(String)) — a bare `[]` would infer
        // Array(Nothing) and break the recursive term's arrayConcat, which
        // always concatenates a real String element (ClickHouse Code 70).
        assert!(
            sql.contains("CAST([] AS Array(String))"),
            "zero-hop branch's empty arrays must be explicitly cast to \
             Array(String) to match the recursive term's column types: {sql}"
        );
    }

    /// #489 (regression guard): `*1..N` (no zero-hop) on the SAME
    /// denormalized schema must be completely unaffected by the zero-hop
    /// fix — no node_universe scan, no hop_count=0 branch, base case stays
    /// exactly the direct 1-hop edge-table scan it always was.
    #[tokio::test]
    async fn denorm_vlp_min_hops_one_unaffected_by_zero_hop_fix_489() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = render(
            &schema,
            "MATCH p = (a:Airport)-[:FLIGHT*1..2]->(b:Airport) RETURN p",
            SqlDialect::ClickHouse,
        )
        .await;

        assert!(
            !sql.contains("node_universe"),
            "min_hops=1 must not go through the new zero-hop node-universe \
             path at all: {sql}"
        );
        assert!(
            !sql.contains("0 as hop_count"),
            "min_hops=1 must not emit a zero-hop branch: {sql}"
        );
        assert!(
            sql.contains("1 as hop_count"),
            "min_hops=1 base case must still start at hop_count=1: {sql}"
        );
    }

    /// #494: `labels(x)` on a multi-type VLP end node must resolve the
    /// PER-ROW actual label from the CTE's `end_type` discriminator column,
    /// not a static array literal of every statically-possible label.
    /// Before the fix, the gate meant to detect "genuine multi-type VLP"
    /// (`table_ctx.is_cte_reference()`) was never actually set true by the
    /// multi-type VLP auto-inference pass, so `labels(x)` always fell
    /// through to the static-union branch. Live-verified: AUTHORED-reached
    /// rows show `[Post]`, FOLLOWS-reached rows show `[User]` — never the
    /// static `[Post, User]` union on every row.
    #[tokio::test]
    async fn multi_type_vlp_labels_resolves_per_row_end_type_494() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x) RETURN labels(x)",
            SqlDialect::ClickHouse,
        )
        .await;

        assert!(
            sql.contains("t.end_type"),
            "labels(x) must reference the VLP CTE's per-row end_type \
             column: {sql}"
        );
        assert!(
            !sql.contains("'User', 'Post'") && !sql.contains("'Post', 'User'"),
            "labels(x) must not be a static array literal of every \
             statically-possible label: {sql}"
        );
    }

    /// #494 (regression guard): a genuinely polymorphic node backed by a
    /// SINGLE table (no VLP CTE at all — `labels.len() > 1` purely from
    /// schema-level type ambiguity) must keep its existing static-label
    /// rendering. This is NOT a multi-type VLP CTE reference, so the #494
    /// fix's `node_is_multi_type_rel_endpoint` guard must not touch it.
    #[tokio::test]
    async fn labels_on_non_vlp_polymorphic_node_unaffected_by_494_fix() {
        let schema = load_schema("schemas/test/test_fixtures.yaml");
        let sql = render(
            &schema,
            "MATCH (n) RETURN label(n), count(*)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !sql.contains("t.end_type") && !sql.contains(" AS t\n") && !sql.contains(" AS t "),
            "a non-VLP polymorphic single-table node must not be rewritten \
             to reference a nonexistent VLP CTE alias 't': {sql}"
        );
    }

    /// #503: ORDER BY on an anchor property that participates in a
    /// UNION+aggregate render (undirected relationship, or a denorm/coupled
    /// OPTIONAL MATCH) must reference the OUTER query's aliased output
    /// column, not the raw un-mapped expression. The has_aggregation branch
    /// of the UNION renderer (`to_sql_query.rs`) used to emit
    /// `plan.order_by.to_sql()` verbatim — e.g. bare `a.code` — which
    /// ClickHouse parses as a qualified `table.column` reference. No table
    /// alias `a` exists at that outer scope (only `__union` does), so this
    /// was a loud UNKNOWN_IDENTIFIER. The non-aggregation UNION path already
    /// handled this correctly by referencing a synthetic `__order_col_N`
    /// column; the aggregation path diverged and never got the same
    /// treatment — that divergence is the root cause the issue describes
    /// ("plain ORDER BY without an aggregate already works").
    ///
    /// Live-verified on `db_denormalized` (8 flights, 7 airports): degree
    /// counts (ATL 2, DEN 2, JFK 2, LAX 5, ORD 3, PHX 1, SFO 1) match
    /// hand-written ground truth
    /// (`SELECT code, count() FROM (SELECT origin_code ... UNION ALL SELECT
    /// dest_code ...) GROUP BY code`), in ascending `a.code` order, for both
    /// the plain undirected MATCH and the OPTIONAL undirected MATCH shapes.
    #[tokio::test]
    async fn union_aggregate_order_by_anchor_property_503() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());

        let plain = normalize(
            &render(
                &schema,
                "MATCH (a:Airport)-[r:FLIGHT]-(b:Airport) RETURN a.code, count(r) ORDER BY a.code",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            plain.contains("ORDER BY `a.code` ASC") || plain.contains("ORDER BY `a.code`asc"),
            "#503: ORDER BY must reference the backtick-quoted outer alias, \
             not a bare `a.code` table-qualified reference (undefined at \
             this scope):\n{plain}"
        );
        assert!(
            !plain.contains("ORDER BY a.code") && !plain.contains("ORDER BY __order_col"),
            "#503: ORDER BY must not emit the raw unquoted expression or a \
             dangling synthetic column reference:\n{plain}"
        );
        // GROUP BY must also stay bound to a column the inner UNION branches
        // actually project (`__order_col_N` is deliberately excluded from
        // aggregation UNION branches — see `build_union_inner_select`) — a
        // sibling bug in the same rendering block (#503 family).
        assert!(
            !plain.contains("GROUP BY `__order_col"),
            "#503: GROUP BY must not reference an excluded __order_col_N \
             synthetic column:\n{plain}"
        );

        let optional = normalize(
            &render(
                &schema,
                "MATCH (a:Airport) OPTIONAL MATCH (a)-[r:FLIGHT]-(b:Airport) \
                 RETURN a.code, count(r) ORDER BY a.code",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            optional.contains("ORDER BY `a.code`"),
            "#503 (OPTIONAL variant): ORDER BY must reference the \
             backtick-quoted outer alias:\n{optional}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:Airport)-[r:FLIGHT]-(b:Airport) RETURN a.code, count(r) ORDER BY a.code",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(plain, again, "#503: nondeterministic render");
        }
    }

    /// #503 (aggregate alias / multi-key ORDER BY): a mix of an aggregate
    /// alias and a plain property in ORDER BY — `ORDER BY cnt DESC, a.code
    /// ASC` — must backtick-quote BOTH references. Locks the general fix
    /// (not denorm-specific): the pre-existing standard-schema
    /// `relationship_degree`-style pattern (`ORDER BY connections DESC,
    /// a.name ASC`) had the SAME bug — a bare `a.name` in ORDER BY at the
    /// UNION-aggregate outer scope — and a dangling `GROUP BY
    /// __order_col_N` reference; both are fixed by the same change.
    #[tokio::test]
    async fn union_aggregate_order_by_multi_key_alias_and_property_503() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = normalize(
            &render(
                &schema,
                "MATCH (a:User)-[:FOLLOWS]-(b:User) \
                 RETURN a.name, count(DISTINCT b) AS connections \
                 ORDER BY connections DESC, a.name ASC",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            sql.contains("ORDER BY `connections` DESC, `a.name` ASC"),
            "#503: both ORDER BY keys (aggregate alias and anchor property) \
             must be backtick-quoted outer-alias references:\n{sql}"
        );
        assert!(
            !sql.contains("GROUP BY `__order_col"),
            "#503: GROUP BY must not reference an excluded __order_col_N \
             synthetic column:\n{sql}"
        );
    }

    /// #507: the coupled-schema anchor scan CTE (`__denorm_scan_a`, built by
    /// the OPTIONAL-denorm CTE+LEFT JOIN path shared with #502/#505/#506)
    /// must collapse to NODE grain (one row per distinct node id) before the
    /// LEFT JOIN, not TABLE grain (one row per unique combination of every
    /// exposed column). On `zeek_merged_test.yaml`, `IP@conn_log` exposes
    /// both `ip` (the identity) and `port` (a per-connection attribute) —
    /// `UNION DISTINCT` over both columns dedups on the (ip, port) PAIR, so
    /// an IP with 3 distinct ports fans out any downstream per-node
    /// aggregate LEFT JOINed against the CTE by 3x.
    ///
    /// Live-verified on the zeek fixture: 192.168.1.10 has 3 distinct
    /// `id.orig_p` values as a connection source but exactly 3 REQUESTED
    /// (dns_log) rows — pre-fix this rendered `count(r) = 9` (3 grain rows ×
    /// 3 matches); post-fix it correctly returns 3, matching hand-written
    /// ground truth (`... LEFT JOIN ... SETTINGS join_use_nulls = 1`, to
    /// mirror ClickHouse's default non-Nullable LEFT JOIN column fill which
    /// would otherwise mask the NULL-extension semantics `#502` relies on).
    /// 192.168.1.20 (2 distinct ports, 1 REQUESTED row) went from 2 to the
    /// correct 1.
    #[tokio::test]
    async fn denorm_anchor_scan_cte_collapses_to_node_grain_507() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql = normalize(
            &render(
                &schema,
                "MATCH (a:IP) OPTIONAL MATCH (a)-[r:REQUESTED]->(d:Domain) \
                 RETURN a.ip, count(r)",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        // The anchor CTE must wrap the raw origin/dest UNION in an outer
        // GROUP BY keyed on the node's own id property, picking a
        // deterministic representative (`min`) for the non-identity `port`
        // column — not a bare `UNION DISTINCT` over both columns.
        assert!(
            sql.contains("GROUP BY \"ip\"") && sql.contains("min(\"port\")"),
            "#507: anchor scan CTE must collapse to node grain via an outer \
             GROUP BY on the id property, with non-identity columns wrapped \
             in a deterministic aggregate:\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:IP) OPTIONAL MATCH (a)-[r:REQUESTED]->(d:Domain) \
                     RETURN a.ip, count(r)",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(sql, again, "#507: nondeterministic render");
        }
    }

    /// #507 (no-op guard): when the anchor node's standalone scan exposes
    /// ONLY its id property (no other columns to conflate), the grain-fix
    /// wrap must be skipped entirely — `UNION DISTINCT` over a single column
    /// is already node grain. Locks that single-property anchors (e.g. the
    /// zeek `dns_log`-only IP role used by `RESOLVED_TO`) render exactly as
    /// before #507 (no gratuitous extra SELECT/GROUP BY wrapper).
    #[tokio::test]
    async fn denorm_anchor_scan_cte_no_wrap_for_single_property_507() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql = normalize(
            &render(
                &schema,
                "MATCH (rip:ResolvedIP) OPTIONAL MATCH (d:Domain)-[r:RESOLVED_TO]->(rip) \
                 RETURN rip.ip, count(r)",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            !sql.contains("GROUP BY \"ip\"") && !sql.to_lowercase().contains("min(\""),
            "#507: single-property anchor scan must NOT get the grain-fix \
             wrap (UNION DISTINCT over one column is already node grain):\n{sql}"
        );
    }

    /// #509: a non-count aggregate over a BARE node variable (`collect(b)`)
    /// must produce a resolvable `PropertyAccess` argument, matching the
    /// treatment `count(node)` already gets. Before this fix, only "count"
    /// triggered the analyzer's node-identity rewrite; every other aggregate
    /// left the bare, unbound Cypher alias in place — invalid SQL
    /// (ClickHouse UNKNOWN_IDENTIFIER). Covers BOTH a denormalized anchor
    /// (the render-side #493 resolver needs a PropertyAccess to rewrite onto
    /// the embedded edge column) and a standard schema (the reference must
    /// resolve to the joined table's real id column either way) — this bug
    /// was NOT denorm-specific.
    ///
    /// Live-verified on db_denormalized (8 flights, 7 airports):
    /// `collect(b)` per origin airport matches `groupArray(dest_code)`
    /// ground truth exactly (e.g. LAX -> [JFK, ATL, ORD], PHX -> [] for the
    /// airport with zero outgoing flights via OPTIONAL MATCH).
    #[tokio::test]
    async fn aggregate_over_bare_node_variable_resolves_id_column_509() {
        let denorm_schema = load_schema(SchemaId::Denormalized.yaml_path());
        let denorm_sql = normalize(
            &render(
                &denorm_schema,
                "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b) \
                 RETURN a.code, collect(b)",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            denorm_sql.contains(".dest_code) AS \"collect(b)\"")
                || denorm_sql.contains(".dest_code) as \"collect(b)\""),
            "#509: collect(b) over a denormalized bare node must resolve to \
             the embedded edge column, not the raw unbound alias `b`:\n{denorm_sql}"
        );
        assert!(
            !denorm_sql.contains("groupArray(b)") && !denorm_sql.contains("(b)\n"),
            "#509: collect(b) must not leave the bare unbound Cypher alias \
             in the rendered SQL:\n{denorm_sql}"
        );

        let standard_schema = load_schema(SchemaId::Standard.yaml_path());
        let standard_sql = normalize(
            &render(
                &standard_schema,
                "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) \
                 RETURN a.name, collect(b)",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            standard_sql.contains(".followed_id) AS \"collect(b)\""),
            "#509: collect(b) over a standard-schema bare node must resolve \
             to the joined table's real id column:\n{standard_sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &denorm_schema,
                    "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b) \
                     RETURN a.code, collect(b)",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(denorm_sql, again, "#509: nondeterministic render");
        }
    }

    /// #509 (WITH-clause guard): `collect(u)` immediately followed by
    /// `UNWIND` of the SAME variable is a no-op pattern the
    /// `CollectUnwindElimination` optimizer recognizes and eliminates
    /// entirely (WITH u passes through, no groupArray/UNWIND round-trip).
    /// That optimizer pattern-matches on `collect(u)`'s argument STILL being
    /// a bare `TableAlias` — the #509 fix lives in the RETURN-only
    /// `select_builder.rs::extract_select_items` path (which a `WithClause`
    /// node never reaches: it recurses into `wc.input`, never `wc.items`),
    /// so it must NOT rewrite the WITH clause's own `collect(u)` before the
    /// optimizer sees it. Locks that the no-op elimination still fires
    /// (pre-#509 behavior, unaffected).
    #[tokio::test]
    async fn aggregate_over_bare_node_variable_with_clause_unaffected_509() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = normalize(
            &render(
                &schema,
                "MATCH (u:User) WITH u, collect(u) as users \
                 UNWIND users as user RETURN user.name LIMIT 3",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            !sql.to_lowercase().contains("grouparray")
                && !sql.to_lowercase().contains("array join"),
            "#509: WITH collect(u)+UNWIND(same var) no-op elimination must \
             still fire — got a real groupArray/ARRAY JOIN round-trip \
             instead of the optimized pass-through:\n{sql}"
        );
    }

    /// #510: a WITH-clause aggregate over a denorm/coupled anchor
    /// (`WITH a, count(r) AS c`) must GROUP BY the anchor CTE's
    /// Cypher-property-named column, never the raw physical db column — and
    /// the accompanying SELECT-list item for the anchor must be sourced from
    /// the same CTE, never the LEFT-JOINed (NULL-extended on an
    /// OPTIONAL-miss row) edge alias. Two separate GROUP BY construction
    /// sites exist in this codebase (see the triplication note on
    /// `composite_id_group_by_columns` in `group_by_builder.rs`) — this
    /// covers `expand_table_alias_to_group_by_id_only`, the one that
    /// actually fires for this WITH shape.
    ///
    /// Live-verified on the zeek fixture: `MATCH (a:IP) OPTIONAL MATCH
    /// (a)-[r:REQUESTED]->(d:Domain) WITH a, count(r) AS c RETURN a.ip, c`
    /// returns the same 5-row NULL-sensitive counts as #507's fix
    /// (192.168.1.10 -> 3, 192.168.1.20 -> 1, the other three IPs -> 0),
    /// matching hand-written ground truth exactly. Also live-verified on
    /// db_denormalized: outgoing FLIGHT counts per airport (PHX -> 0) match
    /// ground truth, and a `WHERE c > 0` after the WITH correctly drops PHX.
    #[tokio::test]
    async fn with_aggregate_over_denorm_anchor_group_by_and_select_510() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql = normalize(
            &render(
                &schema,
                "MATCH (a:IP) OPTIONAL MATCH (a)-[r:REQUESTED]->(d:Domain) \
                 WITH a, count(r) AS c RETURN a.ip, c",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            sql.contains("GROUP BY a.ip"),
            "#510: WITH-aggregate GROUP BY must reference the anchor CTE's \
             `ip` column, not the raw physical `id.orig_h`:\n{sql}"
        );
        assert!(
            !sql.contains("GROUP BY a.\"id.orig_h\""),
            "#510: GROUP BY must not reference the raw physical db column \
             (invalid SQL — the anchor CTE only exposes `ip`):\n{sql}"
        );
        assert!(
            sql.contains("a.ip AS \"p1_a_ip\""),
            "#510: the WITH-exported anchor property must be SELECTed from \
             the anchor CTE alias `a`, not the NULL-extended edge alias:\n{sql}"
        );
        assert!(
            !sql.contains("r.\"id.orig_h\" AS \"p1_a_ip\""),
            "#510: the anchor property must not be sourced from the \
             LEFT-JOINed edge alias (NULL-extended on an OPTIONAL-miss row):\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:IP) OPTIONAL MATCH (a)-[r:REQUESTED]->(d:Domain) \
                     WITH a, count(r) AS c RETURN a.ip, c",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(sql, again, "#510: nondeterministic render");
        }
    }

    /// #519: an inline property-map pattern on a denormalized node (`(a:Airport
    /// {code: 'JFK'})`) inside a multi-hop pattern must render the SAME
    /// schema-mapped physical column a functionally-equivalent WHERE clause
    /// already does — never the raw, unmapped Cypher property name.
    /// `convert_properties` (match_clause/helpers.rs) builds its equality
    /// expression directly from the raw property key with no schema mapping
    /// at all; the fix applies the same property-mapping rewrite the
    /// sibling `LogicalPlan::Filter` branch of `collect_graphrel_predicates`
    /// already does, uniformly to `GraphRel.where_predicate` regardless of
    /// whether it originated from an inline map or a WHERE clause.
    ///
    /// Live-verified on db_denormalized: `MATCH (a:Airport {code: 'JFK'})
    /// -[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport) RETURN a.code, b.code,
    /// c.code` returns 3 rows (JFK->LAX->{JFK,ATL,ORD}), byte-identical to
    /// the equivalent `WHERE a.code = 'JFK'` query's live result. Also
    /// verified for a dest-role inline map (`(b:Airport {code: 'LAX'})`,
    /// correctly resolving to `dest_code`) and on the zeek coupled schema.
    #[tokio::test]
    async fn inline_property_map_on_denorm_node_resolves_mapped_column_519() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());

        let origin_role = normalize(
            &render(
                &schema,
                "MATCH (a:Airport {code: 'JFK'})-[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport) \
                 RETURN a.code, b.code, c.code",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            origin_role.contains("origin_code = 'JFK'"),
            "#519: inline map on an origin-role denorm node must resolve to \
             `origin_code`, not the raw Cypher property `code`:\n{origin_role}"
        );
        assert!(
            !origin_role.contains("WHERE t1.code"),
            "#519: must not reference the raw unmapped `code` column \
             (UNKNOWN_IDENTIFIER — flights_denorm has no `code` column):\n{origin_role}"
        );

        // Cross-check against the functionally-equivalent WHERE-clause form —
        // both must resolve identically.
        let where_form = normalize(
            &render(
                &schema,
                "MATCH (a:Airport)-[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport) \
                 WHERE a.code = 'JFK' RETURN a.code, b.code, c.code",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert_eq!(
            origin_role, where_form,
            "#519: an inline property-map pattern must render byte-identically \
             to its functionally-equivalent WHERE-clause form"
        );

        // Dest-role inline map: must resolve to `dest_code`, not `code`.
        let dest_role = normalize(
            &render(
                &schema,
                "MATCH (a:Airport)-[:FLIGHT]->(b:Airport {code: 'LAX'}) \
                 RETURN a.code, b.code",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            dest_role.contains("dest_code = 'LAX'"),
            "#519: inline map on a dest-role denorm node must resolve to \
             `dest_code`, not the raw Cypher property `code`:\n{dest_role}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:Airport {code: 'JFK'})-[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport) \
                     RETURN a.code, b.code, c.code",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(origin_role, again, "#519: nondeterministic render");
        }
    }

    /// FIXED (#520): `WITH <bare node alias>, count(*) AS n` over an undirected
    /// multi-hop pattern (`(a)-[:FOLLOWS]-(b)-[:FOLLOWS]-(c)`, #492's 4-branch
    /// direction-permutation UNION) used to emit `GROUP BY a.user_id` against a
    /// `__union` derived table whose branches only ever projected `a.full_name`
    /// — `a.user_id` was never selected by any branch (ClickHouse
    /// UNKNOWN_IDENTIFIER, Code 47).
    ///
    /// Root cause: `build_union_inner_select` (the shared WITH-CTE
    /// aggregate+union SELECT builder, `to_sql_query.rs`) only exported columns
    /// referenced by AGGREGATE ARGUMENTS into each UNION branch — it never
    /// looked at the plan's own `GROUP BY` list. An implicit-grouping key like
    /// `a.user_id` (from a bare passthrough alias `a` in `WITH a, count(*) AS n`)
    /// was therefore never exported by any branch, so the outer `GROUP BY
    /// a.user_id` dangled against the `__union` derived table (no real `a`
    /// table exists at that scope).
    ///
    /// Fix: `build_union_inner_select` now also takes the plan's GROUP BY
    /// expressions (`extra_required_exprs`) and exports their referenced
    /// columns the same way as aggregate-argument columns (NULL-padded
    /// per-branch via the #476 mechanism when a column doesn't exist on a given
    /// branch's table). `build_aliased_group_by` backtick-quotes a qualified
    /// `alias.column` GROUP BY key that has no outer-SELECT-alias match,
    /// referencing the newly-exported dotted-alias column instead of a
    /// nonexistent outer-scope table alias.
    ///
    /// This also resolves the second concern raised in #520 (direction swaps
    /// not applied inside WITH-CTE union projections): once the referenced
    /// columns are exported from EACH branch using the branch's own (already
    /// correctly per-orientation-swapped) FROM/JOIN structure, no separate
    /// swap step is needed — live-verified below for both the anchor alias
    /// (`a`) and the swapped middle alias (`b`).
    ///
    /// Live-verified (2026-07-10, social_benchmark, ClickHouse): `WITH a,
    /// count(*) AS n RETURN a.name, n` returns 5 rows (Alice=13, Bob=15,
    /// Charlie=15, Diana=12, Eve=9) — BYTE-IDENTICAL to the non-WITH control
    /// `RETURN a.name, count(*) AS n`. `WITH a, b, count(*) AS n RETURN a.name,
    /// b.name, n` (projecting the swapped middle alias `b`) also matches its
    /// non-WITH control row-for-row (e.g. Alice/Bob=6, Bob/Alice=12).
    #[tokio::test]
    async fn undirected_multihop_with_aggregate_group_by_export_520() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (a:User)-[:FOLLOWS]-(b)-[:FOLLOWS]-(c) WITH a, count(*) AS n \
             RETURN a.name, n",
            SqlDialect::ClickHouse,
        )
        .await;

        assert!(
            sql.contains("GROUP BY `a.user_id`"),
            "#520: GROUP BY must reference the exported dotted-alias column \
             `a.user_id` (backtick-quoted, resolving against the __union \
             derived table), not the raw unquoted `a.user_id`:\n{sql}"
        );
        // The id column must now be exported by EVERY UNION branch (NULL-padded
        // where the branch's own table doesn't carry it) so the outer GROUP BY
        // has something real to reference.
        let export_count = sql.matches("a.user_id AS \"a.user_id\"").count();
        assert!(
            export_count >= 4,
            "#520: `a.user_id` must be exported by every UNION branch (expected \
             >= 4 branches for the 4-way direction-permutation union), found {} \
             exports:\n{}",
            export_count,
            sql
        );
    }

    /// B1 (adversarial review of #507/#520, blocking): #507's node-grain fix
    /// was incomplete for an UNDIRECTED pattern on a cross-table "coupled"
    /// denorm schema (label spans TWO physical tables, e.g. zeek `IP` on
    /// both `conn_log` and `dns_log` — no separate table involved here, but
    /// the SAME node appearing under two DIFFERENT role-specific physical
    /// columns within `conn_log` itself: `id.orig_h` vs `id.resp_h`).
    ///
    /// An undirected pattern's `UnionDistribution` split produces TWO
    /// `GraphRel` branches (one per direction), and the special OPTIONAL
    /// denorm CTE + LEFT JOIN rendering path (`plan_builder.rs`, shared by
    /// #502/#505/#506/#507) runs ONCE PER BRANCH, independently resolving
    /// the node-grain dedup key each time. The old (role-restricted)
    /// resolution used `edge_side_node_properties(vs, anchor_is_left)` —
    /// only the role matching the CURRENT branch's `anchor_is_left`. On this
    /// schema, the id column (`id.orig_h`) only equals the "from"-role's
    /// `ip` mapping value, never the "to"-role's (`id.resp_h`) — so the
    /// SECOND branch (`anchor_is_left=false`) silently failed to resolve the
    /// id property, skipping the #507 node-grain wrap for that branch's CTE
    /// only. Two DIFFERENT CTE bodies (`__denorm_scan_a` wrapped,
    /// `__denorm_scan_a_2` NOT wrapped) then got unioned together with
    /// inconsistent grain — a SILENT wrong-result bug (a node's connections
    /// fragmenting across multiple output rows instead of collapsing into
    /// one), not a loud failure.
    ///
    /// Fixed by making the id-property lookup role-AGNOSTIC (search every
    /// Union branch's BOTH from- and to-role property maps for a value
    /// match against the schema's canonical `id_column`, accepting a match
    /// found via either role) — the anchor scan CTE always exposes the
    /// union of every role's Cypher property NAMES identically, so a match
    /// found via any role is valid regardless of which role the current
    /// branch happens to need.
    ///
    /// Live-verified on the zeek fixture: raw ground truth
    /// (`SELECT count() FROM zeek.conn_log WHERE id.orig_h = '192.168.1.10'
    /// OR id.resp_h = '192.168.1.10'`) = 4. Pre-fix, this query returned 3
    /// separate rows for 192.168.1.10 (c = 4, 1, 1 — confirmed via
    /// git-stash A/B); post-fix, exactly ONE row with c = 4.
    #[tokio::test]
    async fn undirected_coupled_schema_anchor_cte_single_grain_b1() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql = normalize(
            &render(
                &schema,
                "MATCH (a:IP) OPTIONAL MATCH (a)-[r:ACCESSED]-(b:IP) \
                 RETURN a.ip, a.port, count(r) AS c",
                SqlDialect::ClickHouse,
            )
            .await,
        );

        // Exactly ONE denorm-scan CTE for the anchor — no "_2" duplicate
        // (which would indicate the two UnionDistribution branches produced
        // inconsistent CTE bodies again).
        assert!(
            sql.contains("__denorm_scan_a") && !sql.contains("__denorm_scan_a_2"),
            "B1: exactly one __denorm_scan_a CTE must be shared by both \
             direction branches — a `__denorm_scan_a_2` duplicate indicates \
             the branches disagreed on node-grain wrapping again:\n{sql}"
        );
        // Both branches must reference the SAME (wrapped, node-grain) CTE.
        assert_eq!(
            sql.matches("LEFT JOIN zeek.conn_log AS r ON a.ip =")
                .count(),
            2,
            "B1: expected both direction branches to LEFT JOIN against the \
             anchor CTE:\n{sql}"
        );
        // The CTE itself must carry the node-grain wrap (GROUP BY the id
        // property, `min()` for the non-identity `port` column) — this is
        // what actually collapses the grain; its absence is the bug.
        assert!(
            sql.contains("GROUP BY \"ip\"") && sql.contains("min(\"port\")"),
            "B1: the shared anchor CTE must carry the #507 node-grain wrap:\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:IP) OPTIONAL MATCH (a)-[r:ACCESSED]-(b:IP) \
                     RETURN a.ip, a.port, count(r) AS c",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(sql, again, "B1: nondeterministic render");
        }
    }

    /// R2 (adversarial review of #503): #503's ORDER BY-to-outer-alias
    /// forward resolution only matched by exact `(table_alias, column)`
    /// identity (`same_property_ref`) or literal expression-text equality —
    /// both fail for a NON-id denormalized property (e.g. `a.state`), where
    /// the ORDER BY item keeps the anchor's original Cypher alias (`a`) with
    /// the mapped column (`origin_state`), while the matching SELECT item
    /// was independently rebound to the branch's physical alias (`r`/`t1`)
    /// by the UNION-branch resolver — same mapped COLUMN, different alias.
    /// Pre-fix this emitted a raw, unquoted `ORDER BY a.origin_state` —
    /// UNKNOWN_IDENTIFIER (no table `a` at the outer UNION scope).
    ///
    /// Fixed with an additional, narrowly-scoped fallback: match by column
    /// name alone when EXACTLY ONE non-order SELECT item carries that
    /// column (ambiguous cases — more than one candidate — deliberately do
    /// NOT guess, falling through to the pre-existing raw-expression
    /// behavior, no worse than before).
    ///
    /// Live-verified on db_denormalized: `ORDER BY a.state` returns airports
    /// in ascending state order (AZ, CA, CA, CO, GA, IL, NY), matching
    /// hand-derived ground truth from the 8-flight fixture.
    ///
    /// NOTE: a related but DISTINCT case remains broken and is NOT fixed by
    /// this change — `ORDER BY a.port` on the zeek COUPLED cross-table
    /// schema (`schemas/dev/zeek_merged_test.yaml`), where the ORDER BY
    /// item resolves to the RAW physical column (`a."id.orig_p"`) rather
    /// than the anchor CTE's exposed name (`a.port`) — a different
    /// mechanism (needs the #510-style CTE forward-resolution applied to
    /// ORDER BY specifically, not yet done) where the column names
    /// themselves differ, not just the alias, so this column-name fallback
    /// cannot bridge it.
    #[tokio::test]
    async fn union_aggregate_order_by_non_id_denorm_property_r2() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = normalize(
            &render(
                &schema,
                "MATCH (a:Airport)-[r:FLIGHT]-(b:Airport) \
                 RETURN a.code, a.state, count(r) AS c ORDER BY a.state",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            sql.contains("ORDER BY `a.state` ASC"),
            "R2: ORDER BY on a non-id denormalized property must reference \
             the backtick-quoted outer alias, not a raw unmapped physical \
             column under the anchor's original Cypher alias:\n{sql}"
        );
        assert!(
            !sql.contains("ORDER BY a.origin_state") && !sql.contains("ORDER BY a.dest_state"),
            "R2: ORDER BY must not emit the raw per-branch-mapped column \
             under the anchor's stale Cypher alias:\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:Airport)-[r:FLIGHT]-(b:Airport) \
                     RETURN a.code, a.state, count(r) AS c ORDER BY a.state",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(sql, again, "R2: nondeterministic render");
        }
    }

    /// R3 (adversarial review of #509): #509's bare-node-in-aggregate fix is
    /// keyed on expression SHAPE (a bare node-variable reference anywhere
    /// inside an aggregate-containing expression), not the aggregate's
    /// NAME — so it deliberately fires uniformly for
    /// `min(b)`/`max(b)`/`sum(b)`/`avg(b)`/`collect(b)` over a bare node,
    /// all resolving to the aggregate applied to the node's id column.
    /// Cypher doesn't define ordering/sum/average over a node, so there is
    /// no "correct" answer being contradicted — this locks the deliberate,
    /// review-confirmed choice (a deterministic, non-crashing render is
    /// preferred over the pre-#509 unbound-alias crash) so a future change
    /// narrowing this to `collect()`-only is a conscious decision with a
    /// failing test to update, not an accidental scope change.
    #[tokio::test]
    async fn aggregate_over_bare_node_variable_uniform_across_names_r3() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        for agg in ["collect", "min", "max", "sum", "avg"] {
            let cypher = format!(
                "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) \
                 RETURN a.name, {agg}(b)"
            );
            let sql = normalize(&render(&schema, &cypher, SqlDialect::ClickHouse).await);
            assert!(
                sql.contains(".followed_id)"),
                "R3: {agg}(b) over a bare node must resolve to the joined \
                 table's real id column, not the raw unbound alias `b` \
                 (deliberate, uniform-by-shape treatment across all \
                 aggregate names):\n{sql}"
            );
        }
    }

    /// #465/#476/#520 fixed the NULL-padding half of this shape (see history
    /// below); #529 R5 (adversarial review of the #529 shape-1 loud guard)
    /// found the query is STILL silently wrong for a different reason, and
    /// this now trips the same #529 guard as the rest of that family.
    ///
    /// History: `table_valid_columns` (the #476/#520 UNION branch
    /// NULL-padding helper, `to_sql_query.rs`) only consulted
    /// `property_mappings` when computing which physical columns are valid
    /// for a branch's own table — never `from_properties`/`to_properties`,
    /// the documented denormalized pattern where a node's real columns live
    /// there instead (`schemas/dev/flights_denormalized.yaml`'s `Airport`
    /// node has EMPTY `property_mappings`, with `code`/`city`/`state`
    /// entirely resolved via `from_node_properties`/`to_node_properties`).
    /// This made every branch of the 2-hop undirected direction-permutation
    /// UNION NULL-pad `b.city`, collapsing all distinct cities into one
    /// `NULL` row. Fixed by routing `table_valid_columns` through
    /// `NodeSchema::all_valid_physical_columns()`. #551 additionally fixed
    /// the outer `GROUP BY` to reference the node's real identity column
    /// (`origin_code`) instead of a coincidentally-exported display property.
    ///
    /// R5 finding (this round, prompted by adversarially re-verifying the
    /// #529 shape-1 guard rather than trusting this test's own "verified
    /// correct" claim): independently hand-computed ground truth from the
    /// live `db_denormalized` fixture proves the ABOVE fixes did NOT make
    /// this query correct — `SFO` appears in EXACTLY ONE flight row
    /// (`SFO->ORD`, graph degree 1), so ZERO valid 2-hop chains
    /// `(a)-[:FLIGHT]-(b)-[:FLIGHT]-(c)` can pass through it (both hops of
    /// the pattern would need to touch `SFO`, but only one edge does) — yet
    /// this query's live execution returns `San Francisco=2`, an impossible
    /// result. Root cause: the 4-way direction-permutation UNION's JOIN
    /// condition legitimately varies per branch (`t2.origin_code =
    /// t1.dest_code` vs. `t2.dest_code = t1.dest_code` etc. — correctly
    /// selecting which `t2` rows join), but the SELECTed/grouped column for
    /// `b` is ALWAYS `t2.origin_code`, even in the two branches whose JOIN
    /// tied `t2.dest_code` instead — the shared node in THOSE matches is
    /// actually `t2`'s DEST side, not its origin side, so grouping by
    /// `t2.origin_code` silently reports the wrong node identity. This is
    /// the exact "#529 shape 1 bug 3" mechanism (SELECT never alternates
    /// role with the branch) one join-hop removed from the WITH-clause
    /// anchor — this test's own prior "verified correct, 6 distinct rows"
    /// claim was mistaken (not independently re-derived from the raw
    /// fixture data at the time). Now correctly caught by the #529 shape-1
    /// loud guard (`table_role_dependent_property_names` /
    /// `render_plan/plan_builder_utils.rs`), widened in this round to check
    /// every alias reachable through a JOIN, not just the WITH clause's own
    /// FROM anchor.
    #[tokio::test]
    async fn denorm_with_aggregate_group_by_middle_node_via_join_known_broken_529() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let cypher = "MATCH (a:Airport)-[:FLIGHT]-(b:Airport)-[:FLIGHT]-(c:Airport) \
                      WITH b, count(*) AS n RETURN b.city, n";

        let err = try_render(&schema, cypher, SqlDialect::ClickHouse)
            .await
            .expect_err(
                "465/#529: this query now renders successfully — if the #529 \
                 shape-1 role-alternation bug was genuinely fixed (not just \
                 guarded), live-verify against the raw flights_denorm fixture \
                 (SFO has graph degree 1 — zero valid 2-hop chains should pass \
                 through it) before replacing this test with a regression test.",
            );
        assert!(
            err.contains("not yet supported") && err.contains("529"),
            "465/#529: expected the #529 shape-1 loud guard error, got a \
             different error:\n{err}"
        );

        // Determinism (even the loud-guard path must be deterministic).
        for _ in 0..5 {
            let again = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
            assert!(
                again.is_err(),
                "465/#529: nondeterministic guard — sometimes rendered \
                 successfully, sometimes errored"
            );
        }
    }
}

mod coupled_anchor_optional_family_504_508_529_530_471 {
    use super::*;

    /// #504 — KNOWN BROKEN, investigated and deliberately NOT fixed here.
    ///
    /// `MATCH (a:IP)-[:REQUESTED]->(d) OPTIONAL MATCH (d)-[:RESOLVED_TO]->(rip)
    /// RETURN a.ip, d.name, count(rip)` on the coupled `zeek_merged_test`
    /// schema renders `count(t1.answers)` with NO join/array-unnest at all —
    /// both REQUESTED and RESOLVED_TO are genuinely the SAME physical
    /// `dns_log` row (a correct `JoinStrategy::CoupledSameRow` decision —
    /// confirmed via `are_edges_coupled`, `graph_catalog/graph_schema.rs:1632`
    /// and `pattern_schema.rs:1009-1021`), so there is no missing JOIN to add.
    ///
    /// The actual bug: `RESOLVED_TO.to_id = "answers"` is a ClickHouse
    /// `Array(String)` column (schema: `edge_id: [uid, answers]`,
    /// `to_node_properties: { ip: answers }` on `ResolvedIP`), but the schema
    /// catalog has NO concept of an array-valued edge/property column
    /// anywhere — `map_clickhouse_type` (`src/graph_catalog/schema_types.rs`)
    /// collapses `Array(String)` to plain `String`, and neither
    /// `RelationshipSchema` nor `NodeSchema` carry a multiplicity/array flag.
    /// The analyzer's NULL-correct OPTIONAL-count rewrite (`count(rip)` ->
    /// `count(rip.answers)`, `projection_tagging.rs:1464-1467,1600`) resolves
    /// `rip.answers` through the owning edge's embedded column
    /// (`select_builder.rs` "Case 6a", ~1499-1526) straight to `t1.answers` —
    /// but `count(t1.answers)` in ClickHouse counts NON-NULL ROWS, not array
    /// ELEMENTS, so every dns_log row reports count=1 regardless of how many
    /// DNS answers it actually has (0 would also wrongly read 1, though no
    /// fixture row is empty today). A correct render needs a NEW capability —
    /// schema-level array-column awareness plus `ARRAY JOIN`/`LEFT ARRAY
    /// JOIN` wiring reusing the existing `ArrayJoinItem` render primitive
    /// (today only fed by UNWIND, see `render_plan/mod.rs:292` and
    /// `plan_builder.rs`'s `extract_array_join`) — a multi-file feature
    /// addition, not a bounded bug fix, so it is explicitly OUT OF SCOPE for
    /// this fix stream. `fold_optional_edge_node_join_with_predicate`
    /// (`plan_optimizer.rs:1479`) was ruled out: this query produces zero
    /// JOINs (`FROM zeek.dns_log AS t1` alone), so that pass never runs here.
    ///
    /// Confirmed identical wrong SQL for the PLAIN (non-optional) two-hop
    /// form too (`MATCH (a:IP)-[:REQUESTED]->(d) MATCH
    /// (d)-[:RESOLVED_TO]->(rip) ...`) — this is NOT an OPTIONAL-MATCH/
    /// NULL-extension bug at the SQL-shape level, it is the array-count
    /// semantics gap, which happens to be exposed via #504's OPTIONAL query.
    ///
    /// Live-verified ground truth (`SELECT "id.orig_h", query,
    /// length(answers) FROM zeek.dns_log`): `cdn.example.com` has 2 answers
    /// (`['93.184.216.34','93.184.216.35']`), all other rows have 1. Running
    /// the ACTUAL generated SQL live returns `count = 1` for ALL FOUR rows,
    /// including `cdn.example.com` — silently wrong (should be 2).
    ///
    /// If this test starts failing because the SQL shape changed, treat that
    /// as a PROMPT to live-verify the new behavior against the ground truth
    /// above (especially the `cdn.example.com` / count=2 row) before
    /// replacing this with a real regression test — do not assume a shape
    /// change alone means it's fixed.
    #[tokio::test]
    async fn coupled_array_valued_edge_count_wrong_no_unnest_known_broken_504() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql = normalize(
            &render(
                &schema,
                "MATCH (a:IP)-[:REQUESTED]->(d) \
                 OPTIONAL MATCH (d)-[:RESOLVED_TO]->(rip) \
                 RETURN a.ip, d.name, count(rip)",
                SqlDialect::ClickHouse,
            )
            .await,
        );

        // No JOIN at all — both edges genuinely collapse onto one dns_log row.
        assert!(
            !sql.contains("JOIN"),
            "#504 KNOWN BROKEN characterization stale — a JOIN appeared; if \
             array-unnest support was added, live-verify the cdn.example.com \
             row now returns count=2 before replacing this test:\n{sql}"
        );
        // The count() argument is still the raw array column, not an
        // ARRAY JOIN-unnested per-element reference — the actual bug. The
        // scan alias (t0/t1/...) is an internal counter detail, not part of
        // the characterization — match on the shape, not the exact alias.
        assert!(
            sql.contains("count(") && sql.contains(".answers)"),
            "#504 KNOWN BROKEN characterization stale — count() no longer \
             targets the raw `answers` array column directly; if array-unnest \
             (ARRAY JOIN) support was added, live-verify row-level counts \
             (esp. cdn.example.com = 2) before replacing this test:\n{sql}"
        );
        assert!(
            !sql.contains("ARRAY JOIN") && !sql.contains("arrayJoin"),
            "#504 KNOWN BROKEN characterization stale — an ARRAY JOIN/\
             arrayJoin() now appears, suggesting array-unnest support was \
             added; live-verify before replacing this test:\n{sql}"
        );

        // Determinism (even a known-broken render must be deterministic).
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:IP)-[:REQUESTED]->(d) \
                     OPTIONAL MATCH (d)-[:RESOLVED_TO]->(rip) \
                     RETURN a.ip, d.name, count(rip)",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(sql, again, "#504: nondeterministic render");
        }
    }

    /// #508 (FIXED, 2-table coupled case only): a textually-reversed OPTIONAL
    /// MATCH on the coupled `zeek_merged_test` schema —
    /// `MATCH (d:Domain) OPTIONAL MATCH (rip:ResolvedIP)<-[:RESOLVED_TO]-(d)`
    /// instead of the equivalent `MATCH (d:Domain) OPTIONAL MATCH
    /// (d)-[:RESOLVED_TO]->(rip:ResolvedIP)` — rendered TWO distinct wrong
    /// symptoms pre-fix: (1) `LEFT JOIN zeek.dns_log AS t1 ON 1 = 0` (an
    /// always-empty impossible join, even though `d` genuinely IS
    /// `RESOLVED_TO`'s `from`-node and a real join exists), and (2)
    /// `d.ip AS "rip.ip"` — `rip`'s own property silently attributed to the
    /// ANCHOR alias `d` (label conflation; `d`'s CTE doesn't even expose an
    /// `ip` column, so this specific case surfaced as a clean
    /// UNKNOWN_IDENTIFIER, but the same mechanism could silently misattribute
    /// a REAL column under a different schema shape).
    ///
    /// Root cause: `anchor_is_left` (`plan_builder.rs`'s
    /// `optional_denorm_union_anchor_is_left`) records a STRUCTURAL fact
    /// (which side of the `GraphRel` plan tree the anchor's standalone scan
    /// Union sits on) that both the JOIN-key derivation and the SELECT-list
    /// anchor-property rewrite WRONGLY treated as if it also meant "the
    /// anchor is the edge's `from`-node" — true for the common
    /// left-anchor/outgoing shape, but false here: reversing the arrow keeps
    /// `d` as `RESOLVED_TO`'s `from`-node while flipping which structural
    /// side its Union sits on. Fixed via `resolve_anchor_is_from_side`
    /// (`plan_builder_helpers.rs`), which tries the `anchor_is_left`-implied
    /// side first (a no-op for every already-working shape — verified
    /// against `denorm_optional_anchor_property_from_scan_cte_475`,
    /// `denorm_optional_edge_column_not_hijacked_by_anchor_475`, and
    /// `undirected_coupled_schema_anchor_cte_single_grain_b1`, all still
    /// passing byte-identical) and falls back to the opposite side only when
    /// the primary attempt can't resolve a mapping — so a genuinely
    /// label-impossible hop (e.g. an `IP` anchor for `RESOLVED_TO`, which
    /// expects `Domain`) still correctly falls through to the impossible-join
    /// `1 = 0` fallback (BOTH attempts fail).
    ///
    /// Scope: fixed for this 2-table coupled shape (`zeek_merged_test`'s
    /// `Domain`/`ResolvedIP` via `dns_log`). NOT independently verified against
    /// 3+-table coupling or other schema patterns (FK-edge, polymorphic,
    /// composite-id) — the ratchet-enforced dispatch (`edge_side_node_properties`,
    /// a `PatternSchemaContext`-adjacent schema-catalog API) means the fix is
    /// pattern-agnostic in principle, but only THIS shape was live-verified.
    ///
    /// Live-verified on the zeek fixture: the reversed query now renders
    /// BYTE-IDENTICAL to the forward query, and both execute returning all 4
    /// dns_log rows correctly matched (`example.com`, `malware.bad`,
    /// `google.com`, `cdn.example.com`), matching the forward direction's
    /// already-correct result.
    #[tokio::test]
    async fn coupled_reversed_optional_match_resolves_anchor_side_508() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let forward = normalize(
            &render(
                &schema,
                "MATCH (d:Domain) OPTIONAL MATCH (d)-[:RESOLVED_TO]->(rip:ResolvedIP) \
                 RETURN d.name, rip.ip",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        let reversed = normalize(
            &render(
                &schema,
                "MATCH (d:Domain) OPTIONAL MATCH (rip:ResolvedIP)<-[:RESOLVED_TO]-(d) \
                 RETURN d.name, rip.ip",
                SqlDialect::ClickHouse,
            )
            .await,
        );

        assert!(
            !reversed.contains("1 = 0"),
            "#508: reversed OPTIONAL MATCH must not fall into the \
             impossible-join fallback — a real join exists:\n{reversed}"
        );
        assert!(
            reversed.contains(r#".answers AS "rip.ip""#) && !reversed.contains(r#"d.answers"#),
            "#508: rip.ip must resolve from the LEFT-JOINed edge alias \
             (ResolvedIP's own `answers` column), not be conflated onto the \
             anchor `d`:\n{reversed}"
        );
        assert!(
            !reversed.contains(r#"d.ip AS "rip.ip""#),
            "#508: rip.ip must NOT be attributed to the anchor's `d.ip` — \
             `d`'s CTE doesn't even expose an `ip` column (label \
             conflation):\n{reversed}"
        );
        assert_eq!(
            forward, reversed,
            "#508: a textually-reversed but semantically identical OPTIONAL \
             MATCH must render identically to the forward form:\n\
             FORWARD:\n{forward}\nREVERSED:\n{reversed}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (d:Domain) OPTIONAL MATCH (rip:ResolvedIP)<-[:RESOLVED_TO]-(d) \
                     RETURN d.name, rip.ip",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(reversed, again, "#508: nondeterministic render");
        }
    }

    /// #543 (FIXED): a textually-reversed OPTIONAL MATCH on a SYMMETRIC
    /// coupled edge — `IP-[:ACCESSED]->IP` over `conn_log`, where BOTH
    /// endpoints have valid `from`/`to` property mappings (unlike #508's
    /// `Domain`/`ResolvedIP`, which is asymmetric — only one side ever
    /// resolves) — silently rendered a DIFFERENT, both-non-empty JOIN
    /// depending on arrow direction, because #508's
    /// `resolve_anchor_is_from_side` fallback only fires when the PRIMARY
    /// attempt finds NO mapping at all; on a symmetric edge the primary
    /// attempt always finds SOME (wrong) mapping, so the fallback never runs
    /// and the underlying bug survives untouched.
    ///
    /// Root cause (upstream of anything #508 touches): `GraphRel`'s doc
    /// comment establishes a codebase-wide invariant — `left` is ALWAYS the
    /// edge's `from`-side, `right` is ALWAYS the `to`-side, regardless of
    /// arrow direction (`compute_connection_aliases` swaps aliases for
    /// `Direction::Incoming` to enforce this). `traverse_connected_pattern_with_mode`
    /// (`query_planner/logical_plan/match_clause/traversal.rs`) has TWO
    /// separate branches for building an OPTIONAL MATCH's `GraphRel` — one for
    /// when the pattern's START alias is already bound (the common case,
    /// anchor written first), one for when the END alias is already bound
    /// (anchor written second, e.g. `MATCH (a) OPTIONAL MATCH (b)<-[:R]-(a)`).
    /// The FIRST branch already called `compute_connection_aliases` and
    /// swapped `left`/`right` node placement for `Direction::Incoming`
    /// (needed for #506). The SECOND branch did NOT — it unconditionally set
    /// `left_connection = start_node_alias`, `right_connection =
    /// end_node_alias` regardless of direction, so a `Direction::Incoming`
    /// pattern landing in this branch silently violated the `left=from`
    /// invariant: the anchor (bound alias, `end_node_alias`) ended up on
    /// `right` instead of `left`, and downstream JOIN-key/property resolution
    /// (which trusts `left=from` per the documented convention) picked the
    /// wrong physical column for BOTH the anchor and the optional node — a
    /// real, resolvable join, just wired backwards, not the "no mapping
    /// found" case #508's fallback detects.
    ///
    /// Fixed by making the second branch match the first: compute
    /// `left_conn`/`right_conn` via `compute_connection_aliases` and swap
    /// `left`/`right` node placement for `Direction::Incoming`, exactly
    /// mirroring the already-correct, already-tested first branch (this
    /// exact `anchor_is_left=false`-for-`Incoming` configuration is already
    /// produced and handled correctly by that branch, e.g. #506 — this fix
    /// makes the second branch produce it too instead of the opposite
    /// (wrong) pairing).
    ///
    /// Live-verified against `zeek_merged_test` (`conn_log`, 5 rows): forward
    /// and reversed now render byte-identical SQL and BOTH execute returning
    /// the SAME 7 rows: `(10.0.0.99, NULL)`, `(142.250.80.46, NULL)`,
    /// `(192.168.1.10, 10.0.0.99)`, `(192.168.1.10, 192.168.1.20)`,
    /// `(192.168.1.10, 93.184.216.34)`, `(192.168.1.20, 142.250.80.46)`,
    /// `(93.184.216.34, 192.168.1.10)` — matching `conn_log`'s
    /// `id.orig_h`/`id.resp_h` pairs exactly (a is genuinely the FROM side in
    /// both textual forms). Pre-fix, the reversed form instead rendered a
    /// backwards JOIN and returned a DIFFERENT, non-empty 5-row set with
    /// swapped/wrong pairings (e.g. `(10.0.0.99, 192.168.1.10)` — backwards;
    /// `conn_log` actually has `192.168.1.10 -> 10.0.0.99`, not the reverse).
    #[tokio::test]
    async fn coupled_symmetric_edge_reversed_optional_match_resolves_from_side_543() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let forward = normalize(
            &render(
                &schema,
                "MATCH (a:IP) OPTIONAL MATCH (a)-[:ACCESSED]->(b:IP) RETURN a.ip, b.ip",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        let reversed = normalize(
            &render(
                &schema,
                "MATCH (a:IP) OPTIONAL MATCH (b:IP)<-[:ACCESSED]-(a) RETURN a.ip, b.ip",
                SqlDialect::ClickHouse,
            )
            .await,
        );

        assert_eq!(
            forward, reversed,
            "#543: a textually-reversed but semantically identical OPTIONAL \
             MATCH over a SYMMETRIC coupled edge must render identically to \
             the forward form:\nFORWARD:\n{forward}\nREVERSED:\n{reversed}"
        );
        assert!(
            reversed.contains(r#"ON a.ip = "#) && reversed.contains(r#"."id.orig_h""#),
            "#543: the anchor `a` must join on the edge's FROM column \
             (`id.orig_h`) — it genuinely IS `ACCESSED`'s from-node in both \
             textual forms:\n{reversed}"
        );
        assert!(
            reversed.contains(r#"."id.resp_h" AS "b.ip""#),
            "#543: `b` must resolve from the edge's TO column \
             (`id.resp_h`):\n{reversed}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:IP) OPTIONAL MATCH (b:IP)<-[:ACCESSED]-(a) RETURN a.ip, b.ip",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(reversed, again, "#543: nondeterministic render");
        }
    }

    /// #529 shape 2 (FIXED): `WITH a, count(r) AS c` after an OPTIONAL
    /// undirected pattern on a coupled anchor (`MATCH (a:IP) OPTIONAL MATCH
    /// (a)-[r:ACCESSED]-(b:IP) WITH a, count(r) AS c RETURN a.ip, c`) emitted
    /// `GROUP BY a."id.orig_h"` — a RAW physical column reference against a
    /// `__union` derived table that only exposes the CTE column name
    /// (`p1_a_ip`) — invalid SQL (UNKNOWN_IDENTIFIER).
    ///
    /// Root cause: an undirected OPTIONAL MATCH is expanded (by
    /// `bidirectional_union.rs`) into a `Union` of two direction-permutation
    /// `GraphRel` branches BEFORE `plan_builder_helpers::
    /// find_inner_optional_denorm_graphrel` ever runs. That helper has no
    /// generic `Union` traversal arm (deliberately — a DIFFERENT caller,
    /// `to_render_plan_with_ctx`'s top-level OPTIONAL-denorm-Union detection,
    /// needs it to return `None` for exactly this outer-Union shape, so its
    /// OWN two-branch rendering runs instead of an incorrect single-branch
    /// delegation — regression-tested below). So the anchor was invisible to
    /// `denorm_scan_cte_anchor_id_property`/`_properties`
    /// (`plan_builder_helpers.rs`), which fell back to the raw physical
    /// column. Fixed by unwrapping a `Union` of branches ONLY inside
    /// `denorm_scan_cte_anchor_names_and_id_col` — the shared lookup behind
    /// those two specific callers — trying each branch in turn, leaving
    /// `find_inner_optional_denorm_graphrel` itself untouched.
    ///
    /// Live-verified on the zeek fixture: `a.ip = '192.168.1.10'` (which
    /// touches 4 distinct `conn_log` rows unambiguously, per the raw ground
    /// truth already established by `undirected_coupled_schema_anchor_cte_single_grain_b1`)
    /// returns `c = 4` — unaffected by the fix (matches the pre-existing,
    /// separately-tracked LEFT-JOIN NULL-extension behavior for this same
    /// mechanism; this test only locks the SQL becoming valid + the
    /// unambiguous count, not a general audit of undirected OPTIONAL MATCH
    /// counting semantics, which is out of scope here).
    #[tokio::test]
    async fn undirected_optional_with_aggregate_coupled_anchor_group_by_529() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let repro = "MATCH (a:IP) OPTIONAL MATCH (a)-[r:ACCESSED]-(b:IP) \
                     WITH a, count(r) AS c RETURN a.ip, c";
        let sql = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);

        assert!(
            !sql.contains(r#"GROUP BY a."id.orig_h""#),
            "#529 shape 2: GROUP BY must not reference the raw physical \
             column — the __union derived table only exposes the CTE \
             column:\n{sql}"
        );
        assert!(
            sql.contains("GROUP BY `p1_a_ip`") || sql.contains("GROUP BY \"p1_a_ip\""),
            "#529 shape 2: GROUP BY must reference the properly-quoted CTE \
             column exposed by the inner UNION:\n{sql}"
        );

        // Sibling B1 path (top-level OPTIONAL-denorm-Union detection) must be
        // unaffected: still renders its own full two-branch union, not a
        // single-branch delegation (the risk the narrow fix scoping avoids).
        let b1_repro = "MATCH (a:IP) OPTIONAL MATCH (a)-[r:ACCESSED]-(b:IP) \
                         RETURN a.ip, a.port, count(r) AS c";
        let b1_sql = normalize(&render(&schema, b1_repro, SqlDialect::ClickHouse).await);
        assert!(
            b1_sql.contains("__denorm_scan_a") && !b1_sql.contains("__denorm_scan_a_2"),
            "#529 shape 2 fix must not regress B1 (single shared anchor CTE):\n{b1_sql}"
        );
        assert_eq!(
            b1_sql
                .matches("LEFT JOIN zeek.conn_log AS r ON a.ip =")
                .count(),
            2,
            "#529 shape 2 fix must not regress B1 (both direction branches \
             LEFT JOIN against the anchor CTE):\n{b1_sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
            assert_eq!(sql, again, "#529 shape 2: nondeterministic render");
        }
    }

    /// #529 shape 1 — R4/R5: bugs 1+2 of the 3-bug package FIXED; bug 3
    /// deliberately left unfixed but now fails LOUDLY (a clean
    /// `RenderBuildError::UnsupportedFeature`) instead of silently returning
    /// wrong data. Full history below for context on what changed and why.
    ///
    /// CORRECTION (R5, adversarial review): earlier text here (and this
    /// round's own CHANGELOG entry) described pre-fix `main` as producing a
    /// "loud crash"/`UNKNOWN_IDENTIFIER` for this exact query. That was
    /// WRONG — live-verified (R5): the malformed alias below is used
    /// CONSISTENTLY as both its own definition and every reference to it, so
    /// ClickHouse parses it as one (oddly-named but valid) identifier and
    /// EXECUTES the query successfully, returning a single silently-wrong
    /// `(NULL, 0)` row (bug 2's NULL-padding gap collapses both UNION
    /// branches to `NULL` before bug 1's malformed alias ever matters at
    /// execution time — confirmed by running pre-fix `main`'s exact captured
    /// SQL directly against ClickHouse: `SELECT \N  0`, no error). Pre-fix
    /// `main` was ALREADY silently wrong for this query, not loudly broken —
    /// bugs 1+2 change the SHAPE of the wrongness (a single collapsed
    /// `NULL`/`0` row -> a plausible-looking, fully-formed but still-wrong
    /// multi-row table, per bug 3), which is exactly why bug 3's loud guard
    /// is necessary rather than optional polish.
    ///
    /// Original bug 1 (alias construction, FIXED): `MATCH (a:IP)-[r:ACCESSED]-
    /// (b:IP) WITH a, count(r) AS c RETURN a.ip, c` (a PLAIN, non-optional
    /// undirected self-edge feeding a `WITH`-aggregate) used to generate a
    /// malformed CTE column alias containing an embedded, doubled
    /// double-quote inside a backtick identifier: `` `r."id.orig_h"` `` — a
    /// syntactically-valid-but-bizarre identifier name, not a parse error
    /// (see correction above). Root cause: `build_union_inner_select`
    /// (`src/sql_generator/emitters/clickhouse/to_sql_query.rs`) reused a
    /// dialect-rendered VALUE expression (`r."id.orig_h"`, `PropertyValue::
    /// to_sql`'s correct double-quoting of a physical column that itself
    /// embeds a `.`, e.g. Zeek's `id.orig_h`) as a bare column ALIAS by
    /// feeding it straight into `quote_alias()`, which re-escaped the
    /// embedded `"` by doubling it. Fixed via a `split_agg_arg_col`/
    /// `agg_arg_alias_key` pair of pure functions (locate the quoted-suffix
    /// physical column by its outermost matching `"..."` pair rather than a
    /// naive `rsplit('.')`, which mis-splits INSIDE the quoted identifier)
    /// used consistently at all four sites that previously mis-parsed this
    /// same value-expression text: the alias-construction site itself,
    /// `agg_arg_col_valid_for_branch`'s physical-column extraction,
    /// `build_outer_aggregate_select`'s outer aggregate backtick-reference,
    /// and `build_aliased_group_by`'s analogous GROUP BY case.
    ///
    /// Original bug 2 (NULL-padding validity gap, FIXED): `table_valid_columns`
    /// (the #476 NULL-padding validity check) only ever consulted
    /// `NodeSchema::all_valid_physical_columns()` — a bare aggregate argument
    /// on the relationship variable itself (`count(r)`, normalized to `r`'s
    /// own edge_id, `uid`) named a column only the RELATIONSHIP schema knows
    /// about, so the check always returned false and BOTH UNION branches got
    /// `NULL AS "r.uid"` — `count(r)` rendered as valid SQL but ALWAYS
    /// returned 0, regardless of the true row count. Fixed via a new
    /// `RelationshipSchema::all_valid_physical_columns` companion accessor
    /// (`graph_catalog/graph_schema.rs`, covering `property_mappings`,
    /// `edge_id`/`from_id`/`to_id`, and `from_node_properties`/
    /// `to_node_properties`), folded into `table_valid_columns` alongside
    /// the existing node-schema check.
    ///
    /// Bug 3 (role alternation — STILL NOT FIXED, now loudly guarded): even
    /// with bugs 1+2 fixed, the rendered SQL's two UNION ALL branches were
    /// BYTE-IDENTICAL — both project `r."id.orig_h"` for `a`'s identity;
    /// neither ever projects `r."id.resp_h"`. For an UNDIRECTED self-edge
    /// this must alternate (exactly as the already-fixed, already-tested
    /// OPTIONAL sibling shape's `__denorm_scan_a` CTE does: one branch per
    /// role) — live execution with only bugs 1+2 fixed confirmed silently
    /// wrong results (rows belonging to the un-selected role dropped
    /// entirely; rows appearing in both roles double-counted).
    ///
    /// R4 root-cause (deeper than any prior round reached): traced through
    /// TWO independent, compounding causes, both upstream of SQL generation:
    /// (a) `bidirectional_union.rs`'s `apply_direction_combination_inner`
    /// builds a per-branch `column_swaps` map specifically to correct
    /// Incoming-direction denormalized node references, but only ever
    /// applies it to `LogicalPlan::Projection` items — its `LogicalPlan::
    /// WithClause` arm (and `transform_bidirectional`'s own `WithClause`
    /// arm) never threads it through `with_clause.items`, so a bare
    /// WITH-aggregate's pass-through/aggregate columns never get the
    /// correction. (b) Deeper still: even if that were wired up,
    /// `find_id_column_for_alias` (`render_plan/plan_builder.rs`) resolves a
    /// node's identity via the SAME static `NodeSchema::node_id` field
    /// regardless of which role (from/to) the node plays in a given
    /// direction branch — for a coupled/embedded table this is architecturally
    /// wrong (the physical identity column genuinely differs by role), and
    /// no per-branch override path reaches it from a WITH clause today. Fully
    /// correcting this needs either (a) threading `column_swaps`-equivalent
    /// role correction through `WithClause` items too, or (b) making
    /// `find_id_column_for_alias` (and the WITH-projection attachment in
    /// `build_chained_with_match_cte_plan`, which currently attaches ONE
    /// shared `SelectItems` to `rendered.select` and reuses it verbatim for
    /// EVERY UNION branch — confirmed harmless for the common case where a
    /// node's identity is role-INDEPENDENT, e.g. a normalized self-edge via a
    /// separate node table — role-aware for the coupled/embedded case
    /// specifically. Genuinely planner-level, cross-cutting work — not
    /// attempted this round.
    ///
    /// Guarded instead: `table_role_dependent_property_names`/
    /// `collect_property_accesses` and their call site in
    /// `build_chained_with_match_cte_plan` (`render_plan/
    /// plan_builder_utils.rs`) detect the exact precondition for this unfixed
    /// case — a multi-branch UNION whose WITH projection references a
    /// SPECIFIC property that is itself role-dependent (differs between
    /// from/to role) through ANY alias in scope, whether that alias is the
    /// FROM anchor or reached through a JOIN — and raise
    /// `RenderBuildError::UnsupportedFeature` instead of silently returning
    /// corrupted data.
    ///
    /// R5 (adversarial review) note: an EARLIER version of this guard was
    /// scoped to "no branch has any JOIN at all", reasoning that a multi-hop
    /// pattern's JOIN CONDITION (not its SELECT list) usually encodes role.
    /// That was live-verified WRONG — a 2-hop undirected chain grouping by
    /// the first hop's own anchor has a JOIN too (for the second hop), so it
    /// slipped through and silently corrupted results (see
    /// `undirected_two_hop_chain_grouped_by_anchor_known_broken_529_shape1`).
    /// Widened to check every alias in scope, which ALSO caught
    /// `denorm_with_aggregate_group_by_middle_node_no_null_collapse_465_blocking`
    /// (believed safe/fixed at the time) as ITSELF silently wrong — see
    /// `denorm_with_aggregate_group_by_middle_node_via_join_known_broken_529`.
    /// The widened guard checks the SPECIFIC property being accessed (not
    /// just "does this alias's table back a role-dependent node"), verified
    /// via `undirected_optional_with_aggregate_unaffected_by_529_guard_widening`
    /// not to false-positive on the already-fixed shape-2 OPTIONAL case
    /// below, where relationship alias `r` shares its physical table with
    /// the role-dependent `IP` node but only ever accesses `r`'s own
    /// role-independent `uid`.
    ///
    /// --- prior-round history (kept for context) ---
    ///
    /// This does NOT go through the OPTIONAL-denorm-CTE-anchor machinery
    /// (`find_inner_optional_denorm_graphrel` /
    /// `denorm_scan_cte_anchor_id_property`, shape 2's fix site) at all — no
    /// `OPTIONAL` keyword, no anchor CTE. R2's `normalize_union_branches`
    /// lead (`plan_builder_helpers.rs`) was a dead end — a look-alike
    /// mechanism, not the source (see below).
    ///
    /// R3 pinned the exact alias-construction mutation site with `gdb`
    /// (`build_union_inner_select`'s "add aggregate argument columns" loop)
    /// but deferred landing it after discovering it would unmask bugs 2 and 3
    /// above. #508/#530/#543/#471/#529-shape-2 are all fixed in this stream
    /// (R1 fixed #508/shape-2; R2 fixed #543/#530/#471).
    #[tokio::test]
    async fn undirected_plain_with_aggregate_bug1_2_fixed_bug3_loudly_guarded_529_shape1() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let repro = "MATCH (a:IP)-[r:ACCESSED]-(b:IP) WITH a, count(r) AS c RETURN a.ip, c";

        // Bug 3 (role alternation) is unfixed — must fail LOUDLY via a clean
        // Rust-level error, not silently return corrupted data and not crash
        // with a malformed-SQL ClickHouse parse error (the pre-R4 behavior).
        let err = try_render(&schema, repro, SqlDialect::ClickHouse)
            .await
            .expect_err(
                "#529 shape 1: bug 3 (role alternation) is still unfixed — this \
                 must still fail, just LOUDLY via a clean render error, not \
                 silently succeed with wrong data. If this now succeeds, bug 3 \
                 was actually fixed — replace this with a real regression test \
                 that live-verifies row-level correctness (see the doc comment \
                 above for the exact mechanism that would need to change).",
            );
        assert!(
            err.contains("not yet supported") && err.contains("529"),
            "#529 shape 1: expected the dedicated #529-shape-1 loud guard \
             error, got a different error — investigate before assuming bug 3 \
             is still the cause:\n{err}"
        );

        // Determinism (even the loud-guard path must be deterministic).
        for _ in 0..5 {
            let again = try_render(&schema, repro, SqlDialect::ClickHouse).await;
            assert!(
                again.is_err(),
                "#529 shape 1: nondeterministic guard — sometimes rendered \
                 successfully, sometimes errored"
            );
        }
    }

    /// #529 shape 1 regression (bugs 1+2 FIXED): the DIRECTED form of the
    /// same pattern (`-[r:ACCESSED]->` instead of `-[r:ACCESSED]-`) has only
    /// ONE direction, so it never enters the multi-branch UNION bug 3's loud
    /// guard targets — this must render and execute correctly, proving bugs 1
    /// (malformed alias) and 2 (NULL-padding gap for relationship-owned
    /// columns) are genuinely fixed, not just guarded away.
    ///
    /// Live-verified against the zeek fixture (`zeek.conn_log`, `cg query`):
    /// returns exactly 3 rows, `count(r)` grouped by `a`'s `id.orig_h`,
    /// matching independently-computed ground truth from the raw table data
    /// (each row's `count` equals the number of `conn_log` rows with that
    /// `id.orig_h` as the ORIGIN — directed, so no role alternation is
    /// needed here at all).
    #[tokio::test]
    async fn undirected_plain_with_aggregate_directed_variant_regression_529_shape1() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql = normalize(
            &render(
                &schema,
                "MATCH (a:IP)-[r:ACCESSED]->(b:IP) WITH a, count(r) AS c RETURN a.ip, c",
                SqlDialect::ClickHouse,
            )
            .await,
        );

        // No malformed embedded-quote alias (bug 1) and no unconditional
        // NULL-padding of `r`'s own edge_id column (bug 2) — both columns
        // render as real references.
        assert!(
            !sql.contains(r#"AS "r.""#),
            "#529 shape 1 bug 1 regression — malformed doubled-quote alias \
             reappeared:\n{sql}"
        );
        assert!(
            !sql.contains(r#"NULL AS "r."#),
            "#529 shape 1 bug 2 regression — r's own edge_id column is being \
             NULL-padded again (count(r) would always return 0):\n{sql}"
        );
        assert!(
            sql.contains(r#"r."id.orig_h""#) && sql.contains("r.uid"),
            "#529 shape 1: expected a's identity (r.\"id.orig_h\") and r's own \
             edge_id (r.uid) to both be real column references:\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:IP)-[r:ACCESSED]->(b:IP) WITH a, count(r) AS c RETURN a.ip, c",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(
                sql, again,
                "#529 shape 1 directed variant: nondeterministic render"
            );
        }
    }

    /// #529 shape 1, R5 (adversarial review finding, now fixed): the guard's
    /// FIRST scoping (v1: "no branch has any JOIN") missed a 2-hop undirected
    /// CHAIN grouping by the first hop's own anchor — `(a)-[r:ACCESSED]-
    /// (x:IP)-[r2:ACCESSED]-(c:IP) WITH a, count(r) AS n ...` has a JOIN (for
    /// the second hop), so v1 let it through even though `a`'s own identity
    /// (via `r`, the FROM anchor) never alternates role, same as the
    /// single-hop shape. Live-verified pre-fix (adversarial review): silently
    /// dropped 2 of 5 groups and corrupted every remaining count on both
    /// `zeek_merged_test.yaml` and `flights_denormalized.yaml`. The v3 guard
    /// (property-name-aware, checks every alias reachable via FROM or any
    /// JOIN, not just hop count or join presence) now catches this — locks
    /// BOTH the anchor-grouped form (this test) and, via
    /// `denorm_with_aggregate_group_by_middle_node_via_join_known_broken_529`,
    /// the "group by a node reached through a JOIN" form the adversarial
    /// review's re-verification additionally uncovered.
    #[tokio::test]
    async fn undirected_two_hop_chain_grouped_by_anchor_known_broken_529_shape1() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let cypher = "MATCH (a:IP)-[r:ACCESSED]-(x:IP)-[r2:ACCESSED]-(c:IP) \
                      WITH a, count(r) AS n RETURN a.ip, n";

        let err = try_render(&schema, cypher, SqlDialect::ClickHouse)
            .await
            .expect_err(
                "#529 shape 1 (2-hop chain by anchor): this query now renders \
                 successfully — if bug 3 (role alternation) was genuinely \
                 fixed, live-verify row-level counts against hand-enumerated \
                 ground truth from the raw conn_log fixture before replacing \
                 this test with a regression test.",
            );
        assert!(
            err.contains("not yet supported") && err.contains("529"),
            "#529 shape 1 (2-hop chain by anchor): expected the #529 shape-1 \
             loud guard error, got a different error:\n{err}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
            assert!(
                again.is_err(),
                "#529 shape 1 (2-hop chain by anchor): nondeterministic guard"
            );
        }
    }

    /// #529 shape 1, R6 (adversarial review finding, now fixed): the guard's
    /// property-access collector (`collect_property_accesses`,
    /// `render_plan/plan_builder_utils.rs`) hand-rolled a PARTIAL `match`
    /// over `RenderExpr` (only `PropertyAccessExp`/`OperatorApplicationExp`/
    /// `AggregateFnCall`/`ScalarFnCall`/`Case`'s `when_then`+`else_expr`),
    /// silently dropping everything else via a `_ => {}` catch-all —
    /// including `List`, so a role-dependent property reached through a
    /// LIST LITERAL in the WITH projection (rather than a bare select item)
    /// slipped past the guard entirely. Live-verified pre-fix (adversarial
    /// review): `WITH count(r) AS c, [a.ip] AS tags RETURN c, tags` rendered
    /// and EXECUTED with no guard firing, both UNION branches emitting
    /// byte-identical non-alternating SQL — the same corruption signature as
    /// the bare-select-item shapes above (2 of 5 IPs dropped, wrong counts),
    /// just reached through the list literal instead.
    ///
    /// Fixed by making `collect_property_accesses`'s match EXHAUSTIVE over
    /// every `RenderExpr` variant (no `_` catch-all — a future variant
    /// addition is now a compile error here, not a silent gap) — see that
    /// function's own doc comment for the full variant-by-variant audit and
    /// `collect_property_accesses_tests` (same file) for direct per-variant
    /// unit coverage (`List`, `MapLiteral`, `ArraySubscript`/`ArraySlicing`,
    /// simple-CASE's `expr` scrutinee, `ReduceExpr`, `InSubquery`).
    #[tokio::test]
    async fn undirected_with_aggregate_role_property_inside_list_literal_known_broken_529_shape1() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let cypher =
            "MATCH (a:IP)-[r:ACCESSED]-(b:IP) WITH count(r) AS c, [a.ip] AS tags RETURN c, tags";

        let err = try_render(&schema, cypher, SqlDialect::ClickHouse)
            .await
            .expect_err(
                "#529 shape 1 (role property inside a list literal): this query \
                 now renders successfully — if this is a genuine fix (not \
                 another guard-scoping gap), live-verify row-level counts \
                 against hand-enumerated ground truth from the raw conn_log \
                 fixture before replacing this test with a regression test.",
            );
        assert!(
            err.contains("not yet supported") && err.contains("529"),
            "#529 shape 1 (role property inside a list literal): expected the \
             #529 shape-1 loud guard error, got a different error:\n{err}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = try_render(&schema, cypher, SqlDialect::ClickHouse).await;
            assert!(
                again.is_err(),
                "#529 shape 1 (role property inside a list literal): \
                 nondeterministic guard"
            );
        }
    }

    /// #529 shape 2 (FIXED, regression guard against the R5 guard-widening):
    /// the already-fixed OPTIONAL+undirected+WITH-aggregate shape
    /// (`MATCH (a:IP) OPTIONAL MATCH (a)-[r:ACCESSED]-(b:IP) WITH a,
    /// count(r) AS c ...`) resolves `a` through the `__denorm_scan_a` anchor
    /// CTE (a synthetic table name, not a role-dependent schema table) and
    /// `count(r)` through `r`'s own role-INDEPENDENT edge_id (`uid`) — it
    /// must NOT trip the #529 shape-1 guard. An earlier draft of the R5
    /// guard widening (checking "does this alias's TABLE back a
    /// role-dependent node" instead of "does this SPECIFIC PROPERTY resolve
    /// to a role-dependent column") was too coarse and flagged `r.uid`
    /// merely because `r` and the role-dependent `IP` node happen to share
    /// the same physical `conn_log` table — caught by this exact test before
    /// landing.
    #[tokio::test]
    async fn undirected_optional_with_aggregate_unaffected_by_529_guard_widening() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql = normalize(
            &render(
                &schema,
                "MATCH (a:IP) OPTIONAL MATCH (a)-[r:ACCESSED]-(b:IP) \
                 WITH a, count(r) AS c RETURN a.ip, c",
                SqlDialect::ClickHouse,
            )
            .await,
        );
        assert!(
            sql.contains("__denorm_scan_a"),
            "#529 shape 2 regressed by the R5 guard widening — expected the \
             already-fixed anchor-CTE shape to still render:\n{sql}"
        );
    }

    /// #530 (FIXED): an inline property-map filter on an OPTIONAL MATCH
    /// anchor — `MATCH (a:Airport {code: 'JFK'}) OPTIONAL MATCH
    /// (a)-[r:FLIGHT]-(b:Airport) RETURN a.code, r, b.code` on the
    /// denormalized flights schema — used to render `WHERE a.code = 'JFK'`
    /// inside BOTH branches of the anchor scan CTE (`__denorm_scan_a`), where
    /// the real physical columns are `origin_code`/`dest_code` — no `code`
    /// column exists on `db_denormalized.flights_denorm` — UNKNOWN_IDENTIFIER.
    /// The outer query's OWN WHERE (`r.origin_code = 'JFK'` /
    /// `r.dest_code = 'JFK'`, from the SAME inline map, folded into the main
    /// GraphRel's predicate) already rendered correctly — confirming this was
    /// a SEPARATE site from #519's fix (`collect_graphrel_predicates`).
    ///
    /// Root cause (pinned more precisely than the original filing, and more
    /// precisely than the R1 investigation, which pointed at
    /// `union_distribution.rs`'s Filter-over-Union distribution): the actual
    /// site is `FilterIntoGraphRel`'s `GraphNode` arm
    /// (`src/query_planner/optimizer/filter_into_graph_rel.rs`, the
    /// `LogicalPlan::GraphNode(graph_node)` case) — it fetches the anchor's
    /// filters from `plan_ctx`'s shared per-alias `TableCtx` (still holding
    /// the RAW, unmapped Cypher property name, e.g. `code`) and injects them
    /// unchanged into `ViewScan.view_filter`. After `type_inference.rs`'s
    /// `materialize_standalone_denorm_scans` expands the denormalized
    /// anchor's single (Empty) scan into a `Union` of per-role branches
    /// (`GraphNode(ViewScan)` for `origin` and `dest`, EACH still carrying the
    /// SAME alias `a` but its OWN concrete `property_mapping` — `code` ->
    /// `origin_code` for one branch, `code` -> `dest_code` for the other),
    /// this optimizer pass visits BOTH branches under that shared alias and
    /// injects the identical raw predicate into each, instead of remapping it
    /// through THAT branch's own resolved mapping.
    ///
    /// Fixed via a new `rewrite_expression_with_concrete_property_map`
    /// (`query_planner/logical_expr/expression_rewriter.rs`) — rewrites a
    /// `PropertyAccessExp` for one target alias using an ALREADY-CONCRETE,
    /// per-branch `property_mapping` (not a schema/label lookup, which cannot
    /// distinguish a node's `from`-role from its `to`-role) — applied at the
    /// `FilterIntoGraphRel` injection site, gated on `view_scan.is_denormalized`
    /// so non-denormalized scans (the overwhelming majority) are a complete
    /// no-op. A second, defense-in-depth instance of the SAME bug shape
    /// (predicate cloned unchanged across denormalized Union branches) was
    /// also fixed in `union_distribution.rs`'s and
    /// `materialize_standalone_denorm_scans`'s own `Filter`-over-`Union`
    /// arms via a shared `remap_predicate_for_denorm_union_branch` helper,
    /// though neither of those two sites is actually exercised by this
    /// specific repro (confirmed via targeted tracing) — kept for
    /// consistency in case a differently-shaped query reaches them.
    ///
    /// Live-verified against `db_denormalized`: the fixed SQL executes and
    /// returns exactly 2 rows — `flight_id=1` (`LAX`->`JFK`) and `flight_id=5`
    /// (`JFK`->`LAX`) — matching
    /// `SELECT origin_code, dest_code, flight_id FROM db_denormalized.flights_denorm
    /// WHERE origin_code='JFK' OR dest_code='JFK'` (2 rows: exactly those two).
    #[tokio::test]
    async fn denorm_optional_inline_map_filter_resolves_per_branch_530() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = normalize(
            &render(
                &schema,
                "MATCH (a:Airport {code: 'JFK'}) OPTIONAL MATCH (a)-[r:FLIGHT]-(b:Airport) \
                 RETURN a.code, r, b.code",
                SqlDialect::ClickHouse,
            )
            .await,
        );

        assert!(
            !sql.contains("WHERE a.code = 'JFK'"),
            "#530: the anchor scan CTE's internal filter must not reference \
             the raw, unmapped `a.code` — `db_denormalized.flights_denorm` \
             has no `code` column:\n{sql}"
        );
        assert!(
            sql.contains("WHERE a.origin_code = 'JFK'"),
            "#530: the origin-role branch of the anchor scan CTE must filter \
             on its own physical column `origin_code`:\n{sql}"
        );
        assert!(
            sql.contains("WHERE a.dest_code = 'JFK'"),
            "#530: the dest-role branch of the anchor scan CTE must filter \
             on its own physical column `dest_code`:\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:Airport {code: 'JFK'}) OPTIONAL MATCH (a)-[r:FLIGHT]-(b:Airport) \
                     RETURN a.code, r, b.code",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(sql, again, "#530: nondeterministic render");
        }
    }

    /// #471 (FIXED): `MATCH (a:Airport) RETURN a.state, count(*) ORDER BY
    /// a.state` on the denormalized flights schema used to emit
    /// `ORDER BY a.origin_state` — the raw DB column under the anchor's stale
    /// Cypher alias — instead of the `__union` derived table's exposed
    /// `` `a.state` `` alias (which the SELECT list and GROUP BY, in the SAME
    /// query, already correctly use). Live-confirmed pre-fix:
    /// `UNKNOWN_IDENTIFIER 'a.origin_state'` — the query WITHOUT `ORDER BY`
    /// executed correctly.
    ///
    /// Root cause (pinned more precisely than the original filing, and one
    /// analyzer stage further upstream than `to_sql_query.rs`'s ORDER BY
    /// matching, which the original filing suspected): `FilterTagging`'s
    /// `LogicalPlan::OrderBy` case (`query_planner/analyzer/filter_tagging.rs`)
    /// calls `apply_property_mapping` on every ORDER BY item — `GroupBy`'s
    /// own case, right above it, never calls this at all, and stays
    /// correctly unmapped for the EXACT same alias/property. For a
    /// standalone (no owning-edge role context) denormalized node reference,
    /// that mapping bottoms out in `ViewResolver::resolve_node_property_with_role`'s
    /// `role=None` arm, which — by its own doc comment — picks
    /// `from_properties` FIRST as an arbitrary "default for node-only
    /// queries", noting "UNION ALL for both positions is handled at a higher
    /// level." True for SELECT (`property_expansion` walks each Union branch
    /// independently and re-resolves per branch) but NOT for ORDER BY, which
    /// took that guessed column (`origin_state`) as final — the unmapped
    /// `to_sql_query.rs`-level fallback chain (alias-text match ->
    /// `same_property_ref` -> expression-text match -> `unambiguous_column_match`)
    /// could never bridge a raw mapped DB column back to the union's own
    /// already-resolved alias.
    ///
    /// Fixed by skipping the mapping for exactly this ambiguous case — a new
    /// `FilterTagging::order_by_property_is_ambiguous_denorm_standalone`
    /// check (true only when: the item is a `PropertyAccessExp`; the alias's
    /// node has NO owning-edge role context via `find_owning_edge_for_node`;
    /// the node is denormalized; AND the property maps to DIFFERENT physical
    /// columns on the from vs to role) leaves the ORDER BY item in the SAME
    /// unmapped form GROUP BY/SELECT already use — so the existing
    /// alias-text ORDER BY-to-SELECT matching in `to_sql_query.rs` (which
    /// already handles GROUP BY's identical unmapped form) finds it, with NO
    /// changes needed there. A property that exists on only one role, or
    /// maps to the SAME column on both, is left mapped exactly as before
    /// (genuinely unambiguous — no behavior change for those or for any
    /// non-denormalized/edge-embedded case).
    ///
    /// Live-verified against `db_denormalized`: the fixed SQL executes and
    /// returns all 6 states in ascending order (`AZ, CA, CO, GA, IL, NY`),
    /// each with `count(*) = 1` — matching the no-`ORDER BY` form's
    /// already-correct 6-state/count-1-each result, just now correctly
    /// ordered.
    #[tokio::test]
    async fn denorm_union_aggregate_order_by_resolves_union_alias_471() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = normalize(
            &render(
                &schema,
                "MATCH (a:Airport) RETURN a.state, count(*) ORDER BY a.state",
                SqlDialect::ClickHouse,
            )
            .await,
        );

        assert!(
            !sql.contains("ORDER BY a.origin_state") && !sql.contains("ORDER BY a.dest_state"),
            "#471: ORDER BY must not reference either role's raw physical \
             column (the inner UNION branches legitimately reference them \
             as their OWN source columns — only the ORDER BY clause itself \
             must not):\n{sql}"
        );
        assert!(
            sql.contains(r#"SELECT `a.state` AS "a.state""#),
            "#471: the outer SELECT must still resolve through the union's \
             `a.state` alias:\n{sql}"
        );
        assert!(
            sql.contains("ORDER BY `a.state` ASC") || sql.contains(r#"ORDER BY "a.state" ASC"#),
            "#471: ORDER BY must reference the same quoted union alias the \
             SELECT list and GROUP BY already use:\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(
                &render(
                    &schema,
                    "MATCH (a:Airport) RETURN a.state, count(*) ORDER BY a.state",
                    SqlDialect::ClickHouse,
                )
                .await,
            );
            assert_eq!(sql, again, "#471: nondeterministic render");
        }
    }

    /// #555 (disclosed trade-off of #471, now resolved): `MATCH (n:Airport)
    /// RETURN n.city ORDER BY n.state` — ordering by a role-ambiguous denorm
    /// property that is NOT itself projected in SELECT. #471's ambiguity
    /// guard correctly leaves `n.state` unmapped (same as this test's #471
    /// sibling above), but `to_sql_query.rs`'s "only case handled" fallback
    /// — reuse an existing SELECT item's alias-mapped expression — has
    /// nothing to reuse here (`state` was never requested), so the raw,
    /// physically-nonexistent `n.state` reached ClickHouse directly:
    /// `UNKNOWN_IDENTIFIER 'n.state'` (verified pre-fix, live, against
    /// `db_denormalized` — the query WITHOUT `ORDER BY` executes fine).
    ///
    /// Fixed by extending `add_order_by_columns_to_select`'s standalone
    /// (no-path-context) branch: when the existing-SELECT-alias match finds
    /// nothing, fall back to reading THIS branch's own already role-resolved
    /// `ViewScan.property_mapping` directly (`resolve_standalone_denorm_order_by_expr`
    /// / `union_branch_own_property_column`) — the exact same per-branch
    /// source `property_expansion` draws SELECT's own denorm columns from.
    /// Each UNION branch now projects its OWN role-correct physical column
    /// (`origin_state` on the from-role branch, `dest_state` on the to-role
    /// branch) as its `__order_col_N` helper, mirroring the #546 typed
    /// `id()` salvage key's "resolve per branch, never guess once" shape.
    ///
    /// Live-verified against `db_denormalized` (2026-07-11): the fixed SQL
    /// executes and returns cities ordered by state ascending (Phoenix/AZ,
    /// Los Angeles+San Francisco/CA, Denver/CO, Atlanta/GA, Chicago/IL,
    /// New York/NY) — previously `Code: 47 UNKNOWN_IDENTIFIER`.
    #[tokio::test]
    async fn denorm_order_by_unprojected_ambiguous_prop_resolves_per_branch_555() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = render(
            &schema,
            "MATCH (n:Airport) RETURN n.city ORDER BY n.state",
            SqlDialect::ClickHouse,
        )
        .await;

        assert!(
            sql.contains(r#"n.origin_state AS "__order_col_0""#)
                && sql.contains(r#"n.dest_state AS "__order_col_0""#),
            "#555: each branch must project ITS OWN role-correct physical \
             column as the order helper — from-role branch on origin_state, \
             to-role branch on dest_state:\n{sql}"
        );
        assert!(
            !sql.contains(r#"n.state AS "__order_col_0""#),
            "#555: must never emit the raw, physically-nonexistent `n.state` \
             (the pre-fix UNKNOWN_IDENTIFIER shape):\n{sql}"
        );
        assert!(
            sql.contains("ORDER BY __union.`__order_col_0` ASC"),
            "#555: outer query must order by the per-branch helper column:\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = render(
                &schema,
                "MATCH (n:Airport) RETURN n.city ORDER BY n.state",
                SqlDialect::ClickHouse,
            )
            .await;
            assert_eq!(sql, again, "#555: nondeterministic render");
        }
    }
}

/// #524/#528/#538/#525: a batch of VLP CTE definition/binding bugs surfaced by
/// the #486/#489/#494/#500/#511/#517/#518/#521 adversarial review
/// (2026-07-10). #524 and #538 are fixed here; #528 is characterized as a
/// deeper, still-open bug (deliberately left unfixed — see its test's doc
/// comment); #525 is a documented, non-currently-broken robustness finding
/// (no code change — see the doc comment on this module for details).
mod vlp_cte_binding_family_524_528_538_525 {
    use super::*;

    /// #524: a fixed-length hop BEFORE a variable-length hop on a
    /// denormalized (virtual-node) schema — `(a)-[:REL1]->(b)-[:REL2*1..N]->(c)`
    /// — used to emit an unconditional (`ON 1 = 1`) JOIN for the leading fixed
    /// hop, because `PlanCtx`'s VLP-endpoint registry only learned that `b`
    /// was a VLP endpoint once the VLP relationship's OWN `GraphRel` was
    /// visited by `collect_graph_joins`'s single left-to-right pass — which
    /// happens AFTER the leading fixed hop's JOIN is generated. On a
    /// denormalized schema (no separate node table) the unconditional JOIN
    /// referenced a nonexistent column (`t.start_dest_code`), live-verified
    /// pre-fix to crash with ClickHouse `Code: 47 UNKNOWN_IDENTIFIER`.
    ///
    /// #521 (merged earlier) already fixed the REVERSE ordering (VLP hop
    /// visited first, so a LATER fixed hop already sees the endpoint) via the
    /// same `PlanCtx::is_vlp_endpoint`/`get_vlp_join_reference` machinery in
    /// `join_generation.rs`. This fixes the forward ordering by adding
    /// `GraphJoinInference::pre_register_vlp_endpoints` — a pre-pass that
    /// walks the WHOLE pattern tree for each join-collection scope and
    /// registers every required VLP's endpoints in `PlanCtx` BEFORE the
    /// main traversal begins, so visitation order no longer matters.
    ///
    /// Live-verified against `db_denormalized`: the fixed query returns rows
    /// (e.g. `ATL -> JFK`, `LAX -> LAX`, …) matching a hand-written raw-JOIN
    /// ground-truth query exactly (19 distinct (a,b,c) triples for `*1..2`).
    #[tokio::test]
    async fn vlp_fixed_leading_leg_denormalized_correlates_to_vlp_cte_524() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = render(
            &schema,
            "MATCH (a:Airport)-[:FLIGHT]->(b:Airport)-[:FLIGHT*1..2]->(c:Airport) \
             RETURN a.code, c.code",
            SqlDialect::ClickHouse,
        )
        .await;

        assert!(
            sql.to_uppercase().contains("WITH RECURSIVE"),
            "expected a genuine VLP recursive CTE for b:Airport-[*1..2]->c:Airport: {sql}"
        );
        assert!(
            !sql.contains("ON 1 = 1") && !sql.contains("ON 1=1"),
            "the leading a->b FLIGHT hop must emit a real correlated JOIN, \
             not an unconditional `ON 1 = 1` marker: {sql}"
        );
        assert!(
            sql.contains("t.start_id"),
            "the leading leg's JOIN condition must correlate against the \
             VLP CTE's start_id column: {sql}"
        );
    }

    /// #524 (companion correctness bug, found while verifying the fix above
    /// against live data): the denormalized VLP recursive CTE's `start_id`
    /// column was being REASSIGNED on every recursive hop
    /// (`next.{from_col} as start_id`, i.e. `vp.end_id` restated) instead of
    /// threaded through unchanged (`vp.start_id as start_id`) — silently
    /// drifting to the latest intermediate node for any `hop_count > 1` row.
    /// This is invisible for `*1..1`-equivalent single-hop VLP (the only row
    /// shape is the base case, where `start_id` is still correct), but
    /// corrupts exactly the correlation the #524 fix above depends on for
    /// real multi-hop paths: `t1.dest_code = t.start_id` would join the
    /// leading hop against the WRONG (drifted) node for `hop_count == 2`
    /// rows. Fixed in `DenormalizedCteStrategy::generate_recursive_case_sql`
    /// (`cte_manager/mod.rs`) by preserving `vp.start_id`, mirroring the
    /// already-correct pattern used one function away
    /// (`add_recursive_property_selections`'s `vp.start_{property}`) and the
    /// non-denormalized `TraditionalCteStrategy`/`FkEdgeCteStrategy`, which
    /// already did this correctly.
    ///
    /// Live-verified: with the fix, `SELECT DISTINCT a,b,c FROM
    /// (leading-hop-correlated 2-hop VLP query)` on `db_denormalized`
    /// produces EXACTLY the same 19-row set as a hand-written raw-JOIN
    /// ground-truth query (e.g. includes `LAX->ATL->JFK` via the 2-hop
    /// `ATL->LAX->JFK` VLP leg — impossible to get right if `start_id` had
    /// drifted to `LAX` instead of staying `ATL`).
    #[tokio::test]
    async fn denorm_vlp_recursive_start_id_threaded_not_reassigned_524() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = render(
            &schema,
            "MATCH (b:Airport)-[:FLIGHT*1..2]->(c:Airport) RETURN b.code, c.code",
            SqlDialect::ClickHouse,
        )
        .await;

        // The recursive term must thread the CTE's own start_id forward...
        assert!(
            sql.contains("vp.start_id as start_id"),
            "recursive case must carry `vp.start_id` forward unchanged: {sql}"
        );
        // ...and must NOT reassign it to the current hop's own origin column
        // (which is just `vp.end_id` restated under a different name).
        assert!(
            !sql.contains("next.origin_code as start_id"),
            "recursive case must not reassign start_id to next.origin_code \
             (silently drifts to the latest intermediate node): {sql}"
        );
    }

    /// #538: `id()`/GROUP BY on a denormalized virtual-node VLP endpoint with
    /// a bare any-type pattern (`-[r]-`, no relationship-type label) crashed
    /// `Code 47 UNKNOWN_IDENTIFIER` on a column (e.g. `t.end_query`) the VLP
    /// CTE never defines — it only exposes the generic `end_id`/`start_id`
    /// columns. Two independent bugs, both root-caused via targeted tracing:
    ///
    /// 1. `select_builder.rs`'s `id(alias)` SELECT-item resolution looked up
    ///    the node's schema-declared `node_id` column name (e.g. Domain's
    ///    `node_id: query` in `schemas/dev/zeek_merged_test.yaml`) and
    ///    prefixed it `end_`/`start_` — correct for an ordinary denormalized
    ///    node with a real per-type property mapping, but wrong for a
    ///    multi-type VLP endpoint, whose CTE only ever exposes the generic
    ///    `end_id`/`start_id` columns. Fixed by checking
    ///    `PlanCtx::get_vlp_endpoint` FIRST (the same mechanism
    ///    `join_generation.rs` already uses for the analogous #524 problem)
    ///    and resolving directly to `start_id`/`end_id` when the alias is a
    ///    VLP endpoint — bypassing the schema `node_id` lookup entirely.
    ///    (Note: `typed_var.source()` is NOT a reliable signal here — it
    ///    still reports `VariableSource::Match` for a VLP-endpoint node at
    ///    this point in the pipeline, confirmed live.)
    /// 2. `plan_builder.rs`'s `find_id_column_for_alias` (used for GROUP
    ///    BY/ORDER BY `id()` resolution) tried the actual GraphNode's real
    ///    `ViewScan.id_column` BEFORE its own "VLP ENDPOINT FALLBACK" — for a
    ///    multi-type VLP endpoint, type inference on an unlabeled/any-type
    ///    node attaches a ViewScan for ONE candidate label purely for
    ///    planning purposes, and this ordering trusted that per-type column
    ///    over the generic CTE column that's actually correct. Fixed by
    ///    checking `variable_length.is_some() && labels.len() > 1`
    ///    (multi-type VLP) FIRST and returning the generic start_id/end_id
    ///    fallback immediately in that case, before ever trusting a found
    ///    GraphNode's per-type id column. Single-type VLP is unaffected —
    ///    unchanged ordering there already resolved correctly.
    ///
    /// Live-verified on the zeek fixture: `id(b)` returns real values
    /// (`google.com`, `192.168.1.10`, …) with correct per-key counts, and
    /// `RETURN b, count(*)` groups correctly by node identity across mixed
    /// IP/Domain endpoint types.
    #[tokio::test]
    async fn denorm_virtual_node_vlp_endpoint_id_and_group_by_resolve_to_generic_cte_column_538() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");

        let sql = render(
            &schema,
            "MATCH (a:IP)-[r*1..2]-(b) RETURN id(b), count(*)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("t.end_id") && sql.contains("GROUP BY t.end_id"),
            "id(b) SELECT item and its GROUP BY key must both resolve to the \
             VLP CTE's generic end_id column, not a per-type schema column \
             like end_query: {sql}"
        );
        assert!(
            !sql.contains("end_query"),
            "must not reference the nonexistent end_query column: {sql}"
        );

        let sql2 = render(
            &schema,
            "MATCH (a:IP)-[r*1..2]-(b) RETURN b, count(*)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql2.contains(r#"t.end_id AS "b.id""#) && sql2.contains("GROUP BY t.end_id"),
            "bare RETURN b's id column and its GROUP BY key must both \
             resolve to the VLP CTE's generic end_id column: {sql2}"
        );
        assert!(
            !sql2.contains("end_query"),
            "must not reference the nonexistent end_query column: {sql2}"
        );
    }

    /// #528 — FIXED (this branch); this replaces the prior KNOWN-BROKEN
    /// characterization test per its own instructions. The earlier
    /// characterization blamed a from_builder/cte_extraction divergence; the
    /// true root cause (found via targeted tracing this round) sat one pass
    /// earlier: `VlpTransitivityCheck` judged a multi-type VLP's transitivity
    /// by `rel_types.first()` ALONE (an explicit `TODO: Handle multiple
    /// types`), so `[:AUTHORED|LIKED|FOLLOWS*1..2]` (non-transitive type
    /// listed FIRST) had its `variable_length` silently stripped — the
    /// failing axis was the TYPE-LIST ORDER, not the 3-types-or-labeled-end
    /// framing: `[:FOLLOWS|AUTHORED|LIKED*1..2]` (transitive type first)
    /// always worked, and even the 2-type `[:AUTHORED|LIKED*1..2]` failed.
    /// The stripped GraphRel still had `labels.len() > 1`, so
    /// `from_builder.rs::find_multi_type_graph_rel`'s syntactic check
    /// emitted `FROM vlp_multi_type_u_x` for a CTE the render phase only
    /// builds when `variable_length` survives → undefined-CTE crash.
    ///
    /// Fix: `VlpTransitivityCheck` now skips the single-type clamp for
    /// multi-type VLPs entirely — per-hop path validity is enumerated
    /// against the schema by `multi_type_vlp_expansion` (which handles
    /// zero-hop and undirected too, and naturally yields only the 1-hop
    /// branches when longer chains are impossible). This also fixes the
    /// silent-wrong-results side of the same bug: the old clamp dropped
    /// legitimate 2-hop paths (e.g. FOLLOWS→AUTHORED) whenever the first
    /// listed type happened to be non-transitive.
    ///
    /// Live-verified (2026-07-11, `social` fixture): `count(*)` for the
    /// repro returns 39, exactly matching a hand-written 4-branch UNION
    /// ground-truth query; type-list order is now irrelevant (39/39 directed,
    /// 217/217 undirected across both orders). The prior fix attempt's
    /// regression case (undirected `[:FOLLOWS|AUTHORED|LIKED*1..2]-(x)`)
    /// still renders its CTE (this fix touches the analyzer, not
    /// from_builder's alias correspondence).
    ///
    /// NOTE (pre-existing, NOT introduced or fixed here, candidates for new
    /// issues): on EVERY multi-type labeled-end VLP — including the
    /// previously-"working" FOLLOWS-first order, verified byte-identical on
    /// a pre-branch binary — (a) `ORDER BY x.title` resolves to a
    /// nonexistent `t.end_post_title` CTE column (Code 47), and (b)
    /// `RETURN x.title` extracts `JSONExtractString(end_properties,
    /// 'post_title')` while the CTE builds the JSON with cypher-name keys
    /// (`... AS title`), silently returning ''.
    #[tokio::test]
    async fn multi_type_vlp_nontransitive_first_type_order_invariant_528() {
        let schema = load_schema(SchemaId::Standard.yaml_path());

        // The filed repro: non-transitive AUTHORED listed first, labeled end.
        let sql = render(
            &schema,
            "MATCH (u:User)-[:AUTHORED|LIKED|FOLLOWS*1..2]->(x:Post) \
             RETURN u.name, x.title",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("WITH vlp_multi_type_u_x AS ("),
            "#528: the multi-type VLP CTE must actually be DEFINED: {sql}"
        );
        assert!(
            sql.contains("FROM vlp_multi_type_u_x AS t"),
            "#528: outer query must consume the defined CTE: {sql}"
        );
        // The 2-hop chains (FOLLOWS then AUTHORED/LIKED) that the old
        // first-type clamp silently dropped must be enumerated.
        assert!(
            sql.contains("2 AS hop_count"),
            "#528: legitimate 2-hop branches (FOLLOWS→AUTHORED/LIKED) must \
             not be clamped away by first-type transitivity: {sql}"
        );

        // Type-list order must not change the generated branch set: the
        // FOLLOWS-first spelling of the same query must produce the same
        // number of UNION ALL branches.
        let sql_follows_first = render(
            &schema,
            "MATCH (u:User)-[:FOLLOWS|AUTHORED|LIKED*1..2]->(x:Post) \
             RETURN u.name, x.title",
            SqlDialect::ClickHouse,
        )
        .await;
        assert_eq!(
            sql.matches("UNION ALL").count(),
            sql_follows_first.matches("UNION ALL").count(),
            "#528: branch enumeration must be type-list-order invariant:\n\
             AUTHORED-first:\n{sql}\nFOLLOWS-first:\n{sql_follows_first}"
        );
    }

    /// #528 (fix must not over-reach): all-non-transitive multi-type VLP
    /// (`[:AUTHORED|LIKED*1..2]`, no chainable type at all) must render only
    /// the possible 1-hop branches — the enumeration naturally proves no
    /// 2-hop chain exists (Post has no outgoing AUTHORED/LIKED), which is
    /// exactly what the old first-type clamp APPROXIMATED for this case.
    /// Semantically identical results, now without the order dependence.
    #[tokio::test]
    async fn multi_type_vlp_all_nontransitive_only_one_hop_branches_528() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (u:User)-[:AUTHORED|LIKED*1..2]->(x:Post) RETURN u.name, x.title",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("WITH vlp_multi_type_u_x AS ("),
            "#528: CTE must be defined for the all-non-transitive case too: {sql}"
        );
        assert!(
            sql.contains("1 AS hop_count") && !sql.contains("2 AS hop_count"),
            "#528: no 2-hop branch can exist for AUTHORED|LIKED (Post has no \
             outgoing edges of these types) — enumeration must prove that, \
             not invent branches: {sql}"
        );
    }

    /// #528 (single-type behavior unchanged): the multi-type skip must not
    /// perturb the single-type non-transitive clamp/error paths (#496) —
    /// directed min-1 clamps to a plain join, undirected errors loudly.
    #[tokio::test]
    async fn single_type_nontransitive_clamp_unchanged_by_528_fix() {
        let schema = load_schema(SchemaId::FkEdge.yaml_path());

        let sql = render(
            &schema,
            "MATCH (o:Order)-[:PLACED_BY*1..2]->(c) RETURN count(*)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !sql.to_uppercase().contains("WITH RECURSIVE"),
            "single-type directed non-transitive VLP must still clamp to a \
             plain join: {sql}"
        );

        let err = try_render(
            &schema,
            "MATCH (o:Order)-[:PLACED_BY*1..2]-(c) RETURN count(*)",
            SqlDialect::ClickHouse,
        )
        .await
        .expect_err("single-type undirected non-transitive VLP must still error (#496)");
        assert!(err.contains("#496"), "expected a #496 error: {err}");
    }

    /// #525 — FIXED (fix/544-vlp-family-remnants); this replaces the prior
    /// fragile-shape characterization per its own instructions. The
    /// denormalized VLP base case's WHERE clause used to include
    /// `hop_count <= N`, where `hop_count` is a SELECT-list alias
    /// (`1 AS hop_count`) in the SAME SELECT, not a real column — standard
    /// SQL forbids a WHERE clause from referencing a SELECT-list alias, and
    /// it only "worked" because ClickHouse non-standardly substitutes the
    /// alias's expression, evaluating `1 <= N` (a tautology for any
    /// `max_hops >= 1`, never a real bound — the real recursion bound is the
    /// recursive term's `vp.hop_count < N`).
    ///
    /// Fixed exactly per the previously-deferred sketch: push the literal
    /// comparison `1 <= {max_hops}` directly
    /// (`DenormalizedCteStrategy::build_where_clause`, `cte_manager/mod.rs`)
    /// — standard-SQL-portable and PROVABLY identical in behavior, since
    /// it's literally the expression ClickHouse evaluated after alias
    /// substitution. Live-verified (2026-07-11, `db_denormalized`): the
    /// bounded `*1..2` query below returns the same rows before and after.
    #[tokio::test]
    async fn denorm_vlp_base_case_hop_count_bound_is_portable_literal_525() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = render(
            &schema,
            "MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport) RETURN a.code, b.code",
            SqlDialect::ClickHouse,
        )
        .await;

        assert!(
            sql.contains("WHERE 1 <= 2"),
            "#525: base case WHERE clause must bound via the portable \
             literal comparison (what ClickHouse always evaluated anyway), \
             not a SELECT-alias reference: {sql}"
        );
        assert!(
            !sql.contains("WHERE hop_count <="),
            "#525: base case WHERE clause must not reference the hop_count \
             SELECT alias (non-standard SQL, ClickHouse-specific alias \
             substitution): {sql}"
        );
        // The REAL recursion bound must be untouched.
        assert!(
            sql.contains("vp.hop_count < 2"),
            "#525: the recursive term's genuine hop bound must be preserved: {sql}"
        );
    }
}

/// Regression tests for the #536/#537/#539/#540/#541/#526/#527 family — a
/// follow-up batch found during the fix/484-walker-drift-family review
/// rounds (merged fb6aca89). Several of these are literally the SAME
/// underlying question that #484 (`walker_drift_family` module above)
/// tackled — "does this alias resolve to a real column in the current SQL
/// scope, or does it only exist inside an inner UNION ALL/pattern_union/VLP
/// CTE branch" — but at DIFFERENT call sites #484 didn't touch:
///
/// - #541/#539/#526 all reuse (or newly apply) the proven-sound
///   `graph_rel_vlp_endpoint_role`/`rel_alias_uses_pattern_union` discriminators
///   from #484's round 3, at `projection_tagging.rs`'s `count`/`count(DISTINCT
///   ...)` rewrite call sites.
/// - #537 is a related-but-distinct composite-id gap: `find_id_column_for_alias`'s
///   GraphNode-branch priority returns a single (first) composite column for a
///   VLP endpoint instead of deferring to the VLP CTE's already-composite-safe
///   resolution.
/// - #527 is a related-but-distinct gap in `labels()`/`label()`'s static-label
///   fallback for a genuinely polymorphic node reached WITHOUT a VLP CTE.
/// - #536 is the highest-priority, standalone silent-wrong-results bug in
///   `ast_transform::id_function.rs` (see its own unit tests in that module —
///   the golden harness's `render()` never exercises that AST pass, matching
///   the embedded/`cg` code path, so a SQL golden test can't reach it).
/// - #540 could NOT be reproduced as literally filed (see the `540` test
///   below) despite substantial effort (`cg sql`/`cg query` against live
///   ClickHouse, multiple query shapes, before AND after this branch's other
///   fixes) — locked here as a passing regression test instead of a fix.
mod label_id_resolution_family_536_537_539_540_541_526_527 {
    use super::*;

    /// #526: `count(r)`/`count(DISTINCT r)` where `r` is bound via the
    /// `pattern_union` CTE (both endpoints unlabeled, e.g. `MATCH
    /// (n)-[r]-(o)`) used to resolve through `table_ctx.get_label_opt()`,
    /// which — for a pattern_union relationship — arbitrarily returns just
    /// the FIRST candidate relationship type's schema (e.g. `AUTHORED`,
    /// whose `from_id` is `user_id`), emitting `count(r.user_id)` — a column
    /// that doesn't exist on the `pattern_union_r` CTE's actual output shape
    /// (`start_type`/`start_id`/`end_type`/`end_id`/`path_relationships`/...).
    /// Live-verified (2026-07-10, local `social` fixture): pre-fix crashes
    /// ClickHouse Code 47 (`Identifier 'r.user_id' cannot be resolved`);
    /// post-fix `count(r)` returns 46 (matches
    /// `(authored+follows+likes) * 2` for the undirected doubling), and
    /// `count(DISTINCT r)` returns 40 (fewer than 46 — some rows share an
    /// identical (type, start, end) identity).
    #[tokio::test]
    async fn pattern_union_relationship_count_resolves_cte_columns_526() {
        let schema = load_schema(SchemaId::Standard.yaml_path());

        let sql = render(
            &schema,
            "MATCH (n)-[r]-(o) RETURN count(r)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !sql.contains("r.user_id") && !sql.contains("r.follower_id"),
            "#526: count(r) over a pattern_union relationship must not \
             reference a per-label schema column (e.g. r.user_id/r.follower_id) \
             that doesn't exist on the pattern_union_r CTE's output — this is \
             a ClickHouse Code 47 UNKNOWN_IDENTIFIER crash:\n{sql}"
        );
        assert!(
            sql.contains("count(r.start_id)"),
            "#526: count(r) must resolve to the CTE's label-agnostic \
             start_id column (NULL-sensitive, matches the non-pattern_union \
             single-edge-id-column style):\n{sql}"
        );

        let distinct_sql = render(
            &schema,
            "MATCH (n)-[r]-(o) RETURN count(DISTINCT r)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            distinct_sql.contains(
                "tuple(r.start_type, r.start_id, r.end_type, r.end_id, r.path_relationships[1])"
            ),
            "#526: count(DISTINCT r) must build a full row-identity tuple \
             over the CTE's label-agnostic columns (type-paired with id, \
             since start_id/end_id are label-agnostic strings that can \
             collide across label spaces, plus the relationship type/direction \
             to distinguish otherwise-identical (start,end) pairs reached via \
             a different relationship type):\n{distinct_sql}"
        );
    }

    /// #539: `count(id(x))`/`count(DISTINCT id(x))` didn't recognize an
    /// `id()`/`elementId()`-wrapped alias as an aggregate identity target at
    /// all (only a BARE alias or `DISTINCT` bare-alias matched) — so it fell
    /// through untouched, leaving the raw `id()` ScalarFnCall in place, which
    /// the SQL generator then replaced with the generic function-registry
    /// placeholder (`id` -> `toInt64(0)`), silently collapsing
    /// `count(DISTINCT id(x))` to 1 regardless of how many distinct nodes
    /// actually exist. Live-verified (2026-07-10, local `social` fixture):
    /// pre-fix `count(DISTINCT id(o))` for `MATCH (n)-[r:FOLLOWS]->(o:User)`
    /// returned 1 (`count(DISTINCT toInt64(0))`); post-fix returns 5
    /// (matches `count(DISTINCT r.followed_id) = 5`) — reproduces even for a
    /// SINGLE-label node, independent of the multi-label raw-union axis the
    /// title emphasizes. Also live-verified on the 3-label raw-union shape
    /// (`examples/data_security/data_security.yaml`,
    /// `(folder:Folder)-[:CONTAINS]->(item)`): pre-fix
    /// `count(DISTINCT id(item))` returned 1; post-fix returns 200 (matches
    /// an independently-computed `count(DISTINCT id)` over the 3 unioned
    /// child tables).
    #[tokio::test]
    async fn count_distinct_id_function_resolves_identity_not_placeholder_539() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (n)-[r:FOLLOWS]->(o:User) RETURN count(DISTINCT id(o))",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !sql.contains("toInt64(0)"),
            "#539: count(DISTINCT id(o)) must not fall back to the \
             function-registry id() placeholder — every row would silently \
             count as the same value:\n{sql}"
        );
        assert!(
            sql.contains("count(DISTINCT r.followed_id)"),
            "#539: count(DISTINCT id(o)) over a single-label node must \
             resolve to the real joined id column:\n{sql}"
        );

        let non_distinct_sql = render(
            &schema,
            "MATCH (n)-[r:FOLLOWS]->(o:User) RETURN count(id(o))",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !non_distinct_sql.contains("toInt64(0)"),
            "#539: count(id(o)) (non-DISTINCT) must also resolve to a real \
             column, not the id() placeholder:\n{non_distinct_sql}"
        );

        // Multi-label raw-union shape (data_security junction-table schema):
        // 3 candidate labels (File/Group/User) share the CONTAINS relationship.
        let junction_schema = load_schema("examples/data_security/data_security.yaml");
        let junction_sql = render(
            &junction_schema,
            "MATCH (folder:Folder)-[:CONTAINS]->(item) RETURN count(DISTINCT id(item))",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !junction_sql.contains("toInt64(0)"),
            "#539: count(DISTINCT id(item)) over a 3-label raw union must not \
             collapse to the id() placeholder either:\n{junction_sql}"
        );
    }

    /// #541: `graph_rel_connection_role` (introduced by #483 for
    /// `projection_tagging.rs`'s `count(DISTINCT alias)` rewrite) assumes any
    /// UNDIRECTED GraphRel endpoint collapses into a single-alias
    /// `multi_type_vlp_joins`/`bidirectional_union` CTE exposing label-agnostic
    /// `end_id`/`type` columns — false for a junction-table/polymorphic schema
    /// whose relationship dispatches through a discriminator/junction table
    /// (single relationship type, ambiguous NODE label): `(folder:Folder)
    /// -[:CONTAINS]-(item)` on `data_security.yaml` is undirected
    /// (`graph_rel_connection_role(item).is_some()`) yet still clones the
    /// whole `GraphRel` per candidate label into a raw `UNION ALL` (no VLP CTE
    /// — `variable_length` is `None`), so `item.end_id`/`item.type` reference
    /// columns that don't exist on ANY of the per-label branches (each has its
    /// own real columns: fs_id/group_id/user_id). Fixed by switching this call
    /// site to the round-3, proven-sound `graph_rel_vlp_endpoint_role`
    /// discriminator (`variable_length.is_some()`) — the exact fix #484's
    /// review already applied to `group_by_builder.rs`/`filter_tagging.rs` for
    /// the same unsound assumption, just not yet to this THIRD call site.
    /// Live-verified (2026-07-10): pre-fix crashes ClickHouse Code 47
    /// (`item.end_id` unresolvable); post-fix `count(DISTINCT item)` returns
    /// 200 (matches an independently-computed `count(DISTINCT id)` over the
    /// 3 unioned child tables — same ground truth as the #539 junction-table
    /// case above, since both count the same underlying CONTAINS children).
    #[tokio::test]
    async fn graph_rel_connection_role_replaced_by_vlp_endpoint_role_in_count_distinct_alias_541() {
        let schema = load_schema("examples/data_security/data_security.yaml");
        let sql = render(
            &schema,
            "MATCH (folder:Folder)-[:CONTAINS]-(item) RETURN count(DISTINCT item)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !sql.contains("item.end_id")
                && !sql.contains("item.type")
                && !sql.contains("item.start_id"),
            "#541: count(DISTINCT item) over a junction-table undirected \
             endpoint must not reference the multi_type_vlp_joins CTE's \
             end_id/type columns — this junction-table schema has no such \
             CTE, only a raw per-label UNION with real fs_id/group_id/user_id \
             columns per branch (Code 47 UNKNOWN_IDENTIFIER otherwise):\n{sql}"
        );
        assert!(
            sql.contains("tuple(`item.fs_id`, `item.group_id`, `item.user_id`)"),
            "#541: must fall through to the per-label id-column tuple \
             (#467-style), using the REAL per-branch columns:\n{sql}"
        );
    }

    /// #537: `id(a2)`/GROUP BY over a composite-id VLP endpoint (Account
    /// keyed by `(bank_id, account_number)`) used to resolve to only the
    /// FIRST composite-id column (`find_id_column_for_alias`'s GraphNode
    /// branch returns the endpoint's underlying `ViewScan.id_column`, a
    /// single `String` by construction, BEFORE ever reaching its own VLP
    /// fallback), silently merging every distinct account at the same bank
    /// into one GROUP BY group. Fixed by building
    /// `concat(toString(c1),'|',toString(c2),...)` over the RAW composite
    /// schema columns (mirroring `emit_id_expr`'s own pipe-join convention),
    /// reusing the `graph_rel_vlp_endpoint_role` discriminator plus #457's
    /// `composite_id_group_by_columns` `is_composite()` gate. (Referencing the
    /// VLP CTE's OWN pre-joined `start_id`/`end_id` sentinel column directly
    /// was tried and discarded — it collides with
    /// `rewrite_render_expr_for_vlp_with_endpoint_info`'s endpoint-property
    /// rewriter, whose direct lookup is keyed by genuine schema property
    /// names and whose fallback blindly re-prefixes, producing
    /// `end_end_id` — a hard Code 47, worse than the original silent merge.)
    /// Live-verified (2026-07-10, `db_composite_id` fixture): pre-fix
    /// `id(a2)` GROUP BY collapsed 8 distinct accounts into 2 groups (one per
    /// bank, counts 7/8, sum 15); post-fix correctly distinguishes all 8
    /// accounts (counts sum to the SAME 15 total rows, now correctly
    /// disaggregated) — scope note: this fix is for the STANDARD schema
    /// pattern (both VLP endpoint nodes have real tables); FK-edge/
    /// denormalized/mixed VLP generators were not touched and are not
    /// claimed fixed by this change.
    #[tokio::test]
    async fn composite_id_vlp_endpoint_id_function_uses_full_tuple_537() {
        let schema = load_schema(SchemaId::CompositeId.yaml_path());
        let sql = render(
            &schema,
            "MATCH (a:Account)-[:TRANSFERRED*1..3]->(a2:Account) RETURN id(a2), count(*)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !sql.contains("end_end_id") && !sql.contains("t.end_bank_id AS"),
            "#537: must not double-prefix into end_end_id, and must not \
             collapse to just the first composite column (end_bank_id) \
             alone:\n{sql}"
        );
        assert!(
            sql.contains("concat(toString(t.end_bank_id), '|', toString(t.end_account_number))"),
            "#537: id(a2) must concat BOTH composite id columns \
             (bank_id AND account_number), matching emit_id_expr's own \
             pipe-join convention:\n{sql}"
        );
        // The GROUP BY key must be the SAME full-tuple expression (a SQL
        // requirement: every non-aggregate SELECT item must appear, in full,
        // in GROUP BY), not a truncated single-column key.
        assert!(
            sql.matches("concat(toString(t.end_bank_id), '|', toString(t.end_account_number))")
                .count()
                >= 2,
            "#537: the full composite tuple expression must appear in BOTH \
             the SELECT list and the GROUP BY clause:\n{sql}"
        );
    }

    /// #527: `labels()`/`label()` on a genuinely polymorphic node backed by a
    /// SINGLE shared physical table (schema `label_column`/`label_value` —
    /// e.g. Folder/File both mapping to `ds_fs_objects`, distinguished by
    /// `fs_type`) reached WITHOUT a VLP CTE at all (an unlabeled `MATCH (n)`
    /// whose candidate labels resolve to the IDENTICAL table+columns, so
    /// type-inference/dedup collapses the would-be per-label UNION into a
    /// single scan with NO per-branch `WHERE fs_type = 'X'` filter) used to
    /// return a static label list instead of reading the actual per-row
    /// discriminator column — the non-VLP sibling of #494 (which fixed this
    /// same bug class for multi-type VLP path nodes). Live-verified
    /// (2026-07-10, `data_security` fixture): `MATCH (n) WHERE n.fs_id IN
    /// ['1','1001'] RETURN n.fs_id, labels(n)` — fs_id 1 is a Folder, fs_id
    /// 1001 is a File; pre-fix both rows showed the SAME static label;
    /// post-fix each row correctly shows its own actual label
    /// (`[Folder]`/`[File]`).
    #[tokio::test]
    async fn polymorphic_labels_reads_per_row_discriminator_527() {
        let schema = load_schema("examples/data_security/data_security.yaml");

        let labels_sql = render(
            &schema,
            "MATCH (n) WHERE n.fs_id IN ['1','1001'] RETURN labels(n)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            labels_sql.contains("[toString(n.fs_type)]"),
            "#527: labels(n) on a single-table polymorphic node must read \
             the per-row label_column discriminator, not a static literal \
             list:\n{labels_sql}"
        );

        let label_sql = render(
            &schema,
            "MATCH (n) WHERE n.fs_id IN ['1','1001'] RETURN label(n)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            label_sql.contains("n.fs_type AS"),
            "#527: label(n) must read the per-row label_column discriminator \
             too:\n{label_sql}"
        );

        // Regression guard: an explicitly-labeled match must still be safe —
        // reading the live column returns the IDENTICAL value the static
        // literal would (the label match already constrains every row to
        // that discriminator value), so this is a behavior-preserving switch
        // for the already-correct case too.
        let explicit_sql = render(
            &schema,
            "MATCH (f:Folder) RETURN labels(f) LIMIT 3",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            explicit_sql.contains("[toString(f.fs_type)]"),
            "#527 regression guard: an explicitly-labeled node's labels() may \
             now read the live column instead of a literal, but must still \
             produce a value-correct result (schema-filtered branch \
             guarantees fs_type = 'Folder' for every row):\n{explicit_sql}"
        );
    }

    /// #540: could NOT be reproduced as literally filed, despite substantial
    /// effort — `MATCH (n)-[r:FOLLOWS]-(o) RETURN id(o), count(*) ORDER BY
    /// id(o)` on `social_benchmark.yaml` (the issue's exact repro) renders
    /// and EXECUTES correctly both before and after this branch's other
    /// fixes (live-verified against ClickHouse: 7 rows, follower counts
    /// 7/4/3/2/2/1/1, internally consistent with the graph's FOLLOWS edges).
    /// `extract_order_by_columns_for_union`'s `__order_col_N` mechanism is
    /// simply never reached for this single-ORDER-BY-item query: `id(o)`
    /// resolves to a real column (`o.user_id`) via the SAME safe/precise
    /// discriminator this whole family exercises, and GROUP BY/ORDER BY both
    /// reference the resolved `id(o)` alias directly — no synthetic
    /// `__order_col_*` column is ever introduced. Locked as a passing
    /// regression test (not a "known broken" characterization) since the
    /// literal repro is, in fact, NOT broken.
    ///
    /// NOTE: while investigating this, a DIFFERENT, deeper latent bug was
    /// found in the same subsystem — a multi-item `ORDER BY` where the FIRST
    /// item is an unsafe raw-union `id()` (dropped by
    /// `extract_order_by_columns_for_union`'s skip logic) and a LATER item
    /// survives produces a UNION with MISMATCHED ARITY across nested
    /// direction-sub-branches (`add_order_by_columns_to_select` doesn't
    /// recurse into a NESTED `Union`-of-`Union` shape from combined
    /// `bidirectional_union` direction-splitting + per-label raw union) — a
    /// separate, structurally deeper bug NOT covered by this test and
    /// deliberately left unfixed (out of scope for this batch; needs
    /// `add_order_by_columns_to_select` to recurse into nested union
    /// branches, a materially larger change than this fix family's other
    /// items). Repro sketch (junction-table schema, NON-aggregating so the
    /// `order_by_columns` UNION-rendering path is reached instead of the
    /// has_aggregation path): `MATCH (folder:Folder)-[:CONTAINS]-(item)
    /// RETURN item.name ORDER BY id(item), item.name`.
    #[tokio::test]
    async fn order_by_id_unlabeled_rel_typed_endpoint_540() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (n)-[r:FOLLOWS]-(o) RETURN id(o), count(*) ORDER BY id(o)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !sql.contains("__order_col"),
            "#540: this single-ORDER-BY-item query must never introduce a \
             synthetic __order_col_N column reference — if this now fails, \
             live-verify against ClickHouse before treating it as a genuine \
             regression:\n{sql}"
        );
        assert!(
            sql.contains("GROUP BY `id(o)`") && sql.contains("ORDER BY `id(o)` ASC"),
            "#540: id(o) must resolve to a real, consistently-referenced \
             column in both GROUP BY and ORDER BY:\n{sql}"
        );
    }
}

/// Regression tests for the #549/#550 family: WITH-aggregate CTE export on
/// denormalized nodes whose schema shape goes beyond what any real in-repo
/// schema exercises — found (and filed as follow-ups) during the adversarial
/// review of fix/465-with-aggregate-family (merged 995267c6). Both reproduce
/// on a dedicated synthetic schema, `schemas/dev/flights_denorm_mixed_sources.yaml`
/// (a SEPARATE file rather than an extension of `flights_denormalized.yaml`:
/// adding labels to the shared denormalized schema changes the shape of every
/// fully-unlabeled pattern query against SchemaId::Denormalized —
/// pattern_union grows extra per-label branches — breaking unrelated goldens).
/// The synthetic labels map onto the SAME physical `db_denormalized
/// .flights_denorm` table, so everything here is live-verifiable against the
/// standard fixture (scripts/setup/setup_denormalized_data.sh).
mod with_aggregate_denorm_mixed_sources_family_549_550 {
    use super::*;

    const MIXED_SOURCES_SCHEMA: &str = "schemas/dev/flights_denorm_mixed_sources.yaml";

    /// #549: a node with non-empty `property_mappings` ALSO combined with
    /// `from_node_properties`/`to_node_properties` on the SAME node
    /// (`AirportWithCarrier`: role-varying `code`/`city`/`state` in the role
    /// maps, role-INvariant `carrier` in `property_mappings`). A
    /// `property_mappings`-sourced column referenced downstream of a
    /// WITH-aggregate barrier alongside a from/to-sourced column used to be
    /// silently DROPPED from the CTE export entirely (not NULL-padded — just
    /// absent): the CTE exported only `p1_b_city`, while the outer SELECT
    /// still referenced `b_n.carrier` — ClickHouse Code 47 UNKNOWN_IDENTIFIER
    /// at execution.
    ///
    /// Root cause: the role-specific node property maps copied onto
    /// `RelationshipSchema::from_node_properties`/`to_node_properties` at
    /// schema-load time (`build_relationship_schema`/
    /// `resolve_denormalized_edge_props`, `graph_catalog/config.rs`) were
    /// read straight from `NodeSchema::from_properties`/`to_properties`,
    /// never consulting the node's own `property_mappings` — so every
    /// downstream property-resolution site (ViewScan construction,
    /// `PatternSchemaContext`, `properties_builder.rs`, the WITH-CTE export)
    /// simply never knew `carrier` existed on this node.
    ///
    /// Fixed at that single load-time source via
    /// `NodeSchema::denorm_role_properties()` (graph_schema.rs): merges
    /// `property_mappings` entries into the role map for property names not
    /// already covered — role-specific mappings always win on a collision,
    /// and `node_id` property names are excluded entirely (their
    /// auto-generated identity entries in `property_mappings` are spurious
    /// for denormalized nodes; letting them merge would grow a phantom
    /// property on the real zeek schema, whose id NAME `id.orig_h` differs
    /// from every role-map name). All real in-repo schemas render
    /// byte-identically pre/post fix (their denorm nodes carry only the
    /// auto-identity `node_id` entry in `property_mappings`).
    ///
    /// Live-verified (2026-07-11, db_denormalized, ClickHouse via `cg query`):
    /// pre-fix this query fails at execution (`Unknown identifier 'carrier'`
    /// shape — the CTE exports no carrier column); post-fix it executes and
    /// returns 4 rows.
    ///
    /// CAVEAT (adversarial review, 2026-07-11): at the time this test was
    /// written, this query returned 4 rows, NOT correct node-identity
    /// grouping (5 rows: LAX=6, ORD=2, JFK=1, ATL=1, DEN=1) — a PRE-EXISTING,
    /// independent bug where single-id denorm nodes fell through
    /// `expand_table_alias_to_group_by_id_only`'s "Fallback 2: use first
    /// property" (plan_builder_utils.rs), grouping by `t2.carrier` instead of
    /// node identity. That bug is now FIXED by #551 (see
    /// `single_id_denorm_with_aggregate_family_551` below) — this test only
    /// ever locked the property-EXPORT shape, not the grouping, so it is
    /// unaffected by and does not re-assert the #551 fix.
    #[tokio::test]
    async fn property_mappings_column_survives_with_aggregate_barrier_549() {
        let schema = load_schema(MIXED_SOURCES_SCHEMA);
        let cypher = "MATCH (a:AirportWithCarrier)-[:CARRIER_FLIGHT]->(b:AirportWithCarrier)\
                      -[:CARRIER_FLIGHT]->(c:AirportWithCarrier) \
                      WITH b, count(*) AS n RETURN b.city, b.carrier, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        // The property_mappings-sourced column must be exported by the
        // WITH-aggregate CTE under the standard p{N}_{alias}_{property} name.
        // Assert on the physical column, not the specific tN alias (see the
        // normalize() note on cross-test alias-counter dependence).
        let export_re = regex::Regex::new(r#"anyLast\(t\d+\.carrier\) AS "p1_b_carrier""#).unwrap();
        assert!(
            export_re.is_match(&sql),
            "#549 regressed: the property_mappings-sourced `carrier` column \
             must be exported by the WITH-aggregate CTE:\n{sql}"
        );
        // ... and the outer SELECT must reference that exported CTE column,
        // never a raw `carrier` column the CTE does not expose.
        assert!(
            sql.contains("b_n.p1_b_carrier AS \"b.carrier\""),
            "#549 regressed: outer SELECT must reference the exported \
             p1_b_carrier CTE column:\n{sql}"
        );
        assert!(
            !sql.contains("b_n.carrier"),
            "#549 regressed: outer SELECT references `b_n.carrier`, a column \
             the CTE does not export (ClickHouse Code 47 at execution):\n{sql}"
        );
        // The from/to-sourced sibling column must still resolve through the
        // role-specific mapping (never a raw `city` column).
        let city_re = regex::Regex::new(r#"anyLast\(t\d+\.origin_city\) AS "p1_b_city""#).unwrap();
        assert!(
            city_re.is_match(&sql),
            "#549: the role-mapped `city` export must be unaffected:\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(sql, again, "#549: nondeterministic render");
        }
    }

    /// #549 guard: the fix must NOT change how the real coupled-denormalized
    /// schemas render. `flights_denormalized.yaml`'s `Airport` and the zeek
    /// nodes carry ONLY the auto-generated `node_id` identity entry in
    /// `property_mappings` (e.g. `code -> code`), which `denorm_role_properties`
    /// deliberately excludes from the merge — `code` must keep resolving to
    /// the role-specific `origin_code`/`dest_code`, never leak a dangling
    /// physical `code` column reference.
    #[tokio::test]
    async fn real_denorm_schema_unaffected_by_549_merge() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let cypher = "MATCH (a:Airport)-[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport) \
                      WITH b, count(*) AS n RETURN b.city, b.code, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let code_re = regex::Regex::new(r#"anyLast\(t\d+\.origin_code\) AS "p1_b_code""#).unwrap();
        assert!(
            code_re.is_match(&sql),
            "#549 guard: Airport's node_id property `code` must still resolve \
             through the role-specific map (origin_code), not a raw `code` \
             column:\n{sql}"
        );
        let dangling_re = regex::Regex::new(r#"t\d+\.code\b"#).unwrap();
        assert!(
            !dangling_re.is_match(&sql),
            "#549 guard: the auto-identity `code -> code` property_mappings \
             entry must never leak into rendering (flights_denorm has no \
             `code` column):\n{sql}"
        );
    }

    /// #550: a composite (list) `node_id` on a denormalized node
    /// (`AirportComposite`, `node_id: [code, state]`), referenced in a
    /// WITH-aggregate query, used to emit `GROUP BY b.code, b.state` — a
    /// dangling table alias (no table `b` in the CTE body's scope; the denorm
    /// edge table is `tN`) over nonexistent physical columns (only
    /// `origin_code`/`origin_state` exist) — ClickHouse Code 47
    /// UNKNOWN_IDENTIFIER at execution.
    ///
    /// Root cause: `expand_table_alias_to_group_by_id_only`'s composite guard
    /// (the #457 WITH→CTE-path copy, `plan_builder_utils.rs`) qualified the
    /// schema's raw `node_id.columns()` — CYPHER PROPERTY names — with the
    /// Cypher pattern alias, with no property→physical-column resolution at
    /// all. Fixed by resolving each id property through the plan's own
    /// `get_properties_with_table_alias` (the same call the single-column
    /// denorm fallback and `group_by_builder.rs`'s `table_alias_to_use`
    /// resolution already use — it dispatches per schema pattern internally),
    /// yielding the role-specific physical columns and the actual edge table
    /// alias. Standard composite-id schemas resolve to identity mappings with
    /// no alias override, verified byte-identical pre/post fix (git-stash
    /// A/B on `composite_node_id_test.yaml`).
    ///
    /// Live-verified (2026-07-11, db_denormalized, ClickHouse via `cg query`):
    /// post-fix returns 5 rows matching the non-WITH control
    /// (`RETURN b.city, count(*) AS n`) row-for-row (New York=1, Chicago=2,
    /// Atlanta=1, Denver=1, Los Angeles=6).
    #[tokio::test]
    async fn composite_node_id_with_aggregate_group_by_resolves_physical_columns_550() {
        let schema = load_schema(MIXED_SOURCES_SCHEMA);
        let cypher = "MATCH (a:AirportComposite)-[:COMPOSITE_FLIGHT]->(b:AirportComposite)\
                      -[:COMPOSITE_FLIGHT]->(c:AirportComposite) \
                      WITH b, count(*) AS n RETURN b.city, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        // GROUP BY must reference BOTH composite id columns, resolved to the
        // role-specific physical columns on the edge table's alias.
        let group_by_re =
            regex::Regex::new(r#"GROUP BY t\d+\.origin_code, t\d+\.origin_state"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "#550 regressed: GROUP BY must reference the role-mapped physical \
             composite id columns on the edge alias:\n{sql}"
        );
        // Never the dangling Cypher-alias/property-name form.
        assert!(
            !sql.contains("GROUP BY b.code") && !sql.contains("b.state"),
            "#550 regressed: GROUP BY references the dangling Cypher alias \
             `b` / unmapped property names (ClickHouse Code 47):\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(sql, again, "#550: nondeterministic render");
        }
    }

    /// #550 guard: the composite-guard rewrite must leave STANDARD
    /// composite-id schemas byte-identical (verified via git-stash A/B during
    /// the fix; locked here structurally). `Account`'s id properties carry
    /// identity mappings and the node has its own table, so GROUP BY keeps
    /// the Cypher alias + raw id columns.
    #[tokio::test]
    async fn standard_composite_id_with_aggregate_group_by_unchanged_550() {
        let schema = load_schema(SchemaId::CompositeId.yaml_path());
        let cypher = "MATCH (c:Customer)-[:OWNS]->(a:Account) \
                      WITH a, count(*) AS n RETURN a.account_type, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        assert!(
            sql.contains("GROUP BY a.bank_id, a.account_number"),
            "#550 guard: standard composite-id GROUP BY must keep the \
             pre-#550 alias + identity-column form:\n{sql}"
        );
    }
}

/// Regression tests for #551: a single-id denormalized node behind a
/// WITH-aggregate barrier used to GROUP BY the FIRST PROPERTY returned by
/// `get_properties_with_table_alias` (alphabetically sorted by Cypher
/// property name) instead of the node's actual identity column — silently
/// wrong grouping, not a crash, since every candidate column is a real
/// physical column on the edge table.
///
/// Found during adversarial review of fix/549-with-aggregate-denorm-edge-cases
/// (merged 5559ab2e); PRE-EXISTING on `main` (byte-identical GROUP BY
/// before/after that branch). #550 (same branch) had already implemented the
/// analogous identity-resolution treatment for the COMPOSITE-id path via
/// `get_properties_with_table_alias`, but gated it on
/// `node_schema.node_id.is_composite()`, leaving the (far more common)
/// single-id path on the old "Fallback 2: use first property" behavior.
///
/// Root cause (two layers):
/// 1. `expand_table_alias_to_group_by_id_only`'s single-id resolution chain
///    (`find_id_column_for_alias`, then the non-recursive
///    `find_label_for_alias`) never reaches a denormalized node's true
///    identity when that node sits BETWEEN two edges in a chain
///    (`(a)-[:T]->(b)-[:T]->(c)`): `b` has no `ViewScan`/`GraphNode` of its
///    own — its properties come straight off the edge `ViewScan`'s
///    role-specific property maps — so both lookups fail and execution
///    fell all the way to "Fallback 2: use first property".
/// 2. Even the RECURSIVE label lookup the #550 composite guard uses
///    (`cte_extraction::get_node_label_for_alias`) only matches actual
///    `GraphNode`s, so it ALSO misses this shape for an UNLABELED chain node
///    (`(b)` with no explicit `:Label` — the shape of #551's own repro,
///    where type inference never synthesizes a `GraphNode` for `b` at all).
///
/// Fix: `find_denorm_connection_node_label` (new helper,
/// `plan_builder_utils.rs`) resolves the label for a `GraphRel`
/// `left_connection`/`right_connection` with no backing `GraphNode`, by
/// reading the relationship TYPE off `GraphRel.labels` and looking up that
/// type's `from_node`/`to_node` in the schema catalog (`left_connection` is
/// always the FROM side, `right_connection` always the TO side — the same
/// invariant `get_properties_with_table_alias` already relies on for this
/// shape). `expand_table_alias_to_group_by_id_only` tries the existing
/// recursive `GraphNode` lookup FIRST (covers a labeled chain node), then
/// falls back to this new connection-based lookup (covers the unlabeled
/// case); either way, once a label resolves, the (non-composite) id
/// property name is mapped to its role-specific physical column via
/// `get_properties_with_table_alias` — mirroring #550's treatment exactly,
/// just for the single-id path. Resolution failure at any step falls
/// through unchanged to the pre-#551 "first property" fallback.
///
/// Live-verified (2026-07-11, db_denormalized, ClickHouse via `cg query`):
/// pre-fix the issue's exact (unlabeled) repro returns 4 rows (Los
/// Angeles/American=5, Chicago/United=2, Atlanta/Delta=3, Denver/Southwest=1
/// — Atlanta/Delta silently absorbed LAX's Delta flights, Los
/// Angeles/American absorbed JFK's); post-fix it returns 5 rows matching
/// true node-identity grouping (Los Angeles/American=6, Chicago/United=2,
/// New York/American=1, Atlanta/Delta=1, Denver/Southwest=1).
mod single_id_denorm_with_aggregate_family_551 {
    use super::*;

    const MIXED_SOURCES_SCHEMA: &str = "schemas/dev/flights_denorm_mixed_sources.yaml";

    /// #551 (issue's own repro shape): UNLABELED single-id denorm chain node
    /// `b` sitting between two edges. No `GraphNode` exists anywhere in the
    /// plan for `b` — this is the shape the `find_denorm_connection_node_label`
    /// connection-based fallback specifically covers (the recursive
    /// `GraphNode`-based lookup used by the LABELED sibling test below misses
    /// it entirely).
    #[tokio::test]
    async fn unlabeled_single_id_denorm_group_by_resolves_identity_551() {
        let schema = load_schema(MIXED_SOURCES_SCHEMA);
        let cypher = "MATCH (a)-[:CARRIER_FLIGHT]->(b)-[:CARRIER_FLIGHT]->(c) \
                      WITH b, count(*) AS n RETURN b.city, b.carrier, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let group_by_re = regex::Regex::new(r#"GROUP BY t\d+\.origin_code\b"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "#551 regressed: GROUP BY must reference the node's identity \
             column (origin_code) on the edge alias, not the first property \
             returned by property resolution:\n{sql}"
        );
        // Never the old silently-wrong "first property" fallback.
        assert!(
            !sql.contains("GROUP BY t1.carrier") && !sql.contains("GROUP BY t2.carrier"),
            "#551 regressed: GROUP BY references `carrier` (an arbitrary \
             non-identity property) instead of the node's identity column:\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(sql, again, "#551: nondeterministic render");
        }
    }

    /// #551 sibling shape: same query, but `b` is explicitly LABELED
    /// (`(b:AirportWithCarrier)`), so type inference DOES synthesize a
    /// `GraphNode` for it and the recursive `GraphNode`-based label lookup
    /// (`cte_extraction::get_node_label_for_alias`) resolves it directly,
    /// without needing the connection-based fallback. Both paths must land
    /// on the same correct identity column.
    #[tokio::test]
    async fn labeled_single_id_denorm_group_by_resolves_identity_551() {
        let schema = load_schema(MIXED_SOURCES_SCHEMA);
        let cypher = "MATCH (a:AirportWithCarrier)-[:CARRIER_FLIGHT]->(b:AirportWithCarrier)\
                      -[:CARRIER_FLIGHT]->(c:AirportWithCarrier) \
                      WITH b, count(*) AS n RETURN b.city, b.carrier, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let group_by_re = regex::Regex::new(r#"GROUP BY t\d+\.origin_code\b"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "#551 regressed (labeled variant): GROUP BY must reference the \
             node's identity column (origin_code):\n{sql}"
        );
    }

    /// #551 on the REAL `flights_denormalized.yaml` schema (not the
    /// synthetic mixed-sources fixture): before the fix this rendered
    /// `GROUP BY t2.origin_city`, which happened to look "correct" only
    /// because the fixture's cities are 1:1 with airport codes — a
    /// coincidence, not a guarantee. Post-fix it must reference the actual
    /// declared identity property (`code` -> `origin_code`).
    #[tokio::test]
    async fn real_denorm_schema_group_by_resolves_identity_not_city_551() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let cypher = "MATCH (a:Airport)-[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport) \
                      WITH b, count(*) AS n RETURN b.city, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let group_by_re = regex::Regex::new(r#"GROUP BY t\d+\.origin_code\b"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "#551 regressed: real denorm schema must GROUP BY the declared \
             node_id property (origin_code), never a coincidentally-1:1 \
             display property like origin_city:\n{sql}"
        );
    }

    /// #551 guard: a STANDARD (non-denormalized) single-id node reached
    /// through a multi-hop chain must render byte-identically to before the
    /// fix — it already resolves through `find_id_column_for_alias`
    /// (THIRD, ahead of the new #551 fallback) via its own real
    /// `ViewScan`/`GraphNode`, so the new connection-based label resolution
    /// must never even be reached.
    #[tokio::test]
    async fn standard_schema_multi_hop_group_by_unchanged_551() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let cypher = "MATCH (a:User)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c) \
                      WITH b, count(*) AS n RETURN b.name, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        assert!(
            sql.contains("GROUP BY b.user_id"),
            "#551 guard: standard-schema multi-hop GROUP BY must keep the \
             pre-#551 alias + real id-column form:\n{sql}"
        );
    }

    /// #551 guard: the fix must not disturb the #550 composite-id path — a
    /// LABELED composite-id denorm chain node must still emit both identity
    /// columns via the SECOND (composite) check, never fall into the new
    /// single-id resolution added by this fix.
    #[tokio::test]
    async fn composite_id_denorm_unaffected_by_551_guard() {
        let schema = load_schema(MIXED_SOURCES_SCHEMA);
        let cypher = "MATCH (a:AirportComposite)-[:COMPOSITE_FLIGHT]->(b:AirportComposite)\
                      -[:COMPOSITE_FLIGHT]->(c:AirportComposite) \
                      WITH b, count(*) AS n RETURN b.city, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let group_by_re =
            regex::Regex::new(r#"GROUP BY t\d+\.origin_code, t\d+\.origin_state"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "#551 guard: #550's composite-id GROUP BY resolution must be \
             unaffected:\n{sql}"
        );
    }
}

/// Regression tests for #560 (composite-id counterpart of #551) and #561
/// (the non-WITH implicit-GROUP-BY sibling of #551/#560), plus #562
/// (dormant hardening of the `find_denorm_connection_node_label` helper
/// #551 introduced — see `plan_builder_utils::tests::
/// find_denorm_connection_node_label_refuses_ambiguous_multi_registration_562`
/// for #562's own dedicated unit tests, since no real fixture reaches that
/// guard through full query rendering).
///
/// #560 root cause: `group_by_builder::composite_id_group_by_columns`
/// (shared by both the WITH→CTE path, `expand_table_alias_to_group_by_id_only`,
/// and the non-WITH path, `handle_table_alias_group_by`/
/// `handle_wildcard_group_by`) gated purely on `cte_extraction::
/// get_node_label_for_alias` — the same RECURSIVE-but-`GraphNode`-only
/// lookup #551 showed misses a fully UNLABELED denormalized chain node
/// (type inference never synthesizes a `GraphNode` for it at all). #551's
/// own commit explicitly flagged this as a known, unfixed gap on the
/// composite-id side. Fix: `composite_id_group_by_columns` now falls back
/// to `find_denorm_connection_node_label` (#551's connection-based lookup)
/// exactly like the single-id path already did — one shared helper, so both
/// id shapes get the same coverage.
///
/// #561 root cause: `extract_group_by`'s free-standing `GroupBy` arm in
/// `plan_builder_utils.rs` (the location the issue named) is DEAD CODE —
/// every real caller (`plan_builder.rs`) goes through the `GroupByBuilder`
/// TRAIT implementation in `group_by_builder.rs` instead (confirmed by
/// exhaustive call-site search: `mod plan_builder_utils` is private and
/// nothing calls the free function outside its own recursion/unit tests).
/// The issue's LITERAL repro (`RETURN b.city, count(*) AS n`, no WITH) is
/// consequently NOT a bug: `b.city` is a scalar PROPERTY projection, which
/// the live code correctly passes through as a direct property-value GROUP
/// BY key (`process_group_by_expressions`'s "Case 3", untouched by any
/// identity-optimization logic) — matching both real Cypher aggregation
/// semantics and this repo's own established precedent (`group_two_keys`:
/// `RETURN u.country, u.city, count(u)` groups by the VALUES, not node
/// identity; live-verified via ClickHouse: grouping this exact repro by
/// `origin_code` instead, as the issue requested, would incorrectly split
/// two same-city, different-code airports into separate output rows sharing
/// a displayed city — see `scalar_property_only_group_by_no_with_groups_by_value_not_identity`
/// below). The GENUINE non-WITH sibling of #551/#560 is the WHOLE-NODE shape
/// (`RETURN b, count(*)`, no WITH) — `handle_table_alias_group_by`/
/// `handle_wildcard_group_by` (`group_by_builder.rs`) hit the exact same
/// `find_id_column_for_alias`-misses-a-denorm-chain-node gap #551 fixed for
/// the WITH path, but had no fallback: it fell to a literal `"id"` sentinel
/// column that downstream rendered as the dangling unmapped raw property
/// name (`code`, never `origin_code`) — a hard ClickHouse `Code: 47
/// UNKNOWN_IDENTIFIER`, not merely a wrong value. Fix: both functions now
/// call the SAME new shared helper (`plan_builder_utils::
/// resolve_single_id_denorm_column`) #551's own fix logic was refactored
/// into, so the WITH and non-WITH paths cannot drift a third time (per the
/// #551 review's explicit ask).
///
/// Live-verified (2026-07-11, db_denormalized, ClickHouse via `cg query`):
/// - #560 (`WITH b, count(*) AS n RETURN b.city, n` over `COMPOSITE_FLIGHT`,
///   unlabeled `b`): pre-fix `GROUP BY t2.origin_city` (5 rows, coincidentally
///   "correct" only because city is 1:1 with code+state in this fixture);
///   post-fix `GROUP BY t2.origin_code, t2.origin_state`, same 5 rows,
///   cross-checked against a hand-written raw SQL self-join
///   (LAX=6, ORD=2, ATL=1, DEN=1, JFK=1).
/// - #561 (`RETURN b, count(*) AS n`, no WITH, labeled `b:Airport`): pre-fix
///   `cg query` FAILS with `Code: 47. DB::Exception: Identifier 't2.code'
///   cannot be resolved from table with name t2` (hard error, every run);
///   post-fix returns 5 rows (LAX=6, ORD=2, ATL=1, DEN=1, JFK=1), matching
///   a hand-written raw SQL cross-check on `origin_code`.
///
/// Found but NOT fixed (pre-existing, separate root cause, out of scope):
/// the UNLABELED variant of #561's whole-node shape (`RETURN b, count(*)`
/// where `b` has no `:Label`) still fails identically on live ClickHouse.
/// There, `find_id_column_for_alias` does NOT return `Err` (which is the
/// condition this fix's new fallback checks) — it returns `Ok("code")`,
/// the UNMAPPED raw Cypher property name, because for this specific
/// unlabeled-connection shape a `GraphNode` gets synthesized with a
/// directly-attached `ViewScan` whose `id_column` was itself populated with
/// the raw property name instead of the role-mapped physical column. That
/// is a bug in `find_id_column_for_alias`'s/type-inference's OWN success
/// path (wrong value, not a missing fallback), a different mechanism than
/// #551/#560/#561's shared "resolution fails, falls through to the wrong
/// generic default" shape, and is NOT exercised by any of #560/#561/#562's
/// own repros — filing as a follow-up rather than fixing here to keep this
/// change's blast radius to the three named issues.
mod group_by_identity_family_560_561 {
    use super::*;

    const MIXED_SOURCES_SCHEMA: &str = "schemas/dev/flights_denorm_mixed_sources.yaml";

    /// #560 (issue's own repro shape): UNLABELED composite-id denorm chain
    /// node `b` sitting between two `COMPOSITE_FLIGHT` edges, behind a WITH
    /// barrier. Must emit BOTH composite identity columns, never collapse to
    /// the single non-identity `origin_city` display property.
    #[tokio::test]
    async fn unlabeled_composite_id_denorm_group_by_resolves_identity_560() {
        let schema = load_schema(MIXED_SOURCES_SCHEMA);
        let cypher = "MATCH (a)-[:COMPOSITE_FLIGHT]->(b)-[:COMPOSITE_FLIGHT]->(c) \
                      WITH b, count(*) AS n RETURN b.city, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let group_by_re =
            regex::Regex::new(r#"GROUP BY t\d+\.origin_code, t\d+\.origin_state"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "#560 regressed: GROUP BY must reference BOTH composite identity \
             columns (origin_code, origin_state), not the first property \
             (origin_city):\n{sql}"
        );
        assert!(
            !sql.contains("GROUP BY t1.origin_city") && !sql.contains("GROUP BY t2.origin_city"),
            "#560 regressed: GROUP BY must never collapse to the single \
             non-identity origin_city column:\n{sql}"
        );

        for _ in 0..5 {
            let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(sql, again, "#560: nondeterministic render");
        }
    }

    /// #561 (issue's TRUE sibling shape — see the module doc comment for why
    /// the issue's literal `.city`-property repro is not actually a bug):
    /// LABELED denormalized chain node `b`, WHOLE-NODE GROUP BY key
    /// (`RETURN b, count(*)` — the whole-node `b` alias, which triggers the
    /// ID-only GROUP BY optimization), with NO WITH clause. Must resolve the
    /// real identity column, never the dangling unmapped `code` / literal
    /// `"id"` sentinel.
    #[tokio::test]
    async fn labeled_whole_node_group_by_no_with_resolves_identity_561() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let cypher = "MATCH (a:Airport)-[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport) \
                      RETURN b, count(*) AS n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let group_by_re = regex::Regex::new(r#"GROUP BY t\d+\.origin_code\b"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "#561 regressed: whole-node non-WITH GROUP BY must reference the \
             node's real identity column (origin_code), never a dangling \
             unmapped placeholder:\n{sql}"
        );
        assert!(
            !sql.contains("GROUP BY t0.code")
                && !sql.contains("GROUP BY t1.code")
                && !sql.contains("GROUP BY t2.code")
                && !sql.contains("GROUP BY t0.id")
                && !sql.contains("GROUP BY t1.id")
                && !sql.contains("GROUP BY t2.id"),
            "#561 regressed: GROUP BY must never reference the unmapped raw \
             property name or the literal \"id\" sentinel:\n{sql}"
        );

        for _ in 0..5 {
            let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(sql, again, "#561: nondeterministic render");
        }
    }

    /// #561 guard: the composite-id whole-node, no-WITH shape must ALSO
    /// resolve correctly, since `composite_id_group_by_columns` (shared by
    /// both the WITH and non-WITH GROUP BY paths) now carries the #560 fix.
    #[tokio::test]
    async fn labeled_composite_id_whole_node_group_by_no_with_resolves_identity_561() {
        let schema = load_schema(MIXED_SOURCES_SCHEMA);
        let cypher = "MATCH (a:AirportComposite)-[:COMPOSITE_FLIGHT]->(b:AirportComposite)\
                      -[:COMPOSITE_FLIGHT]->(c:AirportComposite) \
                      RETURN b, count(*) AS n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let group_by_re =
            regex::Regex::new(r#"GROUP BY t\d+\.origin_code, t\d+\.origin_state"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "#561 guard: composite-id whole-node non-WITH GROUP BY must key \
             on BOTH identity columns:\n{sql}"
        );
    }

    /// #561 NON-bug guard: `RETURN b.city, count(*)` — a scalar PROPERTY
    /// projection, NOT the whole node `b` — must remain UNCHANGED. Grouping
    /// by the property VALUE is the correct Cypher semantics here (matches
    /// `group_two_keys`'s established precedent). This is deliberately the
    /// issue's ORIGINAL literal repro, locked as a "not a bug" guard: if a
    /// future change makes this start grouping by `origin_code` instead, it
    /// has REGRESSED correctness (two different-code, same-city airports
    /// would wrongly render as two output rows sharing one displayed city
    /// instead of merging into one, per real Cypher aggregation semantics).
    #[tokio::test]
    async fn scalar_property_only_group_by_no_with_groups_by_value_not_identity() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let cypher = "MATCH (a:Airport)-[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport) \
                      RETURN b.city, count(*) AS n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let group_by_re = regex::Regex::new(r#"GROUP BY t\d+\.origin_city\b"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "a scalar b.city projection (no WITH, no whole-node b) must \
             group by the property VALUE per Cypher semantics, not the \
             node's identity column:\n{sql}"
        );
    }

    /// #551 guard, re-verified with the #560/#561 fixes in place: the
    /// single-id WITH-aggregate path must still render byte-identically —
    /// `resolve_single_id_denorm_column` (the new shared helper both paths
    /// now call) must reproduce #551's own fix output exactly.
    #[tokio::test]
    async fn single_id_with_aggregate_unaffected_by_560_561_shared_helper() {
        let schema = load_schema(MIXED_SOURCES_SCHEMA);
        let cypher = "MATCH (a)-[:CARRIER_FLIGHT]->(b)-[:CARRIER_FLIGHT]->(c) \
                      WITH b, count(*) AS n RETURN b.city, b.carrier, n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let group_by_re = regex::Regex::new(r#"GROUP BY t\d+\.origin_code\b"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "#551 must remain unaffected by the #560/#561 shared-helper \
             refactor:\n{sql}"
        );
    }

    /// #563: the TRUE unlabeled sibling of #561's LABELED
    /// `labeled_whole_node_group_by_no_with_resolves_identity_561` above —
    /// same query shape, `b` has NO `:Label`. This used to hard-crash
    /// ClickHouse with `Code: 47 UNKNOWN_IDENTIFIER 't2.code'` (verified
    /// pre-fix, live, against `db_denormalized`) even though #561 had
    /// already fixed the labeled twin.
    ///
    /// Root cause (distinct from #561's own fix, confirmed by #561's
    /// reviewer): `find_id_column_for_alias` (`render_plan/plan_builder.rs`)
    /// returned `Ok("code")` — not `Err` — for this shape, so the #551/#560/
    /// #561 fallback chain (which only engages on `Err`) never triggered.
    /// The `Ok` came from a GENUINE `GraphNode` that DOES exist for the
    /// unlabeled alias (synthesized by `type_inference.rs`'s "infer a single
    /// label from context" path, `LogicalPlan::GraphNode` case) whose
    /// `ViewScan.id_column` was set straight from the schema's raw Cypher
    /// `node_id` property name, never mapped through `from_node_properties`/
    /// `to_node_properties` like `try_generate_view_scan`'s single-table case
    /// does for a genuinely LABELED node.
    ///
    /// Fixed by `denorm_id_column_is_unresolved` (`plan_builder.rs`):
    /// `find_id_column_for_alias`'s SUCCESS path now refuses to trust
    /// `scan.id_column` when it is itself a KEY in the scan's own
    /// `from_node_properties`/`to_node_properties` (the reliable signal that
    /// it's still a raw property name, never a real physical column — a
    /// resolved column is always a VALUE there, never a key) — falling
    /// through to `Err` so the existing, already-correct fallback chain
    /// resolves it via `get_properties_with_table_alias` instead.
    ///
    /// Live-verified against `db_denormalized` (2026-07-11): the fixed SQL
    /// executes and returns 5 rows keyed by airport identity (LAX=6, ORD=2,
    /// ATL=1, JFK=1, DEN=1) — previously `Code: 47 UNKNOWN_IDENTIFIER`.
    #[tokio::test]
    async fn unlabeled_whole_node_group_by_no_with_resolves_identity_563() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let cypher = "MATCH (a:Airport)-[:FLIGHT]->(b)-[:FLIGHT]->(c:Airport) \
                      RETURN b, count(*) AS n";
        let sql = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);

        let group_by_re = regex::Regex::new(r#"GROUP BY t\d+\.origin_code\b"#).unwrap();
        assert!(
            group_by_re.is_match(&sql),
            "#563 regressed: whole-node non-WITH GROUP BY over an UNLABELED \
             denorm chain node must reference the node's real identity \
             column (origin_code), never a dangling unmapped placeholder:\n{sql}"
        );
        assert!(
            !sql.contains("GROUP BY t0.code")
                && !sql.contains("GROUP BY t1.code")
                && !sql.contains("GROUP BY t2.code")
                && !sql.contains("GROUP BY t0.id")
                && !sql.contains("GROUP BY t1.id")
                && !sql.contains("GROUP BY t2.id"),
            "#563 regressed: GROUP BY must never reference the unmapped raw \
             property name or the literal \"id\" sentinel:\n{sql}"
        );

        for _ in 0..5 {
            let again = normalize(&render(&schema, cypher, SqlDialect::ClickHouse).await);
            assert_eq!(sql, again, "#563: nondeterministic render");
        }
    }
}

/// Regression tests for the #546/#547 ORDER BY id() family — milder siblings
/// of #536's GROUP BY/ORDER BY id()-resolution-when-label-unresolvable bug,
/// found during that fix's adversarial review (both pre-existing on `main`,
/// reproducing identically before and after fix/536).
mod order_by_id_union_family_546_547 {
    use super::*;

    /// #546: `MATCH (n) RETURN n ORDER BY id(n)` on a genuinely unsafe raw
    /// multi-label union (bare unlabeled node scan, no VLP/pattern_union
    /// connection to anchor a safe single-alias column) used to silently
    /// DROP the ORDER BY clause entirely — `extract_order_by_columns_for_union`
    /// drops the unresolved `id()` call (there is no single addressable
    /// column for it in the OUTER `__union` scope), and the renderer's
    /// "removed" branch gave up on the whole clause rather than trying
    /// anything else, so rows silently came back unordered with no error.
    ///
    /// Fix (reworked after adversarial review): each union branch projects
    /// its OWN id column — read from the branch's FROM-bound `ViewScan`, the
    /// scan built from exactly that branch's label's schema — as a
    /// `__order_col_{N}` helper wrapped in the typed total-order key
    /// `tuple(toInt128OrNull(toString(id)), toString(id))`, and the outer
    /// query orders by that helper. The review's Bug 1 killed the first
    /// attempt (a `coalesce` over the branches' ALREADY-stringified display
    /// columns): a String key sorts numeric ids LEXICOGRAPHICALLY, which
    /// only LOOKED right on single-digit fixtures. Live-verified with
    /// MULTI-DIGIT ids (2026-07-11, users/posts ids 1..12 via ClickHouse):
    /// `LIMIT 8` returns ids 1,1,2,2,3,3,4,4 — the lexicographic key
    /// returned 1,1,10,10,11,11,12,12.
    #[tokio::test]
    async fn order_by_id_bare_multi_label_union_uses_per_branch_typed_key_546() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (n) RETURN n ORDER BY id(n)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("ORDER BY __union.`__order_col_0` ASC"),
            "#546: ORDER BY id(n) over a bare multi-label union must not be \
             silently dropped — it must order on the per-branch helper \
             column:\n{sql}"
        );
        // Each branch's helper must be that branch's OWN id column, in the
        // typed (numerically-ordering) key shape — NOT a coalesce over the
        // stringified display columns (review Bug 1: lexicographic 1,10,11,
        // ...,2 ordering on multi-digit numeric ids).
        assert!(
            sql.contains(
                "tuple(toInt128OrNull(toString(n.user_id)), toString(n.user_id)) AS \"__order_col_0\""
            ) && sql.contains(
                "tuple(toInt128OrNull(toString(n.post_id)), toString(n.post_id)) AS \"__order_col_0\""
            ),
            "#546: every union branch must project its own label's id column \
             as the typed __order_col_0 helper key:\n{sql}"
        );
        assert!(
            !sql.contains("coalesce(__union."),
            "#546: the reverted lexicographic coalesce-over-display-columns \
             key must not come back:\n{sql}"
        );
    }

    /// #546 (review Bug 2 — name coincidence): `Comment`'s id column is
    /// `comment_id`, but Comment ALSO has a plain property `user_id` (its
    /// author's id) that shares `Author`'s id-column NAME. The reverted
    /// first salvage collected coalesce candidates by iterating EVERY schema
    /// label (`expand_node_type("$any")`) and name-matching projected
    /// columns, so the key became `coalesce(n.user_id, n.comment_id)` —
    /// `n.user_id` is non-NULL on BOTH branches (Author's own id AND
    /// Comment's author reference), so Comments sorted by their AUTHOR's id.
    /// Live-reproduced (2026-07-11): with authors 1..12 and comments whose
    /// author = 13 - comment_id, comment_12 (author 1) sorted FIRST.
    ///
    /// The rework reads each branch's own `ViewScan.id_column` (built from
    /// exactly that branch's label's schema), so the Comment branch keys on
    /// `comment_id` and the Author branch on `user_id` — no cross-label name
    /// matching exists to hijack. Live-verified (2026-07-11): comments
    /// interleave by their OWN comment_id (comment_1..comment_4 in the first
    /// 8 rows), not by author id.
    #[tokio::test]
    async fn order_by_id_name_coincidence_property_does_not_hijack_key_546() {
        let schema = load_schema("schemas/dev/review546_name_coincidence.yaml");
        let sql = render(
            &schema,
            "MATCH (n) RETURN n ORDER BY id(n)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("ORDER BY __union.`__order_col_0` ASC"),
            "#546: the salvage must still fire on this shape:\n{sql}"
        );
        // The Comment branch's key must be ITS OWN id column (comment_id) —
        // never the author-reference user_id that merely shares Author's
        // id-column name. The Comment branch is identifiable by its `body`
        // projection being non-NULL.
        assert!(
            sql.contains(
                "tuple(toInt128OrNull(toString(n.comment_id)), toString(n.comment_id)) AS \"__order_col_0\""
            ),
            "#546 Bug 2: the Comment branch must order by its own comment_id:\n{sql}"
        );
        assert!(
            !sql.contains("coalesce(__union."),
            "#546 Bug 2: no outer coalesce key (the mechanism that let \
             `n.user_id` hijack Comment ordering) may be emitted:\n{sql}"
        );
    }

    /// #546 (former residual, now covered by the rework): a bare
    /// `RETURN id(n)` used to have no salvageable key — the first attempt
    /// could only coalesce over ALREADY-projected columns, and this shape
    /// projects none. The per-branch helper key doesn't depend on existing
    /// projections (each branch can always project its own id column), so
    /// this now orders correctly too. The RETURNED `id(n)` value itself is
    /// still the pre-existing #484-family `toInt64(0)` placeholder — that
    /// documented limitation is unchanged; only the ORDERING is real now.
    #[tokio::test]
    async fn order_by_id_bare_projection_only_now_salvaged_546() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (n) RETURN id(n) ORDER BY id(n)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("ORDER BY __union.`__order_col_0` ASC")
                && sql.contains("tuple(toInt128OrNull(toString(n.user_id)), toString(n.user_id))"),
            "#546: a bare `RETURN id(n)` now gets the same per-branch typed \
             ordering key (the helper column doesn't depend on the RETURN \
             projection):\n{sql}"
        );
    }

    /// #547: a multi-item ORDER BY where an unresolvable raw-union item is
    /// dropped by `extract_order_by_columns_for_union` and a LATER item
    /// survives used to misalign: the final ORDER BY clause looked up
    /// survivors by their ORIGINAL positional index into a list that only
    /// contains SURVIVORS (shorter than the original list whenever anything
    /// was dropped), silently attaching an EARLIER (dropped) item's sort
    /// direction to a LATER item's column and losing the later item's own
    /// ordering key entirely.
    ///
    /// Fix: derive each surviving column's original ORDER BY item from the
    /// index encoded in its own `__order_col_{N}` alias (assigned by
    /// `extract_order_by_columns_for_union` from the ORIGINAL list position),
    /// rather than a naive parallel walk of the original list that silently
    /// desynchronizes whenever an earlier item was dropped.
    ///
    /// Uses `elementId(n)` as the dropped item: after the #546 rework,
    /// `id(n)` on this shape is salvaged (see the mixed test below), while
    /// `elementId()` still has no raw-union resolution and keeps the
    /// documented drop — so it still exercises the dropped-then-survivor
    /// alignment path this test locks.
    #[tokio::test]
    async fn order_by_multi_item_survivor_after_dropped_item_keeps_own_key_547() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (n) RETURN n ORDER BY elementId(n), 1",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("ORDER BY"),
            "#547: the surviving literal ORDER BY key must not vanish just \
             because an earlier item (elementId(n)) was unresolvable:\n{sql}"
        );
        // `elementId(n)` is dropped, but the literal survivor must keep its
        // own `__order_col_1` alias (its ORIGINAL position in the 2-item
        // list) rather than being misattributed to the first (dropped)
        // item's position — and it must be the ONLY column in the clause.
        assert!(
            sql.contains("ORDER BY __union.`__order_col_1` ASC") && !sql.contains("__order_col_0"),
            "#547: the second ORDER BY item (literal `1`) must be referenced \
             by ITS OWN encoded index (__order_col_1), not misattributed to \
             the first (dropped) item's position, and no stray __order_col_0 \
             (which was never generated, since elementId(n) was dropped) \
             should appear:\n{sql}"
        );
    }

    /// #546 rework bonus (the review's non-blocking finding 3): a mixed
    /// `ORDER BY id(n), <survivor>` used to silently drop the id key while
    /// keeping the survivor — the first #546 salvage only ran when EVERY
    /// item had been dropped. The rework salvages `id(n)` at extraction
    /// time, so it flows through the normal surviving-columns path alongside
    /// the survivor and BOTH keys are kept, each under its own encoded
    /// index.
    #[tokio::test]
    async fn order_by_mixed_id_and_survivor_keeps_both_keys_546() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (n) RETURN n ORDER BY id(n), 1",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            // #556: the id(n) salvage key now carries an explicit NULLS LAST
            // (dialect-agreement fix) between the two ORDER BY items.
            sql.contains(
                "ORDER BY __union.`__order_col_0` ASC NULLS LAST, __union.`__order_col_1` ASC"
            ),
            "#546/finding 3: BOTH the salvaged id(n) key (__order_col_0) and \
             the surviving literal (__order_col_1) must appear, in query \
             order:\n{sql}"
        );
        assert!(
            sql.contains("tuple(toInt128OrNull(toString(n.user_id)), toString(n.user_id))"),
            "#546/finding 3: the id(n) key must be the per-branch typed id \
             key, not a placeholder:\n{sql}"
        );
    }

    /// #546 rework residual (documented, deliberate): a multi-label union
    /// containing a COMPOSITE-id label keeps the pre-#546 drop.
    /// `ViewScan.id_column` is a single `String` by construction and holds
    /// only the FIRST composite component (#537's known truncation), so a
    /// key built from it would silently interleave distinct composite ids —
    /// the salvage is all-or-nothing across branches and refuses instead
    /// (`branch_scan_label_has_composite_id`), preferring the documented
    /// unordered-with-warning behavior over a plausible-looking wrong order.
    #[tokio::test]
    async fn order_by_id_composite_id_union_keeps_documented_drop_546() {
        let schema = load_schema(SchemaId::CompositeId.yaml_path());
        let sql = render(
            &schema,
            "MATCH (n) RETURN n ORDER BY id(n)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            !sql.contains("ORDER BY"),
            "#546 residual: a composite-id label in the union has no \
             single-column per-branch id key — if this now contains an \
             ORDER BY, the salvage learned composite keys; update this test \
             to assert the full composite ordering instead:\n{sql}"
        );
    }

    /// #546 rework: DESC direction is preserved on the salvaged key.
    /// Live-verified (2026-07-11, users/posts ids 1..12): `DESC LIMIT 4`
    /// returns ids 12,12,11,11.
    #[tokio::test]
    async fn order_by_id_desc_direction_preserved_546() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (n) RETURN n ORDER BY id(n) DESC",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("ORDER BY __union.`__order_col_0` DESC"),
            "#546: DESC must survive onto the salvaged per-branch key:\n{sql}"
        );
    }
}

/// Regression test for #548: `get_table_alias_if_single_table_condition`
/// (`filter_tagging.rs`) correctly handles a single cross-table reference in
/// an IN-list (`x.a IN [y.b]`, fixed by #542), but a 2+-element list where
/// MULTIPLE elements reference DIFFERENT tables (`x.a IN [y.b, z.c]`) was
/// still misclassified as single-table — because the List arm's own
/// "elements disagree -> None" conflict signal was indistinguishable, to
/// every caller's `if let Some(alias) = ...` pattern, from "this operand
/// carries no alias info at all", so the conflict was silently dropped
/// instead of propagated as "not single-table". Downstream this leaked a raw,
/// never-bound Cypher alias into the WHERE clause (e.g. `srcip2."id.orig_h"`
/// referencing a table alias that only exists as an un-joined Cypher name,
/// never a real JOIN alias) — the exact same class of bug #542 fixed for the
/// single-cross-table-element case.
mod filter_tagging_in_list_family_548 {
    use super::*;

    /// Live-verified (2026-07-11, `schemas/dev/zeek_merged_test.yaml`):
    /// before the fix, the WHERE clause read `t1."id.orig_h" =
    /// toString(srcip2."id.orig_h") OR t1."id.orig_h" =
    /// toString(destip."id.resp_h")` — both `srcip2` and `destip` are raw,
    /// unbound Cypher aliases (neither is a real FROM/JOIN alias; only `t1`/
    /// `t2` are). After the fix both resolve to `conn_log`'s real bound JOIN
    /// alias (`t2`).
    #[tokio::test]
    async fn in_list_multi_element_cross_alias_correlation_resolves_bound_aliases_548() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql = render(
            &schema,
            "MATCH (srcip1:IP)-[:REQUESTED]->(d:Domain), \
             (srcip2:IP)-[:ACCESSED]->(destip:IP) \
             WHERE srcip1.ip IN [srcip2.ip, destip.ip] \
             RETURN srcip1.ip, d.name, destip.ip",
            SqlDialect::ClickHouse,
        )
        .await;

        let where_clause = sql
            .rsplit("WHERE")
            .next()
            .expect("query must have a WHERE clause");
        assert!(
            !where_clause.contains("srcip1.")
                && !where_clause.contains("srcip2.")
                && !where_clause.contains("destip."),
            "#548: no raw Cypher alias should leak into the WHERE clause as \
             an unbound SQL identifier — every reference must resolve to its \
             bound JOIN alias (e.g. `t1`/`t2`):\n{sql}"
        );
        let conn_log_alias = sql
            .split("zeek.conn_log AS ")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .unwrap_or_else(|| panic!("expected a `JOIN zeek.conn_log AS <alias>`:\n{sql}"));
        assert!(
            where_clause.contains(&format!("{conn_log_alias}.")), // `srcip2` and `destip` are two DIFFERENT references onto the
            // same `conn_log` table (two columns of the same JOIN), so
            // both may legitimately share one alias occurrence count of
            // 2 in the WHERE clause — just assert the alias is used.
            "#548: expected both correlated list elements (`srcip2`, \
             `destip`) to resolve to conn_log's real bound JOIN alias \
             `{conn_log_alias}` in the WHERE clause, not vanish or stay \
             unbound:\n{sql}"
        );
    }
}

/// Regression tests for the #544/#545/#528/#525 family — the follow-up batch
/// from the fix/524-vlp-cte-binding-family adversarial review (see the
/// `vlp_cte_binding_family_524_528_538_525` module above for the prior
/// round's fixes and the original #528/#525 characterizations).
///
/// #544's root cause generalizes well beyond the issue's literal repro shape:
/// the ENTIRE VLP pipeline (the `PlanCtx` endpoint registry keyed by node
/// alias, the render phase's single shared `VLP_CTE_FROM_ALIAS` (`"t"`) outer
/// alias, and `rewrite_vlp_select_aliases`'s single-CTE rewrite) supports
/// exactly ONE required recursive VLP per MATCH scope, plus one deliberate
/// exception (same-end fan-in, explicitly JOINed by `to_sql_query.rs`). Every
/// other multi-VLP configuration — fixed-hop-bridged (the filed repro),
/// directly chained, fan-out, disjoint — was live-verified to silently DROP
/// all but one VLP's CTE and conflate its endpoints with the surviving one.
/// Silently wrong results being the worst possible outcome, the fix makes the
/// planner reject these loudly (`AnalyzerError::UnsupportedPattern`, marked
/// FATAL so `initial_analyzing`'s GraphJoinInference catch re-raises instead
/// of falling back to the untransformed — and silently wrong — plan) until
/// the render phase can emit N independently-aliased VLP CTEs.
mod vlp_family_remnants_544_545_528_525 {
    use super::*;

    /// #544 (the filed repro shape): two independent VLPs with non-overlapping
    /// aliases bridged by a fixed hop. Pre-fix this rendered `a.name`/`b.name`
    /// from the c→d CTE's start/end columns (the a→b VLP was silently dropped
    /// and never even built) — byte-verified silently wrong. Must now fail
    /// loudly with the #544 rejection.
    #[tokio::test]
    async fn multi_vlp_fixed_hop_bridge_rejected_loudly_544() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let err = try_render(
            &schema,
            "MATCH (a:User)-[*1..2]->(b:User)-[:FOLLOWS]->(c:User)-[*2..3]->(d:User) \
             RETURN a.name, b.name, c.name, d.name",
            SqlDialect::ClickHouse,
        )
        .await
        .expect_err("two VLPs bridged by a fixed hop must error, not conflate endpoints");
        assert!(err.contains("#544"), "expected a #544 error: {err}");
    }

    /// #544 (chained variant): two directly adjacent VLPs sharing the middle
    /// node. Pre-fix only `vlp_b_c` was emitted — `a.name` silently became
    /// b's start_name and the a→b hop constraint vanished entirely.
    #[tokio::test]
    async fn chained_vlps_rejected_loudly_544() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let err = try_render(
            &schema,
            "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS*1..2]->(c:User) \
             RETURN a.name, c.name",
            SqlDialect::ClickHouse,
        )
        .await
        .expect_err("chained VLPs must error, not silently drop the first VLP");
        assert!(err.contains("#544"), "expected a #544 error: {err}");
    }

    /// #544 (fan-out variant): one start fanning out through two VLPs. Unlike
    /// same-end fan-in (below), fan-OUT has no render-phase support at all:
    /// pre-fix `vlp_x_a` was silently dropped and `a.name` was projected from
    /// `vlp_x_b`'s end column (`a.name` == `b.name` for every row).
    #[tokio::test]
    async fn fan_out_vlps_rejected_loudly_544() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let err = try_render(
            &schema,
            "MATCH (x:User)-[:FOLLOWS*1..2]->(a:User), (x)-[:FOLLOWS*1..2]->(b:User) \
             RETURN x.name, a.name, b.name",
            SqlDialect::ClickHouse,
        )
        .await
        .expect_err("fan-out VLPs must error, not conflate the two end nodes");
        assert!(err.contains("#544"), "expected a #544 error: {err}");
    }

    /// #544 (guard must NOT over-fire): same-end fan-in is the one multi-VLP
    /// configuration with genuine render support (`to_sql_query.rs` detects
    /// it and INNER JOINs the sibling CTEs on end_id). It must keep working.
    ///
    /// KNOWN REMAINING GAP (deliberately not locked here): projecting the
    /// SECOND fan-in VLP's start-node properties (e.g. `RETURN b.name`) still
    /// resolves to the shared `t` alias (`t.start_name`, i.e. `a`'s value)
    /// instead of `t_fi_0.start_name` — same root cause as #544 (single
    /// shared VLP alias), fixable only by the same render-phase multi-alias
    /// work. End-alias projections (x) and the join structure are correct.
    #[tokio::test]
    async fn fan_in_same_end_vlps_still_render_544() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (a:User)-[:FOLLOWS*1..2]->(x:User), (b:User)-[:FOLLOWS*1..2]->(x:User) \
             RETURN x.name",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("vlp_a_x") && sql.contains("vlp_b_x"),
            "fan-in must still build BOTH VLP CTEs: {sql}"
        );
        assert!(
            sql.contains("ON t_fi_0.end_id = t.end_id"),
            "fan-in must still INNER JOIN the sibling CTE on end_id: {sql}"
        );
    }

    /// #544 (guard must NOT over-fire): a single VLP with fixed hops on
    /// either side — the #521/#524 shapes — is one VLP per scope and must be
    /// completely unaffected by the multi-VLP rejection.
    #[tokio::test]
    async fn single_vlp_with_fixed_hops_unaffected_by_544_guard() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS*1..2]->(c:User)-[:AUTHORED]->(p:Post) \
             RETURN a.name, p.title",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.to_uppercase().contains("WITH RECURSIVE"),
            "single VLP flanked by fixed hops must still render its CTE: {sql}"
        );
    }

    /// #617: undirected single-type VLP compiles to ONE directed walk over a
    /// doubled-edge CTE — the former two-monotone-arm Union could not
    /// represent mixed-direction paths (a→m←b) and silently under-counted
    /// 40-60% at >=2 hops (live-verified vs a trail-semantics oracle:
    /// *2..2 38→64, *1..2 58→84, *2..3 92→226, *3..3 54→162).
    #[tokio::test]
    async fn undirected_vlp_single_doubled_edge_walk_617() {
        let schema = load_schema(SchemaId::Standard.yaml_path());

        // Range: single recursive walk over the doubled-edge CTE, trail
        // uniqueness on ORIGINAL-orientation identity.
        let sql = render(
            &schema,
            "MATCH (a:User)-[:FOLLOWS*1..2]-(b:User) RETURN count(*)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("undir_edges_a_b AS (") && sql.contains("vlp_a_b"),
            "range undirected VLP must walk the doubled-edge CTE: {sql}"
        );
        assert!(
            !sql.contains("vlp_b_a"),
            "range undirected VLP must not emit a reverse arm: {sql}"
        );
        assert!(
            sql.contains("NOT has(vp.path_edges, tuple(rel.__cg_orig_from, rel.__cg_orig_to))"),
            "trail uniqueness must compare original-orientation identity: {sql}"
        );
        // Both orientations of every edge, original identity carried along.
        assert!(
            sql.contains("SELECT e.followed_id AS follower_id, e.follower_id AS followed_id")
                && sql.contains("e.follower_id AS __cg_orig_from"),
            "doubled-edge CTE must emit the reverse orientation with original identity: {sql}"
        );

        // Exact >=2 hops: flat chain over the SAME doubled-edge CTE, pairwise
        // uniqueness on the original-identity columns.
        let sql = render(
            &schema,
            "MATCH (a:User)-[:FOLLOWS*2..2]-(b:User) RETURN a.name, b.name",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("JOIN undir_edges_a_b AS r1")
                && sql.contains("JOIN undir_edges_a_b AS r2"),
            "exact-bound undirected chain must hop over the doubled-edge CTE: {sql}"
        );
        assert!(
            sql.contains(
                "NOT (r1.__cg_orig_from = r2.__cg_orig_from AND r1.__cg_orig_to = r2.__cg_orig_to)"
            ),
            "pairwise uniqueness must compare original-orientation identity: {sql}"
        );
        assert_eq!(
            sql.matches("UNION ALL").count(),
            1, // the doubled-edge CTE's own two orientations — no query arms
            "exact-bound undirected must be a single arm: {sql}"
        );

        // *1..1 is complete under the legacy two-arm split — unchanged.
        let sql = render(
            &schema,
            "MATCH (a:User)-[:FOLLOWS*1..1]-(b:User) RETURN count(*)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("UNION ALL") && !sql.contains("undir_edges"),
            "*1..1 undirected must stay on the two-arm path: {sql}"
        );

        // shortestPath undirected stays on the legacy two-arm split.
        let sql = render(
            &schema,
            "MATCH p = shortestPath((a:User)-[:FOLLOWS*1..3]-(b:User)) \
             WHERE a.user_id = 1 RETURN b.user_id, length(p)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("vlp_b_a") && !sql.contains("undir_edges"),
            "undirected shortestPath must keep the two-arm split: {sql}"
        );

        // OPTIONAL undirected range: LEFT JOIN onto the single-walk CTE
        // (was Code 47 on the two-arm plan).
        let sql = render(
            &schema,
            "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS*1..2]-(b:User) \
             RETURN a.name, count(b)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("LEFT JOIN vlp_") && sql.contains("undir_edges_a_b"),
            "OPTIONAL undirected range must LEFT JOIN the single-walk CTE: {sql}"
        );
    }

    /// #544 (guard must NOT over-fire): an undirected single VLP splits into
    /// two direction-variant Union branches that REUSE the same alias pair —
    /// each branch has exactly one VLP and must not trip the per-scope count.
    #[tokio::test]
    async fn undirected_single_vlp_unaffected_by_544_guard() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (u1:User)-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post) \
             RETURN u1.name, p.title",
            SqlDialect::ClickHouse,
        )
        .await;
        // #617: undirected single-type VLP renders as ONE doubled-edge walk
        // (undir_edges_* + a single vlp CTE) — the former two-direction arms
        // (vlp_u1_u2 + vlp_u2_u1) under-counted mixed-direction paths. The
        // #544 guard must leave the chained AUTHORED continuation intact.
        assert!(
            sql.contains("vlp_u1_u2") && sql.contains("undir_edges_u1_u2"),
            "undirected VLP must render the doubled-edge single walk: {sql}"
        );
        assert!(
            !sql.contains("vlp_u2_u1"),
            "undirected VLP must NOT render a reverse direction arm: {sql}"
        );
        assert!(
            sql.contains("authored_bench"),
            "chained AUTHORED hop must survive the #544 guard: {sql}"
        );
    }

    /// #545 — could NOT be reproduced as filed, despite substantial effort
    /// (locked as a PASSING regression test, the same honest outcome as #540
    /// last wave). The claim was: requesting a property with the same
    /// canonical/logical alias on BOTH VLP endpoints of a denormalized
    /// virtual node (e.g. `a.ip`/`b.ip`, both mapping to logical name `ip`)
    /// drops one side's column entirely. Tested (2026-07-11, `cg sql` +
    /// live ClickHouse where syntactically possible) on BOTH zeek schemas
    /// across directed/undirected, GROUP BY, ORDER BY, collect(), `*0..2`,
    /// OPTIONAL MATCH, asymmetric requirements (`RETURN a, b.ip`), and the
    /// deliberately-colliding `seen: ts`/`uid: uid` mappings of
    /// `schemas/test/zeek_merged_collision.yaml` (same logical name AND same
    /// physical column on both roles): the denormalized VLP CTE's
    /// `start_`/`end_` prefixing disambiguated every case — both sides
    /// always exported and projected distinctly, and the `a.seen`/`b.seen`
    /// query returns correct, distinct-per-side values against live data
    /// (multi-hop rows show different start/end timestamps).
    ///
    /// Two ADJACENT (loud, NOT silent) zeek bugs were found while trying to
    /// reproduce — both distinct from #545's same-logical-name axis, and
    /// since fixed (filed as #558 and #559; both verified live against
    /// ClickHouse):
    ///
    /// 1. (#558) Tuple-field (dotted) physical columns, e.g. IP's `ip:
    ///    "id.orig_h"`, were embedded UNQUOTED into the denorm VLP CTE's
    ///    column aliases (`t1.id.orig_h as start_id.orig_h`) — ClickHouse
    ///    Code 62 SYNTAX_ERROR on execution for ANY query touching such a
    ///    property through a denorm VLP CTE. Root:
    ///    `DenormalizedCteStrategy`'s `add_property_selections` /
    ///    `add_recursive_property_selections` (`cte_manager/mod.rs`) built
    ///    the alias text via raw `format!`. Fixed by quoting both the read
    ///    reference and the alias through the dialect-dispatched
    ///    `FunctionMapper::quote_alias` (the same helper the outer query's
    ///    `quote_qualified_col` already used), so a physical column's own
    ///    dots no longer collide with the CTE-column-naming grammar.
    /// 2. (#559) A leading fixed hop into a VLP start endpoint whose label
    ///    maps to MULTIPLE tables with per-role property mappings
    ///    (`(x:IP)-[:ACCESSED]->(a:IP)-[:ACCESSED*1..2]->(b:IP)` on
    ///    `schemas/dev/zeek_merged_test.yaml`) resolved `a.ip` in the outer
    ///    SELECT via the TO-role mapping (`id.resp_h` → nonexistent
    ///    `t."start_id.resp_h"`) while the CTE exported the FROM-role column
    ///    (`start_id.orig_h`) — ClickHouse Code 47 UNKNOWN_IDENTIFIER
    ///    (behind the Code 62 above). The leading hop's JOIN itself was
    ///    already correctly correlated (`t1."id.resp_h" = t.start_id`) —
    ///    #524's fix held; only the projection's role choice was wrong.
    ///    Root: a VLP relationship skips ordinary JOIN inference entirely
    ///    (handled by CTE generation instead), so it never registered its
    ///    own `PatternSchemaContext` — `PlanCtx::get_node_strategy` fell
    ///    back to the alias's OTHER (fixed-hop) registration, resolving the
    ///    node's role from the wrong edge. Fixed by registering the VLP's
    ///    own pattern context even on the JOIN-skip path
    ///    (`graph_join/inference.rs`) and having `get_node_strategy` prefer
    ///    a registered VLP endpoint's own role over the fixed-hop fallback
    ///    (`plan_ctx/mod.rs`) — see `tests/rust/integration/sql_golden_tests.rs`'s
    ///    `vlp_multi_table_label_family_557_558_559` module for dedicated
    ///    regression coverage of both, plus #557 (a third, related bug in
    ///    the same area: multi-type VLP CTE generation for an unlabeled end
    ///    node).
    #[tokio::test]
    async fn same_logical_property_both_vlp_endpoints_projects_both_distinctly_545() {
        // Deliberate collision schema: `seen: ts` on BOTH from/to role maps.
        let schema = load_schema("schemas/test/zeek_merged_collision.yaml");
        let sql = render(
            &schema,
            "MATCH (a:IP)-[:ACCESSED*1..2]->(b:IP) RETURN a.seen, b.seen",
            SqlDialect::ClickHouse,
        )
        .await;
        // NOTE: the edge-table alias number (t1/t654/…) comes from a
        // process-wide counter and is not per-test deterministic — match on
        // the column expression only.
        // #558: the alias (and, since `ts` needs no escaping either way, the
        // read reference too) is quoted through `FunctionMapper::quote_alias`.
        assert!(
            sql.contains(r#"."ts" as "start_ts""#) && sql.contains(r#"."ts" as "end_ts""#),
            "#545: BOTH endpoints' same-logical-name property must be \
             exported under distinct start_/end_ CTE columns: {sql}"
        );
        assert!(
            sql.contains(r#"t.start_ts AS "a.seen""#) && sql.contains(r#"t.end_ts AS "b.seen""#),
            "#545: the outer SELECT must project each side from its own \
             prefixed CTE column: {sql}"
        );
        // The recursive term must thread the start side and advance the end
        // side — NOT collapse them onto one expression.
        assert!(
            sql.contains(r#"vp."start_ts" as "start_ts""#)
                && sql.contains(r#"next."ts" as "end_ts""#),
            "#545: recursive case must keep the two sides independent: {sql}"
        );

        // Same-logical-name `ip` on the dev zeek schema (different physical
        // columns per role) — also both exported, both projected.
        let schema2 = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql2 = render(
            &schema2,
            "MATCH (a:IP)-[:ACCESSED*1..2]->(b:IP) RETURN a.ip, b.ip",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql2.contains(r#"AS "a.ip""#) && sql2.contains(r#"AS "b.ip""#),
            "#545: both endpoint projections must be present: {sql2}"
        );
        assert!(
            sql2.contains("start_id.orig_h") && sql2.contains("end_id.resp_h"),
            "#545: each side must resolve to its own role's physical column \
             (these dotted CTE column names are now properly quoted — #558 — \
             so this is valid ClickHouse SQL, not just correct column-export \
             logic): {sql2}"
        );
    }
}

/// Regression tests for the #557/#558/#559 VLP CTE structural family — three
/// related bugs surfaced together (fixing #558 partially unblocked #559; the
/// third, #557, is a separate CTE-count-mismatch bug in the same area).
mod vlp_multi_table_label_family_557_558_559 {
    use super::*;

    /// #558: `DenormalizedCteStrategy`'s VLP CTE property-column naming used
    /// to embed a raw dotted physical column name straight into an output
    /// alias (`t2.id.orig_h as start_id.orig_h`) — invalid ClickHouse syntax
    /// (Code 62) whenever the underlying column name itself contains a dot
    /// (a Tuple/Nested-style column, e.g. zeek's `id.orig_h`/`id.resp_h`).
    /// Fixed by quoting both the read reference and the alias through
    /// `FunctionMapper::quote_alias`. Confirmed live against ClickHouse
    /// 25.8.12 (docker `clickhouse-dev`) — the pre-fix SQL fails with
    /// `Code: 62. DB::Exception: Syntax error`, the fixed SQL executes and
    /// returns correct multi-hop path rows.
    #[tokio::test]
    async fn denorm_vlp_dotted_property_alias_is_quoted_not_raw_558() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql = render(
            &schema,
            "MATCH (a:IP)-[:ACCESSED*1..2]->(b:IP) RETURN a.ip, b.ip",
            SqlDialect::ClickHouse,
        )
        .await;
        // The invalid pre-fix shape: a bare (unquoted) dotted alias.
        assert!(
            !sql.contains("as start_id.orig_h") && !sql.contains("as end_id.resp_h"),
            "#558: a dotted physical column must never be embedded raw into \
             an unquoted output alias (ClickHouse Code 62): {sql}"
        );
        // The fixed shape: quoted alias, defined AND referenced consistently
        // (base case, recursive case, and the outer SELECT all agree).
        assert!(
            sql.contains(r#"as "start_id.orig_h""#) && sql.contains(r#"as "end_id.resp_h""#),
            "#558: the dotted CTE column alias must be quoted: {sql}"
        );
        assert!(
            sql.contains(r#"vp."start_id.orig_h" as "start_id.orig_h""#),
            "#558: the recursive case must carry the quoted start column \
             forward under the same quoted name: {sql}"
        );
        assert!(
            sql.contains(r#"t."start_id.orig_h" AS "a.ip""#)
                && sql.contains(r#"t."end_id.resp_h" AS "b.ip""#),
            "#558: the outer SELECT must reference the quoted CTE columns: {sql}"
        );
    }

    /// #559: a VLP start endpoint that is ALSO a fixed-hop endpoint
    /// (`(x)-[:ACCESSED]->(a)-[:ACCESSED*1..2]->(b)`) on a denormalized node
    /// label mapped to MULTIPLE physical tables (zeek's `IP`: dns_log AND
    /// conn_log) used to resolve `a`'s property via the FIXED hop's role
    /// (`a` as TO-node of `x->a`, i.e. the `id.resp_h` mapping) instead of
    /// the VLP's OWN role (`a` as FROM-node of `a->b`, i.e. `id.orig_h`) —
    /// producing an outer SELECT reference to a CTE column
    /// (`t."start_id.resp_h"`) that was never actually exported by the CTE
    /// (only `end_id.resp_h` was), ClickHouse Code 47 UNKNOWN_IDENTIFIER.
    /// Root: a VLP relationship skips ordinary JOIN inference (handled by
    /// CTE generation instead), so it never registered its own
    /// `PatternSchemaContext` — `PlanCtx::get_node_strategy` fell back to
    /// the alias's OTHER (fixed-hop) registration. Fixed by registering the
    /// VLP's own pattern context even on the JOIN-skip path and having
    /// `get_node_strategy` prefer a registered VLP endpoint's own role.
    /// Confirmed live against ClickHouse: pre-fix SQL fails Code 47 (or
    /// Code 62 before #558 was fixed too — #559 was found "behind" #558);
    /// fixed SQL executes and returns the single correct 2-hop chain
    /// (`192.168.1.10, 93.184.216.34, 10.0.0.99`) over seeded conn_log rows.
    #[tokio::test]
    async fn vlp_start_endpoint_multi_table_label_resolves_own_role_559() {
        let schema = load_schema("schemas/dev/zeek_merged_test.yaml");
        let sql = render(
            &schema,
            "MATCH (x:IP)-[:ACCESSED]->(a:IP)-[:ACCESSED*1..2]->(b:IP) \
             RETURN x.ip, a.ip, b.ip",
            SqlDialect::ClickHouse,
        )
        .await;
        // The invalid pre-fix shape: outer SELECT references a CTE column
        // under the WRONG (TO-)role name, which the CTE never exports.
        assert!(
            !sql.contains(r#"t."start_id.resp_h""#),
            "#559: `a`'s property must not be resolved via the fixed hop's \
             TO-role mapping (a column the CTE never exports): {sql}"
        );
        // The fixed shape: the CTE exports a's own (FROM-role) property
        // column, and the outer SELECT references exactly that column.
        assert!(
            sql.contains(r#"as "start_id.orig_h""#),
            "#559: the VLP CTE must export the start endpoint's OWN \
             (FROM-role) property column: {sql}"
        );
        assert!(
            sql.contains(r#"t."start_id.orig_h" AS "a.ip""#),
            "#559: the outer SELECT must project `a.ip` from the CTE's \
             correctly-resolved FROM-role column: {sql}"
        );
        // The leading fixed hop's JOIN must stay correlated (#524's fix
        // holds) — never regress to an unconditional `ON 1 = 1` join.
        assert!(
            !sql.contains("ON 1 = 1"),
            "#559 (adjacent #524 regression check): the leading fixed hop's \
             JOIN must stay correlated to the VLP CTE's start endpoint, not \
             degrade to an unconditional join: {sql}"
        );
    }

    /// #557: a multi-type VLP with an UNLABELED end node
    /// (`(a:User)-[:FOLLOWS|AUTHORED|LIKED*1..2]->(b)`) builds an outer
    /// UNION with one branch per candidate end label (`b`: User or Post —
    /// `FOLLOWS` ends in User, `AUTHORED`/`LIKED` end in Post) — but each
    /// candidate-label branch independently computes the SAME formulaic
    /// multi-type VLP CTE name (`vlp_multi_type_{start}_{end}` — the
    /// formula only depends on the Cypher aliases, unchanged across
    /// branches), generating DIFFERENT CTE bodies (each scoped to its own
    /// end label) under a colliding name. The render pipeline's two
    /// independent CTE-name computations (the Union-branch FROM builder,
    /// which correctly renames the second collision to `_2`; and the raw
    /// `extract_ctes_with_context` walker, which used to just concatenate
    /// same-named CTEs and let a downstream name-keyed dedup silently drop
    /// one) disagreed — the outer UNION referenced a `vlp_multi_type_a_b_2`
    /// CTE that was never actually defined in the `WITH` clause,
    /// ClickHouse `Code: 60. Unknown table expression identifier`. Fixed by
    /// routing both computations through the same collision-rename rule
    /// (`merge_cte_deduping_by_name_content` in `cte_extraction.rs`).
    #[tokio::test]
    async fn unlabeled_end_node_multi_type_vlp_defines_every_referenced_cte_557() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (a:User)-[:FOLLOWS|AUTHORED|LIKED*1..2]->(b) RETURN count(*)",
            SqlDialect::ClickHouse,
        )
        .await;

        // Every `FROM <name> AS t` / `FROM <name> AS t\n) AS __union` branch
        // reference in the outer query must have a matching `<name> AS (`
        // CTE definition in the WITH clause — no dangling reference.
        let from_re_ctes: Vec<&str> = sql
            .match_indices("FROM vlp_multi_type_")
            .map(|(idx, _)| {
                let rest = &sql[idx + "FROM ".len()..];
                let end = rest
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(rest.len());
                &rest[..end]
            })
            .collect();
        assert!(
            !from_re_ctes.is_empty(),
            "#557: expected at least one vlp_multi_type_* FROM reference: {sql}"
        );
        for cte_name in &from_re_ctes {
            assert!(
                sql.contains(&format!("{} AS (", cte_name)),
                "#557: outer query references CTE '{}' that is never \
                 defined in the WITH clause: {sql}",
                cte_name
            );
        }

        // Specifically: this query's `b` has two candidate end labels
        // (User via FOLLOWS, Post via AUTHORED/LIKED) — expect exactly the
        // base CTE plus one renamed sibling, both defined.
        assert!(
            sql.contains("vlp_multi_type_a_b AS (") && sql.contains("vlp_multi_type_a_b_2 AS ("),
            "#557: expected both the base and the renamed second CTE for \
             the two candidate end labels: {sql}"
        );

        // The User-ending branches (FOLLOWS alone, FOLLOWS+FOLLOWS) must be
        // present SOMEWHERE across the two CTEs — not silently dropped.
        assert!(
            sql.contains("'User' AS end_type"),
            "#557: the User-ending path combinations (FOLLOWS alone, \
             FOLLOWS+FOLLOWS) must not be dropped: {sql}"
        );
        assert!(
            sql.contains("'Post' AS end_type"),
            "#557: the Post-ending path combinations (AUTHORED, LIKED, \
             FOLLOWS+AUTHORED, FOLLOWS+LIKED) must not be dropped: {sql}"
        );
    }

    /// #557 (guard must NOT over-fire): a single-type VLP with BOTH
    /// endpoints explicitly labeled has no candidate-end-label branching at
    /// all — must render its ordinary single `vlp_a_b` CTE, never a
    /// `vlp_multi_type_*` name nor a spurious `_2` sibling.
    #[tokio::test]
    async fn single_type_vlp_unaffected_by_557_fix() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let sql = render(
            &schema,
            "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN count(*)",
            SqlDialect::ClickHouse,
        )
        .await;
        assert!(
            sql.contains("vlp_a_b AS ("),
            "single-type VLP must still render its ordinary CTE: {sql}"
        );
        assert!(
            !sql.contains("vlp_multi_type_"),
            "single-type, both-endpoints-labeled VLP must not route through \
             the multi-type CTE machinery at all: {sql}"
        );
    }

    /// #569: regression coverage for #559's shape on a SINGLE-TABLE
    /// denormalized schema — this was NOT a bug (#559 already fixes it
    /// correctly there too), just untested. `#559` itself repros on a node
    /// label mapped to MULTIPLE physical tables (zeek's `IP`); this locks the
    /// OTHER axis — `Airport` on `schemas/dev/flights_denormalized.yaml` is a
    /// SINGLE physical table (`flights_denorm`) with separate from-side
    /// (`origin_*`) / to-side (`dest_*`) column mappings, and `a` is both the
    /// fixed hop's end endpoint (to-role) AND the VLP's own start endpoint
    /// (from-role) — the same "VLP start endpoint must resolve its OWN role"
    /// shape #559 fixed, on a denorm-role axis instead of a multi-table-label
    /// axis. Confirmed correct live against `db_denormalized.flights_denorm`
    /// (8 seeded flights): 19 rows, matching hand-enumeration of every
    /// (x, a, b) combination over the 8 edges (also locked byte-for-byte as
    /// the `fixed_hop_then_vlp_hop_559` golden in `DENORM_CORPUS`).
    #[tokio::test]
    async fn fixed_hop_then_vlp_hop_denormalized_single_table_559_regression_569() {
        let schema = load_schema(SchemaId::Denormalized.yaml_path());
        let sql = render(
            &schema,
            "MATCH (x:Airport)-[:FLIGHT]->(a:Airport)-[:FLIGHT*1..2]->(b:Airport) \
             RETURN x.code, a.code, b.code",
            SqlDialect::ClickHouse,
        )
        .await;

        // The VLP CTE must export `a`'s OWN (from-role) property column —
        // never resolve it via the fixed hop's (to-role) mapping, a column
        // the CTE never actually exports (#559's exact failure mode).
        assert!(
            sql.contains(r#""origin_code" as "start_origin_code""#),
            "#569: the VLP CTE must export `a`'s own FROM-role property \
             column (origin_code), not fall back to the fixed hop's TO-role \
             mapping (dest_code): {sql}"
        );
        assert!(
            sql.contains(r#"t.start_origin_code AS "a.code""#),
            "#569: the outer SELECT must project `a.code` from the CTE's \
             correctly-resolved FROM-role column: {sql}"
        );
        // The leading fixed hop's JOIN must stay correlated to the VLP CTE's
        // start endpoint (#524 regression guard, same as #559's own test).
        assert!(
            !sql.contains("ON 1 = 1"),
            "#569 (adjacent #524 regression check): the leading fixed hop's \
             JOIN must stay correlated to the VLP CTE's start endpoint, not \
             degrade to an unconditional join: {sql}"
        );
    }
}

/// #577: STANDARD-schema self-edge/undirected relationship-aggregate
/// NULL-padding — a smaller, more isolated cousin of #529 bug 2. #529 bug 2
/// fixed the COUPLED/denormalized case (node and relationship share one
/// physical table) by folding `RelationshipSchema::all_valid_physical_columns`
/// into `table_valid_columns`'s per-branch column-validity set. But that fix
/// only ever inspected the UNION branch's `from` table. On a STANDARD
/// (non-coupled) schema the relationship's own table (`user_follows_bench`)
/// is NEVER a branch's `from` — the branch's `from` is always a NODE table
/// (`users_bench AS a` / `AS b`); the relationship table is only ever
/// reached via a JOIN. So `table_valid_columns` never saw it, deemed `r`'s
/// own edge_id column (`follower_id`) invalid on EVERY branch, and
/// NULL-padded `count(r)` down to 0 for every group — silently, regardless
/// of the true edge count (ground rule 1). Fixed by having
/// `table_valid_columns` also inspect the branch's `joins` (`JoinItems`),
/// not just its `from`, when building the column-validity set.
///
/// Confirmed live against ClickHouse (`social` benchmark DB, `cg query`):
/// pre-fix returned `count = 0` for every one of the 7 grouped users;
/// fixed SQL returns the true per-user FOLLOWS-degree counts (Alice
/// Johnson: 7, Bob Smith: 4, Charlie Brown: 3, Diana Prince: 2, Eve Adams: 2,
/// Frank Miller: 1, Grace Hopper: 1 — summing to 20, i.e. 2x the 10 seeded
/// FOLLOWS edges, matching independent verification since an undirected
/// match counts each edge from both endpoints).
mod self_edge_standard_schema_count_family_577 {
    use super::*;

    #[tokio::test]
    async fn undirected_self_edge_count_r_not_null_padded_on_standard_schema_577() {
        let schema = load_schema(SchemaId::Standard.yaml_path());
        let repro = "MATCH (a:User)-[r:FOLLOWS]-(b:User) WITH a, count(r) AS c \
                     RETURN a.name, c";
        let sql = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);

        // The pre-#577 bug: r's own edge-id column NULL-padded on EVERY
        // UNION branch (both directions), so count(r) always returns 0.
        assert!(
            !sql.contains(r#"NULL AS "r.follower_id""#),
            "#577 regression — r's own edge_id column (follower_id) is being \
             NULL-padded again on the self-edge UNION branches (count(r) \
             would always return 0 regardless of the true edge count):\n{sql}"
        );
        // Fixed shape: BOTH UNION branches (a-follows-b and b-follows-a)
        // project the real joined column, not a NULL placeholder.
        let real_projections = sql.matches(r#"r.follower_id AS "r.follower_id""#).count();
        assert_eq!(
            real_projections, 2,
            "#577: both self-edge UNION branches must project r's real \
             follower_id column (joined via the relationship table), not \
             NULL-pad it:\n{sql}"
        );

        // Determinism.
        for _ in 0..5 {
            let again = normalize(&render(&schema, repro, SqlDialect::ClickHouse).await);
            assert_eq!(sql, again, "#577: nondeterministic render");
        }
    }
}

/// Regression for #594 (silent wrong results): a Cypher `UNION` whose arms each
/// carry a `WITH` and reuse the SAME aggregate alias must render each arm as an
/// independent WITH-CTE, combined by UNION only.
///
/// Pre-fix, `has_with_clause_in_graph_rel` routed the whole union through
/// `build_chained_with_match_cte_plan`, whose `find_all_with_clauses_grouped`
/// grouped the two same-key (`a`,`c`) arm WITHs and "collected from the first
/// arm only" — emitting ONE CTE (`with_a_c_cte_0`, `HAVING c > 2`) that BOTH
/// arms scanned. arm2's `HAVING c = 1` was silently dropped (1 row live instead
/// of the disjoint 1 + 3 = 4).
#[tokio::test]
async fn union_arms_same_agg_alias_each_get_own_with_cte_594() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (a:User)-[:FOLLOWS]->(x) WITH a, count(*) AS c WHERE c > 2 RETURN a.name AS name \
                  UNION \
                  MATCH (a:User)-[:FOLLOWS]->(x) WITH a, count(*) AS c WHERE c = 1 RETURN a.name AS name";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    // BOTH arms' HAVINGs must survive — the core silent-drop bug.
    assert!(sql.contains("HAVING c > 2"), "arm1 HAVING dropped:\n{sql}");
    assert!(sql.contains("HAVING c = 1"), "arm2 HAVING dropped:\n{sql}");
    // Two INDEPENDENT WITH-CTEs with DISTINCT names — arm2 must get its own.
    assert!(sql.contains("with_a_c_cte_0"), "arm1 CTE missing:\n{sql}");
    assert!(
        sql.contains("with_a_c_cte_1"),
        "arm2 got no independent CTE (arms collapsed onto one):\n{sql}"
    );
    // Each arm aggregates over the BASE table independently — two per-arm
    // GROUP BYs, not one arm chained onto the other's CTE.
    assert_eq!(
        sql.matches("GROUP BY a.user_id").count(),
        2,
        "expected two independent per-arm aggregations:\n{sql}"
    );
    // Arms combine by UNION only — never chained, never cross-joined.
    assert!(
        !sql.contains("CROSS JOIN"),
        "arms must not be cross-joined:\n{sql}"
    );
}

/// Regression for #594 (loud variant, same root cause): a Cypher `UNION` whose
/// arms reuse the alias `a` but bind DIFFERENT aggregate aliases (`c` vs `d`).
///
/// Pre-fix, `find_all_with_clauses_grouped` saw different WITH keys, recursed
/// into both arms, and `build_chained_with_match_cte_plan` CHAINED arm2's CTE
/// onto arm1's (`with_a_d_cte_1 AS (SELECT ... FROM with_a_c_cte_0 ...)`) and
/// CROSS JOINed the two arms — ClickHouse Code 47. Each arm must instead scan
/// the base table under its own CTE.
#[tokio::test]
async fn union_arms_differing_agg_alias_not_chained_or_crossjoined_594() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH (a:User)-[:FOLLOWS]->(x) WITH a, count(*) AS c WHERE c > 2 RETURN a.name AS name \
                  UNION \
                  MATCH (a:User)-[:FOLLOWS]->(x) WITH a, count(*) AS d WHERE d = 1 RETURN a.name AS name";
    let sql = render(&schema, cypher, SqlDialect::ClickHouse).await;

    assert!(sql.contains("HAVING c > 2"), "arm1 HAVING missing:\n{sql}");
    assert!(sql.contains("HAVING d = 1"), "arm2 HAVING missing:\n{sql}");
    // Arms combine by UNION only.
    assert!(
        !sql.contains("CROSS JOIN"),
        "arms must not be cross-joined:\n{sql}"
    );
    // arm1's CTE is read exactly once (by arm1's final SELECT). Pre-fix arm2's
    // CTE body ALSO read `FROM with_a_c_cte_0` (the chain), making it appear
    // twice.
    assert_eq!(
        sql.matches("FROM with_a_c_cte_0").count(),
        1,
        "arm1's CTE must be read once, not chained into arm2's CTE body:\n{sql}"
    );
    // Both arm CTE bodies aggregate the base table independently.
    assert_eq!(
        sql.matches("GROUP BY a.user_id").count(),
        2,
        "expected two independent per-arm aggregations:\n{sql}"
    );
}
