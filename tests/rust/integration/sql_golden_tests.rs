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
    // Same shape, WHERE on the relationship alias r (order_date). Already correct
    // before #474 (rel-alias pre_filter recovery): r and o share orders_fk, so the
    // predicate sits in the LEFT JOIN pre_filter. Locked to prove #474 did not
    // disturb it.
    (
        "optional_where_no_with_rel",
        "MATCH (c:Customer) OPTIONAL MATCH (o:Order)-[r:PLACED_BY]->(c) WHERE r.order_date > '2024-01-01' RETURN c.customer_id, o.order_id",
    ),
    // Mixed conjunction: optional-node predicate AND pure-anchor predicate. The
    // optional-node conjunct (o.total_amount) is now recovered into the LEFT JOIN
    // pre_filter (#474); the pure-anchor conjunct (c.customer_id) stays in the
    // outer WHERE and still drops NULL-extended anchor rows — the SAME pre-existing
    // #472 disease (a pure-anchor OPTIONAL-WHERE conjunct belongs in the LEFT JOIN
    // ON, always safe for a LEFT JOIN). Left as-is here; tracked by #472.
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
    // #487: Cypher UNION whose first arm aggregates over the denormalized
    // virtual-node from/to union. count(a) must hoist over the arm's OWN
    // internal union (origin_code/dest_code, correctly mapped — NOT unmapped
    // `a.code`) while staying WITHIN the arm (live-verified: {7 airports,
    // 8 flights} against db_denormalized).
    (
        "union_agg_per_arm",
        "MATCH (a:Airport) RETURN count(a) AS c UNION ALL MATCH ()-[f:FLIGHT]->() RETURN count(f) AS c",
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
/// t.hop_count)`. The CTE deliberately does NOT project `path_edges`
/// (node-uniqueness cycle detection via `path_nodes`; per-edge arrays were
/// dropped as a memory optimization), so any `path_edges` reference is
/// unbound (ClickHouse `Code: 47 ... Identifier 't.path_edges' cannot be
/// resolved ... Maybe you meant: ['t.path_nodes']` — the pre-fix behavior).
/// Live-verified fixed on the `social` fixture, including `*0..` (zero-hop
/// rows render `[[id], [], 0]`) and undirected `*1..2`. Byte-locked by the
/// `path_vlp` golden.
#[tokio::test]
async fn browser_vlp_path_return_uses_only_cte_defined_columns() {
    let schema = load_schema(SchemaId::Standard.yaml_path());
    let cypher = "MATCH p=(a:User)-[:FOLLOWS*1..2]->(b) RETURN p";

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = render(&schema, cypher, dialect).await;

        // The path tuple must be assembled from CTE-defined columns only.
        assert!(
            !sql.contains("path_edges"),
            "RETURN p must not reference `path_edges` — the recursive VLP CTE \
             never defines it (#469 regression) for {dialect:?}:\n{sql}"
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

/// CHARACTERIZATION (pending the transitivity-clamp fix): the #488 path-var
/// re-registration is deliberately GUARDED to directed patterns with
/// min_hops >= 1, because `VlpTransitivityCheck`'s clamp-to-single-hop is
/// semantically wrong for two shapes (pre-existing on main, filed as its own
/// clamp follow-up issue):
///   - `*0..N` — zero-hop paths are real (the start node itself), so a
///     single-hop clamp drops rows;
///   - undirected — reverse-direction chaining can make >1-hop paths
///     possible; the clamp never consults direction.
/// For those shapes the guard keeps main's LOUD failure — `RETURN p` still
/// renders `tuple(t.path_nodes, ...)` with NO recursive CTE (alias `t`
/// unbound, ClickHouse Code 47 at exec) — rather than silently returning the
/// clamped wrong rows. When the clamp itself is fixed, these shapes should
/// produce real paths and this lock must be replaced with byte-goldens.
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
            let sql = render(&schema, cypher, dialect).await;
            assert!(
                sql.contains("t.path_nodes"),
                "guarded shape must keep the loud unbound-`t` rendering (not \
                 silently clamp) for {dialect:?}: {cypher}\n{sql}"
            );
            assert!(
                !sql.contains("fixed_path"),
                "guarded shape must NOT take the fixed-path route (that would \
                 silently return clamped rows) for {dialect:?}: {cypher}\n{sql}"
            );
            assert!(
                !sql.to_uppercase().contains("WITH RECURSIVE"),
                "no recursive CTE is expected for these shapes until the clamp \
                 fix lands for {dialect:?}: {cypher}\n{sql}"
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
