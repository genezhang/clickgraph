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
    render_plan::{logical_plan_to_render_plan, ToSql},
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
        // NB: the engine currently renders this via the multi-label entity-union
        // path (a `__multi_label_union` CTE), not a projection-list UNION — the
        // golden locks that current behavior.
        "union",
        "MATCH (u:User) RETURN u.name AS x UNION MATCH (p:Post) RETURN p.title AS x",
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

/// FK-edge variation (`schemas/test/fk_edge.yaml`): Order/Customer where the
/// orders_fk table IS the PLACED_BY edge table (customer_id FK column is the
/// relationship — no separate edge table, not denormalized). Mirrors the
/// standard corpus's feature axes for the FK-edge schema pattern.
///
/// Not expressible in this schema (single edge type, from_node Order != to_node
/// Customer, so an edge cannot chain into itself), intentionally omitted:
///   - VLP `*1..N` / multi-hop — no second hop exists out of Customer.
///   - multi-type `[:A|B]` — only one edge type (PLACED_BY).
///   - UNWIND/arrayJoin shapes — same Spark structural gap the standard corpus
///     skips.
///
/// KNOWN-SUSPICIOUS: none currently. `with_match_chain` was confirmed wrong
/// (cartesian) and is FIXED (#451); `optional_match`'s redundant phantom
/// self-join is FIXED (#452).
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
    // path MATCH p=()-[]->() -> fixed_path / pattern_union renderers
    // (#419/#420/#421/#425 family): virtual ids resolved to physical columns,
    // no spurious self-join of the single edge table.
    (
        "path_return",
        "MATCH p = (a:Airport)-[:FLIGHT]->(b:Airport) RETURN p",
    ),
    // --- OPTIONAL MATCH (anchored on Airport, optional outgoing flight) ---
    (
        "optional_match",
        "MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code",
    ),
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
/// KNOWN-SUSPICIOUS: see the comment block above `sql_golden_snapshots` — the
/// `group_by_whole_node` case locks a confirmed GROUP BY correctness bug specific
/// to composite ids (grouping collapses to the FIRST id column only).
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
    // KNOWN-SUSPICIOUS (see comment block below): grouping by the bare node
    // variable `a` (not its explicit properties) collapses GROUP BY to the
    // FIRST id column only (`a.bank_id`), NOT the full composite key.
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

fn load_schema(yaml_path: &str) -> GraphSchema {
    GraphSchemaConfig::from_yaml_file(yaml_path)
        .unwrap_or_else(|e| panic!("load schema {yaml_path}: {e:?}"))
        .to_graph_schema()
        .unwrap_or_else(|e| panic!("convert {yaml_path} to GraphSchema: {e:?}"))
}

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
        let (logical_plan, _plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .unwrap_or_else(|e| panic!("plan: {e:?}"));
        let render_plan = logical_plan_to_render_plan(logical_plan, &schema)
            .unwrap_or_else(|e| panic!("render: {e:?}"));
        render_plan.to_sql()
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
fn normalize(sql: &str) -> String {
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
// GROUP B — ORDER BY over a denorm hop mis-qualifies the node-id column
// (order_skip_limit, skip_only). The SELECT is correct
// (`t0.origin_code AS "a.code"` from `flights_denorm AS t0`) but the ORDER BY
// emits the CYPHER alias, not the table alias: `ORDER BY a.origin_code`. Live CH:
// `DB::Exception: Unknown expression identifier 'a.origin_code'`. The paging
// mechanics (`LIMIT off, n`, and the CH huge-upper-bound for bare SKIP) are
// correct; only the ORDER BY term qualification is wrong. (The WITH-form paging
// `with_skip_limit` is unaffected and executes — 3 rows — so a valid denorm
// paging lock exists.)
//
// GROUP C — semantically wrong result, but VALID SQL (executes):
//   - with_match_chain: `MATCH (a:Airport) WITH a WHERE a.state='CA'
//     MATCH (a)-[:FLIGHT]->(b)` should yield flights FROM CA airports (4 rows).
//     It returns 7. The WITH-CTE materializes `a` as a UNION of an origin branch
//     AND a dest branch, but the dest branch ALSO filters `WHERE a.origin_state
//     = 'CA'` (should be dest_state / or be the same node set), so the airport
//     set is polluted with destinations of CA-origin flights (LAX,SFO,JFK,ORD,ATL
//     instead of {CA airports}). Kept as a lock; a fix should collapse/​correct
//     the dest branch and drop to 4 rows.
//   - optional_match: `MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b)`
//     renders IDENTICALLY to the inner directed hop — a plain
//     `FROM flights_denorm AS t0` with NO LEFT JOIN and no node materialization
//     (8 rows). For the coupled-same-row shape (the node IS the edge row) every
//     origin trivially has its flight, so no NULL-extension rows are produced;
//     this is degenerate-but-current. A real node-materialization fix (Group A)
//     would change this too.
//
// Verified CORRECT (normal locks, not suspicious) — executed on live CH:
// directed_hop_ids (8), directed_hop_props, reverse_hop (8), undirected_hop (16,
// each edge both directions), hop_edge_props (renamed flight_num -> physical
// flight_number), where_edge_prop (distance>1000 -> 6), hop_filter_both
// (CA->NY -> 1), whole_edge_r (8, composite edge_id + all edge props),
// path_return (8), vlp_recursive *1..2 (DenormalizedCteStrategy, 15),
// with_agg_count (out-degree per origin, 6), with_having (>1 -> 1),
// with_skip_limit (3), group_two_keys (distinct state pairs, 7), distinct_hop
// (5 origin states). All hop cases render from the SINGLE `flights_denorm` table
// with NO spurious edge self-join (the #419 class is clean) and correct
// from-side (origin_*) vs to-side (dest_*) column sourcing.
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
//   - composite_id/group_by_whole_node: CONFIRMED BUG, locked as current
//     behavior (not fixed in this slice — Phase-0 golden-locking only, no
//     drive-by fixes). `MATCH (a:Account)-[:TRANSFERRED]->(a2:Account) RETURN a,
//     count(a2) AS n` renders `GROUP BY a.bank_id` — ONLY the FIRST composite-id
//     column, silently dropping `account_number`. Any bank with >1 account (e.g.
//     CHASE, which owns 4 accounts in the fixture data) collapses ALL of that
//     bank's accounts into a single GROUP BY bucket: `count(a2)` sums transfers
//     across every CHASE account as if they were one entity, and the `anyLast()`-
//     wrapped non-aggregated columns (including `a.account_number`, the SECOND
//     half of the node's own identity) return an ARBITRARY one of the collapsed
//     accounts' values. Contrast with `group_by_composite_columns`, which asks
//     for the same shape via explicit properties (`RETURN a.bank_id,
//     a.account_number, count(a2)`) and correctly emits `GROUP BY a.bank_id,
//     a.account_number` — the bug is specific to the bare-node-variable path.
//     Root cause: `ViewScan.id_column` (`src/query_planner/logical_plan/view_scan.rs`)
//     is a single `String`, populated at ViewScan-construction time from only
//     `node_schema.node_id.columns().first()` for ANY node, composite or not
//     (`src/query_planner/logical_plan/match_clause/view_scan.rs`, the
//     non-denormalized branch around the "For non-denormalized nodes, node_id IS
//     the actual column name" comment). `find_id_column_for_alias`
//     (`src/render_plan/plan_builder.rs`) forwards that single column straight
//     through to the GROUP BY node-alias optimization in
//     `handle_table_alias_group_by`/`handle_wildcard_group_by`
//     (`src/render_plan/group_by_builder.rs`), which pushes exactly one
//     `PropertyAccessExp` instead of one per `node_id.columns()`. The same
//     first-column-only value is ALSO used for `count(a)`-style aggregates
//     (`aggregate_count`, `group_by_composite_columns`'s own `count(a2)`), where
//     it is harmless (COUNT only needs one non-null column to detect row
//     existence, not the full identity) — but the GROUP BY consumer needs the
//     FULL key, and gets only the first column. Fixing this needs either a
//     `Vec<String>`-capable id representation on `ViewScan`/`find_id_column_for_alias`,
//     or a composite-aware GROUP BY expansion in `group_by_builder.rs` that calls
//     `node_schema.node_id.columns()` instead. Filed as a follow-up (not fixed
//     here per the Phase-0 protocol — no drive-by fixes).

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
//   - polymorphic/whole_edge_r  [SUSPICIOUS — missing type discriminator]:
//     `MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN r` renders
//     `FROM brahmand.interactions AS r` with NO WHERE clause — no
//     `interaction_type = 'FOLLOWS'`, no from_type/to_type label filter. Because
//     only `r` is projected, the labeled User endpoints are pruned and the type
//     discriminator is lost with them, so it returns ALL 29 interaction rows
//     instead of the 10 FOLLOWS edges. The edge columns (from_id/to_id/timestamp/
//     interaction_weight) themselves project correctly. This is the whole-edge-
//     projection sibling of the #433 element-id/edge-linkage class: the
//     discriminator must survive endpoint pruning on a polymorphic edge. Locked
//     as-is; a fix should filter the edge scan by the pattern's type + label
//     columns even when the endpoints are not otherwise referenced.
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
//       * `polymorphic_unlabeled_endpoints_current_row_multiplication`
//         [SUSPICIOUS — pattern_union row multiplication]: the OUTER query then
//         selects from that already-complete CTE FOUR times, UNION-ALL'd, with
//         byte-identical projections — so every path row is emitted 4×. Live:
//         `(a)-[:FOLLOWS]->(b)` returns 40 rows (should be 10 FOLLOWS),
//         `(a)-[:SHARED]->(b)` / `p=()-[:SHARED]->()` return 12 (should be 3
//         SHARED) — exactly 4× the correct count. The 4 outer copies appear to
//         be a mis-lowering of the endpoint-label cross-product (2 labels × 2
//         labels) onto the outer projection, which the CTE has already accounted
//         for. Asserted as current behavior; a fix should collapse the outer
//         union to a single `SELECT ... FROM pattern_union_*`.
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

/// Companion characterization of the CURRENT (suspicious) behavior of the
/// fully-unlabeled polymorphic path: the `pattern_union_*` CTE already
/// enumerates all four (from_label, to_label) branches, yet the OUTER query
/// selects from it FOUR times UNION-ALL'd with identical projections, so every
/// path row is emitted 4×. Live CH (brahmand fixture): `(a)-[:FOLLOWS]->(b)`
/// returns 40 rows (should be 10), `(a)-[:SHARED]->(b)` returns 12 (should be 3)
/// — exactly 4×. Locked as current behavior so a fix (collapse the outer union
/// to a single `SELECT ... FROM pattern_union_*`) shows as a diff here. If this
/// starts failing because the count dropped to 1, that is the FIX — update the
/// expected count and the KNOWN-SUSPICIOUS note.
#[tokio::test]
async fn polymorphic_unlabeled_endpoints_current_row_multiplication() {
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
        // CURRENT behavior: 4 outer `FROM pattern_union_*` selects (the 4× bug).
        let outer_reads = sql.matches("FROM pattern_union_t").count();
        assert_eq!(
            outer_reads, 4,
            "polymorphic unlabeled pattern ({dname}): expected the current \
             (suspicious) 4× outer union over pattern_union_* (row multiplication, \
             see KNOWN-SUSPICIOUS block); got {outer_reads} outer reads. A drop to \
             1 is the FIX — update this expectation:\n{sql}"
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
