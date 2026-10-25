//! The single read-query translation seam: Cypher statement → SQL.
//!
//! Every read entry point (HTTP `/query` and `/query/sql`, Bolt, `COPY TO` /
//! `apoc.export` inner queries, embedded `cypher_to_sql*`, and through it
//! FFI / Go / Python / `cg`) translates through [`translate_read`]. Before this
//! seam each caller ran its own copy of parse-result → plan → render → SQL, and
//! the copies drifted (`/query/sql` skipped the analyzer passes; counters were
//! reset on some paths and not others). P-4c (`docs/design/EXPLICIT_SCOPE.md`)
//! routes queries to the bound-plan path here, so this is the only place that
//! needs to know which pipeline produced the SQL.
//!
//! The SQL returned here is meant to run on a session that applies
//! [`crate::sql_generator::SEMANTIC_SESSION_SETTINGS`] (every executor does).
//! SQL handed to a user for running elsewhere goes through
//! [`crate::sql_generator::portable_sql`], which carries those settings in the
//! statement itself (#1314).

use std::collections::HashMap;
use std::time::{Duration, Instant};

pub mod prepared;
pub use prepared::{Prelude, PrepareError, PreparedStatement, StatementTargets, DEFAULT_GRAPH};

use crate::bound_plan::lower::{ResultColumn, ResultKind};
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::open_cypher_parser::ast::CypherStatement;
use crate::server::bolt_protocol::result_transformer::{ReturnItemMetadata, ReturnItemType};

/// Caller-supplied inputs that change how a read query is planned or rendered.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    pub tenant_id: Option<String>,
    pub view_parameter_values: Option<HashMap<String, String>>,
    /// The query text `statement` was parsed from (comments stripped): the
    /// bound plan (P-4c) parses it with the clause-list parser.
    pub cypher: String,
    /// The server's id mapper (HTTP, Bolt): the bound plan reads an integer
    /// compared with `id(n)` as the `id()` rewrite does (P-4c S11e). `None`
    /// (library, `cg`): `id(n)` is the node's key.
    pub id_mapper: Option<crate::server::bolt_protocol::id_mapper::IdMapper>,
    /// Neo4j-compat mode for this query (an undeclared property is NULL);
    /// `None`: the server's setting (`server_neo4j_compat`). The corpus
    /// goldens lock both modes in one process (P-4c S14c0).
    pub neo4j_compat: Option<bool>,
    /// Check that the rows of each id of a node embedded in edge tables
    /// hold one value of each property read (#531); `None`: the process
    /// setting (`query_context::check_node_properties`).
    pub check_node_properties: Option<bool>,
}

/// Wall-clock time spent in each translation stage (HTTP metrics).
#[derive(Debug, Clone, Copy, Default)]
pub struct TranslateTimings {
    pub planning: Duration,
    pub render: Duration,
    pub sql_generation: Duration,
}

/// A translated read query.
#[derive(Debug)]
pub struct ReadTranslation {
    /// SQL for an executor that applies the semantic session settings.
    pub sql: String,
    /// What each RETURN item is.
    pub shape: Vec<ResultColumn>,
    pub timings: TranslateTimings,
    /// The SQL holds the server's reading of `id()` numbers
    /// (`ReadOptions::id_mapper` was consulted), which holds only while the
    /// ids are known: it is not cached.
    pub session_ids: bool,
}

impl ReadTranslation {
    /// What each RETURN item is and which columns hold it (§4.13): Bolt, the
    /// HTTP graph output and embedded `query_graph` build nodes and
    /// relationships from it.
    pub fn return_metadata(&self) -> Vec<ReturnItemMetadata> {
        self.shape.iter().map(return_item_metadata).collect()
    }
}

/// The result transformer's view of a lowered RETURN item.
fn return_item_metadata(c: &ResultColumn) -> ReturnItemMetadata {
    let item_type = match &c.kind {
        ResultKind::Value => ReturnItemType::Scalar,
        ResultKind::Node { label } => ReturnItemType::Node {
            labels: vec![label.clone()],
        },
        // The columns are in the stored orientation (`from_id` is the
        // relationship's start node), whatever the pattern's direction.
        ResultKind::Rel {
            rel_type,
            from_label,
            to_label,
        } => ReturnItemType::Relationship {
            rel_types: vec![rel_type.clone()],
            from_label: Some(from_label.clone()),
            to_label: Some(to_label.clone()),
            direction: Some("Outgoing".to_string()),
        },
        // `alias` names the column of the transformer's fallbacks, which an
        // item with explicit columns never reaches.
        ResultKind::NodeId { label } => ReturnItemType::IdFunction {
            alias: c.name.clone(),
            labels: vec![label.clone()],
        },
        ResultKind::Graph(ty) => ReturnItemType::Graph(ty.clone()),
        ResultKind::ElementIdNumber => ReturnItemType::ElementIdNumber,
    };
    ReturnItemMetadata {
        field_name: c.name.clone(),
        item_type,
        columns: Some(c.columns.clone()),
    }
}

/// A query translated by the bound-plan path.
#[derive(Debug)]
pub struct BoundTranslation {
    pub sql: String,
    pub shape: Vec<ResultColumn>,
    /// As [`ReadTranslation::session_ids`].
    pub session_ids: bool,
}

impl BoundTranslation {
    fn into_read_translation(self, start: Instant) -> ReadTranslation {
        ReadTranslation {
            sql: self.sql,
            shape: self.shape,
            timings: TranslateTimings {
                planning: start.elapsed(),
                ..Default::default()
            },
            session_ids: self.session_ids,
        }
    }
}

/// Why a read query was not translated.
#[derive(Debug)]
pub enum TranslateError {
    /// The query is invalid Cypher, as Neo4j rejects it (an undefined or
    /// redeclared variable, a misplaced aggregate, …): the bound plan's
    /// binder (P-4c S10; the legacy pipeline is not tried).
    Invalid(String),
    /// The bound plan does not translate the query (P-4c S11j, S14a): a form
    /// not supported, or one Neo4j also refuses.
    Unsupported(String),
    /// The bound plan broke one of its own invariants: a defect, not the
    /// query's (an internal error, HTTP 500).
    Internal(String),
}

impl std::fmt::Display for TranslateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TranslateError::Invalid(e) => write!(f, "{}", e),
            TranslateError::Unsupported(e) => write!(f, "{}", e),
            TranslateError::Internal(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for TranslateError {}

/// Translate a read query to SQL: the bound plan translates
/// `options.cypher`; `statement` (its legacy parse) only tells a read from
/// a CALL or a write (`is_read` / `not_a_read`). Server entry points call this
/// inside their `with_query_context` scope (schema, dialect, table stats set).
/// A caller without one (library code, unit tests) gets a fresh per-query
/// context holding `schema`, so a translation never runs on process-global
/// state: in particular the generated-alias counters are always this query's
/// own, and resetting them cannot rewind a concurrent query's.
pub fn translate_read(
    statement: CypherStatement<'_>,
    schema: &GraphSchema,
    options: ReadOptions,
) -> Result<ReadTranslation, TranslateError> {
    use crate::server::query_context::{
        has_query_context, set_current_schema, with_query_context_sync, QueryContext,
    };
    if has_query_context() {
        return translate_in_context(statement, schema, options);
    }
    with_query_context_sync(QueryContext::new(None), || {
        set_current_schema(std::sync::Arc::new(schema.clone()));
        translate_in_context(statement, schema, options)
    })
}

fn translate_in_context(
    statement: CypherStatement<'_>,
    schema: &GraphSchema,
    options: ReadOptions,
) -> Result<ReadTranslation, TranslateError> {
    // Deterministic generated aliases: restart THIS query's counters.
    crate::server::query_context::reset_all_counters();
    let start = Instant::now();
    // `id()` comparisons: the bound plan reads them from `cypher` with the
    // server's id mapper (S11e).
    match bound_plan_or_reason(&options.cypher, schema, &options) {
        Err(BoundPlanError::Invalid(e)) => Err(TranslateError::Invalid(e)),
        // Text the clause-list parser rejects is not Cypher (a query without
        // RETURN; user 2026-10-11: as Neo4j, an error). A CALL or updating
        // query (`NotARead`, or text only the legacy parser reads: `CALL p()
        // YIELD x WHERE …`) is not a read: entry points answer standalone
        // procedures and writes before this.
        Err(BoundPlanError::Unparsed(_)) if !is_read(&statement) => Err(not_a_read(&statement)),
        Err(BoundPlanError::Unparsed(reason)) => Err(TranslateError::Invalid(reason)),
        // What the bound plan does not translate is an error (P-4c S11j,
        // S14a).
        Err(BoundPlanError::Fallback(reason)) => Err(not_translated(&reason)),
        Err(BoundPlanError::NotARead(_)) => Err(not_a_read(&statement)),
        Ok(bound) => Ok(bound.into_read_translation(start)),
    }
}

/// The error for a statement that is not a read reaching the read path.
fn not_a_read(statement: &CypherStatement<'_>) -> TranslateError {
    let writes = |q: &crate::open_cypher_parser::ast::OpenCypherQueryAst<'_>| {
        q.create_clause.is_some()
            || q.set_clause.is_some()
            || q.remove_clause.is_some()
            || q.delete_clause.is_some()
    };
    let has_writes = match statement {
        CypherStatement::Query {
            query,
            union_clauses,
        } => writes(query) || union_clauses.iter().any(|u| writes(&u.query)),
        CypherStatement::ProcedureCall(_) | CypherStatement::CopyTo(_) => false,
    };
    TranslateError::Unsupported(if has_writes {
        "write clauses (CREATE / SET / REMOVE / DELETE): ClickGraph is read-only".to_string()
    } else {
        "a procedure CALL here (the server answers a standalone CALL; one inside a query or a \
         UNION is not supported)"
            .to_string()
    })
}

/// Whether the legacy parse of a query is a read: no CALL and no updating
/// clause in any UNION arm.
fn is_read(statement: &CypherStatement<'_>) -> bool {
    let read = |q: &crate::open_cypher_parser::ast::OpenCypherQueryAst<'_>| {
        q.call_clause.is_none()
            && q.create_clause.is_none()
            && q.set_clause.is_none()
            && q.remove_clause.is_none()
            && q.delete_clause.is_none()
    };
    match statement {
        CypherStatement::Query {
            query,
            union_clauses,
        } => read(query) && union_clauses.iter().all(|u| read(&u.query)),
        CypherStatement::ProcedureCall(_) | CypherStatement::CopyTo(_) => false,
    }
}

/// Translate read-query text that only the clause-list parser reads (P-4c
/// S10b): `WITH a MATCH .. MATCH ..`, several UNWINDs after a WITH, a MATCH
/// after an OPTIONAL MATCH after a WITH, free-standing ORDER BY / SKIP /
/// LIMIT. Entry points call it when the legacy parser rejects the text.
///
/// `Ok(None)`: the clause-list parser rejects it too: the caller reports its
/// own parse error. `Err(Invalid)`: a query Neo4j
/// rejects as well. `Err(Unsupported)`: parsed, but not lowered.
pub fn translate_text_read(
    cypher: &str,
    schema: &GraphSchema,
    options: ReadOptions,
) -> Result<Option<ReadTranslation>, TranslateError> {
    use crate::server::query_context::{
        has_query_context, set_current_schema, with_query_context_sync, QueryContext,
    };
    if !has_query_context() {
        return with_query_context_sync(QueryContext::new(None), || {
            set_current_schema(std::sync::Arc::new(schema.clone()));
            translate_text_read(cypher, schema, options)
        });
    }
    crate::server::query_context::reset_all_counters();
    let start = Instant::now();
    match bound_plan_or_reason(cypher, schema, &options) {
        Ok(bound) => Ok(Some(bound.into_read_translation(start))),
        Err(BoundPlanError::Invalid(e)) => Err(TranslateError::Invalid(e)),
        Err(BoundPlanError::Unparsed(reason) | BoundPlanError::NotARead(reason)) => {
            log::debug!("text-only read not translated: {reason}");
            Ok(None)
        }
        // The legacy parser rejects the text too: no pipeline translates it.
        Err(BoundPlanError::Fallback(reason)) => Err(not_translated(&reason)),
    }
}

/// The SQL of an inner read query (`COPY (<query>) TO`, the query argument of
/// `apoc.export.*.query`) of a statement running on graph `graph`,
/// translated as `/query` would (the bound plan; text only the clause-list
/// parser reads included, S10b). It is read through the statement front end
/// (comments, a prelude), and a `USE` in it naming another graph is refused
/// (it was ignored). Error messages are prefixed `Inner Cypher …`; a
/// syntax error is [`TranslateError::Invalid`].
pub fn translate_inner_read(
    cypher: &str,
    schema: &GraphSchema,
    graph: &str,
    id_mapper: crate::server::bolt_protocol::id_mapper::IdMapper,
) -> Result<String, TranslateError> {
    let prepared = PreparedStatement::parse(cypher)
        .and_then(|p| p.targets.within(graph).map(|()| p))
        .map_err(|e| match e {
            PrepareError::TooLarge(e) => TranslateError::Unsupported(format!("Inner Cypher: {e}")),
            e => TranslateError::Invalid(format!("Inner Cypher query error: {e}")),
        })?;
    if prepared.prelude.mode != prepared::ExecutionMode::Run {
        return Err(TranslateError::Unsupported(
            "Inner Cypher not supported: EXPLAIN / PROFILE".to_string(),
        ));
    }
    let cypher = prepared.text();
    // The caller's ids (its session's or request's scope, #1388).
    let options = || ReadOptions {
        cypher: cypher.to_string(),
        id_mapper: Some(id_mapper.clone()),
        ..Default::default()
    };
    let message = |e: TranslateError| match e {
        TranslateError::Invalid(e) => {
            TranslateError::Invalid(format!("Inner Cypher query error: {}", e))
        }
        TranslateError::Unsupported(e) => {
            TranslateError::Unsupported(format!("Inner Cypher not supported: {}", e))
        }
        TranslateError::Internal(e) => {
            TranslateError::Internal(format!("Inner Cypher internal error: {}", e))
        }
    };
    match crate::open_cypher_parser::parse_cypher_statement(cypher) {
        Ok((_, statement)) => translate_read(statement, schema, options())
            .map(|t| t.sql)
            .map_err(message),
        Err(e) => {
            let parse_error = TranslateError::Invalid(format!("Inner Cypher parse error: {}", e));
            match translate_text_read(cypher, schema, options()) {
                Ok(Some(t)) => Ok(t.sql),
                Ok(None) => Err(parse_error),
                Err(e) => Err(message(e)),
            }
        }
    }
}

/// The stack queries translate on, in bytes: `CLICKGRAPH_THREAD_STACK_MB`
/// (default 128; 0 or unparsable is the default), for the servers' worker
/// threads and for a library translation (#1373), so a statement
/// translates in a library as it does on a server whatever thread the
/// caller is on (the main thread's 8 MB, a spawned Rust thread's 2 MB, a
/// cgo or Python thread's). [`prepared::MAX_CLAUSES`] and
/// [`prepared::MAX_NESTING`] are set for the default.
pub fn translation_stack_bytes() -> usize {
    std::env::var("CLICKGRAPH_THREAD_STACK_MB")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|mb| *mb > 0)
        .unwrap_or(128)
        .saturating_mul(1024 * 1024)
}

thread_local! {
    /// This thread has [`translation_stack_bytes`] of stack: a translation
    /// pool worker, or a thread [`on_translation_thread`] started.
    static ON_TRANSLATION_STACK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A statement runs on the caller's thread ([`on_translation_stack`]) when it
/// has at most [`INLINE_TOKENS`] tokens and nests brackets at most
/// [`INLINE_DEPTH`] deep ([`prepared::StatementShape`]): it needs a few
/// hundred KB of stack at most, which a spawned Rust thread (2 MB) has. The
/// parsers and the lowering recurse per clause, per bracket and per
/// operator of a chain (`1 + 1 + …`): anything longer moves.
pub const INLINE_TOKENS: usize = 64;

/// See [`INLINE_TOKENS`].
pub const INLINE_DEPTH: usize = 8;

/// Whether `cypher` needs no more stack than the caller's thread has
/// ([`INLINE_TOKENS`]), or the caller's thread is one with
/// [`translation_stack_bytes`].
fn runs_inline(cypher: &str) -> bool {
    ON_TRANSLATION_STACK.with(|on| on.get()) || {
        let shape =
            prepared::StatementShape::of(&crate::open_cypher_parser::strip_comments(cypher));
        shape.tokens <= INLINE_TOKENS && shape.depth <= INLINE_DEPTH && shape.cases <= INLINE_DEPTH
    }
}

/// The translation pool: one worker per CPU (at most 8), each with
/// [`translation_stack_bytes`] of stack, started on first use. Its workers
/// stay, so a translation sent to one costs a few µs, not a thread start
/// and the faults of a fresh stack (about 100 µs). Only CPU-bound work goes
/// to it ([`on_translation_stack`]): work that waits on a backend gets a
/// thread of its own ([`on_translation_thread`]).
///
/// `None`: the pool could not be started, or this process is a fork of the
/// one that started it (a fork has none of its threads, and a translation
/// sent to it would wait forever: Python's `multiprocessing` forks): the
/// caller uses a thread of its own instead ([`on_translation_thread`]).
fn translation_pool() -> Option<&'static rayon::ThreadPool> {
    static POOL: std::sync::OnceLock<(u32, Option<rayon::ThreadPool>)> = std::sync::OnceLock::new();
    let (pid, pool) = POOL.get_or_init(|| {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(std::thread::available_parallelism().map_or(4, |n| n.get().min(8)))
            .stack_size(translation_stack_bytes())
            .thread_name(|i| format!("clickgraph-translate-{i}"))
            .start_handler(|_| ON_TRANSLATION_STACK.with(|on| on.set(true)))
            .build();
        if let Err(e) = &pool {
            log::warn!("translation pool not started ({e}): a thread per translation");
        }
        (std::process::id(), pool.ok())
    });
    pool.as_ref().filter(|_| *pid == std::process::id())
}

/// Run `f` with the caller's query context (schema, dialect, counters) as
/// its own, and hand back `f`'s result and the context's final state: what
/// moving `f` to another thread needs to be invisible to `f` and the caller.
fn with_carried_context<T, F>(
    context: Option<crate::server::query_context::QueryContext>,
    f: F,
) -> (T, Option<crate::server::query_context::QueryContext>)
where
    F: FnOnce() -> T,
{
    use crate::server::query_context::{snapshot_query_context, with_query_context_sync};
    match context {
        Some(context) => with_query_context_sync(context, || {
            let result = f();
            (result, snapshot_query_context())
        }),
        None => (f(), None),
    }
}

/// Run `f`, which parses or translates the statement `cypher` (CPU-bound:
/// no waiting on a backend), on a translation pool worker with
/// [`translation_stack_bytes`] of stack, and wait for it: the library entry
/// points (the embedded connection, and through it FFI, Go, Python and
/// `cg`) translate on the caller's thread otherwise, whose stack a long
/// statement overflows, aborting the caller's process (#1373). A short
/// statement ([`INLINE_TOKENS`]) runs inline, as do nested calls. The
/// caller's query context is carried over and its final state handed back;
/// a panic is the caller's. `Err`: the pool could not be started.
pub fn on_translation_stack<T, F>(cypher: &str, f: F) -> Result<T, String>
where
    T: Send,
    F: FnOnce() -> T + Send,
{
    if runs_inline(cypher) {
        return Ok(f());
    }
    let Some(pool) = translation_pool() else {
        return on_translation_thread(cypher, f);
    };
    let context = crate::server::query_context::snapshot_query_context();
    let (result, context) = pool.install(move || with_carried_context(context, f));
    if let Some(context) = context {
        crate::server::query_context::restore_query_context(context);
    }
    Ok(result)
}

/// [`on_translation_stack`] for work that also waits on a backend (the
/// embedded connection's `query`, which runs the SQL): on a thread of its
/// own with [`translation_stack_bytes`] of stack, so a long-running query
/// never holds a pool worker. Starting it costs about 100 µs, little next to
/// running a query. `Err`: the thread could not be started.
pub fn on_translation_thread<T, F>(cypher: &str, f: F) -> Result<T, String>
where
    T: Send,
    F: FnOnce() -> T + Send,
{
    if runs_inline(cypher) {
        return Ok(f());
    }
    let context = crate::server::query_context::snapshot_query_context();
    let (result, context) = std::thread::scope(|scope| -> Result<_, String> {
        let handle = std::thread::Builder::new()
            .name("clickgraph-translate".to_string())
            .stack_size(translation_stack_bytes())
            .spawn_scoped(scope, move || {
                ON_TRANSLATION_STACK.with(|on| on.set(true));
                with_carried_context(context, f)
            })
            .map_err(|e| format!("could not start a translation thread: {e}"))?;
        // A panic is the caller's, as if `f` had run on its thread.
        match handle.join() {
            Ok(done) => Ok(done),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    })?;
    if let Some(context) = context {
        crate::server::query_context::restore_query_context(context);
    }
    Ok(result)
}

/// Whether the clause-list parser reads `cypher` (a statement without its
/// prelude): entry points route text the legacy parser rejects to the bound
/// plan when it does (S10b).
pub fn clause_list_parses(cypher: &str) -> bool {
    crate::open_cypher_parser::clause_list::parse_clause_statement(cypher).is_ok()
}

/// The bound-plan path (P-4c): clause list → bind → lower → plain print.
/// `Err` carries why the legacy pipeline translates the query instead, or
/// why the query is invalid ([`BoundPlanError`], as text).
pub fn translate_bound_plan(
    cypher: &str,
    schema: &GraphSchema,
    options: &ReadOptions,
) -> Result<BoundTranslation, String> {
    bound_plan_or_reason(cypher, schema, options).map_err(|e| match e {
        BoundPlanError::Fallback(reason)
        | BoundPlanError::Unparsed(reason)
        | BoundPlanError::NotARead(reason)
        | BoundPlanError::Invalid(reason) => reason,
    })
}

/// Why the bound plan did not translate a query, as its caller reports it:
/// `Unsupported` with the construct (without the pipeline's own wording:
/// "not lowered yet", slice tags such as "(S8)", issue numbers), or
/// `Internal` when the bound plan broke an invariant of its own.
fn not_translated(reason: &str) -> TranslateError {
    let mut r = reason;
    for prefix in ["not lowered yet: ", "not supported by the bound plan yet: "] {
        r = r.strip_prefix(prefix).unwrap_or(r);
    }
    if let Some(internal) = r.strip_prefix("internal: ") {
        return TranslateError::Internal(internal.to_string());
    }
    // Nested under what was lowered: `variable-length relationship: internal: …`.
    if r.contains("internal: ") {
        return TranslateError::Internal(r.to_string());
    }
    // Drop parenthesized tags of the plan: `(S8)`, `(S7 lists)`,
    // `(#672/#1010)`; never inside a backticked name the reason echoes.
    let is_tag = |t: &str| {
        let mut cs = t.chars();
        match (cs.next(), cs.next()) {
            (Some('#'), Some(c)) if c.is_ascii_digit() => t
                .chars()
                .all(|c| c.is_ascii_digit() || c == '#' || c == '/'),
            (Some('S'), Some(c)) if c.is_ascii_digit() => t
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == ' ' || c == 'S'),
            _ => false,
        }
    };
    let mut out = String::new();
    let mut rest = r;
    while let Some(open) = rest.find(" (") {
        let after = &rest[open + 2..];
        let quoted = (out.matches('`').count() + rest[..open].matches('`').count()) % 2 == 1;
        match after.find(')') {
            Some(close) if !quoted && is_tag(&after[..close]) => {
                out.push_str(&rest[..open]);
                rest = &after[close + 1..];
            }
            _ => {
                out.push_str(&rest[..open + 2]);
                rest = after;
            }
        }
    }
    out.push_str(rest);
    TranslateError::Unsupported(out)
}

/// A clause-list parse failure as a message: what the parser expected, and
/// where.
fn syntax_error(
    e: &nom::Err<crate::open_cypher_parser::errors::OpenCypherParsingError<'_>>,
) -> String {
    match e {
        nom::Err::Error(p) | nom::Err::Failure(p) => match p.errors.first() {
            Some((input, expected)) => format!("Invalid syntax: {expected} at {}", near(input)),
            None => "Invalid syntax".to_string(),
        },
        nom::Err::Incomplete(_) => "Invalid syntax: the query is incomplete".to_string(),
    }
}

/// Where in the query a parse failure is: the text from there, shortened.
fn near(input: &str) -> String {
    let input = input.trim();
    if input.is_empty() {
        return "the end of the query".to_string();
    }
    let short: String = input.chars().take(40).collect();
    let more = if input.chars().count() > 40 {
        "…"
    } else {
        ""
    };
    format!(
        "`{}{more}`",
        short.split_whitespace().collect::<Vec<_>>().join(" ")
    )
}

/// Why the bound plan did not translate a query.
#[derive(Debug)]
enum BoundPlanError {
    /// Not supported (yet): the legacy pipeline translates it on the
    /// dialects that keep it; an error on the others.
    Fallback(String),
    /// Not parsed by the clause-list parser.
    Unparsed(String),
    /// Not a read query (`CALL`, an updating clause): its entry point's
    /// handling, or the legacy pipeline's error.
    NotARead(String),
    /// Invalid Cypher, as Neo4j rejects it (the binder's errors other than
    /// `Unsupported`).
    Invalid(String),
}

fn bound_plan_or_reason(
    cypher: &str,
    schema: &GraphSchema,
    options: &ReadOptions,
) -> Result<BoundTranslation, BoundPlanError> {
    use crate::server::query_context::{
        has_query_context, set_current_schema, with_query_context_sync, QueryContext,
    };
    // Printing reads the query context (plain printing, the dialect); a
    // caller without one gets a fresh one, as in `translate_read`.
    if !has_query_context() {
        return with_query_context_sync(QueryContext::new(None), || {
            set_current_schema(std::sync::Arc::new(schema.clone()));
            bound_plan_or_reason(cypher, schema, options)
        });
    }
    use crate::bound_plan::lower::{lower_statement, LowerOptions};
    let (rest, stmt) = crate::open_cypher_parser::clause_list::parse_clause_statement(cypher)
        .map_err(|e| BoundPlanError::Unparsed(syntax_error(&e)))?;
    let rest = rest.trim().trim_end_matches(';').trim();
    if !rest.is_empty() {
        return Err(BoundPlanError::Unparsed(format!(
            "Invalid syntax: unexpected text at {}",
            near(rest)
        )));
    }
    let bound = crate::bound_plan::bind_statement(&stmt, schema).map_err(|e| match e {
        crate::bound_plan::BindError::Unsupported(_) => BoundPlanError::Fallback(e.to_string()),
        crate::bound_plan::BindError::NotARead(_) => BoundPlanError::NotARead(e.to_string()),
        _ => BoundPlanError::Invalid(e.to_string()),
    })?;
    // Whether the lowering read the server's `id()` numbers.
    let consulted = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lowered = lower_statement(
        &bound,
        schema,
        &LowerOptions {
            view_parameter_values: merged_view_parameters(
                options.tenant_id.as_deref(),
                options.view_parameter_values.as_ref(),
            ),
            neo4j_compat: options
                .neo4j_compat
                .unwrap_or_else(crate::server::query_context::server_neo4j_compat),
            check_node_properties: options
                .check_node_properties
                .unwrap_or_else(crate::server::query_context::check_node_properties),
            id_lookup: options.id_mapper.clone().map(|mapper| {
                // The query's schema (the context's: an `Arc`, not a copy).
                let schema = crate::server::query_context::get_current_schema()
                    .unwrap_or_else(|| std::sync::Arc::new(schema.clone()));
                let elements = mapper.clone();
                let (node_read, element_read) = (consulted.clone(), consulted.clone());
                crate::bound_plan::lower::IdLookup {
                    node: std::sync::Arc::new(move |id| {
                        node_read.store(true, std::sync::atomic::Ordering::Relaxed);
                        crate::server::bolt_protocol::id_mapper::resolve_node_id(
                            &mapper,
                            Some(&schema),
                            id,
                        )
                    }),
                    element: std::sync::Arc::new(move |id| {
                        element_read.store(true, std::sync::atomic::Ordering::Relaxed);
                        elements.get_element_id(id)
                    }),
                }
            }),
            // The fold's cast targets spell for the query's dialect
            // (`cast_target`): the context carries it (round 3, "dialect") —
            // previously always ClickHouse here, whatever the context said.
            dialect: crate::server::query_context::get_current_dialect(),
            ..LowerOptions::default()
        },
    )
    .map_err(|e| BoundPlanError::Fallback(e.to_string()))?;
    Ok(BoundTranslation {
        sql: crate::clickhouse_query_generator::to_sql_query::render_plan_to_sql_plain(
            lowered.plan,
        ),
        shape: lowered.shape,
        session_ids: consulted.load(std::sync::atomic::Ordering::Relaxed),
    })
}

/// The view-parameter values a translation applies: the request's, plus
/// `tenant_id` from the request's tenant unless a `tenant_id` view parameter
/// was given explicitly. The legacy planner merges them the same way
/// (`PlanCtx::with_all_parameters`); tenant isolation of parameterized views
/// depends on it.
fn merged_view_parameters(
    tenant_id: Option<&str>,
    view_parameter_values: Option<&HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    match (tenant_id, view_parameter_values) {
        (None, values) => values.cloned(),
        (Some(tenant), values) => {
            let mut merged = values.cloned().unwrap_or_default();
            merged
                .entry("tenant_id".to_string())
                .or_insert_with(|| tenant.to_string());
            Some(merged)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{clause_list_parses, translate_text_read, ReadOptions, TranslateError};

    fn social() -> super::GraphSchema {
        crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(include_str!(
            "../schemas/test/social_integration.yaml"
        ))
        .unwrap()
        .to_graph_schema()
        .unwrap()
    }

    fn on() -> ReadOptions {
        ReadOptions::default()
    }

    /// Syntax only the clause-list parser reads (P-4c S10b) is translated by
    /// the bound plan alone; what it does not translate is the caller's
    /// parse error, and a query Neo4j rejects is invalid.
    #[test]
    fn text_only_syntax_is_translated_by_the_bound_plan() {
        let schema = social();
        for q in [
            "MATCH (a:User) WITH a MATCH (a)-[:FOLLOWS]->(b:User) \
             MATCH (b)-[:FOLLOWS]->(c:User) RETURN count(*) AS n",
            "MATCH (a:User) WITH a UNWIND [1, 2] AS x UNWIND [3, 4] AS y RETURN count(*) AS n",
            "MATCH (a:User) WITH a OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) \
             MATCH (c:User) RETURN count(*) AS n",
            "MATCH (a:User) ORDER BY a.name LIMIT 3 RETURN a.name",
        ] {
            assert!(
                crate::open_cypher_parser::parse_cypher_statement(q).is_err(),
                "the legacy parser reads it now: {q}"
            );
            let t = translate_text_read(q, &schema, on())
                .unwrap()
                .unwrap_or_else(|| panic!("not translated: {q}"));
            let lib = crate::clickhouse_query_generator::cypher_to_sql(q, &schema);
            assert_eq!(lib.as_deref(), Ok(t.sql.as_str()), "{q}");
        }
        // An updating clause, which the clause-list parser rejects: the
        // caller reports its parse error.
        let create = "MATCH (a:User) ORDER BY a.name LIMIT 3 CREATE (b:User) RETURN a.name";
        assert!(
            translate_text_read(create, &schema, on())
                .unwrap()
                .is_none(),
            "{create}"
        );
        // Parsed but not lowered (`id()` other than compared with integers):
        // not supported, which no pipeline translates.
        let id_gt = "MATCH (a:User) WITH a MATCH (a)-[r:FOLLOWS]->(b:User) MATCH (b) \
             WHERE id(r) > 3 RETURN count(*) AS n";
        assert!(
            matches!(
                translate_text_read(id_gt, &schema, on()),
                Err(TranslateError::Unsupported(_))
            ),
            "{id_gt}"
        );
        assert!(matches!(
            translate_text_read(
                "MATCH (a:User) ORDER BY a.name LIMIT 3 RETURN z.name",
                &schema,
                on()
            ),
            Err(TranslateError::Invalid(_))
        ));
    }

    /// The inner query of an export translates as `/query` would, text only
    /// the clause-list parser reads included (S10b review).
    #[test]
    fn an_inner_export_query_translates_as_a_query_does() {
        let schema = social();
        let text_only = "MATCH (a:User) ORDER BY a.name LIMIT 1 RETURN a.name";
        let sql = super::translate_inner_read(
            text_only,
            &schema,
            "g",
            crate::server::bolt_protocol::id_mapper::IdMapper::new(),
        )
        .unwrap();
        assert_eq!(
            Ok(sql),
            crate::clickhouse_query_generator::cypher_to_sql(text_only, &schema)
        );
        let read = "MATCH (a:User) RETURN a.name";
        assert_eq!(
            super::translate_inner_read(
                read,
                &schema,
                "g",
                crate::server::bolt_protocol::id_mapper::IdMapper::new()
            )
            .map_err(|e| e.to_string()),
            crate::clickhouse_query_generator::cypher_to_sql(read, &schema)
        );
        let err = super::translate_inner_read(
            "MATCH (a:User) RETRUN a",
            &schema,
            "g",
            crate::server::bolt_protocol::id_mapper::IdMapper::new(),
        )
        .map_err(|e| e.to_string());
        assert!(
            matches!(&err, Err(e) if e.starts_with("Inner Cypher parse error")),
            "{err:?}"
        );
        let err = super::translate_inner_read(
            "MATCH (a:User) ORDER BY a.name LIMIT 1 RETURN z.name",
            &schema,
            "g",
            crate::server::bolt_protocol::id_mapper::IdMapper::new(),
        )
        .map_err(|e| e.to_string());
        assert!(
            matches!(&err, Err(e) if e.starts_with("Inner Cypher query error")),
            "{err:?}"
        );
        // It runs on the statement's graph: a USE naming another is refused
        // (it was ignored); naming the statement's graph, or a prelude and
        // comments, are read.
        let err = super::translate_inner_read(
            "USE h MATCH (a:User) RETURN a.name",
            &schema,
            "g",
            crate::server::bolt_protocol::id_mapper::IdMapper::new(),
        )
        .map_err(|e| e.to_string());
        assert!(
            matches!(&err, Err(e) if e.contains("Multiple graphs")),
            "{err:?}"
        );
        assert_eq!(
            super::translate_inner_read(
                "// note\nCYPHER replan=force USE g MATCH (a:User) RETURN a.name",
                &schema,
                "g",
                crate::server::bolt_protocol::id_mapper::IdMapper::new(),
            )
            .map_err(|e| e.to_string()),
            crate::clickhouse_query_generator::cypher_to_sql(read, &schema)
        );
    }

    /// A subquery inside a pattern's property map, which the binder does not
    /// take out of its expression, is not supported, not an internal error
    /// (S14c6 review).
    #[test]
    fn a_subquery_in_a_pattern_property_map_is_not_supported() {
        let schema = social();
        // A lone node in an expression is no pattern (#1398): Neo4j reads
        // `(a {name: …})` as the map projection `a {…}`, a type error there.
        for q in [
            "MATCH (a:User) WHERE (a {name: EXISTS { (a)-[:FOLLOWS]->() }}) RETURN a.name",
            "MATCH (a:User) RETURN size((a {name: (a)-[:FOLLOWS]->()})) AS n",
            "MATCH (a:User) WHERE NOT (a {name: EXISTS { (a)-[:FOLLOWS]->() }}) RETURN a.name",
        ] {
            let got = crate::clickhouse_query_generator::cypher_to_sql(q, &schema);
            assert!(got.is_err(), "{q}: {got:?}");
        }
        for q in [
            "MATCH (a:User), (b:User) \
             RETURN shortestPath((a)-[:FOLLOWS {x: EXISTS { (a)-[:FOLLOWS]->() }}]-(b))",
            "MATCH (a:User) WHERE (a)-[:FOLLOWS {x: EXISTS { (a)-[:FOLLOWS]->() }}]->() \
             RETURN a.name",
        ] {
            let got = crate::clickhouse_query_generator::cypher_to_sql(q, &schema);
            assert!(
                matches!(&got, Err(e) if e.starts_with("Not supported: ")),
                "{q}: {got:?}"
            );
        }
    }

    /// A refusal reads as the construct, without the plan's wording; a
    /// broken invariant is an internal error (S11j review).
    #[test]
    fn refusals_read_as_their_construct() {
        let msg = |r: &str| match super::not_translated(r) {
            TranslateError::Unsupported(m) => format!("unsupported: {m}"),
            TranslateError::Internal(m) => format!("internal: {m}"),
            other => format!("{other:?}"),
        };
        assert_eq!(
            msg("not lowered yet: type FOLLOWS is not the standard layout (S8)"),
            "unsupported: type FOLLOWS is not the standard layout"
        );
        assert_eq!(
            msg("not lowered yet: nodes() of a path other than as a RETURN item (S7 lists)"),
            "unsupported: nodes() of a path other than as a RETURN item"
        );
        assert_eq!(
            msg("not lowered yet: ids crossed (#672/#1010) in a walk (a composite id)"),
            "unsupported: ids crossed in a walk (a composite id)"
        );
        assert_eq!(
            msg("not supported by the bound plan yet: CALL"),
            "unsupported: CALL"
        );
        assert_eq!(
            msg("not lowered yet: internal: a path endpoint is not a node"),
            "internal: a path endpoint is not a node"
        );
        assert_eq!(
            msg("not lowered yet: variable-length relationship: internal: no seed"),
            "internal: variable-length relationship: internal: no seed"
        );
        // Not tags: user text, or parentheses of the wording.
        assert_eq!(
            msg("not lowered yet: a value-typed variable `x (S1)` used as a node"),
            "unsupported: a value-typed variable `x (S1)` used as a node"
        );
        assert_eq!(
            msg("not lowered yet: a list (in a map) of nodes (S7 lists)"),
            "unsupported: a list (in a map) of nodes"
        );
    }

    #[test]
    fn text_only_syntax_parses() {
        assert!(clause_list_parses(
            "USE g MATCH (a) ORDER BY a.x LIMIT 1 RETURN a"
        ));
        assert!(clause_list_parses("MATCH (a) LIMIT 1 RETURN a;"));
        assert!(!clause_list_parses("MATCH (a) RETRUN a"));
        // The text-only statement's USE is the statement's graph.
        assert_eq!(
            super::StatementTargets::of("USE g MATCH (a) ORDER BY a.x LIMIT 1 RETURN a")
                .unwrap()
                .use_graph(),
            Some("g")
        );
    }

    use crate::server::query_context::{generate_cte_id, generate_id, reset_all_counters};
    use crate::server::query_context::{with_query_context, QueryContext};

    /// A translation without a caller-provided context still runs on its own
    /// per-query counters: the process-global fallback is neither reset nor
    /// advanced (it was, and concurrent `/query/sql` requests, which had no
    /// context, rewound each other's aliases mid-translation).
    #[test]
    fn translation_without_context_leaves_global_counters_alone() {
        let schema = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(include_str!(
            "../schemas/test/social_integration.yaml"
        ))
        .unwrap()
        .to_graph_schema()
        .unwrap();
        let translate = || {
            let cypher = "MATCH (a:User)-[:FOLLOWS]->()-[:FOLLOWS]->(c:User) RETURN count(*)";
            let (_, stmt) = crate::open_cypher_parser::parse_cypher_statement(cypher).unwrap();
            super::translate_read(
                stmt,
                &schema,
                super::ReadOptions {
                    cypher: cypher.to_string(),
                    ..Default::default()
                },
            )
            .unwrap()
            .sql
        };
        let before: u32 = generate_id()[1..].parse().unwrap();
        let first = translate();
        let after: u32 = generate_id()[1..].parse().unwrap();
        assert_eq!(after, before + 1, "translation touched the global counter");
        assert_eq!(first, translate(), "translation is not deterministic");
    }

    /// A query's generated aliases are numbered by its OWN counters: another
    /// query resetting its counters (every translation does) must not rewind
    /// them. With process-global counters, the inner reset below made the outer
    /// query issue `t2` a second time.
    #[tokio::test]
    async fn generated_alias_counters_are_per_query() {
        let outer = with_query_context(QueryContext::new(None), async {
            reset_all_counters();
            let a = generate_id();
            let b = generate_id();
            let inner = with_query_context(QueryContext::new(None), async {
                reset_all_counters();
                (generate_id(), generate_cte_id())
            })
            .await;
            let c = generate_id();
            let cte = generate_cte_id();
            (a, b, inner, c, cte)
        })
        .await;
        assert_eq!(
            outer,
            (
                "t1".to_string(),
                "t2".to_string(),
                ("t1".to_string(), "cte1".to_string()),
                "t3".to_string(),
                "cte1".to_string(),
            )
        );
    }
    /// Bolt, the HTTP graph output and embedded `query_graph` read the
    /// result shape through `return_metadata` (§4.13).
    #[test]
    fn return_metadata_of_the_result_shape() {
        use crate::server::bolt_protocol::result_transformer::ReturnItemType as T;
        let schema = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(include_str!(
            "../schemas/test/social_integration.yaml"
        ))
        .unwrap()
        .to_graph_schema()
        .unwrap();
        let cypher = "MATCH (a:User)<-[r:FOLLOWS]-(b:User) RETURN a AS x, r, id(b), b.name";
        let (_, stmt) = crate::open_cypher_parser::parse_cypher_statement(cypher).unwrap();
        let bound = super::translate_read(
            stmt,
            &schema,
            super::ReadOptions {
                cypher: cypher.to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        let metadata = bound.return_metadata();
        let x_columns = metadata[0].columns.as_ref().expect("explicit columns");
        assert!(
            x_columns.contains(&("name".to_string(), "x.name".to_string())),
            "{x_columns:?}"
        );
        let items: Vec<(String, String)> = bound
            .return_metadata()
            .into_iter()
            .map(|m| (m.field_name, format!("{:?}", m.item_type)))
            .collect();
        assert_eq!(
            items,
            [
                (
                    "x",
                    format!(
                        "{:?}",
                        T::Node {
                            labels: vec!["User".into()]
                        }
                    )
                ),
                (
                    "r",
                    format!(
                        "{:?}",
                        T::Relationship {
                            rel_types: vec!["FOLLOWS".into()],
                            from_label: Some("User".into()),
                            to_label: Some("User".into()),
                            // The columns are in the stored orientation.
                            direction: Some("Outgoing".into()),
                        }
                    )
                ),
                (
                    "id(b)",
                    format!(
                        "{:?}",
                        T::IdFunction {
                            alias: "id(b)".into(),
                            labels: vec!["User".into()],
                        }
                    )
                ),
                ("b.name", format!("{:?}", T::Scalar)),
            ]
            .map(|(n, t)| (n.to_string(), t))
        );
    }
}
