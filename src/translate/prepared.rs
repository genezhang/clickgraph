//! The statement front end every entry point shares: what a statement says
//! before and around its clauses, read once.
//!
//! - The prelude: `CYPHER [version] [option=value ...]` and `EXPLAIN` /
//!   `PROFILE`, in any order and repeated, as Neo4j reads them
//!   ([`Prelude::parse`]). Before this, `/query` cut the text at the first
//!   clause keyword it found anywhere in it (`CYPHER replan=force USE g
//!   MATCH ..` lost its `USE`, #1371), `/query/sql` dropped one word, and
//!   Bolt and the library entry points did not read a prelude at all.
//! - The graph each UNION arm names with `USE`, and the one rule that picks
//!   the statement's graph ([`StatementTargets::graph`]): the first arm's
//!   `USE`, else the graph the request names (HTTP `schema_name`, the Bolt
//!   session's database), else [`DEFAULT_GRAPH`]. A later arm's `USE` that
//!   names another graph is an error, as in Neo4j (#1370): it was ignored,
//!   and the arm silently returned no rows.
//! - Whether any arm updates the graph ([`StatementTargets::is_write`]): the
//!   embedded dispatcher read only the first arm (#1447).
//!
//! HTTP `/query` and `/query/sql`, Bolt, and the embedded connection (and
//! through it FFI, Go, Python and `cg`) all read statements through here.

use crate::open_cypher_parser::clause_list::{parse_clause_statement, Clause, ClauseQuery};

/// The most clauses a statement may have, all UNION arms together (#1373).
/// Translation recurses once per clause in several stages (the legacy
/// parser reads a WITH chain as nested queries; the lowering recurses over
/// the clauses), so a long enough chain overflows any thread's stack, and a
/// stack overflow aborts the process: one 255 KB request of 15,000 `WITH a
/// LIMIT 100` aborted a (debug-build) server, and 800 aborted `cg`. Real
/// queries have tens of clauses (the LDBC ones at most about 30).
pub const MAX_CLAUSES: usize = 1000;

/// The graph a statement runs on when neither the statement (`USE`) nor the
/// request names one: the schema registered as `default` (the configured
/// `default_schema`, else the first schema loaded).
pub const DEFAULT_GRAPH: &str = "default";

/// The `replan` option of a `CYPHER` prelude: how `/query` uses its SQL
/// cache.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ReplanOption {
    /// Use a cached translation if there is one.
    #[default]
    Default,
    /// Translate again (and refresh the cache).
    Force,
    /// Use the cache (`/query` translates when nothing is cached).
    Skip,
}

/// `EXPLAIN` / `PROFILE` in the prelude.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    #[default]
    Run,
    Explain,
    Profile,
}

/// Why a statement's prelude or its graphs are refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareError {
    /// An invalid `CYPHER` option or execution mode (Neo4j:
    /// `Neo.ClientError.Statement.ArgumentError`).
    InvalidOption(String),
    /// The UNION arms name different graphs (Neo4j: "Multiple graphs in the
    /// same query not allowed").
    MultipleGraphs(String),
    /// The statement has more than [`MAX_CLAUSES`] clauses.
    TooLarge(String),
}

impl std::fmt::Display for PrepareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrepareError::InvalidOption(e)
            | PrepareError::MultipleGraphs(e)
            | PrepareError::TooLarge(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PrepareError {}

/// A statement's prelude, and the text after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prelude {
    /// The statement without comments and without the prelude: what the
    /// parsers read.
    pub text: String,
    pub replan: ReplanOption,
    pub mode: ExecutionMode,
}

impl Prelude {
    /// Read the prelude of `raw` (comments are stripped first). Options
    /// other than `replan` are accepted and ignored (planner hints such as
    /// `runtime=slotted` do not change the answer); so is a version number.
    /// Unlike Neo4j, which refuses an option or a version it does not know
    /// (`CYPHER foo=bar`, `CYPHER 4.4`), and an invalid value of a known
    /// one (`runtime=bogus`): only `replan` is checked.
    pub fn parse(raw: &str) -> Result<Prelude, PrepareError> {
        let stripped = crate::open_cypher_parser::strip_comments(raw);
        let (replan, mode, rest) = scan_prelude(&stripped)?;
        Ok(Prelude {
            text: rest.to_string(),
            replan,
            mode,
        })
    }
}

/// `text` after its prelude (the whole text when the prelude is invalid):
/// the statement a cache key names.
pub fn without_prelude(text: &str) -> &str {
    scan_prelude(text).map_or(text, |(_, _, rest)| rest)
}

/// The `CYPHER` options and execution mode at the start of `text`, and the
/// text after them.
fn scan_prelude(text: &str) -> Result<(ReplanOption, ExecutionMode, &str), PrepareError> {
    let mut replan: Option<ReplanOption> = None;
    let mut mode: Option<ExecutionMode> = None;
    let mut rest = text.trim_start();
    loop {
        if let Some(r) = keyword(rest, "CYPHER") {
            rest = r.trim_start();
            if let Some((_version, r)) = version(rest) {
                rest = r.trim_start();
            }
            // `name = value` pairs, until the first token that is not one.
            while let Some((name, r)) = identifier(rest) {
                let Some(r) = r.trim_start().strip_prefix('=') else {
                    break;
                };
                let r = r.trim_start();
                let Some((value, r)) = identifier(r).or_else(|| version(r)) else {
                    return Err(PrepareError::InvalidOption(format!(
                        "the CYPHER option `{name}` has no value"
                    )));
                };
                if name.eq_ignore_ascii_case("replan") {
                    let option = match value.to_ascii_lowercase().as_str() {
                        "force" => ReplanOption::Force,
                        "skip" => ReplanOption::Skip,
                        "default" => ReplanOption::Default,
                        _ => {
                            return Err(PrepareError::InvalidOption(format!(
                                "{value} is not a valid option for replan. Valid options are: \
                                 force, skip, default"
                            )))
                        }
                    };
                    if replan.is_some_and(|r| r != option) {
                        return Err(PrepareError::InvalidOption(
                            "Can't specify multiple conflicting values for replan".to_string(),
                        ));
                    }
                    replan = Some(option);
                } else {
                    log::debug!("CYPHER option `{name}={value}` ignored");
                }
                rest = r.trim_start();
            }
            continue;
        }
        let explain = keyword(rest, "EXPLAIN").map(|r| (ExecutionMode::Explain, r));
        let profile = || keyword(rest, "PROFILE").map(|r| (ExecutionMode::Profile, r));
        if let Some((m, r)) = explain.or_else(profile) {
            if mode.is_some_and(|old| old != m) {
                return Err(PrepareError::InvalidOption(
                    "Can't specify multiple conflicting values for execution mode".to_string(),
                ));
            }
            mode = Some(m);
            rest = r.trim_start();
            continue;
        }
        break;
    }
    Ok((replan.unwrap_or_default(), mode.unwrap_or_default(), rest))
}

fn is_identifier_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// `text` after the keyword `kw` (any case), when it starts with it as a
/// whole word.
fn keyword<'a>(text: &'a str, kw: &str) -> Option<&'a str> {
    let head = text.get(..kw.len())?;
    let rest = &text[kw.len()..];
    (head.eq_ignore_ascii_case(kw) && !rest.starts_with(is_identifier_char)).then_some(rest)
}

/// A leading identifier (`[A-Za-z_][A-Za-z0-9_]*`), and the text after it.
fn identifier(text: &str) -> Option<(&str, &str)> {
    if !text.starts_with(|c: char| c.is_alphabetic() || c == '_') {
        return None;
    }
    let end = text
        .find(|c: char| !is_identifier_char(c))
        .unwrap_or(text.len());
    Some((&text[..end], &text[end..]))
}

/// A leading version number (`5`, `4.4`), and the text after it.
fn version(text: &str) -> Option<(&str, &str)> {
    if !text.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let end = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(text.len());
    // Not a number followed by more of an identifier (`5x`).
    if text[end..].starts_with(is_identifier_char) {
        return None;
    }
    Some((&text[..end], &text[end..]))
}

/// What a statement's arms name and do: the graph of each arm's `USE`, and
/// whether any arm updates the graph.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatementTargets {
    /// Each UNION arm's `USE` graph, in order (empty: a standalone procedure
    /// call; one, from a leading `USE`, for text neither parser reads).
    arm_graphs: Vec<Option<String>>,
    is_write: bool,
}

impl StatementTargets {
    /// Read the arms of `text` (a statement without its prelude): with the
    /// clause-list parser, else the legacy one (text only it reads:
    /// `CALL p() YIELD x WHERE ..`, `COPY (..) TO ..`, whose inner query's
    /// arms are read). Text neither reads names no graph and writes nothing:
    /// the caller's parse reports the error.
    ///
    /// A statement of more than [`MAX_CLAUSES`] clauses is refused here,
    /// before a parser that recurses per clause reads it.
    pub fn of(text: &str) -> Result<StatementTargets, PrepareError> {
        if let Ok((_, stmt)) = parse_clause_statement(text) {
            let arms: Vec<&ClauseQuery<'_>> = std::iter::once(&stmt.first)
                .chain(stmt.unions.iter().map(|(_, q)| q))
                .collect();
            let clauses: usize = arms.iter().map(|q| q.clauses.len()).sum();
            if clauses > MAX_CLAUSES {
                return Err(PrepareError::TooLarge(format!(
                    "Not supported: a statement of {clauses} clauses (at most {MAX_CLAUSES})"
                )));
            }
            return Ok(StatementTargets {
                arm_graphs: arms
                    .iter()
                    .map(|q| q.use_clause.as_ref().map(|u| u.database_name.to_string()))
                    .collect(),
                is_write: arms.iter().flat_map(|q| &q.clauses).any(|c| {
                    matches!(
                        c,
                        Clause::Create(_) | Clause::Set(_) | Clause::Remove(_) | Clause::Delete(_)
                    )
                }),
            });
        }
        // Text the clause-list parser rejects: its clause keywords bound its
        // clauses (the legacy parser reads it next, recursing per WITH).
        check_clause_keywords(text)?;
        use crate::open_cypher_parser::ast::{CypherStatement, OpenCypherQueryAst};
        let writes = |q: &OpenCypherQueryAst<'_>| {
            q.create_clause.is_some()
                || q.set_clause.is_some()
                || q.remove_clause.is_some()
                || q.delete_clause.is_some()
        };
        let graph =
            |q: &OpenCypherQueryAst<'_>| q.use_clause.as_ref().map(|u| u.database_name.to_string());
        Ok(
            match crate::open_cypher_parser::parse_cypher_statement(text) {
                Ok((
                    _,
                    CypherStatement::Query {
                        query,
                        union_clauses,
                    },
                )) => StatementTargets {
                    arm_graphs: std::iter::once(graph(&query))
                        .chain(union_clauses.iter().map(|u| graph(&u.query)))
                        .collect(),
                    is_write: writes(&query) || union_clauses.iter().any(|u| writes(&u.query)),
                },
                Ok((_, CypherStatement::CopyTo(copy))) => StatementTargets {
                    is_write: false,
                    ..StatementTargets::of(copy.query)?
                },
                Ok((_, CypherStatement::ProcedureCall(_))) => StatementTargets::default(),
                // Neither parser reads it (the MCP servers' `CALL
                // apoc.meta.schema() .. UNWIND ..`, which the server answers
                // without parsing): a leading `USE` still names its graph.
                Err(_) => StatementTargets {
                    arm_graphs: vec![leading_use(text)],
                    is_write: false,
                },
            },
        )
    }

    /// The graph the first arm's `USE` names.
    pub fn use_graph(&self) -> Option<&str> {
        self.arm_graphs.first().and_then(|g| g.as_deref())
    }

    /// The one graph the arms' `USE` clauses name (`None`: no `USE`), for an
    /// entry point that has a single schema and does not know its graph
    /// name (the embedded database, which does not read `USE`). Two arms
    /// naming different graphs is an error, as in [`Self::graph`].
    pub fn named_graph(&self) -> Result<Option<&str>, PrepareError> {
        let mut named = self.arm_graphs.iter().flatten();
        let Some(first) = named.next() else {
            return Ok(None);
        };
        match named.find(|g| !same_graph(g, first)) {
            Some(other) => Err(multiple_graphs(first, other)),
            None => Ok(Some(first)),
        }
    }

    /// Refuse an arm's `USE` naming another graph than `graph`: for a query
    /// inside a statement running on `graph` (the query argument of
    /// `apoc.export.*.query`).
    pub fn within(&self, graph: &str) -> Result<(), PrepareError> {
        match self
            .arm_graphs
            .iter()
            .flatten()
            .find(|g| !same_graph(g, graph))
        {
            Some(other) => Err(PrepareError::MultipleGraphs(format!(
                "Multiple graphs in the same query not allowed: the statement runs on graph \
                 `{graph}`, and the inner query's USE names `{other}`"
            ))),
            None => Ok(()),
        }
    }

    /// Whether any arm has an updating clause (CREATE / SET / REMOVE /
    /// DELETE).
    pub fn is_write(&self) -> bool {
        self.is_write
    }

    /// The graph the statement runs on: the first arm's `USE`, else
    /// `requested` (the request's or session's graph), else
    /// [`DEFAULT_GRAPH`]. An arm without `USE` runs on it too; a later arm's
    /// `USE` naming another graph is an error (Neo4j: "Multiple graphs in
    /// the same query not allowed"). Neo4j runs an arm without `USE` on the
    /// session's graph, so it also refuses `USE g .. UNION ..` from a
    /// session on another graph, which runs here on `g` (as before #1370).
    pub fn graph(&self, requested: Option<&str>) -> Result<String, PrepareError> {
        let graph = self
            .use_graph()
            .or(requested)
            .unwrap_or(DEFAULT_GRAPH)
            .to_string();
        if let Some(other) = self
            .arm_graphs
            .iter()
            .skip(1)
            .flatten()
            .find(|g| !same_graph(g, &graph))
        {
            return Err(multiple_graphs(&graph, other));
        }
        Ok(graph)
    }
}

/// Refuse a statement the clause-list parser does not read when it has
/// more than [`MAX_CLAUSES`] clause keywords ([`clause_keywords`]).
fn check_clause_keywords(text: &str) -> Result<(), PrepareError> {
    if clause_keywords(text) > MAX_CLAUSES {
        return Err(PrepareError::TooLarge(format!(
            "Not supported: a statement of more than {MAX_CLAUSES} clauses"
        )));
    }
    Ok(())
}

/// The clause keywords of `text` (comments stripped), without parsing it:
/// an upper bound of its clauses (`STARTS WITH` counts too). Strings and
/// backticked names are skipped, and a property key (`n.limit`).
pub fn clause_keywords(text: &str) -> usize {
    const KEYWORDS: [&str; 14] = [
        "MATCH", "UNWIND", "WITH", "RETURN", "CALL", "CREATE", "MERGE", "SET", "REMOVE", "DELETE",
        "LIMIT", "SKIP", "ORDER", "UNION",
    ];
    let mut count = 0usize;
    let mut chars = text.char_indices().peekable();
    let mut previous = ' ';
    while let Some((i, c)) = chars.next() {
        match c {
            '\'' | '"' | '`' => {
                // To the closing quote (a backslash escapes in strings).
                while let Some((_, d)) = chars.next() {
                    if d == '\\' && c != '`' {
                        chars.next();
                    } else if d == c {
                        break;
                    }
                }
                previous = c;
            }
            c if c.is_alphabetic() || c == '_' => {
                let mut end = i + c.len_utf8();
                while let Some(&(j, d)) = chars.peek() {
                    if !is_identifier_char(d) {
                        break;
                    }
                    end = j + d.len_utf8();
                    chars.next();
                }
                let word = &text[i..end];
                if previous != '.' && KEYWORDS.iter().any(|k| word.eq_ignore_ascii_case(k)) {
                    count += 1;
                }
                previous = 'a';
            }
            c => {
                if !c.is_whitespace() {
                    previous = c;
                }
            }
        }
    }
    count
}

/// Whether two graph names name the same graph: the same name, or on a
/// server `default` and the schema it aliases (the configured
/// `default_schema`, else the first schema loaded).
fn same_graph(a: &str, b: &str) -> bool {
    crate::server::graph_catalog::same_schema(a, b)
}

fn multiple_graphs(graph: &str, other: &str) -> PrepareError {
    PrepareError::MultipleGraphs(format!(
        "Multiple graphs in the same query not allowed: the query runs on graph `{graph}`, and a \
         UNION arm's USE names `{other}`"
    ))
}

/// The graph of a leading `USE name` (or a backticked name) in `text`.
fn leading_use(text: &str) -> Option<String> {
    let rest = keyword(text.trim_start(), "USE")?.trim_start();
    let name = match rest.strip_prefix('`') {
        Some(quoted) => &quoted[..quoted.find('`')?],
        None => {
            let end = rest
                .find(|c: char| !(is_identifier_char(c) || c == '.'))
                .unwrap_or(rest.len());
            &rest[..end]
        }
    };
    (!name.is_empty()).then(|| name.to_string())
}

/// A statement read by the front end: its prelude, and what its arms name
/// and do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedStatement {
    pub prelude: Prelude,
    pub targets: StatementTargets,
}

impl PreparedStatement {
    /// Read `raw`'s prelude and arms. An entry point that rewrites the text
    /// after the prelude before parsing it (Bolt substitutes parameters)
    /// reads the arms of its rewritten text instead
    /// ([`StatementTargets::of`]).
    pub fn parse(raw: &str) -> Result<PreparedStatement, PrepareError> {
        let prelude = Prelude::parse(raw)?;
        let targets = StatementTargets::of(&prelude.text)?;
        Ok(PreparedStatement { prelude, targets })
    }

    /// The statement without comments and without the prelude.
    pub fn text(&self) -> &str {
        &self.prelude.text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prelude(raw: &str) -> (ReplanOption, ExecutionMode, String) {
        let p = Prelude::parse(raw).unwrap();
        (p.replan, p.mode, p.text)
    }

    /// #1371: the prelude ends at the first token that is not part of it;
    /// a `USE` after it is the statement's.
    #[test]
    fn prelude_keeps_the_statement_after_it() {
        let (replan, mode, text) = prelude("CYPHER replan=force USE g MATCH (n) RETURN n");
        assert_eq!(replan, ReplanOption::Force);
        assert_eq!(mode, ExecutionMode::Run);
        assert_eq!(text, "USE g MATCH (n) RETURN n");
        // A keyword inside a string, or a later clause, is not where the
        // prelude ends.
        let (_, _, text) = prelude("CYPHER replan=skip RETURN 'match' AS m");
        assert_eq!(text, "RETURN 'match' AS m");
        let (_, _, text) = prelude("cypher 5 Replan = FORCE runtime=slotted with 1 AS x RETURN x");
        assert_eq!(text, "with 1 AS x RETURN x");
        let (replan, mode, text) = prelude("CYPHER EXPLAIN CYPHER replan=skip RETURN 1");
        assert_eq!(
            (replan, mode, text.as_str()),
            (ReplanOption::Skip, ExecutionMode::Explain, "RETURN 1")
        );
        let (_, mode, text) = prelude("// probe\nPROFILE MATCH (n) RETURN n");
        assert_eq!(
            (mode, text.as_str()),
            (ExecutionMode::Profile, "MATCH (n) RETURN n")
        );
        // No prelude: the text as written (comments stripped).
        let (replan, mode, text) = prelude("MATCH (n) RETURN n");
        assert_eq!(
            (replan, mode, text.as_str()),
            (
                ReplanOption::Default,
                ExecutionMode::Run,
                "MATCH (n) RETURN n"
            )
        );
        // A word that only starts like a keyword is not one.
        let (_, mode, text) = prelude("EXPLAINED");
        assert_eq!((mode, text.as_str()), (ExecutionMode::Run, "EXPLAINED"));
    }

    /// Invalid options are refused, as Neo4j does.
    #[test]
    fn prelude_refuses_invalid_options() {
        for raw in [
            "CYPHER replan=bogus RETURN 1",
            "CYPHER replan=force replan=skip RETURN 1",
            "EXPLAIN PROFILE RETURN 1",
            "CYPHER replan= RETURN 1",
        ] {
            assert!(
                matches!(Prelude::parse(raw), Err(PrepareError::InvalidOption(_))),
                "{raw}"
            );
        }
        // The same value twice, and the same mode twice, are not conflicts.
        assert!(Prelude::parse("CYPHER replan=force replan=FORCE RETURN 1").is_ok());
        assert!(Prelude::parse("EXPLAIN EXPLAIN RETURN 1").is_ok());
    }

    fn graph(text: &str, requested: Option<&str>) -> Result<String, PrepareError> {
        StatementTargets::of(text).unwrap().graph(requested)
    }

    /// #1370: the first arm's USE, else the request's graph, else the
    /// default; a later arm naming another graph is refused.
    #[test]
    fn statement_graph_and_later_arm_use() {
        let q = "MATCH (a:User) RETURN a.name AS n";
        assert_eq!(graph(q, None).unwrap(), DEFAULT_GRAPH);
        assert_eq!(graph(q, Some("s")).unwrap(), "s");
        assert_eq!(graph(&format!("USE g {q}"), Some("s")).unwrap(), "g");
        // Every arm naming the statement's graph, or none: accepted.
        let same = format!("USE g {q} UNION USE g {q}");
        assert_eq!(graph(&same, Some("s")).unwrap(), "g");
        assert_eq!(graph(&format!("USE g {q} UNION {q}"), None).unwrap(), "g");
        assert_eq!(
            graph(&format!("{q} UNION USE s {q}"), Some("s")).unwrap(),
            "s"
        );
        // A later arm naming another graph: refused.
        for (text, requested) in [
            (format!("{q} UNION USE g {q}"), Some("s")),
            (format!("{q} UNION ALL USE g {q}"), None),
            (format!("USE g {q} UNION {q} UNION USE h {q}"), None),
        ] {
            assert!(
                matches!(
                    graph(&text, requested),
                    Err(PrepareError::MultipleGraphs(_))
                ),
                "{text}"
            );
        }
        // Text only the legacy parser reads keeps its arms' USE too.
        let call = "USE g CALL db.labels() YIELD label WHERE label <> 'x' RETURN label";
        assert_eq!(graph(call, None).unwrap(), "g");
        // A COPY TO's graph is its inner query's.
        let copy = "COPY (USE g MATCH (a:User) RETURN a.name) TO '/tmp/x.csv'";
        assert_eq!(graph(copy, None).unwrap(), "g");
        // Text neither parser reads: a leading USE still names the graph
        // (the caller's parse reports the error, or the server answers it).
        assert_eq!(graph("USE g MATCH (", Some("s")).unwrap(), "g");
        assert_eq!(graph("USE `my-g` CALL x() UNWIND [", None).unwrap(), "my-g");
        assert_eq!(graph("MATCH (", Some("s")).unwrap(), "s");
    }

    /// On a server, `default` and the schema it aliases are one graph: a
    /// statement may name it either way (review of #1370).
    #[test]
    fn the_default_alias_is_the_same_graph() {
        use crate::server::graph_catalog::{forget_default_alias_of, set_default_schema_target};
        // The alias is process-wide: this is the only lib test that sets
        // it, and only to names no other test uses.
        let q = "MATCH (a:User) RETURN a.name AS n";
        // Reloading the aliased schema (or `default`) makes them two graphs.
        set_default_schema_target(Some("alias_target_t".to_string()));
        forget_default_alias_of("alias_unrelated_v");
        let kept = graph(&format!("{q} UNION USE alias_target_t {q}"), None);
        forget_default_alias_of("alias_target_t");
        let reloaded = graph(&format!("{q} UNION USE alias_target_t {q}"), None);
        set_default_schema_target(Some("alias_target_t".to_string()));
        forget_default_alias_of("default");
        let reloaded_default = graph(&format!("{q} UNION USE alias_target_t {q}"), None);
        assert!(kept.is_ok(), "{kept:?}");
        assert!(matches!(reloaded, Err(PrepareError::MultipleGraphs(_))));
        assert!(matches!(
            reloaded_default,
            Err(PrepareError::MultipleGraphs(_))
        ));
        set_default_schema_target(Some("alias_target_t".to_string()));
        let got = [
            graph(&format!("{q} UNION USE alias_target_t {q}"), None),
            graph(
                &format!("USE default {q} UNION USE alias_target_t {q}"),
                None,
            ),
            graph(
                &format!("{q} UNION USE default {q}"),
                Some("alias_target_t"),
            ),
        ];
        let other = graph(&format!("{q} UNION USE alias_other_u {q}"), None);
        set_default_schema_target(None);
        assert_eq!(
            got.map(|g| g.unwrap()),
            ["default", "default", "alias_target_t"].map(String::from)
        );
        assert!(matches!(other, Err(PrepareError::MultipleGraphs(_))));
    }

    /// A single-schema entry point checks only that the arms' USEs agree.
    #[test]
    fn named_graph_of_the_arms() {
        let q = "MATCH (a:User) RETURN a.name AS n";
        let named = |t: &str| {
            StatementTargets::of(t)
                .unwrap()
                .named_graph()
                .map(|g| g.map(String::from))
        };
        assert_eq!(named(q).unwrap(), None);
        assert_eq!(
            named(&format!("{q} UNION USE g {q}")).unwrap().as_deref(),
            Some("g")
        );
        assert_eq!(
            named(&format!("USE g {q} UNION {q} UNION USE g {q}"))
                .unwrap()
                .as_deref(),
            Some("g")
        );
        assert!(matches!(
            named(&format!("USE g {q} UNION USE h {q}")),
            Err(PrepareError::MultipleGraphs(_))
        ));
    }

    /// #1373: more than `MAX_CLAUSES` clauses, all arms together, are
    /// refused; text the clause-list parser does not read is bounded by its
    /// clause keywords (strings, backticked names and property keys aside).
    #[test]
    fn statement_size_limit() {
        let chain = |n: usize| format!("MATCH (a) {}RETURN a", "WITH a ".repeat(n));
        assert!(StatementTargets::of(&chain(MAX_CLAUSES - 2)).is_ok());
        assert!(matches!(
            StatementTargets::of(&chain(MAX_CLAUSES - 1)),
            Err(PrepareError::TooLarge(_))
        ));
        let arms = vec!["MATCH (a) RETURN a"; MAX_CLAUSES / 2 + 1].join(" UNION ");
        assert!(matches!(
            StatementTargets::of(&arms),
            Err(PrepareError::TooLarge(_))
        ));
        let unparsed = format!("{} GARBAGE", chain(MAX_CLAUSES));
        assert!(matches!(
            StatementTargets::of(&unparsed),
            Err(PrepareError::TooLarge(_))
        ));
        assert_eq!(
            clause_keywords("MATCH (n) WHERE n.limit = 'WITH x' AND n.`match` > 1 RETURN n"),
            2
        );
        assert_eq!(clause_keywords("match (n) with n Return n"), 3);
    }

    /// #1447: an update in any arm makes the statement a write.
    #[test]
    fn write_in_any_arm() {
        let read = "MATCH (x:Person) RETURN x.name AS n";
        let write = "MATCH (y:Person) SET y.name = 'w' RETURN y.name AS n";
        assert!(!StatementTargets::of(read).unwrap().is_write());
        assert!(StatementTargets::of(write).unwrap().is_write());
        assert!(StatementTargets::of(&format!("{read} UNION {write}"))
            .unwrap()
            .is_write());
        assert!(StatementTargets::of("CREATE (:Person {name: 'a'})")
            .unwrap()
            .is_write());
        assert!(!StatementTargets::of("CALL db.labels()").unwrap().is_write());
    }
}
