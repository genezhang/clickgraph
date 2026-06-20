//! Native-function pass-through — dialect-keyed policy.
//!
//! ClickGraph lets a Cypher query reach a backend's native SQL functions
//! that have no Cypher built-in, via a dotted prefix on the function name
//! (dot notation chosen for Neo4j-ecosystem consistency with `apoc.*` /
//! `gds.*`). Each dialect owns its prefixes and its notion of which
//! functions are aggregates:
//!
//! | Dialect    | Scalar prefix | Aggregate prefix | Aggregate detection        |
//! |------------|---------------|------------------|----------------------------|
//! | ClickHouse | `ch.`         | `chagg.`         | registry, `chagg.` override |
//! | Databricks | `dbx.`        | — (none)         | registry only               |
//!
//! ## One prefix vs two — why they differ
//! The original ClickHouse pass-through shipped *before* an aggregate
//! registry existed, so users had to declare intent: `ch.` for scalars,
//! `chagg.` for aggregates. A registry removes that need — the system
//! looks the type up — so Databricks ships with a single `dbx.` prefix
//! and a [`databricks::SPARK_AGGREGATE_FUNCTIONS`] registry. ClickHouse
//! keeps `chagg.` as an escape hatch because CH's aggregate surface is
//! effectively unbounded (combinators: `-If`, `-Array`, `-State`,
//! `-Merge`, parametric…) and can't be fully enumerated; Spark's is
//! bounded, so `dbx.` alone suffices and a missing function is fixed by
//! extending the registry, not by teaching users a second prefix.
//!
//! ## Two entry points, two stages
//! - [`classify_passthrough`] runs at **plan time** (scalar vs aggregate
//!   decides GROUP BY) and is deliberately **dialect-agnostic**: it
//!   recognizes every dialect's prefixes so the plan is shaped correctly
//!   regardless of which backend ultimately runs. The dialect isn't
//!   reliably set this early.
//! - [`strip_passthrough`] runs at **emit time**, where the active
//!   dialect *is* set (task-local [`QueryContext`]). It strips the active
//!   dialect's prefix and **rejects a foreign prefix** (e.g. `ch.` on the
//!   Databricks backend) with a helpful error — closing the gap where a
//!   `ch.` name used to leak verbatim into Spark SQL.
//!
//! Mirrors the sibling [`function_mapper`] module's `for_dialect` shape.
//!
//! [`QueryContext`]: crate::server::query_context::QueryContext
//! [`function_mapper`]: crate::sql_generator::function_mapper

pub(crate) mod clickhouse;
pub(crate) mod databricks;

use crate::sql_generator::SqlDialect;

/// Whether a pass-through call is a scalar or an aggregate function — the
/// distinction the planner needs to decide GROUP BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PassthroughKind {
    Scalar,
    Aggregate,
}

/// Why a pass-through name could not be resolved against the active dialect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PassthroughError {
    /// A prefix from a *different* backend was used (e.g. `ch.` while the
    /// active backend is Databricks). Carries a ready-to-surface message.
    WrongBackend(String),
    /// The prefix was present but no function name followed it (e.g. `ch.`).
    EmptyName(String),
}

impl std::fmt::Display for PassthroughError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PassthroughError::WrongBackend(m) | PassthroughError::EmptyName(m) => f.write_str(m),
        }
    }
}

/// Per-dialect pass-through rules. `pub(crate)` for the same reason as
/// [`FunctionMapper`](crate::sql_generator::function_mapper::FunctionMapper):
/// external code can't name it, so it can't implement it.
pub(crate) trait PassthroughPolicy: Send + Sync {
    /// Prefix for scalar (and registry-detected aggregate) pass-through.
    fn scalar_prefix(&self) -> &'static str;

    /// Prefix that *forces* aggregate treatment regardless of the
    /// registry. `None` for dialects whose registry is authoritative.
    fn agg_prefix(&self) -> Option<&'static str>;

    /// Whether `stripped` (the bare function name, prefix removed) names a
    /// known aggregate in this dialect.
    fn is_aggregate(&self, stripped: &str) -> bool;
}

/// Dialects that have a pass-through policy. Kept in sync with the
/// `match` in [`for_dialect`]; used by [`classify_passthrough`] and the
/// foreign-prefix scan in [`strip_passthrough`].
const POLICY_DIALECTS: &[SqlDialect] = &[SqlDialect::ClickHouse, SqlDialect::Databricks];

/// Returns the pass-through policy for an explicit dialect, or `None` for a
/// dialect that has no policy yet. Returning `None` (rather than panicking)
/// keeps this safe on the per-function-call hot path: dialects without a
/// pass-through policy also have no SQL emitter, so they fail earlier at
/// `emitter_for` — here they simply mean "no pass-through handling".
fn for_dialect(dialect: SqlDialect) -> Option<&'static dyn PassthroughPolicy> {
    static CLICKHOUSE: clickhouse::ClickhousePassthrough = clickhouse::ClickhousePassthrough;
    static DATABRICKS: databricks::DatabricksPassthrough = databricks::DatabricksPassthrough;
    match dialect {
        SqlDialect::ClickHouse => Some(&CLICKHOUSE),
        SqlDialect::Databricks => Some(&DATABRICKS),
        _ => None,
    }
}

/// Strip a prefix and require a non-empty remainder. Returns `None` when
/// `name` doesn't start with `prefix` at all.
fn strip_nonempty<'a>(name: &'a str, prefix: &str) -> Option<Result<&'a str, ()>> {
    name.strip_prefix(prefix)
        .map(|rest| if rest.is_empty() { Err(()) } else { Ok(rest) })
}

/// Plan-time classification: is `name` a pass-through call, and if so is it
/// scalar or aggregate? **Dialect-agnostic on purpose** — see module docs.
///
/// Returns `None` for a plain (non-prefixed) function or a bare prefix with
/// no function name (the empty-name case is reported later, at emit time).
pub(crate) fn classify_passthrough(name: &str) -> Option<PassthroughKind> {
    for &dialect in POLICY_DIALECTS {
        let Some(policy) = for_dialect(dialect) else {
            continue;
        };

        // Explicit aggregate prefix wins outright.
        if let Some(agg_prefix) = policy.agg_prefix() {
            if let Some(Ok(_)) = strip_nonempty(name, agg_prefix) {
                return Some(PassthroughKind::Aggregate);
            }
        }

        // Scalar prefix: registry decides scalar vs aggregate.
        if let Some(Ok(rest)) = strip_nonempty(name, policy.scalar_prefix()) {
            return Some(if policy.is_aggregate(rest) {
                PassthroughKind::Aggregate
            } else {
                PassthroughKind::Scalar
            });
        }
    }
    None
}

/// Emit-time resolution against `dialect`.
///
/// - `Ok(Some(bare))` — a pass-through for the active dialect; `bare` is the
///   prefix-stripped function name to emit verbatim.
/// - `Ok(None)` — not a pass-through name at all (caller falls through to
///   normal function mapping).
/// - `Err(WrongBackend)` — a *foreign* dialect's prefix was used; the
///   message names the right one for the active backend.
/// - `Err(EmptyName)` — a prefix with no function name.
pub(crate) fn strip_passthrough(
    name: &str,
    dialect: SqlDialect,
) -> Result<Option<&str>, PassthroughError> {
    // A dialect without a pass-through policy gets no pass-through handling.
    let Some(active) = for_dialect(dialect) else {
        return Ok(None);
    };

    if let Some(agg_prefix) = active.agg_prefix() {
        if let Some(res) = strip_nonempty(name, agg_prefix) {
            return res.map(Some).map_err(|()| empty_name_err(name, agg_prefix));
        }
    }
    if let Some(res) = strip_nonempty(name, active.scalar_prefix()) {
        return res
            .map(Some)
            .map_err(|()| empty_name_err(name, active.scalar_prefix()));
    }

    // Not the active dialect's prefix — is it another backend's? If so,
    // reject with a pointer to the correct prefix rather than letting the
    // foreign name leak into the generated SQL.
    for &other in POLICY_DIALECTS {
        if other == dialect {
            continue;
        }
        let Some(foreign) = for_dialect(other) else {
            continue;
        };
        let hits = name.starts_with(foreign.scalar_prefix())
            || foreign.agg_prefix().is_some_and(|p| name.starts_with(p));
        if hits {
            return Err(PassthroughError::WrongBackend(format!(
                "'{name}' uses the {} pass-through prefix, which is not available on the {} backend. \
                 Use the '{}' prefix instead.",
                other.as_str(),
                dialect.as_str(),
                active.scalar_prefix(),
            )));
        }
    }

    Ok(None)
}

fn empty_name_err(name: &str, prefix: &str) -> PassthroughError {
    PassthroughError::EmptyName(format!(
        "pass-through prefix '{prefix}' requires a function name (e.g. '{prefix}myFunc'); got '{name}'"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_clickhouse_prefixes() {
        // ch. + registry aggregate
        assert_eq!(
            classify_passthrough("ch.uniq"),
            Some(PassthroughKind::Aggregate)
        );
        // ch. + non-aggregate (scalar)
        assert_eq!(
            classify_passthrough("ch.cityHash64"),
            Some(PassthroughKind::Scalar)
        );
        // chagg. forces aggregate even for unknown names
        assert_eq!(
            classify_passthrough("chagg.myCustomAgg"),
            Some(PassthroughKind::Aggregate)
        );
    }

    #[test]
    fn classify_databricks_prefix() {
        // dbx. + Spark registry aggregate
        assert_eq!(
            classify_passthrough("dbx.percentile_approx"),
            Some(PassthroughKind::Aggregate)
        );
        assert_eq!(
            classify_passthrough("dbx.collect_list"),
            Some(PassthroughKind::Aggregate)
        );
        // dbx. + scalar
        assert_eq!(
            classify_passthrough("dbx.get_json_object"),
            Some(PassthroughKind::Scalar)
        );
    }

    #[test]
    fn classify_non_passthrough_is_none() {
        assert_eq!(classify_passthrough("count"), None);
        assert_eq!(classify_passthrough("upper"), None);
        // bare prefix, no function name -> not classified here (caught at emit)
        assert_eq!(classify_passthrough("ch."), None);
        assert_eq!(classify_passthrough("dbx."), None);
        // dbxagg. is NOT a Databricks prefix (single-prefix design)
        assert_eq!(classify_passthrough("dbxagg.foo"), None);
    }

    #[test]
    fn strip_active_dialect_prefix() {
        assert_eq!(
            strip_passthrough("ch.cityHash64", SqlDialect::ClickHouse),
            Ok(Some("cityHash64"))
        );
        assert_eq!(
            strip_passthrough("chagg.myAgg", SqlDialect::ClickHouse),
            Ok(Some("myAgg"))
        );
        assert_eq!(
            strip_passthrough("dbx.percentile_approx", SqlDialect::Databricks),
            Ok(Some("percentile_approx"))
        );
    }

    #[test]
    fn strip_plain_function_is_none() {
        assert_eq!(strip_passthrough("count", SqlDialect::ClickHouse), Ok(None));
        assert_eq!(strip_passthrough("upper", SqlDialect::Databricks), Ok(None));
    }

    #[test]
    fn strip_rejects_foreign_prefix() {
        // ch. on the Databricks backend -> rejected, points at dbx.
        let err = strip_passthrough("ch.uniq", SqlDialect::Databricks).unwrap_err();
        match err {
            PassthroughError::WrongBackend(m) => {
                assert!(m.contains("clickhouse"), "{m}");
                assert!(m.contains("databricks"), "{m}");
                assert!(m.contains("dbx."), "{m}");
            }
            other => panic!("expected WrongBackend, got {other:?}"),
        }
        // chagg. on Databricks too
        assert!(matches!(
            strip_passthrough("chagg.myAgg", SqlDialect::Databricks),
            Err(PassthroughError::WrongBackend(_))
        ));
        // dbx. on the ClickHouse backend -> rejected, points at ch.
        let err = strip_passthrough("dbx.collect_list", SqlDialect::ClickHouse).unwrap_err();
        match err {
            PassthroughError::WrongBackend(m) => assert!(m.contains("ch."), "{m}"),
            other => panic!("expected WrongBackend, got {other:?}"),
        }
    }

    #[test]
    fn strip_rejects_empty_name() {
        assert!(matches!(
            strip_passthrough("ch.", SqlDialect::ClickHouse),
            Err(PassthroughError::EmptyName(_))
        ));
        assert!(matches!(
            strip_passthrough("dbx.", SqlDialect::Databricks),
            Err(PassthroughError::EmptyName(_))
        ));
    }
}
