//! ClickHouse pass-through policy.
//!
//! Two prefixes, for historical reasons (the pass-through predates the
//! aggregate registry — see the parent module docs):
//! - `ch.` — scalar, or a registry-detected aggregate (`ch.uniq` →
//!   aggregate because `uniq` is in [`CH_AGGREGATE_FUNCTIONS`]).
//! - `chagg.` — forces aggregate treatment for *any* name, the escape
//!   hatch for CH's unbounded combinator aggregates (`-If`, `-Array`,
//!   `-State`, `-Merge`, parametric) that a static registry can't cover.
//!
//! The prefix constants, the registry, and the `is_ch_*` helpers remain
//! defined in `function_translator` (where they're tested and used by the
//! CH emitter); this policy delegates to them so there is a single source
//! of truth.

use super::PassthroughPolicy;
use crate::clickhouse_query_generator::{
    is_ch_aggregate_function, CH_AGG_PREFIX, CH_PASSTHROUGH_PREFIX,
};

pub(crate) struct ClickhousePassthrough;

impl PassthroughPolicy for ClickhousePassthrough {
    fn scalar_prefix(&self) -> &'static str {
        CH_PASSTHROUGH_PREFIX // "ch."
    }

    fn agg_prefix(&self) -> Option<&'static str> {
        Some(CH_AGG_PREFIX) // "chagg."
    }

    fn is_aggregate(&self, stripped: &str) -> bool {
        is_ch_aggregate_function(stripped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes() {
        let p = ClickhousePassthrough;
        assert_eq!(p.scalar_prefix(), "ch.");
        assert_eq!(p.agg_prefix(), Some("chagg."));
    }

    #[test]
    fn registry_backed_aggregate_detection() {
        let p = ClickhousePassthrough;
        assert!(p.is_aggregate("uniq"));
        assert!(p.is_aggregate("quantile"));
        assert!(!p.is_aggregate("cityHash64"));
        assert!(!p.is_aggregate("upper"));
    }
}
