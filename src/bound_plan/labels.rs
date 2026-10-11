//! Label inference over one clause's pattern (P-4c S3, `EXPLICIT_SCOPE.md` §4.7).
//!
//! Constraint propagation to a fixed point: a node's label set shrinks to the
//! labels some feasible relationship of an adjacent pattern element allows at
//! its end, and a relationship's type set shrinks to the types that have a
//! feasible (from-label, type, to-label) combination. Feasibility comes from
//! the schema catalog only.
//!
//! * A variable bound before the clause brings its label set as a fixed input;
//!   the clause never narrows it (in an OPTIONAL MATCH that would drop input
//!   rows). Only variables the clause introduces are narrowed.
//! * A variable-length segment propagates through the reachable label sets for
//!   its hop range; `*0..` lets the end be the start.
//! * An unknown label or type is simply an empty set: the pattern matches
//!   nothing (Cypher returns no rows, not an error).

use std::collections::{BTreeMap, BTreeSet};

use crate::graph_catalog::graph_schema::GraphSchema;

use super::types::RelDirection;

/// (type, from-label, to-label) triples the schema defines. A polymorphic
/// endpoint (`$any`) stands for every node label.
pub(crate) struct Feasibility {
    pub all_labels: BTreeSet<String>,
    pub all_types: BTreeSet<String>,
    triples: Vec<(String, String, String)>,
}

impl Feasibility {
    pub fn from_schema(schema: &GraphSchema) -> Self {
        let all_labels: BTreeSet<String> = schema.all_node_schemas().keys().cloned().collect();
        let all_types: BTreeSet<String> = schema.get_rel_type_index().keys().cloned().collect();
        let mut triples = Vec::new();
        // Every definition by its key (`TYPE::FROM::TO`, or `TYPE` for a
        // polymorphic one): the type index hides a polymorphic definition of
        // a type that also has others, which the lowering reads (S8c).
        for (key, rel) in schema.get_relationships_schemas() {
            let t = key.split("::").next().unwrap_or(key);
            if !all_types.contains(t) {
                continue;
            }
            let froms = expand_any(&rel.from_node, rel.from_label_values.as_ref(), &all_labels);
            let tos = expand_any(&rel.to_node, rel.to_label_values.as_ref(), &all_labels);
            for f in &froms {
                for to in &tos {
                    triples.push((t.to_string(), f.clone(), to.clone()));
                }
            }
        }
        triples.sort();
        triples.dedup();
        Feasibility {
            all_labels,
            all_types,
            triples,
        }
    }
}

/// The labels an end written `label` can be: every label for `$any`, or the
/// ones the schema closes it to (`from_label_values` / `to_label_values`,
/// as the lowering's definitions read them:
/// `GraphSchema::with_discriminators_as_filters`).
fn expand_any(label: &str, values: Option<&Vec<String>>, all: &BTreeSet<String>) -> Vec<String> {
    if label == "$any" || label.is_empty() {
        all.iter()
            .filter(|l| values.is_none_or(|vs| vs.contains(l)))
            .cloned()
            .collect()
    } else {
        vec![label.to_string()]
    }
}

/// One relationship of the clause, between node slots `left` and `right`.
pub(crate) struct RelSlot {
    pub left: usize,
    pub right: usize,
    pub direction: RelDirection,
    pub length: Option<(u32, Option<u32>)>,
    /// Narrowed in place (unless `fixed`).
    pub types: BTreeSet<String>,
    pub fixed: bool,
}

/// One node of the clause.
pub(crate) struct NodeSlot {
    pub labels: BTreeSet<String>,
    pub fixed: bool,
}

/// Narrow `nodes` and `rels` to a fixed point.
///
/// Every update is an intersection with the current set, so the sets only
/// shrink and the loop terminates.
pub(crate) fn infer(f: &Feasibility, nodes: &mut [NodeSlot], rels: &mut [RelSlot]) {
    loop {
        let mut changed = false;
        for r in rels.iter_mut() {
            let (l, rr) = (r.left, r.right);
            let left = nodes[l].labels.clone();
            let right = nodes[rr].labels.clone();
            let (mut new_left, mut new_right, new_types) = match r.length {
                // A closed hop `(a)-[r]->(a)`: one node is both ends, so only
                // triples whose two ends carry the same label can match.
                None if l == rr => closed_single_hop(f, &left, &r.types, r.direction),
                None => single_hop(f, &left, &right, &r.types, r.direction),
                Some((min, max)) => multi_hop(f, &left, &right, &r.types, r.direction, min, max),
            };
            if l == rr {
                // A closed variable-length segment: the node needs a label
                // feasible at both ends.
                new_left = new_left.intersection(&new_right).cloned().collect();
                new_right = new_left.clone();
            }
            let new_left: BTreeSet<String> = new_left.intersection(&left).cloned().collect();
            let new_right: BTreeSet<String> = new_right.intersection(&right).cloned().collect();
            let new_types: BTreeSet<String> = new_types.intersection(&r.types).cloned().collect();
            if !nodes[l].fixed && new_left != nodes[l].labels {
                nodes[l].labels = new_left;
                changed = true;
            }
            if !nodes[rr].fixed && new_right != nodes[rr].labels {
                nodes[rr].labels = new_right;
                changed = true;
            }
            if !r.fixed && new_types != r.types {
                r.types = new_types;
                changed = true;
            }
        }
        if !changed {
            return;
        }
    }
}

/// Triples usable in direction `dir` between `left` and `right`, as
/// (type, left-label, right-label).
fn oriented<'f>(
    f: &'f Feasibility,
    types: &'f BTreeSet<String>,
    dir: RelDirection,
) -> impl Iterator<Item = (&'f str, &'f str, &'f str)> + 'f {
    f.triples
        .iter()
        .filter(move |(t, _, _)| types.contains(t))
        .flat_map(move |(t, from, to)| {
            let forward = (t.as_str(), from.as_str(), to.as_str());
            let backward = (t.as_str(), to.as_str(), from.as_str());
            match dir {
                RelDirection::Right => vec![forward],
                RelDirection::Left => vec![backward],
                RelDirection::Either => vec![forward, backward],
            }
        })
}

fn single_hop(
    f: &Feasibility,
    left: &BTreeSet<String>,
    right: &BTreeSet<String>,
    types: &BTreeSet<String>,
    dir: RelDirection,
) -> (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>) {
    let (mut l, mut r, mut t) = (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    for (ty, a, b) in oriented(f, types, dir) {
        if left.contains(a) && right.contains(b) {
            l.insert(a.to_string());
            r.insert(b.to_string());
            t.insert(ty.to_string());
        }
    }
    (l, r, t)
}

fn closed_single_hop(
    f: &Feasibility,
    labels: &BTreeSet<String>,
    types: &BTreeSet<String>,
    dir: RelDirection,
) -> (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>) {
    let (mut l, mut t) = (BTreeSet::new(), BTreeSet::new());
    for (ty, a, b) in oriented(f, types, dir) {
        if a == b && labels.contains(a) {
            l.insert(a.to_string());
            t.insert(ty.to_string());
        }
    }
    (l.clone(), l, t)
}

/// Labels reachable from `from` in k hops for k in [min, max] (max None:
/// unbounded), following `step`.
fn reachable(
    from: &BTreeSet<String>,
    step: &BTreeMap<String, BTreeSet<String>>,
    min: u32,
    max: Option<u32>,
) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    if min == 0 {
        result.extend(from.iter().cloned());
    }
    let mut frontier = from.clone();
    // Each step is a function of the previous frontier alone, and frontiers
    // are subsets of a finite label universe. Once a frontier seen at a step
    // k >= min comes back, every later frontier is one already seen (and
    // already added to `result`), so the search can stop.
    let mut seen: BTreeSet<BTreeSet<String>> = BTreeSet::new();
    let mut before_min: BTreeMap<BTreeSet<String>, u32> = BTreeMap::new();
    let cap = max.unwrap_or(u32::MAX);
    let mut k = 0u32;
    while k < cap {
        k += 1;
        frontier = frontier
            .iter()
            .flat_map(|l| step.get(l).into_iter().flatten().cloned())
            .collect();
        if frontier.is_empty() {
            break;
        }
        if k < min {
            // Before `min` only the frontier matters: on a repeat, skip
            // whole periods to just below `min`.
            if let Some(&j) = before_min.get(&frontier) {
                let period = k - j;
                k += (min - 1 - k) / period * period;
            } else {
                before_min.insert(frontier.clone(), k);
            }
            continue;
        }
        result.extend(frontier.iter().cloned());
        if !seen.insert(frontier.clone()) {
            break;
        }
    }
    result
}

fn multi_hop(
    f: &Feasibility,
    left: &BTreeSet<String>,
    right: &BTreeSet<String>,
    types: &BTreeSet<String>,
    dir: RelDirection,
    min: u32,
    max: Option<u32>,
) -> (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>) {
    let mut fwd: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut bwd: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut used_types = BTreeSet::new();
    for (ty, a, b) in oriented(f, types, dir) {
        fwd.entry(a.to_string()).or_default().insert(b.to_string());
        bwd.entry(b.to_string()).or_default().insert(a.to_string());
        used_types.insert(ty.to_string());
    }
    let right_reach: BTreeSet<String> = reachable(left, &fwd, min, max);
    let left_reach: BTreeSet<String> = reachable(right, &bwd, min, max);
    let new_left: BTreeSet<String> = left.intersection(&left_reach).cloned().collect();
    let new_right: BTreeSet<String> = right.intersection(&right_reach).cloned().collect();
    // A segment with no feasible endpoint pair matches nothing (min >= 1);
    // keep the type set of the feasible edges, or empty.
    let types = if new_left.is_empty() || new_right.is_empty() {
        BTreeSet::new()
    } else {
        used_types
    };
    (new_left, new_right, types)
}
