//! Write-clause builder: AST → LogicalPlan write variants.
//!
//! Converts parsed write-clause AST nodes (`CreateClause`, `SetClause`,
//! `DeleteClause`, `RemoveClause`) into the corresponding `LogicalPlan`
//! variants (`Create`, `SetProperties`, `Delete`, `Remove`).
//!
//! Per the embedded-writes design (Phase 1), this builder:
//! - Resolves node/relationship labels against the `GraphSchema`.
//! - Validates property names belong to the target schema.
//! - Rejects schemas with `source:` set (read-only source-backed targets).
//! - Rejects FK-edge relationships (out of scope for v1).
//!
//! Executor- and engine-level checks (e.g., "is this an embedded chdb
//! database?") happen later in `write_guard`.

use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    open_cypher_parser::ast::{
        ConnectedPattern, CreateClause, DeleteClause, Direction as AstDirection, Expression,
        NodePattern, Operator as AstOperator, OperatorApplication, PathPattern, Property,
        PropertyKVPair, RelationshipPattern, RemoveClause, SetClause,
    },
    query_planner::{
        logical_expr::{Direction, LogicalExpr},
        logical_plan::{
            errors::LogicalPlanError, generate_id, Create, CreateNode, CreatePattern, CreateRel,
            Delete, LogicalPlan, Remove, RemoveItem, SetItem, SetProperties, WriteProperty,
        },
    },
};

type Result<T> = std::result::Result<T, LogicalPlanError>;

/// Build a `LogicalPlan::Create` from a parsed `CreateClause`.
///
/// `input` is the preceding read pipeline (e.g., `MATCH (a) CREATE (b)`),
/// or `LogicalPlan::Empty` for standalone CREATE.
pub fn build_create(
    create: &CreateClause<'_>,
    input: Arc<LogicalPlan>,
    schema: &GraphSchema,
) -> Result<Arc<LogicalPlan>> {
    let mut patterns = Vec::new();
    // Aliases already bound by the read pipeline (`MATCH (x) CREATE (x)-[:R]->(y)`).
    // Endpoints whose name is in this set resolve to references rather than
    // synthesised CreateNodes; everything else (including a bare `(y)` that
    // isn't bound upstream) gets a fresh `__Unlabeled` CreateNode so the
    // emitted relationship's start/end aliases always have a corresponding
    // node pattern in the same CREATE.
    let mut bound_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
    collect_bound_aliases(&input, &mut bound_aliases);
    for path in &create.path_patterns {
        collect_patterns_from_path(path, schema, &mut patterns, &mut bound_aliases)?;
    }
    if patterns.is_empty() {
        return Err(LogicalPlanError::QueryPlanningError(
            "CREATE clause must specify at least one node or relationship".to_string(),
        ));
    }
    Ok(Arc::new(LogicalPlan::Create(Create { input, patterns })))
}

/// Collect every alias bound by `plan` (GraphNode / GraphRel aliases at any
/// depth). Used by CREATE-pattern endpoint resolution to distinguish a bare
/// `(name)` reference to an upstream binding from a fresh anonymous node.
fn collect_bound_aliases(plan: &LogicalPlan, out: &mut std::collections::HashSet<String>) {
    match plan {
        LogicalPlan::GraphNode(n) => {
            out.insert(n.alias.clone());
            collect_bound_aliases(&n.input, out);
        }
        LogicalPlan::GraphRel(r) => {
            out.insert(r.alias.clone());
            collect_bound_aliases(&r.left, out);
            collect_bound_aliases(&r.center, out);
            collect_bound_aliases(&r.right, out);
        }
        LogicalPlan::Filter(f) => collect_bound_aliases(&f.input, out),
        LogicalPlan::Projection(p) => collect_bound_aliases(&p.input, out),
        LogicalPlan::GroupBy(gb) => collect_bound_aliases(&gb.input, out),
        LogicalPlan::OrderBy(ob) => collect_bound_aliases(&ob.input, out),
        LogicalPlan::Skip(s) => collect_bound_aliases(&s.input, out),
        LogicalPlan::Limit(l) => collect_bound_aliases(&l.input, out),
        LogicalPlan::Cte(c) => collect_bound_aliases(&c.input, out),
        LogicalPlan::GraphJoins(gj) => collect_bound_aliases(&gj.input, out),
        LogicalPlan::Unwind(u) => collect_bound_aliases(&u.input, out),
        LogicalPlan::Union(u) => u.inputs.iter().for_each(|p| collect_bound_aliases(p, out)),
        LogicalPlan::CartesianProduct(cp) => {
            collect_bound_aliases(&cp.left, out);
            collect_bound_aliases(&cp.right, out);
        }
        LogicalPlan::WithClause(wc) => collect_bound_aliases(&wc.input, out),
        LogicalPlan::Create(c) => collect_bound_aliases(&c.input, out),
        LogicalPlan::SetProperties(sp) => collect_bound_aliases(&sp.input, out),
        LogicalPlan::Delete(d) => collect_bound_aliases(&d.input, out),
        LogicalPlan::Remove(r) => collect_bound_aliases(&r.input, out),
        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => {}
    }
}

/// Build a `LogicalPlan::SetProperties` from a parsed `SetClause`.
///
/// Property names are validated against the alias's schema when the alias
/// resolves to a statically-known label in `input`. Aliases whose label can
/// only be inferred later (e.g., bound across a WITH boundary) are accepted
/// without validation here; downstream passes catch them.
pub fn build_set(
    set: &SetClause<'_>,
    input: Arc<LogicalPlan>,
    schema: &GraphSchema,
) -> Result<Arc<LogicalPlan>> {
    let mut items = Vec::with_capacity(set.set_items.len());
    for op in &set.set_items {
        let item = set_item_from_op(op)?;
        validate_alias_property(&item.target_alias, &item.property, &input, schema, "SET")?;
        items.push(item);
    }
    Ok(Arc::new(LogicalPlan::SetProperties(SetProperties {
        input,
        items,
    })))
}

/// Build a `LogicalPlan::Delete` from a parsed `DeleteClause`.
///
/// Validates that each target alias is bound by `input` and (when its label
/// resolves statically) that the target schema is writable.
pub fn build_delete(
    del: &DeleteClause<'_>,
    input: Arc<LogicalPlan>,
    schema: &GraphSchema,
) -> Result<Arc<LogicalPlan>> {
    let mut targets = Vec::with_capacity(del.delete_items.len());
    for expr in &del.delete_items {
        match expr {
            Expression::Variable(name) => {
                validate_alias_writable(name, &input, schema, "DELETE")?;
                targets.push((*name).to_string());
            }
            other => {
                return Err(LogicalPlanError::QueryPlanningError(format!(
                    "DELETE only accepts bare variable references; got `{:?}`",
                    other
                )));
            }
        }
    }
    Ok(Arc::new(LogicalPlan::Delete(Delete {
        input,
        targets,
        detach: del.is_detach,
    })))
}

/// Build a `LogicalPlan::Remove` from a parsed `RemoveClause`.
///
/// Property validation works the same way as `build_set`.
pub fn build_remove(
    rem: &RemoveClause<'_>,
    input: Arc<LogicalPlan>,
    schema: &GraphSchema,
) -> Result<Arc<LogicalPlan>> {
    let mut items = Vec::with_capacity(rem.remove_items.len());
    for prop in &rem.remove_items {
        validate_alias_property(prop.base, prop.key, &input, schema, "REMOVE")?;
        items.push(RemoveItem {
            target_alias: prop.base.to_string(),
            property: prop.key.to_string(),
        });
    }
    Ok(Arc::new(LogicalPlan::Remove(Remove { input, items })))
}

// ---------------------------------------------------------------------------
// Alias / property validation for SET / DELETE / REMOVE
// ---------------------------------------------------------------------------

/// If `alias`'s label can be resolved from `input`, verify the schema is
/// writable (no `source:`, no FK-edge for relationships).
fn validate_alias_writable(
    alias: &str,
    input: &Arc<LogicalPlan>,
    schema: &GraphSchema,
    clause: &str,
) -> Result<()> {
    if let Some(label) = find_alias_label(alias, input) {
        if let Some(node_schema) = schema.node_schema_opt(&label) {
            if node_schema.source.is_some() {
                return Err(LogicalPlanError::InvalidSchema {
                    label,
                    reason: format!(
                        "label resolves to a source-backed (read-only) table; cannot {}",
                        clause
                    ),
                });
            }
        }
        return Ok(());
    }
    // Relationship aliases are bound by MATCH but don't carry a node label.
    // Defer to the render-plan builder (Phase 2) so it can emit the precise
    // rel-DELETE error message; here we just confirm the alias is bound.
    if find_alias_rel_type(alias, input).is_some() {
        return Ok(());
    }
    // Phase 5e: an untyped `MATCH (n)` binds `n` via a labelless GraphNode
    // at this stage — Union expansion across writable node tables happens
    // in a later analyzer pass. If we can find such a binding, defer the
    // writable-schema check to the renderer (which sees the expanded
    // Union and will validate each branch's schema). Without this, the
    // planner rejects every untyped MATCH+DELETE/SET/REMOVE before the
    // renderer ever gets a chance to fan out across labels.
    if alias_is_bound_anywhere(alias, input) {
        return Ok(());
    }
    Err(LogicalPlanError::QueryPlanningError(format!(
        "{} target `{}` is not bound by a preceding MATCH clause",
        clause, alias
    )))
}

/// Walk the plan tree looking for *any* `GraphNode` or `GraphRel` that
/// binds `alias`, regardless of whether a static label is attached.
/// Used by `validate_alias_writable` and `validate_alias_property` to
/// accept untyped `MATCH (n)` bindings that haven't been expanded into
/// per-label Union branches yet.
fn alias_is_bound_anywhere(alias: &str, plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphNode(n) if n.alias == alias => true,
        LogicalPlan::GraphRel(r) if r.alias == alias => true,
        LogicalPlan::GraphNode(n) => alias_is_bound_anywhere(alias, &n.input),
        LogicalPlan::GraphRel(r) => {
            alias_is_bound_anywhere(alias, &r.left)
                || alias_is_bound_anywhere(alias, &r.center)
                || alias_is_bound_anywhere(alias, &r.right)
        }
        LogicalPlan::Filter(f) => alias_is_bound_anywhere(alias, &f.input),
        LogicalPlan::Projection(p) => alias_is_bound_anywhere(alias, &p.input),
        LogicalPlan::GroupBy(gb) => alias_is_bound_anywhere(alias, &gb.input),
        LogicalPlan::OrderBy(ob) => alias_is_bound_anywhere(alias, &ob.input),
        LogicalPlan::Skip(s) => alias_is_bound_anywhere(alias, &s.input),
        LogicalPlan::Limit(l) => alias_is_bound_anywhere(alias, &l.input),
        LogicalPlan::Cte(c) => alias_is_bound_anywhere(alias, &c.input),
        LogicalPlan::GraphJoins(gj) => alias_is_bound_anywhere(alias, &gj.input),
        LogicalPlan::Unwind(u) => alias_is_bound_anywhere(alias, &u.input),
        LogicalPlan::Union(u) => u.inputs.iter().any(|p| alias_is_bound_anywhere(alias, p)),
        LogicalPlan::CartesianProduct(cp) => {
            alias_is_bound_anywhere(alias, &cp.left) || alias_is_bound_anywhere(alias, &cp.right)
        }
        LogicalPlan::WithClause(wc) => alias_is_bound_anywhere(alias, &wc.input),
        LogicalPlan::Create(c) => alias_is_bound_anywhere(alias, &c.input),
        LogicalPlan::SetProperties(sp) => alias_is_bound_anywhere(alias, &sp.input),
        LogicalPlan::Delete(d) => alias_is_bound_anywhere(alias, &d.input),
        LogicalPlan::Remove(r) => alias_is_bound_anywhere(alias, &r.input),
        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => false,
    }
}

/// Validate that `property` exists on `alias`'s schema (when statically
/// resolvable) and that the schema is writable.
fn validate_alias_property(
    alias: &str,
    property: &str,
    input: &Arc<LogicalPlan>,
    schema: &GraphSchema,
    clause: &str,
) -> Result<()> {
    let Some(label) = find_alias_label(alias, input) else {
        // Label unresolved at this point. Phase 5e: an untyped
        // `MATCH (n) SET n.k = …` is fine — `alias_is_bound_anywhere`
        // confirms `n` is bound by *some* GraphNode/GraphRel and the
        // renderer's per-label fan-out validates the property against
        // each resolved schema. But if the alias isn't bound anywhere,
        // fail fast with the same diagnostic `validate_alias_writable`
        // produces, instead of letting the renderer surface a less
        // direct "not bound" error after dispatch.
        if !alias_is_bound_anywhere(alias, input) {
            return Err(LogicalPlanError::QueryPlanningError(format!(
                "{} target `{}` is not bound by a preceding MATCH clause",
                clause, alias
            )));
        }
        return Ok(());
    };
    if let Some(node_schema) = schema.node_schema_opt(&label) {
        if node_schema.source.is_some() {
            return Err(LogicalPlanError::InvalidSchema {
                label,
                reason: format!(
                    "label resolves to a source-backed (read-only) table; cannot {}",
                    clause
                ),
            });
        }
        let known = node_schema.property_mappings.contains_key(property)
            || node_schema.column_names.iter().any(|c| c == property);
        if !known {
            return Err(LogicalPlanError::QueryPlanningError(format!(
                "property `{}` is not defined for node label `{}` ({} clause)",
                property, label, clause
            )));
        }
    }
    Ok(())
}

/// Walk the plan tree looking for a `GraphNode` or `GraphRel` whose alias
/// matches and which carries a static label. Returns `None` if the alias is
/// not found or its label is not statically resolvable here.
fn find_alias_label(alias: &str, plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) if node.alias == alias => node.label.clone(),
        LogicalPlan::GraphNode(node) => find_alias_label(alias, &node.input),
        LogicalPlan::GraphRel(rel) => find_alias_label(alias, &rel.left)
            .or_else(|| find_alias_label(alias, &rel.center))
            .or_else(|| find_alias_label(alias, &rel.right)),
        LogicalPlan::Filter(f) => find_alias_label(alias, &f.input),
        LogicalPlan::Projection(p) => find_alias_label(alias, &p.input),
        LogicalPlan::GroupBy(gb) => find_alias_label(alias, &gb.input),
        LogicalPlan::OrderBy(ob) => find_alias_label(alias, &ob.input),
        LogicalPlan::Skip(s) => find_alias_label(alias, &s.input),
        LogicalPlan::Limit(l) => find_alias_label(alias, &l.input),
        LogicalPlan::Cte(c) => find_alias_label(alias, &c.input),
        LogicalPlan::GraphJoins(gj) => find_alias_label(alias, &gj.input),
        LogicalPlan::Unwind(u) => find_alias_label(alias, &u.input),
        LogicalPlan::Union(u) => u.inputs.iter().find_map(|p| find_alias_label(alias, p)),
        LogicalPlan::CartesianProduct(cp) => {
            find_alias_label(alias, &cp.left).or_else(|| find_alias_label(alias, &cp.right))
        }
        LogicalPlan::WithClause(wc) => find_alias_label(alias, &wc.input),
        LogicalPlan::Create(c) => find_alias_label(alias, &c.input),
        LogicalPlan::SetProperties(sp) => find_alias_label(alias, &sp.input),
        LogicalPlan::Delete(d) => find_alias_label(alias, &d.input),
        LogicalPlan::Remove(r) => find_alias_label(alias, &r.input),
        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => None,
    }
}

/// Walk the plan tree looking for a `GraphRel` whose alias matches. Returns
/// the first declared relationship type if found.
fn find_alias_rel_type(alias: &str, plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(r) if r.alias == alias => {
            r.labels.as_ref().and_then(|l| l.first().cloned())
        }
        LogicalPlan::GraphRel(r) => find_alias_rel_type(alias, &r.left)
            .or_else(|| find_alias_rel_type(alias, &r.center))
            .or_else(|| find_alias_rel_type(alias, &r.right)),
        LogicalPlan::GraphNode(n) => find_alias_rel_type(alias, &n.input),
        LogicalPlan::Filter(f) => find_alias_rel_type(alias, &f.input),
        LogicalPlan::Projection(p) => find_alias_rel_type(alias, &p.input),
        LogicalPlan::GroupBy(gb) => find_alias_rel_type(alias, &gb.input),
        LogicalPlan::OrderBy(ob) => find_alias_rel_type(alias, &ob.input),
        LogicalPlan::Skip(s) => find_alias_rel_type(alias, &s.input),
        LogicalPlan::Limit(l) => find_alias_rel_type(alias, &l.input),
        LogicalPlan::Cte(c) => find_alias_rel_type(alias, &c.input),
        LogicalPlan::GraphJoins(gj) => find_alias_rel_type(alias, &gj.input),
        LogicalPlan::Unwind(u) => find_alias_rel_type(alias, &u.input),
        LogicalPlan::Union(u) => u.inputs.iter().find_map(|p| find_alias_rel_type(alias, p)),
        LogicalPlan::CartesianProduct(cp) => {
            find_alias_rel_type(alias, &cp.left).or_else(|| find_alias_rel_type(alias, &cp.right))
        }
        LogicalPlan::WithClause(wc) => find_alias_rel_type(alias, &wc.input),
        LogicalPlan::Create(c) => find_alias_rel_type(alias, &c.input),
        LogicalPlan::SetProperties(sp) => find_alias_rel_type(alias, &sp.input),
        LogicalPlan::Delete(d) => find_alias_rel_type(alias, &d.input),
        LogicalPlan::Remove(r) => find_alias_rel_type(alias, &r.input),
        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => None,
    }
}

// ---------------------------------------------------------------------------
// CREATE helpers
// ---------------------------------------------------------------------------

fn collect_patterns_from_path(
    path: &PathPattern<'_>,
    schema: &GraphSchema,
    out: &mut Vec<CreatePattern>,
    bound_aliases: &mut std::collections::HashSet<String>,
) -> Result<()> {
    match path {
        PathPattern::Node(node_pat) => {
            let node = create_node_from_pattern(node_pat, schema)?;
            if let Some(alias) = &node.alias {
                bound_aliases.insert(alias.clone());
            }
            out.push(CreatePattern::Node(node));
        }
        PathPattern::ConnectedPattern(connections) => {
            for conn in connections {
                collect_patterns_from_connection(conn, schema, out, bound_aliases)?;
            }
        }
        PathPattern::ShortestPath(_) | PathPattern::AllShortestPaths(_) => {
            return Err(LogicalPlanError::QueryPlanningError(
                "shortestPath / allShortestPaths are not allowed in CREATE".to_string(),
            ));
        }
    }
    Ok(())
}

fn collect_patterns_from_connection(
    conn: &ConnectedPattern<'_>,
    schema: &GraphSchema,
    out: &mut Vec<CreatePattern>,
    bound_aliases: &mut std::collections::HashSet<String>,
) -> Result<()> {
    let start_borrow = conn.start_node.borrow();
    let end_borrow = conn.end_node.borrow();

    // For each endpoint: if labeled or property-bearing, emit a CreateNode
    // (we're creating it). Bare `(name)` resolves to a reference if `name`
    // is already bound (by `input` or by an earlier pattern in the same
    // CREATE); otherwise we synthesise an `__Unlabeled` CreateNode so the
    // emitted relationship's start/end aliases always correspond to a node
    // pattern. Anonymous endpoints (no name, no label) get a fresh alias.
    let start_alias = endpoint_alias_or_create(&start_borrow, schema, out, bound_aliases)?;
    let end_alias = endpoint_alias_or_create(&end_borrow, schema, out, bound_aliases)?;

    let rel = create_rel_from_pattern(&conn.relationship, start_alias, end_alias, schema)?;
    // Note: standalone CREATE pattern is `start, rel, end` — the Rel pattern
    // is emitted *after* both endpoints so the planner can register endpoint
    // aliases before resolving the relationship.
    out.push(CreatePattern::Rel(rel));
    Ok(())
}

fn endpoint_alias_or_create(
    pat: &NodePattern<'_>,
    schema: &GraphSchema,
    out: &mut Vec<CreatePattern>,
    bound_aliases: &mut std::collections::HashSet<String>,
) -> Result<String> {
    // Bare reference (`(name)`, no labels, no props) to an alias that is
    // already bound — by the read pipeline or by an earlier CREATE pattern
    // in the same clause — resolves to a reference. No new node.
    if pat.labels.is_none() && pat.properties.is_none() {
        if let Some(name) = pat.name {
            if bound_aliases.contains(name) {
                return Ok(name.to_string());
            }
        }
    }
    // Otherwise we're creating a node. `create_node_from_pattern` defaults a
    // missing label to `__Unlabeled`. Reuse the user-supplied name as alias
    // when present (so `CREATE (root)-[:LINK]->(root)` binds `root` once and
    // the second occurrence falls into the bound-reference branch above);
    // otherwise synthesise an opaque id.
    let mut node = create_node_from_pattern(pat, schema)?;
    let alias = node.alias.clone().unwrap_or_else(generate_id);
    if node.alias.is_none() {
        node.alias = Some(alias.clone());
    }
    bound_aliases.insert(alias.clone());
    out.push(CreatePattern::Node(node));
    Ok(alias)
}

/// Sentinel label used when a CREATE pattern declares no label
/// (e.g. `CREATE ()` or `CREATE (n {prop: 1})`). The active schema must
/// register a writable node under this label for the CREATE to succeed —
/// production schemas typically don't, so the missing-schema error fires
/// just as it would for any other unknown label. Test harnesses (notably
/// `clickgraph-tck`) catalogue `__Unlabeled` automatically.
pub(crate) const UNLABELED_DEFAULT: &str = "__Unlabeled";

fn create_node_from_pattern(pat: &NodePattern<'_>, schema: &GraphSchema) -> Result<CreateNode> {
    let label = match pat.labels.as_ref() {
        None => UNLABELED_DEFAULT.to_string(),
        Some(labels) if labels.len() == 1 => labels[0].to_string(),
        Some(labels) => {
            return Err(LogicalPlanError::QueryPlanningError(format!(
                "CREATE node patterns must declare at most one label; got {:?}",
                labels
            )));
        }
    };

    let node_schema = schema.node_schema(&label).map_err(|_| {
        LogicalPlanError::NodeNotFound(format!(
            "label `{}` is not defined in the active graph schema",
            label
        ))
    })?;

    if node_schema.source.is_some() {
        return Err(LogicalPlanError::InvalidSchema {
            label: label.clone(),
            reason: "node label resolves to a source-backed (read-only) table".to_string(),
        });
    }

    let properties = build_write_properties(
        pat.properties.as_deref(),
        |key| {
            node_schema.property_mappings.contains_key(key)
                || node_schema.column_names.iter().any(|c| c == key)
        },
        &format!("node label `{}`", label),
    )?;

    Ok(CreateNode {
        alias: pat.name.map(|n| n.to_string()),
        label,
        properties,
    })
}

fn create_rel_from_pattern(
    rel: &RelationshipPattern<'_>,
    start_alias: String,
    end_alias: String,
    schema: &GraphSchema,
) -> Result<CreateRel> {
    let labels = rel.labels.as_ref().ok_or_else(|| {
        LogicalPlanError::QueryPlanningError(
            "CREATE relationships must specify a single type (e.g., -[:KNOWS]->)".to_string(),
        )
    })?;
    if labels.len() != 1 {
        return Err(LogicalPlanError::QueryPlanningError(format!(
            "CREATE relationship patterns must declare exactly one type; got {:?}",
            labels
        )));
    }
    let rel_type = labels[0].to_string();

    let rel_schema = schema
        .get_relationships_schema_opt(&rel_type)
        .ok_or_else(|| LogicalPlanError::RelationshipNotFound(rel_type.clone()))?;

    if rel_schema.source.is_some() {
        return Err(LogicalPlanError::InvalidSchema {
            label: rel_type.clone(),
            reason: "relationship resolves to a source-backed (read-only) table".to_string(),
        });
    }
    if rel_schema.is_fk_edge {
        return Err(LogicalPlanError::InvalidSchema {
            label: rel_type.clone(),
            reason:
                "FK-edge relationships are not writable in v1; declare a standard edge schema instead"
                    .to_string(),
        });
    }
    if rel.variable_length.is_some() {
        return Err(LogicalPlanError::QueryPlanningError(
            "variable-length paths are not allowed in CREATE".to_string(),
        ));
    }

    let direction = match rel.direction {
        AstDirection::Outgoing => Direction::Outgoing,
        AstDirection::Incoming => Direction::Incoming,
        AstDirection::Either => {
            return Err(LogicalPlanError::QueryPlanningError(
                "CREATE relationships must have an explicit direction".to_string(),
            ));
        }
    };

    let properties = build_write_properties(
        rel.properties.as_deref(),
        |key| {
            rel_schema.property_mappings.contains_key(key)
                || rel_schema.column_names.iter().any(|c| c == key)
        },
        &format!("relationship type `{}`", rel_type),
    )?;

    Ok(CreateRel {
        alias: rel.name.map(|n| n.to_string()),
        rel_type,
        direction,
        start_alias,
        end_alias,
        properties,
    })
}

fn build_write_properties<F>(
    props: Option<&[Property<'_>]>,
    is_known: F,
    target_desc: &str,
) -> Result<Vec<WriteProperty>>
where
    F: Fn(&str) -> bool,
{
    let mut out = Vec::new();
    let Some(props) = props else { return Ok(out) };
    for property in props {
        match property {
            Property::PropertyKV(PropertyKVPair { key, value }) => {
                if !is_known(key) {
                    return Err(LogicalPlanError::QueryPlanningError(format!(
                        "property `{}` is not defined for {}",
                        key, target_desc
                    )));
                }
                let logical = LogicalExpr::try_from(value.clone())?;
                out.push(WriteProperty {
                    key: (*key).to_string(),
                    value: logical,
                });
            }
            Property::Param(_) => {
                return Err(LogicalPlanError::FoundParamInProperties);
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// SET helpers
// ---------------------------------------------------------------------------

fn set_item_from_op(op: &OperatorApplication<'_>) -> Result<SetItem> {
    if op.operator != AstOperator::Equal || op.operands.len() != 2 {
        return Err(LogicalPlanError::QueryPlanningError(
            "SET items must be of the form `alias.property = expression`".to_string(),
        ));
    }
    let lhs = &op.operands[0];
    let rhs = &op.operands[1];

    let (alias, property) = match lhs {
        Expression::PropertyAccessExp(pa) => (pa.base.to_string(), pa.key.to_string()),
        other => {
            return Err(LogicalPlanError::QueryPlanningError(format!(
                "SET LHS must be a property access (e.g., `a.name`); got `{:?}`",
                other
            )));
        }
    };

    let value = LogicalExpr::try_from(rhs.clone())?;
    Ok(SetItem {
        target_alias: alias,
        property,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::Identifier;
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::graph_catalog::schema_types::SchemaType;
    use crate::graph_catalog::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema};
    use crate::open_cypher_parser::parse_query;
    use crate::query_planner::logical_plan::plan_builder::build_logical_plan;
    use std::collections::HashMap;

    fn person_node(name: &str) -> NodeSchema {
        NodeSchema {
            database: "test".to_string(),
            table_name: name.to_lowercase(),
            column_names: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), SchemaType::String),
            property_mappings: [
                ("id".to_string(), PropertyValue::Column("id".to_string())),
                (
                    "name".to_string(),
                    PropertyValue::Column("name".to_string()),
                ),
                ("age".to_string(), PropertyValue::Column("age".to_string())),
            ]
            .into_iter()
            .collect(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
            source: None,
            property_types: HashMap::new(),
            id_generation: None,
        }
    }

    fn knows_rel() -> RelationshipSchema {
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "knows".to_string(),
            column_names: vec!["from_id".to_string(), "to_id".to_string()],
            from_node: "Person".to_string(),
            to_node: "Person".to_string(),
            from_node_table: "person".to_string(),
            to_node_table: "person".to_string(),
            from_id: Identifier::from("from_id"),
            to_id: Identifier::from("to_id"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
            source: None,
            property_types: HashMap::new(),
        }
    }

    fn build_test_schema() -> GraphSchema {
        let mut nodes = HashMap::new();
        nodes.insert("Person".to_string(), person_node("Person"));

        let mut rels = HashMap::new();
        rels.insert("KNOWS::Person::Person".to_string(), knows_rel());

        GraphSchema::build(1, "test".to_string(), nodes, rels)
    }

    fn plan(cypher: &str) -> LogicalPlan {
        let ast = parse_query(cypher).expect("parse should succeed");
        let schema = build_test_schema();
        let (plan, _ctx) =
            build_logical_plan(&ast, &schema, None, None, None).expect("planning should succeed");
        Arc::try_unwrap(plan).unwrap_or_else(|arc| (*arc).clone())
    }

    fn plan_err(cypher: &str) -> LogicalPlanError {
        let ast = parse_query(cypher).expect("parse should succeed");
        let schema = build_test_schema();
        build_logical_plan(&ast, &schema, None, None, None).expect_err("planning should fail")
    }

    // ---------- Positive cases ----------

    #[test]
    fn create_standalone_node() {
        let p = plan("CREATE (a:Person {name: 'Alice', age: 30})");
        let create = match p {
            LogicalPlan::Create(c) => c,
            other => panic!("expected Create, got {:?}", other),
        };
        assert_eq!(create.patterns.len(), 1);
        match &create.patterns[0] {
            CreatePattern::Node(node) => {
                assert_eq!(node.label, "Person");
                assert_eq!(node.alias.as_deref(), Some("a"));
                assert_eq!(node.properties.len(), 2);
            }
            other => panic!("expected Node, got {:?}", other),
        }
        assert!(matches!(*create.input, LogicalPlan::Empty));
    }

    #[test]
    fn create_relationship_between_matched_nodes() {
        let p = plan("MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b)");
        let create = match p {
            LogicalPlan::Create(c) => c,
            other => panic!("expected Create, got {:?}", other),
        };
        // ConnectedPattern emits [start_node, rel, end_node]
        assert!(create
            .patterns
            .iter()
            .any(|p| matches!(p, CreatePattern::Rel(r) if r.rel_type == "KNOWS")));
        // input is the MATCH pipeline
        assert!(!matches!(*create.input, LogicalPlan::Empty));
    }

    #[test]
    fn set_property_after_match() {
        let p = plan("MATCH (a:Person) SET a.age = 30");
        let sp = match p {
            LogicalPlan::SetProperties(sp) => sp,
            other => panic!("expected SetProperties, got {:?}", other),
        };
        assert_eq!(sp.items.len(), 1);
        assert_eq!(sp.items[0].target_alias, "a");
        assert_eq!(sp.items[0].property, "age");
    }

    #[test]
    fn delete_simple() {
        let p = plan("MATCH (a:Person) DELETE a");
        let del = match p {
            LogicalPlan::Delete(d) => d,
            other => panic!("expected Delete, got {:?}", other),
        };
        assert_eq!(del.targets, vec!["a".to_string()]);
        assert!(!del.detach);
    }

    #[test]
    fn detach_delete_flag_propagates() {
        let p = plan("MATCH (a:Person) DETACH DELETE a");
        let del = match p {
            LogicalPlan::Delete(d) => d,
            other => panic!("expected Delete, got {:?}", other),
        };
        assert!(del.detach);
    }

    #[test]
    fn remove_property() {
        let p = plan("MATCH (a:Person) REMOVE a.age");
        let rem = match p {
            LogicalPlan::Remove(r) => r,
            other => panic!("expected Remove, got {:?}", other),
        };
        assert_eq!(rem.items.len(), 1);
        assert_eq!(rem.items[0].target_alias, "a");
        assert_eq!(rem.items[0].property, "age");
    }

    // ---------- Negative cases ----------

    #[test]
    fn create_unknown_label_rejected() {
        let err = plan_err("CREATE (a:Alien {name: 'x'})");
        assert!(
            matches!(&err, LogicalPlanError::NodeNotFound(_)),
            "got {:?}",
            err
        );
    }

    #[test]
    fn create_unknown_property_rejected() {
        let err = plan_err("CREATE (a:Person {nickname: 'x'})");
        let msg = err.to_string();
        assert!(msg.contains("nickname"), "got `{}`", msg);
    }

    #[test]
    fn create_unknown_relationship_rejected() {
        let err = plan_err("MATCH (a:Person), (b:Person) CREATE (a)-[:HATES]->(b)");
        assert!(
            matches!(&err, LogicalPlanError::RelationshipNotFound(_)),
            "got {:?}",
            err
        );
    }

    #[test]
    fn create_undirected_relationship_rejected() {
        let err = plan_err("MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]-(b)");
        let msg = err.to_string();
        assert!(msg.contains("explicit direction"), "got `{}`", msg);
    }

    #[test]
    fn create_unlabeled_node_without_unlabeled_schema_is_rejected() {
        // No `__Unlabeled` entry in `build_test_schema()`, so `CREATE ()`
        // hits the same NodeNotFound path as any other unknown label.
        let err = plan_err("CREATE ()");
        assert!(
            matches!(&err, LogicalPlanError::NodeNotFound(msg) if msg.contains("__Unlabeled")),
            "got {:?}",
            err
        );
    }

    #[test]
    fn create_unlabeled_node_dispatches_to_unlabeled_when_schema_has_it() {
        // When the active schema registers an `__Unlabeled` node, anonymous
        // CREATE patterns route there. This is what TCK harnesses opt into
        // via schema_gen — production schemas typically don't define it.
        let mut nodes = HashMap::new();
        nodes.insert("__Unlabeled".to_string(), person_node("Unlabeled"));
        let schema = GraphSchema::build(1, "test".to_string(), nodes, HashMap::new());

        let ast = parse_query("CREATE (), (a), (b {name: 'foo'})").expect("parse");
        let (plan, _ctx) =
            build_logical_plan(&ast, &schema, None, None, None).expect("planning succeeds");
        let create = match &*plan {
            LogicalPlan::Create(c) => c,
            other => panic!("expected Create, got {:?}", other),
        };
        assert_eq!(create.patterns.len(), 3);
        for pat in &create.patterns {
            match pat {
                CreatePattern::Node(n) => assert_eq!(n.label, "__Unlabeled"),
                other => panic!("expected only Node patterns, got {:?}", other),
            }
        }
    }

    #[test]
    fn create_against_source_backed_label_rejected() {
        let mut node = person_node("Source");
        node.source = Some("table_function:numbers(10)".to_string());
        let mut nodes = HashMap::new();
        nodes.insert("Source".to_string(), node);
        let schema = GraphSchema::build(1, "test".to_string(), nodes, HashMap::new());

        let ast = parse_query("CREATE (a:Source {name: 'x'})").expect("parse");
        let err = build_logical_plan(&ast, &schema, None, None, None).expect_err("must error");
        assert!(
            matches!(err, LogicalPlanError::InvalidSchema { .. }),
            "got {:?}",
            err
        );
    }

    #[test]
    fn create_against_fk_edge_rejected() {
        let mut nodes = HashMap::new();
        nodes.insert("Person".to_string(), person_node("Person"));

        let mut rel = knows_rel();
        rel.is_fk_edge = true;
        let mut rels = HashMap::new();
        rels.insert("KNOWS::Person::Person".to_string(), rel);

        let schema = GraphSchema::build(1, "test".to_string(), nodes, rels);

        let ast =
            parse_query("MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b)").expect("parse");
        let err = build_logical_plan(&ast, &schema, None, None, None).expect_err("must error");
        assert!(
            matches!(err, LogicalPlanError::InvalidSchema { .. }),
            "got {:?}",
            err
        );
    }

    // Phase 5b regression coverage: bare-name endpoints in a CREATE'd
    // relationship must synthesise an `__Unlabeled` CreateNode unless the
    // alias is already bound by `input` or by an earlier pattern in the
    // same CREATE — otherwise the emitted Rel points at aliases with no
    // node pattern.
    #[test]
    fn create_self_loop_synthesises_unlabeled_node_once() {
        let mut nodes = HashMap::new();
        nodes.insert("__Unlabeled".to_string(), person_node("Unlabeled"));
        let mut rels = HashMap::new();
        let mut self_rel = knows_rel();
        self_rel.from_node = "__Unlabeled".to_string();
        self_rel.to_node = "__Unlabeled".to_string();
        self_rel.from_node_table = "unlabeled".to_string();
        self_rel.to_node_table = "unlabeled".to_string();
        rels.insert("LINK::__Unlabeled::__Unlabeled".to_string(), self_rel);
        let schema = GraphSchema::build(1, "test".to_string(), nodes, rels);

        let ast = parse_query("CREATE (root)-[:LINK]->(root)").expect("parse");
        let (plan, _ctx) =
            build_logical_plan(&ast, &schema, None, None, None).expect("planning succeeds");
        let create = match &*plan {
            LogicalPlan::Create(c) => c,
            other => panic!("expected Create, got {:?}", other),
        };
        // Exactly one CreateNode for `root`, plus the relationship.
        let node_count = create
            .patterns
            .iter()
            .filter(|p| matches!(p, CreatePattern::Node(_)))
            .count();
        let rel_count = create
            .patterns
            .iter()
            .filter(|p| matches!(p, CreatePattern::Rel(_)))
            .count();
        assert_eq!(node_count, 1, "self-loop must bind `root` exactly once");
        assert_eq!(rel_count, 1);
        for p in &create.patterns {
            if let CreatePattern::Rel(r) = p {
                assert_eq!(r.start_alias, "root");
                assert_eq!(r.end_alias, "root");
            }
            if let CreatePattern::Node(n) = p {
                assert_eq!(n.alias.as_deref(), Some("root"));
                assert_eq!(n.label, "__Unlabeled");
            }
        }
    }

    #[test]
    fn create_with_matched_alias_does_not_resynthesise() {
        // `MATCH (a:Person) CREATE (a)-[:KNOWS]->(b)` — `a` is bound upstream,
        // so it must not produce a fresh CreateNode; `b` is unbound, so it
        // does (as `__Unlabeled` if no label). Built against a schema that
        // declares both Person and __Unlabeled as KNOWS endpoints.
        let mut nodes = HashMap::new();
        nodes.insert("Person".to_string(), person_node("Person"));
        nodes.insert("__Unlabeled".to_string(), person_node("Unlabeled"));
        let mut rels = HashMap::new();
        let mut p_to_u = knows_rel();
        p_to_u.to_node = "__Unlabeled".to_string();
        p_to_u.to_node_table = "unlabeled".to_string();
        rels.insert("KNOWS::Person::__Unlabeled".to_string(), p_to_u);
        let schema = GraphSchema::build(1, "test".to_string(), nodes, rels);

        let ast = parse_query("MATCH (a:Person) CREATE (a)-[:KNOWS]->(b)").expect("parse");
        let (plan, _ctx) =
            build_logical_plan(&ast, &schema, None, None, None).expect("planning succeeds");
        let create = match &*plan {
            LogicalPlan::Create(c) => c,
            other => panic!("expected Create, got {:?}", other),
        };
        let create_nodes: Vec<&CreateNode> = create
            .patterns
            .iter()
            .filter_map(|p| match p {
                CreatePattern::Node(n) => Some(n),
                _ => None,
            })
            .collect();
        // Only `b` should be synthesised — `a` came from MATCH.
        assert_eq!(create_nodes.len(), 1);
        assert_eq!(create_nodes[0].alias.as_deref(), Some("b"));
        assert_eq!(create_nodes[0].label, "__Unlabeled");
    }

    // Anonymous labelled endpoints get auto-generated aliases (PR #277 review).
    #[test]
    fn create_relationship_with_anonymous_endpoints() {
        let p = plan("CREATE (:Person {name: 'a'})-[:KNOWS]->(:Person {name: 'b'})");
        let create = match p {
            LogicalPlan::Create(c) => c,
            other => panic!("expected Create, got {:?}", other),
        };
        // Two anonymous Person nodes + the KNOWS rel = 3 patterns.
        assert_eq!(create.patterns.len(), 3);
        let aliases: Vec<String> = create
            .patterns
            .iter()
            .filter_map(|p| match p {
                CreatePattern::Node(n) => n.alias.clone(),
                _ => None,
            })
            .collect();
        assert_eq!(aliases.len(), 2);
        assert_ne!(aliases[0], aliases[1], "anonymous aliases must be distinct");

        let rel = create.patterns.iter().find_map(|p| match p {
            CreatePattern::Rel(r) => Some(r),
            _ => None,
        });
        let rel = rel.expect("Rel pattern present");
        assert_eq!(rel.start_alias, aliases[0]);
        assert_eq!(rel.end_alias, aliases[1]);
    }

    // SET / REMOVE now validate properties against the bound alias's schema.
    #[test]
    fn set_unknown_property_rejected() {
        let err = plan_err("MATCH (a:Person) SET a.nickname = 'x'");
        let msg = err.to_string();
        assert!(msg.contains("nickname"), "got `{}`", msg);
    }

    #[test]
    fn remove_unknown_property_rejected() {
        let err = plan_err("MATCH (a:Person) REMOVE a.nickname");
        let msg = err.to_string();
        assert!(msg.contains("nickname"), "got `{}`", msg);
    }

    #[test]
    fn set_against_source_backed_label_rejected() {
        let mut node = person_node("Source");
        node.source = Some("table_function:numbers(10)".to_string());
        node.column_names.push("payload".to_string());
        node.property_mappings.insert(
            "payload".to_string(),
            PropertyValue::Column("payload".to_string()),
        );
        let mut nodes = HashMap::new();
        nodes.insert("Source".to_string(), node);
        let schema = GraphSchema::build(1, "test".to_string(), nodes, HashMap::new());

        let ast = parse_query("MATCH (a:Source) SET a.payload = 1").expect("parse");
        let err = build_logical_plan(&ast, &schema, None, None, None).expect_err("must error");
        assert!(
            matches!(err, LogicalPlanError::InvalidSchema { .. }),
            "got {:?}",
            err
        );
    }

    #[test]
    fn delete_unbound_alias_rejected() {
        // Standalone DELETE with no MATCH — alias is not bound anywhere.
        // The parser routes this through a query AST with delete_clause set
        // but no reading clauses; build_delete must reject it.
        let err = plan_err("MATCH (a:Person) DELETE b");
        let msg = err.to_string();
        assert!(msg.contains("not bound"), "got `{}`", msg);
    }
}
