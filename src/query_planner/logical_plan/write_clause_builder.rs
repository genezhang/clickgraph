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
    for path in &create.path_patterns {
        collect_patterns_from_path(path, schema, &mut patterns)?;
    }
    if patterns.is_empty() {
        return Err(LogicalPlanError::QueryPlanningError(
            "CREATE clause must specify at least one node or relationship".to_string(),
        ));
    }
    Ok(Arc::new(LogicalPlan::Create(Create { input, patterns })))
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
    let Some(label) = find_alias_label(alias, input) else {
        // Alias not bound in this plan tree — caller must MATCH it first.
        return Err(LogicalPlanError::QueryPlanningError(format!(
            "{} target `{}` is not bound by a preceding MATCH clause",
            clause, alias
        )));
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
    }
    Ok(())
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
        // Label unresolved at this point — defer to downstream passes.
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

// ---------------------------------------------------------------------------
// CREATE helpers
// ---------------------------------------------------------------------------

fn collect_patterns_from_path(
    path: &PathPattern<'_>,
    schema: &GraphSchema,
    out: &mut Vec<CreatePattern>,
) -> Result<()> {
    match path {
        PathPattern::Node(node_pat) => {
            out.push(CreatePattern::Node(create_node_from_pattern(
                node_pat, schema,
            )?));
        }
        PathPattern::ConnectedPattern(connections) => {
            for conn in connections {
                collect_patterns_from_connection(conn, schema, out)?;
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
) -> Result<()> {
    let start_borrow = conn.start_node.borrow();
    let end_borrow = conn.end_node.borrow();

    // For each endpoint: if labeled, emit a CreateNode (we're creating it);
    // if it's a bare reference (`(a)` with no label), it must already be bound
    // by a prior MATCH/CREATE — record only its alias for the relationship.
    let start_alias = endpoint_alias_or_create(&start_borrow, schema, out, "start")?;
    let end_alias = endpoint_alias_or_create(&end_borrow, schema, out, "end")?;

    let rel = create_rel_from_pattern(&conn.relationship, start_alias, end_alias, schema)?;
    // Note: standalone CREATE pattern is `start, rel, end` — the Rel pattern
    // is emitted *after* both endpoints so the planner can register endpoint
    // aliases before resolving the relationship.
    out.push(CreatePattern::Rel(rel));
    Ok(())
}

/// Resolve a CREATE relationship endpoint to an alias. If the endpoint is
/// labeled, emit a `CreatePattern::Node` for it (we're creating a new node);
/// otherwise it must already be bound by a prior clause, so we only return its
/// alias. Anonymous labelled endpoints are valid Cypher — synthesise an
/// internal alias rather than rejecting them.
fn endpoint_alias_or_create(
    pat: &NodePattern<'_>,
    schema: &GraphSchema,
    out: &mut Vec<CreatePattern>,
    side: &str,
) -> Result<String> {
    if pat.labels.is_some() {
        let mut node = create_node_from_pattern(pat, schema)?;
        let alias = node.alias.clone().unwrap_or_else(generate_id);
        if node.alias.is_none() {
            node.alias = Some(alias.clone());
        }
        out.push(CreatePattern::Node(node));
        Ok(alias)
    } else {
        let name = pat.name.ok_or_else(|| {
            LogicalPlanError::QueryPlanningError(format!(
                "CREATE {} endpoint must be either a labelled node or a reference to a bound alias",
                side
            ))
        })?;
        Ok(name.to_string())
    }
}

fn create_node_from_pattern(pat: &NodePattern<'_>, schema: &GraphSchema) -> Result<CreateNode> {
    let labels = pat.labels.as_ref().ok_or_else(|| {
        LogicalPlanError::QueryPlanningError(
            "CREATE requires every node to specify a label (e.g., (a:Person {...}))".to_string(),
        )
    })?;
    if labels.len() != 1 {
        return Err(LogicalPlanError::QueryPlanningError(format!(
            "CREATE node patterns must declare exactly one label; got {:?}",
            labels
        )));
    }
    let label = labels[0].to_string();

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
    fn create_node_without_label_rejected() {
        let err = plan_err("CREATE (a)");
        let msg = err.to_string();
        assert!(msg.contains("label"), "got `{}`", msg);
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
