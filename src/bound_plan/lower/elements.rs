//! Lists of nodes and relationships (P-4c S7e2a,
//! `docs/design/EXPLICIT_SCOPE.md` §4.4 "Implemented in S7e2a").
//!
//! An element of such a list is a tuple of its columns, typed as its table
//! has them: a node's id and its label's declared properties (in name
//! order), a relationship's stored ends' ids, its `edge_id` (if any) and its
//! type's declared properties ([`Layout`]). Every element of one list is of
//! one label, or of one type between one label pair ([`Elem`]), so the list
//! is an array of one tuple type. A property of an element is its slot (it
//! keeps its type: it can be compared, sorted, aggregated); its identity is
//! its id slots; a NULL element (an OPTIONAL MATCH's) has a NULL id.
//! Returned, a list is mapped to the elements' values (`value.rs`), a WITH
//! carries the tuples.

use crate::graph_catalog::graph_schema::{NodeSchema, RelationshipSchema};
use crate::graph_catalog::schema_types::SchemaType;
use crate::render_plan::render_expr::{Literal, RenderExpr, ScalarFnCall};
use crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
use crate::sql_generator::function_mapper::{current_function_mapper, GraphValues};

use super::unwind::Kind;
use super::value::GraphType;
use super::{unsupported, LowerError, Lowerer, Scan};
use crate::bound_plan::types::VarId;
use crate::query_planner::logical_expr::LogicalExpr;

/// What the elements of a list are: nodes of one label, or relationships of
/// one type between one label pair (one definition).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Elem {
    Node(String),
    Rel {
        rel_type: String,
        from: String,
        to: String,
    },
}

/// The slots of an element's tuple.
pub(super) struct Layout<'s> {
    pub elem: Elem,
    pub node: Option<&'s NodeSchema>,
    pub rel: Option<&'s RelationshipSchema>,
    /// Its declared properties, in name order, after its id slots.
    pub props: Vec<String>,
}

impl Layout<'_> {
    /// The slots before the properties: a node's id; a relationship's from
    /// and to ids, then its `edge_id`.
    fn leading(&self) -> usize {
        match (&self.elem, self.rel) {
            (Elem::Rel { .. }, Some(r)) if r.edge_id.is_some() => 3,
            (Elem::Rel { .. }, _) => 2,
            (Elem::Node(_), _) => 1,
        }
    }

    /// The slots of its identity (0-based): a node's id; a relationship's
    /// `edge_id`, else its ends (#887).
    pub fn identity_slots(&self) -> Vec<usize> {
        match (&self.elem, self.rel) {
            (Elem::Rel { .. }, Some(r)) if r.edge_id.is_some() => vec![2],
            (Elem::Rel { .. }, _) => vec![0, 1],
            (Elem::Node(_), _) => vec![0],
        }
    }

    pub fn prop_slot(&self, prop: &str) -> Option<usize> {
        self.props
            .iter()
            .position(|p| p == prop)
            .map(|i| i + self.leading())
    }
}

/// The layout of a node of `label` (`schema`).
pub(super) fn node_layout<'s>(
    label: &str,
    schema: &'s NodeSchema,
) -> Result<Layout<'s>, LowerError> {
    single(
        schema
            .id_physical_columns()
            .iter()
            .map(String::as_str)
            .collect(),
        "a node",
    )?;
    let mut props: Vec<String> = schema.property_mappings.keys().cloned().collect();
    props.sort();
    Ok(Layout {
        elem: Elem::Node(label.to_string()),
        node: Some(schema),
        rel: None,
        props,
    })
}

/// The layout of a relationship of `rel_type` (definition `schema`).
pub(super) fn rel_layout<'s>(
    rel_type: &str,
    schema: &'s RelationshipSchema,
) -> Result<Layout<'s>, LowerError> {
    single(schema.from_id.columns(), "a relationship's end")?;
    single(schema.to_id.columns(), "a relationship's end")?;
    if let Some(id) = &schema.edge_id {
        single(id.columns(), "a relationship")?;
    }
    let mut props: Vec<String> = schema.property_mappings.keys().cloned().collect();
    props.sort();
    Ok(Layout {
        elem: Elem::Rel {
            rel_type: rel_type.to_string(),
            from: schema.from_node.clone(),
            to: schema.to_node.clone(),
        },
        node: None,
        rel: Some(schema),
        props,
    })
}

/// Slot `i` (0-based) of the tuple `t`.
pub(super) fn slot(t: &RenderExpr, i: usize) -> Result<RenderExpr, LowerError> {
    let Some(spelling) = current_function_mapper().lists() else {
        return unsupported("a list of nodes or relationships in this SQL dialect");
    };
    Ok(RenderExpr::Raw((spelling.element)(
        &render_expr_to_sql_plain(t),
        i + 1,
    )))
}

fn single(cols: Vec<&str>, what: &str) -> Result<String, LowerError> {
    match cols.as_slice() {
        [one] => Ok(one.to_string()),
        _ => unsupported(format!("{what} of a composite id in a list (S8)")),
    }
}

/// A table's columns under `alias` as an element tuple of `layout` (inside
/// a walk, `path.rs`): as [`Lowerer::elem_tuple`] builds it from a scan.
fn table_tuple(layout: &Layout, alias: &str) -> Result<String, LowerError> {
    let col = |c: &str| render_expr_to_sql_plain(&super::col_at(alias, c));
    let mut fields = Vec::new();
    let (mappings, types) = match (layout.node, layout.rel) {
        (Some(n), _) => {
            fields.push(col(&single(
                n.id_physical_columns().iter().map(String::as_str).collect(),
                "a node",
            )?));
            (&n.property_mappings, &n.property_types)
        }
        (_, Some(r)) => {
            fields.push(col(&single(r.from_id.columns(), "a relationship's end")?));
            fields.push(col(&single(r.to_id.columns(), "a relationship's end")?));
            if let Some(id) = &r.edge_id {
                fields.push(col(&single(id.columns(), "a relationship")?));
            }
            (&r.property_mappings, &r.property_types)
        }
        _ => return unsupported("internal: an element layout of no schema"),
    };
    for prop in &layout.props {
        let Some(pv) = mappings.get(prop) else {
            return unsupported(format!("internal: {prop} is not declared"));
        };
        let e = render_expr_to_sql_plain(&RenderExpr::PropertyAccessExp(
            crate::render_plan::render_expr::PropertyAccess {
                table_alias: crate::render_plan::render_expr::TableAlias(alias.to_string()),
                column: pv.clone(),
            },
        ));
        fields.push(if types.get(prop) == Some(&SchemaType::Boolean) {
            current_function_mapper().cast_bool(&e)
        } else {
            e
        });
    }
    Ok(format!(
        "{}({})",
        current_function_mapper().tuple_constructor(),
        fields.join(", ")
    ))
}

/// A walk's element tuples (`path.rs`): one over `alias`, and an empty list
/// of them of their type (a recursive CTE's first rows fix its columns'
/// types: `[]` would be `Array(Nothing)`).
#[derive(Clone)]
pub(super) struct WalkTuples {
    pub one: String,
    pub other: Option<String>,
    pub empty: String,
}

/// The tuples of `elem`'s elements over `alias` (and `other_alias`), with
/// the empty list of them, read from `table`.
pub(super) fn walk_tuples(
    layout: &Layout,
    table: &str,
    alias: &str,
    other_alias: Option<&str>,
) -> Result<WalkTuples, LowerError> {
    const NONE: &str = "__cg_none";
    let Some(spelling) = current_function_mapper().lists() else {
        return unsupported("a list of nodes or relationships in this SQL dialect");
    };
    Ok(WalkTuples {
        one: table_tuple(layout, alias)?,
        other: other_alias.map(|a| table_tuple(layout, a)).transpose()?,
        empty: (spelling.empty_of)(&table_tuple(layout, NONE)?, table, NONE),
    })
}

impl<'s> Lowerer<'s> {
    /// What a node or relationship of the current relation is as a list's
    /// element (`None`: of several labels or types, or no table).
    pub(super) fn elem_of(&self, v: VarId) -> Option<Elem> {
        match self.scans.get(&v)? {
            Scan::Node { label, .. } => Some(Elem::Node(label.clone())),
            Scan::Rel {
                schema, rel_type, ..
            } => Some(Elem::Rel {
                rel_type: rel_type.clone(),
                from: schema.from_node.clone(),
                to: schema.to_node.clone(),
            }),
            _ => None,
        }
    }

    /// The tuple layout of `elem`'s elements.
    pub(super) fn layout(&self, elem: &Elem) -> Result<Layout<'s>, LowerError> {
        match elem {
            Elem::Node(label) => match self.schema.node_schema_opt(label) {
                Some(schema) => node_layout(label, schema),
                None => unsupported(format!("label {label} has no node schema")),
            },
            Elem::Rel { rel_type, from, to } => {
                match self
                    .schema
                    .rel_schemas_for_type(rel_type)
                    .into_iter()
                    .find(|s| s.from_node == *from && s.to_node == *to)
                {
                    Some(schema) => rel_layout(rel_type, schema),
                    None => unsupported(format!("type {rel_type} has no schema")),
                }
            }
        }
    }

    /// Node or relationship `v` of the current relation as a list's element:
    /// the tuple of its columns.
    pub(super) fn elem_tuple(&self, v: VarId) -> Result<RenderExpr, LowerError> {
        let Some(elem) = self.elem_of(v) else {
            return unsupported(
                "a node or relationship of several possible labels or types in a list",
            );
        };
        let layout = self.layout(&elem)?;
        let mut fields = Vec::new();
        match (&elem, layout.rel) {
            (Elem::Node(_), _) => {
                let Some(id) = self.identity(v)? else {
                    return Ok(RenderExpr::Literal(Literal::Null));
                };
                fields.extend(id);
            }
            (Elem::Rel { .. }, Some(r)) => {
                fields.push(self.physical(v, &single(r.from_id.columns(), "")?)?);
                fields.push(self.physical(v, &single(r.to_id.columns(), "")?)?);
                if let Some(id) = &r.edge_id {
                    fields.push(self.physical(v, &single(id.columns(), "")?)?);
                }
            }
            (Elem::Rel { .. }, None) => return unsupported("internal: a relationship layout"),
        }
        // A property the schema declares boolean on a column that holds it as
        // an integer is cast, as a returned column is: it shows a boolean.
        let types = layout
            .node
            .map(|n| &n.property_types)
            .or(layout.rel.map(|r| &r.property_types));
        for p in &layout.props {
            let e = self.property(v, p)?;
            let boolean = types.and_then(|t| t.get(p)) == Some(&SchemaType::Boolean);
            fields.push(if boolean && !matches!(e, RenderExpr::Literal(_)) {
                RenderExpr::Raw(current_function_mapper().cast_bool(&render_expr_to_sql_plain(&e)))
            } else {
                e
            });
        }
        Ok(RenderExpr::ScalarFnCall(ScalarFnCall {
            name: current_function_mapper().tuple_constructor().to_string(),
            args: fields,
        }))
    }

    /// The identity of the element tuple `t`.
    pub(super) fn elem_identity(
        &self,
        t: &RenderExpr,
        elem: &Elem,
    ) -> Result<Vec<RenderExpr>, LowerError> {
        let layout = self.layout(elem)?;
        layout
            .identity_slots()
            .into_iter()
            .map(|i| slot(t, i))
            .collect()
    }

    /// Property `prop` of the element tuple `t`: its slot; an undeclared
    /// property is NULL where the element's properties are complete (or in
    /// Neo4j-compat mode), and otherwise not lowered (the tuple carries
    /// declared properties only).
    pub(super) fn elem_property(
        &self,
        t: &RenderExpr,
        elem: &Elem,
        prop: &str,
    ) -> Result<RenderExpr, LowerError> {
        let layout = self.layout(elem)?;
        if let Some(i) = layout.prop_slot(prop) {
            return slot(t, i);
        }
        let closed = match (layout.node, layout.rel) {
            (Some(n), _) => n.closed_properties,
            (_, Some(r)) => r.closed_properties,
            _ => false,
        };
        if closed || self.options.neo4j_compat {
            return Ok(RenderExpr::Literal(Literal::Null));
        }
        unsupported("an undeclared property of a list's node or relationship")
    }

    /// The value (`value.rs`) of the element tuple whose SQL is `t`: NULL
    /// when its id is.
    pub(super) fn elem_value(
        &self,
        g: &GraphValues,
        t: &str,
        elem: &Elem,
    ) -> Result<String, LowerError> {
        let layout = self.layout(elem)?;
        let Some(spelling) = current_function_mapper().lists() else {
            return unsupported("a list of nodes or relationships in this SQL dialect");
        };
        let at = |i: usize| (spelling.element)(t, i + 1);
        let props: Vec<(String, String)> = layout
            .props
            .iter()
            .map(|p| (p.clone(), at(layout.prop_slot(p).unwrap_or_default())))
            .collect();
        let object = match &elem {
            Elem::Node(label) => super::value::node_object(g, label, &at(0), &props),
            Elem::Rel { rel_type, from, to } => {
                super::value::rel_object(g, rel_type, from, to, &at(0), &at(1), &props)
            }
        };
        let first = at(layout.identity_slots()[0]);
        Ok((g.null_if)(&(spelling.is_null)(&first), &object))
    }

    /// When `item` (lowered to `e`) is a list's node or relationship, or a
    /// list of them: their graph type, and the SQL of their value.
    pub(super) fn elements_value(
        &self,
        item: &LogicalExpr,
        e: &RenderExpr,
    ) -> Result<Option<(GraphType, String)>, LowerError> {
        let ty = |elem: &Elem| match elem {
            Elem::Node(_) => GraphType::Node,
            Elem::Rel { .. } => GraphType::Relationship,
        };
        match self.kind(item) {
            Kind::Element(elem) => {
                let g = super::value::spelling()?;
                let value = self.elem_value(&g, &render_expr_to_sql_plain(e), &elem)?;
                Ok(Some((ty(&elem), value)))
            }
            Kind::List(k) => match *k {
                Kind::Element(elem) => {
                    let g = super::value::spelling()?;
                    let Some(spelling) = current_function_mapper().lists() else {
                        return unsupported("a list of nodes or relationships in this SQL dialect");
                    };
                    let x = "__cg_e";
                    let value = (spelling.map)(
                        x,
                        &self.elem_value(&g, x, &elem)?,
                        &render_expr_to_sql_plain(e),
                    );
                    Ok(Some((GraphType::List(Box::new(ty(&elem))), value)))
                }
                k if self.kinds_hold_elements(&k) => {
                    unsupported("a list of lists of nodes or relationships")
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    fn kinds_hold_elements(&self, k: &Kind) -> bool {
        match k {
            Kind::Element(_) => true,
            Kind::List(k) => self.kinds_hold_elements(k),
            _ => false,
        }
    }

    /// `nodes(p)` / `relationships(p)` (`f`, [`Lowerer::path_elem`]) as a
    /// list of tuples, in path order: the fixed elements' tuples and the
    /// walks' carried ones.
    pub(super) fn path_tuples(
        &self,
        f: &crate::query_planner::logical_expr::ScalarFnCall,
    ) -> Result<RenderExpr, LowerError> {
        let Some((p, nodes)) = self.path_part(f) else {
            return unsupported("internal: not a path's nodes or relationships");
        };
        let Some(elements) = self.paths.get(&p).cloned() else {
            return unsupported(format!("internal: path {p} has no elements"));
        };
        let g = super::value::spelling()?;
        let mut lists: Vec<String> = Vec::new();
        let mut items: Vec<RenderExpr> = Vec::new();
        if nodes {
            items.push(self.elem_tuple(elements.nodes[0])?);
        }
        for (i, r) in elements.rels.iter().enumerate() {
            match self.scans.get(r) {
                Some(Scan::Path { reversed, .. }) => {
                    if !items.is_empty() {
                        lists.push(render_expr_to_sql_plain(&RenderExpr::List(std::mem::take(
                            &mut items,
                        ))));
                    }
                    let mut list = self.walk_tuples_of(*r, nodes, *reversed)?;
                    if nodes {
                        list = (g.tail)(&list);
                    }
                    lists.push(list);
                }
                _ if nodes => items.push(self.elem_tuple(elements.nodes[i + 1])?),
                _ => items.push(self.elem_tuple(*r)?),
            }
        }
        if !items.is_empty() || lists.is_empty() {
            lists.push(render_expr_to_sql_plain(&RenderExpr::List(items)));
        }
        Ok(RenderExpr::Raw((g.concat)(&lists)))
    }

    /// A walk's carried node (`nodes`) or relationship tuples, in path order.
    pub(super) fn walk_tuples_of(
        &self,
        r: VarId,
        nodes: bool,
        reversed: bool,
    ) -> Result<String, LowerError> {
        let column = super::path::TUPLE_COLUMNS[if nodes { 0 } else { 1 }];
        let list = render_expr_to_sql_plain(&self.physical(r, column)?);
        Ok(if reversed {
            (super::value::spelling()?.reverse)(&list)
        } else {
            list
        })
    }
}
