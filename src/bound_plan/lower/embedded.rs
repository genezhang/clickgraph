//! Nodes embedded in edge tables (P-4c S8b, `docs/design/EXPLICIT_SCOPE.md`
//! §4.6 "Implemented in S8b").
//!
//! A label whose nodes are denormalized into edge tables has no table of its
//! own: its nodes are the distinct ids its roles hold. The lowering reads
//! the schema's view in which such a label is an own-table node over a node
//! relation (`GraphSchema::with_node_relations`), whose table is a CTE of
//! the query: every scan, tie, union, walk and value then reads it as it
//! reads a node table. This module builds that CTE, once per query, when a
//! node of the label is first read ([`Lowerer::node_table`]).

use std::sync::Arc;

use crate::graph_catalog::graph_schema::{NodeRelation, NodeSchema};
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::render_expr::{Literal, Operator, OperatorApplication, RenderExpr};
use crate::render_plan::{
    Cte, CteContent, FilterItems, FromTableItem, GroupByExpressions, RenderPlan, SelectItems,
    Union, UnionItems, UnionType, ViewTableRef,
};
use crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
use crate::sql_generator::function_mapper::current_function_mapper;

use super::{col_at, empty_plan, select, unsupported, At, LowerError, Lowerer, Scan};
use crate::bound_plan::types::VarId;

/// The relation's rows, one per row of each source, before grouping.
const ROWS: &str = "__cg_rows";

impl<'s> Lowerer<'s> {
    /// Read node `v`, an end of relationship `r` (its `from` end when
    /// `from_end`), from `r`'s row when its label is embedded there
    /// (EXPLICIT_SCOPE §4.6 node-scan elision): its identity is the row's
    /// end columns and each property it is read for the row's column of it,
    /// so its node relation is not joined. That holds when
    ///
    /// * `v` is a node of the clause read from its relation (not one from an
    ///   earlier clause, nor one already read from another relationship's
    ///   row, nor an end of a variable-length relationship of the clause;
    ///   only the first relationship of the clause it is an end of, whose
    ///   tie reads it first: the caller checks these), and `r` one
    ///   definition read from its table;
    /// * a source of the relation is `r`'s table at that end: the same id
    ///   columns, no `filter:` or view parameters, `r`'s FINAL. Its row then
    ///   holds a node there is, whose properties (a node's, the schema says)
    ///   are its columns;
    /// * `v` is read for properties that source holds only (not as a whole
    ///   node or a list's element: those read the relation).
    ///
    /// A row whose end is NULL is no relationship (and holds no node): such
    /// rows are filtered out, as the join to the relation would drop them.
    pub(super) fn embed_end(
        &mut self,
        r: VarId,
        v: VarId,
        from_end: bool,
    ) -> Result<(), LowerError> {
        if self.elided.contains_key(&v) {
            return Ok(());
        }
        let Some(Scan::Node {
            schema: ns,
            label,
            at: At::Table(alias),
        }) = self.scans.get(&v)
        else {
            return Ok(());
        };
        if *alias != v.name() {
            return Ok(());
        }
        let Some(relation) = self.schema.node_relation(&ns.full_table_name()) else {
            return Ok(());
        };
        let Some(Scan::Rel {
            schema: rs,
            at: At::Table(r_alias),
            both: None,
            ..
        }) = self.scans.get(&r)
        else {
            return Ok(());
        };
        let end = if from_end { &rs.from_id } else { &rs.to_id };
        let end: Vec<&str> = end.columns();
        let Some(source) = relation.sources.iter().find(|s| {
            s.table == rs.full_table_name()
                && s.filter.is_none()
                && s.view_parameters.is_none()
                && s.use_final == rs.should_use_final()
                && relation.id.len() == end.len()
                && relation
                    .id
                    .iter()
                    .zip(&end)
                    .all(|(p, c)| s.columns.iter().any(|(q, col)| q == p && col == c))
        }) else {
            return Ok(());
        };
        let held = |p: &str| source.columns.iter().find(|(q, _)| q == p).map(|(_, c)| c);
        let read = self.demand.get(&v).cloned().unwrap_or_default();
        if read.iter().any(|p| held(p).is_none()) {
            return Ok(()); // a whole node, a list's element, or another source's property
        }
        let r_alias = r_alias.clone();
        let end: Vec<String> = end.iter().map(|c| c.to_string()).collect();
        let physical = relation
            .id
            .iter()
            .zip(&end)
            .map(|(p, c)| (p.clone(), c.clone()))
            .collect();
        let props = read
            .iter()
            .filter_map(|p| held(p).map(|c| (p.clone(), col_at(&r_alias, c))))
            .collect();
        let scan = Scan::Node {
            schema: ns,
            label: label.clone(),
            at: At::Exported {
                alias: r_alias.clone(),
                physical,
                props,
            },
        };
        for c in &end {
            self.filters
                .push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::IsNotNull,
                    operands: vec![col_at(&r_alias, c)],
                }));
        }
        self.scans.insert(v, scan);
        self.elided.insert(v, r);
        Ok(())
    }

    /// The table `ns`'s nodes are read from (its own, or its node relation's
    /// CTE, added to the query's CTEs when first read).
    pub(super) fn node_table(&mut self, ns: &NodeSchema) -> Result<String, LowerError> {
        let name = ns.full_table_name();
        if let Some(relation) = self.schema.node_relation(&name) {
            if !self.ctes.iter().any(|c| c.cte_name == name) {
                self.node_relation_ctes(&name, relation)?;
            }
        }
        Ok(name)
    }

    /// The CTEs of `relation`, read as `name`:
    ///
    /// * `{name}__cg_rows`: one row per row of each source (its table with
    ///   its view parameters, FINAL and `filter:`), its id under the id's
    ///   property names, and each other property `p` its source holds as
    ///   `p__cg{k}` (source `k`; NULL in other sources' rows), so that each
    ///   source's column keeps its own type;
    /// * `{name}`: one row per non-NULL id, each property the value of its
    ///   sources' columns (`any`: the rows of one id hold one value), read
    ///   through `FunctionMapper::one_type_guard` when several sources hold
    ///   it (on each row, before `any`): an error unless their types differ
    ///   only in what changes no value.
    fn node_relation_ctes(
        &mut self,
        name: &str,
        relation: &NodeRelation,
    ) -> Result<(), LowerError> {
        const ROW: &str = "e";
        let rows = format!("{name}{ROWS}");
        let held = |p: &str| -> Vec<usize> {
            relation
                .sources
                .iter()
                .enumerate()
                .filter(|(_, s)| s.columns.iter().any(|(q, _)| q == p))
                .map(|(k, _)| k)
                .collect()
        };
        let others: Vec<&String> = relation
            .properties
            .iter()
            .filter(|p| !relation.id.contains(p))
            .collect();
        let mut input = Vec::new();
        for (k, source) in relation.sources.iter().enumerate() {
            let column = |p: &str| {
                source
                    .columns
                    .iter()
                    .find(|(q, _)| q == p)
                    .map(|(_, c)| col_at(ROW, c))
            };
            let mut items = Vec::new();
            for p in &relation.id {
                let Some(c) = column(p) else {
                    return unsupported(format!("internal: a source of {name} without its id"));
                };
                items.push(select(c, p));
            }
            for p in &others {
                for j in held(p) {
                    let value = match j == k {
                        true => column(p).unwrap_or(RenderExpr::Literal(Literal::Null)),
                        false => RenderExpr::Literal(Literal::Null),
                    };
                    items.push(select(value, &format!("{p}__cg{j}")));
                }
            }
            let filters = match &source.filter {
                Some(f) => match f.to_sql(ROW) {
                    Ok(sql) => Some(RenderExpr::Raw(format!("({sql})"))),
                    Err(e) => return unsupported(format!("schema filter: {e}")),
                },
                None => None,
            };
            let table = ViewTableRef::parameterized_name(
                &source.table,
                source.view_parameters.as_deref(),
                self.options.view_parameter_values.as_ref(),
            );
            input.push(RenderPlan {
                select: SelectItems {
                    items,
                    distinct: false,
                },
                from: FromTableItem(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: table,
                    alias: Some(ROW.to_string()),
                    use_final: source.use_final,
                })),
                filters: FilterItems(filters),
                ..empty_plan()
            });
        }
        if self.ctes.iter().any(|c| c.cte_name == rows) {
            return unsupported(format!("internal: {rows} without {name}"));
        }
        self.ctes.push(Cte::new(
            rows.clone(),
            CteContent::Structured(Box::new(RenderPlan {
                union: UnionItems(Some(Union {
                    input,
                    union_type: UnionType::All,
                    is_cypher_union: false,
                })),
                ..empty_plan()
            })),
            false,
        ));

        let mapper = current_function_mapper();
        let any = mapper.any();
        let Some(spelling) = mapper.unwind() else {
            return unsupported("a node embedded in edge tables in this SQL dialect");
        };
        let ids: Vec<RenderExpr> = relation.id.iter().map(|p| col_at(ROW, p)).collect();
        let mut items: Vec<_> = relation
            .id
            .iter()
            .zip(&ids)
            .map(|(p, c)| select(c.clone(), p))
            .collect();
        for p in &others {
            let columns: Vec<String> = held(p)
                .into_iter()
                .map(|j| render_expr_to_sql_plain(&col_at(ROW, &format!("{p}__cg{j}"))))
                .collect();
            // The guard reads the columns' types (constant over a column, not
            // over an aggregate), so it is applied before `any`.
            let value =
                match columns.as_slice() {
                    [one] => one.clone(),
                    _ => match mapper.one_type_guard(&columns) {
                        Some(v) => v,
                        None => return unsupported(
                            "a property of a node embedded in several sources in this SQL dialect",
                        ),
                    },
                };
            items.push(select(RenderExpr::Raw(format!("{any}({value})")), p));
        }
        let not_null: Vec<String> = ids
            .iter()
            .map(|c| format!("NOT {}", (spelling.is_null)(&render_expr_to_sql_plain(c))))
            .collect();
        self.ctes.push(Cte::new(
            name.to_string(),
            CteContent::Structured(Box::new(RenderPlan {
                select: SelectItems {
                    items,
                    distinct: false,
                },
                from: FromTableItem(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: rows,
                    alias: Some(ROW.to_string()),
                    use_final: false,
                })),
                filters: FilterItems(Some(RenderExpr::Raw(not_null.join(" AND ")))),
                group_by: GroupByExpressions(ids),
                ..empty_plan()
            })),
            false,
        ));
        Ok(())
    }
}
