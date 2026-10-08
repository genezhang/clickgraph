//! The bound plan's graph values (`bound_plan::lower::value`): nodes,
//! relationships and paths in Neo4j's JSON form, as the SQL returns them,
//! decoded by their type ([`GraphType`]).
//!
//! * a node is `{"elementId", "labels", "properties"}`;
//! * a relationship is `{"elementId", "startNodeElementId",
//!   "endNodeElementId", "type", "properties"}`;
//! * a path is the list of its nodes and relationships in turn, starting and
//!   ending with a node.
//!
//! The legacy integer ids come from the session's [`IdMapper`], as for a
//! returned node or relationship.

use std::collections::HashMap;

use serde_json::Value;

use super::graph_objects::{encode_packstream_list, Node, Path, Relationship};
use super::id_mapper::IdMapper;
use crate::bound_plan::lower::GraphType;

fn field<'v>(v: &'v Value, key: &str) -> Result<&'v Value, String> {
    v.get(key)
        .ok_or_else(|| format!("graph value: no `{key}` in {v}"))
}

fn text(v: &Value, key: &str) -> Result<String, String> {
    field(v, key)?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| format!("graph value: `{key}` is not a string in {v}"))
}

fn properties(v: &Value) -> Result<HashMap<String, Value>, String> {
    match field(v, "properties")? {
        Value::Object(m) => Ok(m.iter().map(|(k, x)| (k.clone(), x.clone())).collect()),
        other => Err(format!("graph value: properties are not a map: {other}")),
    }
}

/// A node from its JSON form (legacy id 0).
pub(crate) fn node(v: &Value) -> Result<Node, String> {
    let labels = match field(v, "labels")? {
        Value::Array(ls) => ls
            .iter()
            .map(|l| {
                l.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| format!("graph value: a label is not a string in {v}"))
            })
            .collect::<Result<Vec<_>, _>>()?,
        other => return Err(format!("graph value: labels are not a list: {other}")),
    };
    Ok(Node::new(0, labels, properties(v)?, text(v, "elementId")?))
}

/// A relationship from its JSON form (legacy ids 0).
pub(crate) fn relationship(v: &Value) -> Result<Relationship, String> {
    Ok(Relationship::new(
        0,
        0,
        0,
        text(v, "type")?,
        properties(v)?,
        text(v, "elementId")?,
        text(v, "startNodeElementId")?,
        text(v, "endNodeElementId")?,
    ))
}

/// A path from its list of nodes and relationships in turn: its distinct
/// nodes and relationships, and for each step the relationship (1-based,
/// negative when the step goes against its direction) and the node it
/// reaches (0-based), as Bolt's Path structure has them.
pub(crate) fn path(v: &Value) -> Result<Path, String> {
    let Value::Array(seq) = v else {
        return Err(format!("graph value: a path is not a list: {v}"));
    };
    if seq.len() % 2 == 0 {
        return Err(format!("graph value: a path of {} elements", seq.len()));
    }
    let mut nodes: Vec<Node> = Vec::new();
    let mut rels: Vec<Relationship> = Vec::new();
    let mut indices: Vec<i64> = Vec::new();
    let node_at = |n: Node, nodes: &mut Vec<Node>| -> i64 {
        match nodes.iter().position(|x| x.element_id == n.element_id) {
            Some(i) => i as i64,
            None => {
                nodes.push(n);
                (nodes.len() - 1) as i64
            }
        }
    };
    let mut previous = node(&seq[0])?;
    node_at(previous.clone(), &mut nodes);
    for step in seq[1..].chunks(2) {
        let r = relationship(&step[0])?;
        let next = node(&step[1])?;
        let forward = r.start_node_element_id == previous.element_id;
        let at = match rels.iter().position(|x| x.element_id == r.element_id) {
            Some(i) => i as i64 + 1,
            None => {
                rels.push(r);
                rels.len() as i64
            }
        };
        indices.push(if forward { at } else { -at });
        indices.push(node_at(next.clone(), &mut nodes));
        previous = next;
    }
    Ok(Path::new(nodes, rels, indices))
}

fn assign_node(n: &mut Node, ids: &mut IdMapper) {
    n.id = ids.get_or_assign(&n.element_id);
}

fn assign_rel(r: &mut Relationship, ids: &mut IdMapper) {
    r.id = ids.get_or_assign(&r.element_id);
    r.start_node_id = ids.get_or_assign(&r.start_node_element_id);
    r.end_node_id = ids.get_or_assign(&r.end_node_element_id);
}

/// The Bolt PackStream bytes of a value of type `ty`.
pub(crate) fn packstream(v: &Value, ty: &GraphType, ids: &mut IdMapper) -> Result<Vec<u8>, String> {
    if v.is_null() {
        return Ok(vec![0xC0]);
    }
    Ok(match ty {
        GraphType::Node => {
            let mut n = node(v)?;
            assign_node(&mut n, ids);
            n.to_packstream()
        }
        GraphType::Relationship => {
            let mut r = relationship(v)?;
            assign_rel(&mut r, ids);
            r.to_packstream()
        }
        GraphType::Path => {
            let mut p = path(v)?;
            p.nodes.iter_mut().for_each(|n| assign_node(n, ids));
            p.relationships.iter_mut().for_each(|r| assign_rel(r, ids));
            p.to_packstream()
        }
        GraphType::List(item) => {
            let Value::Array(items) = v else {
                return Err(format!("graph value: a list is not a list: {v}"));
            };
            let encoded = items
                .iter()
                .map(|x| packstream(x, item, ids))
                .collect::<Result<Vec<_>, _>>()?;
            encode_packstream_list(&encoded)
        }
    })
}

/// Every node and relationship of a value of type `ty`, in order (the graph
/// output collects them).
pub fn elements(
    v: &Value,
    ty: &GraphType,
    nodes: &mut Vec<Node>,
    rels: &mut Vec<Relationship>,
) -> Result<(), String> {
    if v.is_null() {
        return Ok(());
    }
    match ty {
        GraphType::Node => nodes.push(node(v)?),
        GraphType::Relationship => rels.push(relationship(v)?),
        GraphType::Path => {
            let p = path(v)?;
            nodes.extend(p.nodes);
            rels.extend(p.relationships);
        }
        GraphType::List(item) => {
            let Value::Array(items) = v else {
                return Err(format!("graph value: a list is not a list: {v}"));
            };
            for x in items {
                elements(x, item, nodes, rels)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn n(id: u32) -> Value {
        json!({"elementId": format!("User:{id}-"), "labels": ["User"], "properties": {"user_id": id}})
    }

    fn r(from: u32, to: u32) -> Value {
        json!({
            "elementId": format!("FOLLOWS:{from}->{to}-"),
            "startNodeElementId": format!("User:{from}-"),
            "endNodeElementId": format!("User:{to}-"),
            "type": "FOLLOWS",
            "properties": {"since": 2020}
        })
    }

    #[test]
    fn a_path_has_its_distinct_nodes_and_signed_steps() {
        // 1 -> 2 <- 3 -> 1: the second relationship against the path, and
        // node 1 again at the end (a trail): nodes 1, 2, 3 once each.
        let p = path(&json!([n(1), r(1, 2), n(2), r(3, 2), n(3), r(3, 1), n(1)])).unwrap();
        let ids: Vec<&str> = p.nodes.iter().map(|x| x.element_id.as_str()).collect();
        assert_eq!(ids, ["User:1-", "User:2-", "User:3-"]);
        assert_eq!(p.relationships.len(), 3);
        assert_eq!(p.indices, vec![1, 1, -2, 2, 3, 0]);
        assert_eq!(p.nodes[0].properties["user_id"], json!(1));
        assert_eq!(p.relationships[0].rel_type, "FOLLOWS");
    }

    #[test]
    fn a_path_of_one_node_has_no_steps() {
        let p = path(&json!([n(7)])).unwrap();
        assert_eq!(p.nodes.len(), 1);
        assert!(p.relationships.is_empty() && p.indices.is_empty());
    }

    #[test]
    fn malformed_values_are_errors() {
        assert!(path(&json!([n(1), r(1, 2)])).is_err());
        assert!(node(&json!({"labels": ["User"], "properties": {}})).is_err());
        assert!(relationship(&n(1)).is_err());
    }

    #[test]
    fn a_list_collects_its_elements() {
        let (mut nodes, mut rels) = (Vec::new(), Vec::new());
        let ty = GraphType::List(Box::new(GraphType::Path));
        elements(
            &json!([[n(1), r(1, 2), n(2)], null]),
            &ty,
            &mut nodes,
            &mut rels,
        )
        .unwrap();
        assert_eq!((nodes.len(), rels.len()), (2, 1));
        assert_eq!(rels[0].start_node_element_id, "User:1-");
    }
}
