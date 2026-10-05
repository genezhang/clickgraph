//! WITH export contract (`docs/design/WITH_EXPORT_CONTRACT.md`, P-4b).
//!
//! One answer to "which CTE column(s) hold the identity of a name a WITH CTE
//! exports". It is derived from what the CTE actually emits: the alias's Cypher
//! property → CTE column map plus the CTE's SELECT column names. It never
//! reconstructs a column from a naming convention, and it says `None` rather than
//! guess.

use crate::graph_catalog::graph_schema::GraphSchema;
use std::collections::{HashMap, HashSet};

/// What kind of value an exported name carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CteExportKind {
    /// A node (whole-node passthrough or aggregate-grouped node).
    Node,
    /// A scalar, list, map or anything else with no graph identity. Its identity
    /// is its own column.
    Value,
}

/// One exported Cypher name inside one WITH CTE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CteExport {
    pub kind: CteExportKind,
    /// CTE columns holding the identity, in `node_id` column order (a value: its
    /// one column). `None` when the CTE does not emit one (an unknown or
    /// disagreeing label, or a node whose id column was not projected).
    pub identity: Option<Vec<String>>,
}

impl CteExport {
    /// Derive the export of `alias`.
    ///
    /// - `per_alias_mapping`: the alias's Cypher property → CTE column map.
    /// - `labels`: the alias's node labels, as carried across barriers.
    /// - `emitted_columns`: the CTE's SELECT column names. Every identity column
    ///   must be one of them.
    ///
    /// A value is an alias whose map is empty or maps only to a column named
    /// after the alias itself (`WITH a.age AS ag`, `WITH count(*) AS n`, an
    /// UNWIND scalar, also after a second passthrough barrier), whatever label
    /// the planner attached to it.
    ///
    /// For a node, each `node_id` column is looked up under every Cypher property
    /// whose schema mapping is that column, then under the column name itself (a
    /// denormalized node's `node_id` names the Cypher property; some CTE shapes
    /// key the identity by its DB column). Every id column must be found and
    /// emitted, and all labels must agree.
    pub fn derive(
        alias: &str,
        labels: &[String],
        per_alias_mapping: &HashMap<String, String>,
        emitted_columns: &HashSet<&str>,
        schema: &GraphSchema,
    ) -> Self {
        let is_value = per_alias_mapping.values().all(|col| col == alias);
        if is_value {
            return CteExport {
                kind: CteExportKind::Value,
                identity: emitted_columns
                    .contains(alias)
                    .then(|| vec![alias.to_string()]),
            };
        }
        let none = CteExport {
            kind: CteExportKind::Node,
            identity: None,
        };
        let mut identity: Option<Vec<String>> = None;
        for label in labels {
            let Ok(node_schema) = schema.node_schema(label) else {
                return none;
            };
            let mut columns = Vec::new();
            for id_col in node_schema.node_id.columns() {
                let mut candidates: Vec<&str> = node_schema
                    .property_mappings
                    .iter()
                    .filter(|(_, mapped)| mapped.raw() == id_col)
                    .map(|(prop, _)| prop.as_str())
                    .collect();
                candidates.sort_unstable();
                candidates.push(id_col);
                let found = candidates
                    .iter()
                    .filter_map(|prop| per_alias_mapping.get(*prop))
                    .find(|col| emitted_columns.contains(col.as_str()));
                match found {
                    Some(col) => columns.push(col.clone()),
                    None => return none,
                }
            }
            match &identity {
                Some(previous) if *previous != columns => return none,
                _ => identity = Some(columns),
            }
        }
        CteExport {
            kind: CteExportKind::Node,
            identity,
        }
    }

    /// The single identity column, when there is exactly one.
    pub fn single_identity(&self) -> Option<&str> {
        match self.identity.as_deref() {
            Some([col]) => Some(col.as_str()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::GraphSchemaConfig;

    fn schema(yaml: &str) -> GraphSchema {
        let config = GraphSchemaConfig::from_yaml_str(yaml).expect("yaml");
        config.to_graph_schema().expect("schema")
    }

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    const RENAMED_ID: &str = r#"
name: t
graph_schema:
  nodes:
    - label: User
      database: db
      table: users
      node_id: user_id
      property_mappings:
        id: user_id
        name: name
  edges: []
"#;

    const COMPOSITE: &str = r#"
name: t
graph_schema:
  nodes:
    - label: Account
      database: db
      table: accounts
      node_id: [bank_id, account_number]
      property_mappings:
        bank_id: bank_id
        account_number: account_number
  edges: []
"#;

    #[test]
    fn node_identity_is_found_under_the_cypher_property_or_the_db_column() {
        let s = schema(RENAMED_ID);
        let labels = vec!["User".to_string()];
        // `WITH u, count(g) AS n` spells it after the Cypher property.
        let by_prop = map(&[("id", "p1_u_id"), ("name", "p1_u_name")]);
        let cols: HashSet<&str> = ["p1_u_id", "p1_u_name", "n"].into();
        let ex = CteExport::derive("u", &labels, &by_prop, &cols, &s);
        assert_eq!(ex.kind, CteExportKind::Node);
        assert_eq!(ex.single_identity(), Some("p1_u_id"));
        // `WITH u` spells it after the DB column.
        let by_col = map(&[("user_id", "p1_u_user_id"), ("name", "p1_u_name")]);
        let cols: HashSet<&str> = ["p1_u_user_id", "p1_u_name"].into();
        let ex = CteExport::derive("u", &labels, &by_col, &cols, &s);
        assert_eq!(ex.single_identity(), Some("p1_u_user_id"));
    }

    #[test]
    fn a_mapped_but_unemitted_identity_is_none_not_a_guess() {
        let s = schema(RENAMED_ID);
        let m = map(&[("id", "p1_u_id"), ("name", "p1_u_name")]);
        let cols: HashSet<&str> = ["p1_u_name"].into();
        let ex = CteExport::derive("u", &["User".to_string()], &m, &cols, &s);
        assert_eq!(ex.kind, CteExportKind::Node);
        assert_eq!(ex.identity, None);
    }

    #[test]
    fn composite_identity_keeps_node_id_order() {
        let s = schema(COMPOSITE);
        let m = map(&[
            ("account_number", "p1_c_account_number"),
            ("bank_id", "p1_c_bank_id"),
        ]);
        let cols: HashSet<&str> = ["p1_c_account_number", "p1_c_bank_id"].into();
        let ex = CteExport::derive("c", &["Account".to_string()], &m, &cols, &s);
        assert_eq!(
            ex.identity,
            Some(vec![
                "p1_c_bank_id".to_string(),
                "p1_c_account_number".to_string()
            ])
        );
        assert_eq!(ex.single_identity(), None);
    }

    #[test]
    fn scalars_are_values_whatever_label_they_carry() {
        let s = schema(RENAMED_ID);
        let labels = vec!["User".to_string()];
        let cols: HashSet<&str> = ["ag", "p1_u_name"].into();
        let ex = CteExport::derive("ag", &labels, &HashMap::new(), &cols, &s);
        assert_eq!(ex.kind, CteExportKind::Value);
        assert_eq!(ex.single_identity(), Some("ag"));
        // A scalar after a second passthrough barrier arrives as `{id: ag}`.
        let ex = CteExport::derive("ag", &labels, &map(&[("id", "ag")]), &cols, &s);
        assert_eq!(ex.kind, CteExportKind::Value);
        assert_eq!(ex.single_identity(), Some("ag"));
    }

    #[test]
    fn unknown_label_has_no_identity() {
        let s = schema(RENAMED_ID);
        let m = map(&[("id", "p1_x_id")]);
        let cols: HashSet<&str> = ["p1_x_id"].into();
        let ex = CteExport::derive("x", &["Nope".to_string()], &m, &cols, &s);
        assert_eq!(ex.identity, None);
    }
}
