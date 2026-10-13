//! The data check of `cg schema validate --data` (#531): the rows of each
//! node embedded in edge tables hold one value of each property per node id.
//!
//! A node embedded in edge tables (`from_node_properties` /
//! `to_node_properties`) is the distinct ids its roles hold
//! ([`GraphSchema::with_node_relations`]); the schema says each property is
//! the node's, so every row of one id holds one value of it. Rows that hold
//! several mean the property is the relationship's, or the id columns do not
//! identify the node: a query reads any of the values, or fails when
//! `check_node_properties` is on. This finds such properties first.
//!
//! [`GraphSchema::with_node_relations`]: super::graph_schema::GraphSchema::with_node_relations

use super::graph_schema::NodeRelation;
use crate::sql_generator::emitters::clickhouse::qualified_column;
use crate::sql_generator::function_mapper::current_function_mapper;

/// How much of the data the check reads (all of it when absent).
#[derive(Debug, Clone, Copy)]
pub struct Sampling {
    /// One node id in `one_in`, by a hash of its key: all rows of each id
    /// it takes, in every source, so that two rows of one id disagreeing are
    /// seen together.
    pub one_in: u64,
    /// At most this many rows of each source.
    pub max_rows: u64,
}

/// The ClickHouse query listing each property of `relation` that the rows
/// of some node id hold several values of: `property`, how many ids
/// (`ids`), one of them (`id`, its text); and, as the property `''`, how
/// many ids were checked. `None` when the relation has no property besides
/// its id; an error (the reason it is not checked) when a source reads a
/// parameterized view.
///
/// Values and ids are compared as DISTINCT compares them
/// (`CypherUnion::distinct_key`, as the query's check does): the sources'
/// columns may differ in type (`Int16` and `Int32`, `FixedString` and
/// `String`), and `-0.0` is `0.0`. Each source's rows (its `filter:` and
/// FINAL) give the hash of the id's key and of each property's key (a source
/// without the property: none); the ids are grouped, counting up to two
/// distinct hashes per property.
pub fn several_values_sql(
    relation: &NodeRelation,
    sampling: Option<&Sampling>,
) -> Result<Option<String>, String> {
    let properties: Vec<&String> = relation
        .properties
        .iter()
        .filter(|p| !relation.id.contains(p))
        .collect();
    if properties.is_empty() {
        return Ok(None);
    }
    // The active dialect's key, ClickHouse's outside a query context (the
    // query is ClickHouse SQL, and `cg` runs `--data` on ClickHouse only).
    let Some(union) = current_function_mapper().cypher_union() else {
        return Err("internal: no distinct key".to_string());
    };
    let key = |c: &str| format!("cityHash64({})", (union.distinct_key)(c));
    let mut arms = Vec::new();
    for source in &relation.sources {
        if source.view_parameters.is_some() {
            return Err(format!("{} is a parameterized view", source.table));
        }
        let column = |p: &str| {
            source
                .columns
                .iter()
                .find(|(q, _)| q == p)
                .map(|(_, c)| qualified_column("e", c))
        };
        let mut ids = Vec::new();
        for p in &relation.id {
            let c =
                column(p).ok_or_else(|| format!("a source in {} without its id", source.table))?;
            ids.push(c);
        }
        let id_keys: Vec<String> = ids.iter().map(|c| key(c)).collect();
        let id_texts: Vec<String> = ids.iter().map(|c| format!("toString({c})")).collect();
        let mut items = vec![
            format!("tuple({}) AS id", id_keys.join(", ")),
            format!("tuple({}) AS id_text", id_texts.join(", ")),
        ];
        for (j, p) in properties.iter().enumerate() {
            match column(p) {
                Some(c) => items.push(format!("if(isNull({c}), NULL, {}) AS h{j}", key(&c))),
                None => items.push(format!("CAST(NULL, 'Nullable(UInt64)') AS h{j}")),
            }
        }
        let mut filters: Vec<String> = ids.iter().map(|c| format!("NOT isNull({c})")).collect();
        if let Some(f) = &source.filter {
            filters.push(format!("({})", f.to_sql("e")?));
        }
        let mut limit = String::new();
        if let Some(s) = sampling {
            filters.push(format!(
                "cityHash64({}) % {} = 0",
                id_keys.join(", "),
                s.one_in.max(1)
            ));
            limit = format!(" LIMIT {}", s.max_rows.max(1));
        }
        let fin = if source.use_final { " FINAL" } else { "" };
        arms.push(format!(
            "SELECT {} FROM {} AS e{fin} WHERE {}{limit}",
            items.join(", "),
            source.table,
            filters.join(" AND ")
        ));
    }
    let counts: Vec<String> = (0..properties.len())
        .map(|j| format!("uniqUpTo(1)(h{j}) > 1"))
        .collect();
    let names: Vec<String> = properties
        .iter()
        .map(|p| format!("'{}'", p.replace('\\', "\\\\").replace('\'', "\\'")))
        .collect();
    // `ids` as UInt32: a 64-bit integer may print quoted in JSON
    // (`output_format_json_quote_64bit_integers`).
    Ok(Some(format!(
        "SELECT property, toUInt32(count()) AS ids, toString(any(id_text)) AS id FROM (\
         SELECT any(id_text) AS id_text, arrayJoin(arrayConcat([''], \
         arrayFilter((p, several) -> several, [{}], [{}]))) AS property \
         FROM ({}) GROUP BY id) GROUP BY property ORDER BY property",
        names.join(", "),
        counts.join(", "),
        arms.join(" UNION ALL ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::filter_parser::SchemaFilter;
    use crate::graph_catalog::graph_schema::NodeSource;

    fn source(table: &str, columns: &[(&str, &str)]) -> NodeSource {
        NodeSource {
            table: table.to_string(),
            view_parameters: None,
            use_final: false,
            filter: None,
            columns: columns
                .iter()
                .map(|(p, c)| (p.to_string(), c.to_string()))
                .collect(),
        }
    }

    fn airport(sources: Vec<NodeSource>) -> NodeRelation {
        NodeRelation {
            label: "Airport".to_string(),
            id: vec!["code".to_string()],
            properties: vec!["city".to_string(), "code".to_string(), "tz".to_string()],
            sources,
        }
    }

    /// #531: each source's rows give the hash of the id's DISTINCT key and
    /// of each property's (none where the source does not hold it);
    /// sampled, one id in N by its key's hash, every row of it, at most M
    /// rows of each source.
    #[test]
    fn several_values_query_reads_each_source() {
        let mut dst = source("db.fl", &[("code", "dst"), ("city", "dst_city")]);
        dst.use_final = true;
        dst.filter = Some(SchemaFilter::new("carrier = 'AA'").unwrap());
        let relation = airport(vec![
            source(
                "db.fl",
                &[("code", "org"), ("city", "org_city"), ("tz", "org_tz")],
            ),
            dst,
        ]);
        let sampling = Sampling {
            one_in: 10,
            max_rows: 500,
        };
        let sql = several_values_sql(&relation, Some(&sampling))
            .unwrap()
            .unwrap();
        for part in [
            "SELECT tuple(cityHash64(tuple(multiIf(dynamicType(CAST(e.org, 'Dynamic'))",
            "tuple(toString(e.org)) AS id_text, if(isNull(e.org_city), NULL, cityHash64(tuple(",
            "CAST(NULL, 'Nullable(UInt64)') AS h1 FROM db.fl AS e FINAL WHERE NOT isNull(e.dst) AND (",
            ") % 10 = 0 LIMIT 500",
            "['city', 'tz'], [uniqUpTo(1)(h0) > 1, uniqUpTo(1)(h1) > 1]",
            "arrayConcat([''],",
            "GROUP BY id) GROUP BY property",
        ] {
            assert!(sql.contains(part), "missing `{part}` in\n{sql}");
        }
        let full = several_values_sql(&relation, None).unwrap().unwrap();
        assert!(!full.contains(" LIMIT "), "{full}");
        assert!(!full.contains("% "), "{full}");
    }

    #[test]
    fn several_values_query_skips_what_it_cannot_read() {
        // Only an id: nothing to check.
        let relation = NodeRelation {
            properties: vec!["code".to_string()],
            ..airport(vec![source("db.fl", &[("code", "org")])])
        };
        assert!(several_values_sql(&relation, None).unwrap().is_none());
        // A parameterized view: not read.
        let mut s = source("db.v", &[("code", "org"), ("city", "c")]);
        s.view_parameters = Some(vec!["tenant".to_string()]);
        let err = several_values_sql(&airport(vec![s]), None).unwrap_err();
        assert!(err.contains("parameterized view"), "{err}");
    }
}
