//! Outcome-loop example tests: the runnable example schema and every worked
//! query from `docs/design/OUTCOME_RECIPES.md` translate end-to-end (no
//! database). This is the user-facing proof that the recipes build.

use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::graph_catalog::graph_schema::GraphSchema;

fn example_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_file("schemas/examples/people_outcome_graph.yaml")
        .expect("load people_outcome_graph.yaml")
        .to_graph_schema()
        .expect("convert example schema")
}

fn sql(schema: &GraphSchema, cypher: &str) -> String {
    clickgraph::translate::translate_bound_plan(
        cypher,
        schema,
        &clickgraph::translate::ReadOptions {
            cypher: cypher.to_string(),
            ..Default::default()
        },
    )
    .unwrap_or_else(|e| panic!("{cypher}: {e}"))
    .sql
}

#[test]
fn example_schema_loads_with_people_and_skills() {
    let schema = example_schema();
    assert_eq!(schema.all_node_schemas().len(), 2);
    // Person -> Skill carries four relationship tables.
    assert!(schema.get_relationships_schemas().len() >= 4);
}

#[test]
fn enrichment_recent_recommendations_translate() {
    let schema = example_schema();
    let got = sql(
        &schema,
        "MATCH (u:Person) WHERE u.person_id = 7 MATCH (u)-[r:RECOMMENDED]->(s:Skill) \
         RETURN s.skill_id AS item, r.propensity AS p ORDER BY r.created_at DESC LIMIT 50",
    );
    assert!(got.contains("outcome.recommendations"), "{got}");
    assert!(got.contains("WHERE"), "{got}");
    assert!(!got.contains("PREWHERE"), "flag off, stays on WHERE:\n{got}");
}

#[test]
fn skill_filter_by_track_translates() {
    let schema = example_schema();
    let got = sql(
        &schema,
        "MATCH (u:Person)-[:HAS_SKILL]->(s:Skill) WHERE u.person_id = 7 AND s.track = 'mgmt' \
         RETURN s.title AS title",
    );
    assert!(got.contains("outcome.person_skills"), "{got}");
    assert!(got.contains("outcome.skills"), "{got}");
}

#[test]
fn high_weight_outcomes_translate() {
    let schema = example_schema();
    let got = sql(
        &schema,
        "MATCH (u:Person)-[o:OUTCOME]->(s:Skill) WHERE u.person_id = 7 AND o.weight > 0.7 \
         RETURN s.skill_id AS item, o.weight AS w ORDER BY o.resolved_at DESC LIMIT 20",
    );
    assert!(got.contains("outcome.outcomes"), "{got}");
    assert!(!got.contains("PREWHERE"), "flag off:\n{got}");
}
