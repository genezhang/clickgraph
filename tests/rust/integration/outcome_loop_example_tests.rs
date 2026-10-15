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
fn contracts_are_valid_json_with_versioned_ids() {
    // Dep-free gate: every contract parses and carries its versioned $id.
    // Full JSON-Schema validation happens in each producer's language
    // (see contracts/README.md snippets); this keeps the repo green.
    for name in [
        "decision-event.v1.json",
        "outcome-event.v1.json",
        "feedback-event.v1.json",
        "score-row.v1.json",
        "strategy-experiment.v1.json",
    ] {
        let text = std::fs::read_to_string(format!("contracts/{name}"))
            .unwrap_or_else(|e| panic!("read {name}: {e}"));
        let v: serde_json::Value =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {name}: {e}"));
        let id = v.get("$id").and_then(|s| s.as_str()).unwrap_or_default();
        assert!(
            id.ends_with(&format!("/{name}")),
            "{name}: $id must be versioned ({id})"
        );
        assert!(
            v.get("type").is_some() && v.get("properties").is_some(),
            "{name}: must be an object schema"
        );
    }
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
    assert!(
        !got.contains("PREWHERE"),
        "flag off, stays on WHERE:\n{got}"
    );
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

#[test]
fn langfuse_score_configs_are_valid() {
    // Every score config parses and names a Langfuse data type. These files
    // are the importable source for score-config creation (UI or API).
    for name in [
        "user_feedback",
        "task_completion",
        "trajectory_steps",
        "session_resolution",
        "personalization_lift",
    ] {
        let path = format!("langfuse/score-configs/{name}.json");
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let v: serde_json::Value =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path}: {e}"));
        assert_eq!(v.get("name").and_then(|s| s.as_str()), Some(name));
        let dt = v
            .get("dataType")
            .and_then(|s| s.as_str())
            .unwrap_or_default();
        assert!(
            ["NUMERIC", "BOOLEAN", "CATEGORICAL"].contains(&dt),
            "{path}: bad dataType {dt}"
        );
    }
}

#[test]
fn regression_dataset_translates_clean() {
    // langfuse/datasets/outcome-regression.jsonl is executable: every item
    // translates against the example schema and meets its fragment gates.
    // Failing production traces land here as new items (permanent regressions).
    let schema = example_schema();
    let text = std::fs::read_to_string("langfuse/datasets/outcome-regression.jsonl")
        .expect("read dataset");
    let mut count = 0;
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let item: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("line {n}: {e}"));
        let id = item.get("id").and_then(|s| s.as_str()).unwrap_or("?");
        let input = item
            .get("input")
            .and_then(|s| s.as_str())
            .unwrap_or_else(|| panic!("{id}: no input"));
        let got = sql(&schema, input);
        for frag in item
            .get("must_contain")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str())
        {
            assert!(got.contains(frag), "{id}: missing `{frag}`:\n{got}");
        }
        for frag in item
            .get("must_not_contain")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str())
        {
            assert!(!got.contains(frag), "{id}: forbidden `{frag}`:\n{got}");
        }
        count += 1;
    }
    assert!(count >= 6, "dataset must hold the regression items");
}
