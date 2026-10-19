//! The statement front end (`clickgraph::translate::prepared`) on Bolt: the
//! session's graph, the prelude, and every UNION arm's `USE`, driven through
//! the real `BoltHandler` with an executor that records the SQL it is given.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;

use clickgraph::executor::{ExecutorError, QueryExecutor};
use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::server::bolt_protocol::handler::BoltHandler;
use clickgraph::server::bolt_protocol::messages::{signatures, BoltMessage, BoltValue};
use clickgraph::server::bolt_protocol::{BoltConfig, BoltContext, BOLT_VERSION_5_1};
use clickgraph::server::GLOBAL_SCHEMAS;

struct Capturing(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl QueryExecutor for Capturing {
    async fn execute_json(
        &self,
        sql: &str,
        _role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
        self.0.lock().unwrap().push(sql.to_string());
        Ok(vec![])
    }
    async fn execute_text(
        &self,
        sql: &str,
        _format: &str,
        _role: Option<&str>,
    ) -> Result<String, ExecutorError> {
        self.0.lock().unwrap().push(sql.to_string());
        Ok(String::new())
    }
}

/// A one-label schema over `db.table`.
fn user_schema(db: &str, table: &str) -> clickgraph::graph_catalog::graph_schema::GraphSchema {
    let yaml = format!(
        "graph_schema:\n  nodes:\n    - label: User\n      database: {db}\n      table: {table}\n      \
         node_id: user_id\n      property_mappings:\n        user_id: user_id\n        name: \
         full_name\n  edges: []\n"
    );
    GraphSchemaConfig::from_yaml_str(&yaml)
        .unwrap()
        .to_graph_schema()
        .unwrap()
}

/// "default" (the benchmark schema, as the other files of this binary
/// register it: the registry is shared) plus several schemas over other
/// tables, whose names are this file's.
async fn register_schemas() {
    let _ = GLOBAL_SCHEMAS.set(tokio::sync::RwLock::new(HashMap::new()));
    let mut map = GLOBAL_SCHEMAS.get().unwrap().write().await;
    map.entry("default".to_string()).or_insert_with(|| {
        GraphSchemaConfig::from_yaml_file("benchmarks/social_network/schemas/social_benchmark.yaml")
            .unwrap()
            .to_graph_schema()
            .unwrap()
    });
    for name in ["bolt_fe_a", "bolt_fe_b", "bolt_fe_c", "bolt_fe_d"] {
        map.entry(name.to_string())
            .or_insert_with(|| user_schema("bolt_fe", &format!("{name}_users")));
    }
}

/// A handler past HELLO and an empty LOGON (Bolt 5.1+), with the session
/// database `db` given in HELLO.
async fn logged_on(
    db: Option<&str>,
) -> (
    BoltHandler,
    Arc<Mutex<BoltContext>>,
    Arc<Mutex<Vec<String>>>,
) {
    let context = Arc::new(Mutex::new(BoltContext::new()));
    context.lock().unwrap().set_version(BOLT_VERSION_5_1);
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut handler = BoltHandler::new(
        context.clone(),
        Arc::new(BoltConfig::default()),
        Arc::new(Capturing(captured.clone())),
    );
    let mut hello = HashMap::from([(
        "user_agent".to_string(),
        Value::String("test/1".to_string()),
    )]);
    if let Some(db) = db {
        hello.insert("db".to_string(), Value::String(db.to_string()));
    }
    let hello = BoltMessage::new(
        signatures::HELLO,
        vec![BoltValue::Json(Value::Object(serde_json::Map::from_iter(
            hello,
        )))],
    );
    let r = handler.handle_message(hello).await.unwrap();
    assert_eq!(r[0].signature, signatures::SUCCESS);
    let logon = BoltMessage::new(signatures::LOGON, vec![]);
    let r = handler.handle_message(logon).await.unwrap();
    assert_eq!(r[0].signature, signatures::SUCCESS);
    (handler, context, captured)
}

/// RUN `query`: `Ok(())` on SUCCESS, else the FAILURE's (code, message).
async fn run(handler: &mut BoltHandler, query: &str) -> Result<(), (String, String)> {
    let r = handler
        .handle_message(BoltMessage::run(query.to_string(), HashMap::new(), None))
        .await
        .unwrap();
    if r[0].signature == signatures::SUCCESS {
        // Back to READY for the next RUN.
        let _ = handler.handle_message(BoltMessage::pull(-1, None)).await;
        return Ok(());
    }
    let field = |k: &str| match &r[0].fields[0] {
        BoltValue::Json(Value::Object(m)) => {
            m.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string()
        }
        _ => String::new(),
    };
    let _ = handler.handle_message(BoltMessage::reset()).await;
    Err((field("code"), field("message")))
}

/// #1372: a session that names no database runs on the default schema, as
/// HTTP does without `schema_name` (it took the first non-"default" key of
/// the schema map, an arbitrary one).
#[tokio::test]
async fn a_session_without_a_database_uses_the_default_schema() {
    register_schemas().await;
    let (mut handler, context, captured) = logged_on(None).await;
    assert_eq!(context.lock().unwrap().schema_name, None);
    run(&mut handler, "MATCH (u:User) RETURN u.name AS n")
        .await
        .unwrap();
    let sql = captured.lock().unwrap().join("\n");
    assert!(sql.contains("social.users_bench"), "{sql}");
    // A session database is kept.
    let (mut handler, _, captured) = logged_on(Some("bolt_fe_b")).await;
    run(&mut handler, "MATCH (u:User) RETURN u.name AS n")
        .await
        .unwrap();
    let sql = captured.lock().unwrap().join("\n");
    assert!(sql.contains("bolt_fe.bolt_fe_b_users"), "{sql}");
}

/// The prelude is read (#1371: Bolt did not read `CYPHER ..` at all) and a
/// `USE` after it chooses the graph; a later UNION arm naming another graph
/// is refused (#1370); EXPLAIN answers an empty plan, PROFILE is refused.
#[tokio::test]
async fn the_prelude_and_every_arms_use_on_bolt() {
    register_schemas().await;
    let (mut handler, _, captured) = logged_on(Some("bolt_fe_a")).await;
    run(
        &mut handler,
        "CYPHER replan=force USE bolt_fe_c MATCH (u:User) RETURN u.name AS n",
    )
    .await
    .unwrap();
    let sql = captured.lock().unwrap().join("\n");
    assert!(sql.contains("bolt_fe.bolt_fe_c_users"), "{sql}");

    let q = "MATCH (u:User) RETURN u.name AS n";
    let (code, message) = run(&mut handler, &format!("{q} UNION USE bolt_fe_d {q}"))
        .await
        .unwrap_err();
    assert!(message.contains("Multiple graphs"), "{code}: {message}");
    assert_eq!(code, "Neo.ClientError.Statement.SyntaxError");
    run(
        &mut handler,
        &format!("USE bolt_fe_d {q} UNION USE bolt_fe_d {q}"),
    )
    .await
    .unwrap();
    // The session's own graph named in a later arm.
    run(&mut handler, &format!("{q} UNION USE bolt_fe_a {q}"))
        .await
        .unwrap();

    captured.lock().unwrap().clear();
    run(
        &mut handler,
        "CYPHER replan=skip EXPLAIN MATCH (u:User) RETURN u",
    )
    .await
    .unwrap();
    assert!(captured.lock().unwrap().is_empty(), "EXPLAIN must not run");
    let (code, _) = run(&mut handler, "PROFILE MATCH (u:User) RETURN u")
        .await
        .unwrap_err();
    assert_eq!(code, "Neo.ClientError.Statement.FeatureNotSupported");
    let (code, message) = run(&mut handler, "CYPHER replan=bogus RETURN 1 AS x")
        .await
        .unwrap_err();
    assert!(
        message.contains("not a valid option for replan"),
        "{message}"
    );
    // Neo4j's codes.
    assert_eq!(code, "Neo.ClientError.Statement.ArgumentError");
    // #1373: too many clauses, refused before anything parses them.
    let long = format!("MATCH (u:User) {}RETURN u.name", "WITH u ".repeat(1500));
    let (code, message) = run(&mut handler, &long).await.unwrap_err();
    assert_eq!(code, "Neo.ClientError.Statement.FeatureNotSupported");
    assert!(message.contains("(at most 1000)"), "{message}");
}
