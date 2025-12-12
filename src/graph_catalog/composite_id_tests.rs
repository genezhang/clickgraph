use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::NodeIdSchema;

#[test]
fn test_single_node_id_sql_tuple() {
    let single_id = NodeIdSchema::single("user_id".to_string(), "UInt64".to_string());

    // Should return plain column reference (no tuple)
    assert_eq!(single_id.sql_tuple("u"), "u.user_id");

    // Test equality
    assert_eq!(single_id.sql_equality("u", "u2"), "u.user_id = u2.user_id");
}

#[test]
fn test_composite_node_id_sql_tuple() {
    let composite_id = NodeIdSchema::composite(
        vec!["bank_id".to_string(), "account_number".to_string()],
        "Tuple(String, String)".to_string(),
    );

    // Should return tuple expression
    assert_eq!(composite_id.sql_tuple("a"), "(a.bank_id, a.account_number)");

    // Test equality with tuple
    assert_eq!(
        composite_id.sql_equality("a", "a2"),
        "(a.bank_id, a.account_number) = (a2.bank_id, a2.account_number)"
    );
}

#[test]
fn test_identifier_to_sql_tuple() {
    // Single
    let single = Identifier::Single("id".to_string());
    assert_eq!(single.to_sql_tuple("t"), "t.id");

    // Composite
    let composite =
        Identifier::Composite(vec!["c1".to_string(), "c2".to_string(), "c3".to_string()]);
    assert_eq!(composite.to_sql_tuple("t"), "(t.c1, t.c2, t.c3)");
}

#[test]
fn test_node_id_schema_columns() {
    let single = NodeIdSchema::single("user_id".to_string(), "UInt64".to_string());
    assert_eq!(single.columns(), vec!["user_id"]);
    assert!(!single.is_composite());

    let composite = NodeIdSchema::composite(
        vec!["col1".to_string(), "col2".to_string()],
        "Tuple".to_string(),
    );
    assert_eq!(composite.columns(), vec!["col1", "col2"]);
    assert!(composite.is_composite());
}

#[test]
fn test_columns_with_alias() {
    let composite = NodeIdSchema::composite(
        vec!["bank_id".to_string(), "account_number".to_string()],
        "Tuple".to_string(),
    );

    assert_eq!(
        composite.columns_with_alias("a"),
        vec!["a.bank_id", "a.account_number"]
    );
}
