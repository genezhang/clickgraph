//! Render-site type classifier.
//!
//! A conservative, best-effort inference of a [`RenderExpr`]'s Cypher-semantic
//! type at SQL-render time. This is the shared primitive behind three fixes that
//! all need operand typing the render layer historically lacked:
//!
//! - **#871** — string `+` must become `concat(...)` when both operands are
//!   string-returning function calls (no string literal present).
//! - **#880** — `toInteger`/`toFloat` on a *string* arg must dispatch to the
//!   OrNull cast so an unparseable value yields NULL instead of throwing.
//! - **#854** — temporal component access / duration arithmetic on a native
//!   `Date`/`DateTime` arg must skip the epoch-millis `fromUnixTimestamp64Milli`
//!   wrap (which only makes sense for epoch-millis BIGINTs).
//!
//! # Conservative-None invariant
//!
//! The classifier returns [`Option<RenderType>`]; `None` means "unknown". Every
//! consumer MUST route `None` to the *identical* legacy behavior — an unknown
//! type may never produce a worse outcome than before this classifier existed.
//! Loud errors may only become correct SQL, never silent-wrong. Accordingly the
//! classifier types only what it can prove: literals, list constructors,
//! registry-classified function calls, comparison/logical operators, and
//! schema-declared columns. Everything else is `None`.

use super::function_registry::{function_return_kind, FnReturnKind};
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::schema_types::SchemaType;
use crate::render_plan::render_expr::{Literal, Operator, RenderExpr};
use std::collections::HashMap;

/// Render-layer type. A superset of [`SchemaType`] with two extra shapes that
/// exist only during rendering:
/// - [`RenderType::Number`] — numeric of unresolved int/float subtype (e.g.
///   `abs(x)`), which no single `SchemaType` can express.
/// - [`RenderType::List`] — an array/list value, which `SchemaType` (a
///   schema/DDL concern) deliberately does not model.
///
/// Kept local to the emitter so `SchemaType` stays a pure schema/DDL type (its
/// exhaustive matches, ~40 construction sites, and serde/YAML config surface are
/// unaffected).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderType {
    String,
    Integer,
    Float,
    /// Numeric of unresolved int/float subtype.
    Number,
    Boolean,
    Date,
    DateTime,
    Uuid,
    List,
}

impl From<SchemaType> for RenderType {
    fn from(t: SchemaType) -> Self {
        match t {
            SchemaType::Integer => RenderType::Integer,
            SchemaType::Float => RenderType::Float,
            SchemaType::String => RenderType::String,
            SchemaType::Boolean => RenderType::Boolean,
            SchemaType::DateTime => RenderType::DateTime,
            SchemaType::Date => RenderType::Date,
            SchemaType::Uuid => RenderType::Uuid,
        }
    }
}

impl RenderType {
    /// True for any numeric render type (used by the arithmetic-operator arm and
    /// by consumers that only care "is this a number").
    pub fn is_numeric(self) -> bool {
        matches!(
            self,
            RenderType::Integer | RenderType::Float | RenderType::Number
        )
    }
}

/// Best-effort classify a [`RenderExpr`]'s render type. `None` = unknown; see the
/// conservative-None invariant in the module docs.
pub fn infer_render_type(expr: &RenderExpr) -> Option<RenderType> {
    match expr {
        // ---- Literals: the only certainties ----
        RenderExpr::Literal(Literal::String(_)) => Some(RenderType::String),
        RenderExpr::Literal(Literal::Integer(_)) => Some(RenderType::Integer),
        RenderExpr::Literal(Literal::Float(_)) => Some(RenderType::Float),
        RenderExpr::Literal(Literal::Boolean(_)) => Some(RenderType::Boolean),
        // Cypher null is typeless.
        RenderExpr::Literal(Literal::Null) => None,

        // ---- List shapes ----
        RenderExpr::List(_) => Some(RenderType::List),
        RenderExpr::ArraySlicing { .. } => Some(RenderType::List),

        // ---- Function calls (scalar + aggregate share the registry) ----
        RenderExpr::ScalarFnCall(f) => infer_fn_call(&f.name, &f.args),
        RenderExpr::AggregateFnCall(agg) => infer_fn_call(&agg.name, &agg.args),

        // ---- Column property access via schema ----
        RenderExpr::PropertyAccessExp(pa) => infer_property_type(&pa.table_alias.0, &pa.column),

        // ---- Operators ----
        RenderExpr::OperatorApplicationExp(op) => infer_operator_type(op.operator, &op.operands),

        // ---- Known scalar-ish shapes ----
        RenderExpr::InSubquery(_) | RenderExpr::ExistsSubquery(_) => Some(RenderType::Boolean),
        RenderExpr::PatternCount(_) => Some(RenderType::Integer),

        // ---- Everything else: unknown (conservative) ----
        // Column (bare, no alias→label), TableAlias, ColumnAlias, Parameter,
        // Raw, Star, MapLiteral, Case, ReduceExpr, ArraySubscript (element type
        // unknown), CteEntityRef.
        //
        // NOTE (#969): a `reduce()` lambda binder DOES have a known type inside
        // its body, but it is deliberately NOT surfaced here. This shared
        // classifier feeds #880 (toInteger/toFloat-OrNull), #854 (temporal), and
        // #962 (size/reverse UTF8) as well as #871 (string-`+`→concat); typing a
        // binder here made #880 rewrite `toInteger(x)` on a string-element binder
        // to `toInt64OrNull` → `Nullable` → Code 53 (a regression). The binder
        // type is instead consulted ONLY by the string-concat detectors
        // (`is_string_operand` / `expression_utils::contains_string_literal`) via
        // `query_context::get_reduce_binder_type`, keeping the conservative-None
        // invariant intact for every other consumer.
        _ => None,
    }
}

/// #969: infer the ELEMENT type of a list-valued expression, if uniform.
///
/// Used to type a `reduce()` iteration variable from its `list`. A `List` of
/// literals/expressions returns the common element type when every non-null
/// element agrees (e.g. `['a','b','c']` → `String`); an empty, mixed-type, or
/// non-`List` expression returns `None` (conservative — the binder then stays
/// untyped and behavior is unchanged).
pub fn infer_list_element_type(expr: &RenderExpr) -> Option<RenderType> {
    let RenderExpr::List(items) = expr else {
        return None;
    };
    let mut elem: Option<RenderType> = None;
    for item in items {
        match infer_render_type(item) {
            // A typeless element (e.g. Cypher null) doesn't constrain the type.
            None => {}
            Some(t) => match elem {
                None => elem = Some(t),
                Some(prev) if prev == t => {}
                // Mixed element types → not uniform.
                Some(_) => return None,
            },
        }
    }
    elem
}

/// Classify a function call by its registry [`FnReturnKind`], recursing for the
/// polymorphic kinds.
fn infer_fn_call(name: &str, args: &[RenderExpr]) -> Option<RenderType> {
    match function_return_kind(&name.to_lowercase()) {
        FnReturnKind::Str => Some(RenderType::String),
        FnReturnKind::Int => Some(RenderType::Integer),
        FnReturnKind::Float => Some(RenderType::Float),
        FnReturnKind::Num => Some(RenderType::Number),
        FnReturnKind::Bool => Some(RenderType::Boolean),
        FnReturnKind::Date => Some(RenderType::Date),
        FnReturnKind::DateTime => Some(RenderType::DateTime),
        FnReturnKind::List => Some(RenderType::List),
        // Return type == arg0's type (reverse/coalesce/nullif/min/max/anyLast).
        FnReturnKind::SameAsArg0 => args.first().and_then(infer_render_type),
        // Element type of a list arg (head/last) — unknowable at render time.
        FnReturnKind::ElementOfArg0 => None,
        FnReturnKind::Unknown => None,
    }
}

/// Resolve a `<alias>.<prop>` property access to its declared schema type, if
/// any. Returns `None` (safe) for rel-var aliases, unknown labels, undeclared
/// properties, or when the alias resolves ambiguously to both a node label and a
/// relationship type.
///
/// The `column` name at this render stage may be EITHER the Cypher property name
/// or the already-mapped physical DB column (the property mapping is applied
/// upstream, so e.g. `p.date` arrives here as `post_date`). `property_types` is
/// keyed by the Cypher name, so we try a direct lookup first, then reverse-map
/// the name through `property_mappings` (DB column → Cypher prop → type).
fn infer_property_type(alias: &str, column: &PropertyValue) -> Option<RenderType> {
    use crate::graph_catalog::expression_parser::PropertyValue as PV;
    use crate::server::query_context::{get_current_schema, get_node_label_for_alias};

    // Only a simple column reference carries a resolvable name. An Expression is
    // a raw computed string — untyped.
    let name = match column {
        PropertyValue::Column(name) => name.as_str(),
        PropertyValue::Expression(_) => return None,
    };

    let schema = get_current_schema()?;
    let label = get_node_label_for_alias(alias)?;

    // Resolve `name` (Cypher prop OR physical DB column) to its declared type via
    // a (property_types, property_mappings) pair. Direct lookup first (name is the
    // Cypher prop); else reverse-map (name is the DB column that some Cypher prop
    // maps to) and look that prop's type up.
    let resolve = |types: &HashMap<String, SchemaType>,
                   mappings: &HashMap<String, PV>|
     -> Option<SchemaType> {
        if let Some(t) = types.get(name) {
            return Some(t.clone());
        }
        for (cypher_prop, mapped) in mappings.iter() {
            if let PV::Column(col) = mapped {
                if col == name {
                    if let Some(t) = types.get(cypher_prop) {
                        return Some(t.clone());
                    }
                }
            }
        }
        None
    };

    // A name could match a node label AND a relationship type. If it matches
    // both, we can't tell which schema applies — return None rather than guess
    // (avoids a wrong classification triggering concat/OrNull/etc.).
    let node = schema.node_schema_opt(&label);
    let rel = schema.get_relationships_schema_opt(&label);
    match (node, rel) {
        (Some(ns), None) => {
            resolve(&ns.property_types, &ns.property_mappings).map(RenderType::from)
        }
        (None, Some(rs)) => {
            resolve(&rs.property_types, &rs.property_mappings).map(RenderType::from)
        }
        // Ambiguous (both) or neither → unknown.
        _ => None,
    }
}

/// Classify an operator application. Comparison/logical operators are always
/// Boolean; arithmetic is Number only when *every* operand is provably numeric;
/// everything else is unknown.
///
/// NOTE: this arm must NOT consult the string-`+`→`concat` gate — that gate calls
/// `infer_render_type`, so special-casing `Addition`→String here would create a
/// mutual recursion. Arithmetic stays Number/None.
fn infer_operator_type(op: Operator, operands: &[RenderExpr]) -> Option<RenderType> {
    match op {
        // Comparison + logical → Boolean.
        Operator::Equal
        | Operator::NotEqual
        | Operator::LessThan
        | Operator::GreaterThan
        | Operator::LessThanEqual
        | Operator::GreaterThanEqual
        | Operator::RegexMatch
        | Operator::And
        | Operator::Or
        | Operator::Not
        | Operator::In
        | Operator::NotIn
        | Operator::StartsWith
        | Operator::EndsWith
        | Operator::Contains
        | Operator::IsNull
        | Operator::IsNotNull => Some(RenderType::Boolean),

        // Arithmetic. Requires every operand provably numeric; otherwise unknown
        // (never assume — string `+` is handled by its own gate). Cypher numeric
        // promotion then fixes the subtype: any Float operand makes the result
        // Float (`1 + 2.0` = 3.0, and a negative float literal `-2.0` lowers to
        // `0 - 2.0`), and exponentiation is always Float in Neo4j (`2^3` = 8.0).
        // Otherwise the int/float subtype stays unresolved (`Number`) — e.g.
        // `abs(x) + 1`. This subtype refinement is consumed by #1055 (toString
        // keeps a whole float's `.0`); every other classifier consumer keys on
        // String/Boolean/Date or the `is_numeric()` umbrella (Float ⊆ numeric),
        // so promoting Number→Float here is transparent to them.
        Operator::Addition
        | Operator::Subtraction
        | Operator::Multiplication
        | Operator::Division
        | Operator::ModuloDivision
        | Operator::Exponentiation => {
            if operands.is_empty() {
                return None;
            }
            let operand_types: Vec<Option<RenderType>> =
                operands.iter().map(infer_render_type).collect();
            if !operand_types
                .iter()
                .all(|t| t.is_some_and(|t| t.is_numeric()))
            {
                return None;
            }
            if op == Operator::Exponentiation
                || operand_types.iter().any(|t| t == &Some(RenderType::Float))
            {
                Some(RenderType::Float)
            } else {
                Some(RenderType::Number)
            }
        }

        Operator::Distinct => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::{
        Literal, OperatorApplication, PropertyAccess, ScalarFnCall, TableAlias,
    };

    fn sfn(name: &str, args: Vec<RenderExpr>) -> RenderExpr {
        RenderExpr::ScalarFnCall(ScalarFnCall {
            name: name.to_string(),
            args,
        })
    }

    fn lit_str(s: &str) -> RenderExpr {
        RenderExpr::Literal(Literal::String(s.to_string()))
    }
    fn lit_int(n: i64) -> RenderExpr {
        RenderExpr::Literal(Literal::Integer(n))
    }
    fn lit_float(f: f64) -> RenderExpr {
        RenderExpr::Literal(Literal::Float(f))
    }

    #[test]
    fn literals_classify() {
        assert_eq!(infer_render_type(&lit_str("x")), Some(RenderType::String));
        assert_eq!(infer_render_type(&lit_int(3)), Some(RenderType::Integer));
        assert_eq!(
            infer_render_type(&RenderExpr::Literal(Literal::Float(1.5))),
            Some(RenderType::Float)
        );
        assert_eq!(
            infer_render_type(&RenderExpr::Literal(Literal::Boolean(true))),
            Some(RenderType::Boolean)
        );
        // null is typeless
        assert_eq!(infer_render_type(&RenderExpr::Literal(Literal::Null)), None);
    }

    #[test]
    fn list_shapes_classify() {
        assert_eq!(
            infer_render_type(&RenderExpr::List(vec![lit_int(1)])),
            Some(RenderType::List)
        );
    }

    #[test]
    fn string_functions_classify() {
        assert_eq!(
            infer_render_type(&sfn("toString", vec![lit_int(1)])),
            Some(RenderType::String)
        );
        assert_eq!(
            infer_render_type(&sfn("toUpper", vec![lit_str("a")])),
            Some(RenderType::String)
        );
        assert_eq!(
            infer_render_type(&sfn("substring", vec![lit_str("hello"), lit_int(1)])),
            Some(RenderType::String)
        );
    }

    #[test]
    fn numeric_functions_classify() {
        // toFloat is Float — the #871 negative control depends on this.
        assert_eq!(
            infer_render_type(&sfn("toFloat", vec![lit_str("1")])),
            Some(RenderType::Float)
        );
        assert_eq!(
            infer_render_type(&sfn("toInteger", vec![lit_str("1")])),
            Some(RenderType::Integer)
        );
        assert_eq!(
            infer_render_type(&sfn("abs", vec![lit_int(-1)])),
            Some(RenderType::Number)
        );
    }

    #[test]
    fn boolean_functions_use_cypher_semantic_not_ch_mapping() {
        // contains/startsWith/endsWith map to CH position/startsWith but are Bool.
        assert_eq!(
            infer_render_type(&sfn("contains", vec![lit_str("ab"), lit_str("a")])),
            Some(RenderType::Boolean)
        );
        assert_eq!(
            infer_render_type(&sfn("startsWith", vec![lit_str("ab"), lit_str("a")])),
            Some(RenderType::Boolean)
        );
    }

    #[test]
    fn temporal_constructors_classify() {
        assert_eq!(
            infer_render_type(&sfn("date", vec![lit_str("2024-06-15")])),
            Some(RenderType::Date)
        );
        assert_eq!(
            infer_render_type(&sfn("datetime", vec![lit_str("2024-06-15")])),
            Some(RenderType::DateTime)
        );
    }

    #[test]
    fn same_as_arg0_recurses() {
        // reverse('abc') → String ; reverse([1,2,3]) → List
        assert_eq!(
            infer_render_type(&sfn("reverse", vec![lit_str("abc")])),
            Some(RenderType::String)
        );
        assert_eq!(
            infer_render_type(&sfn("reverse", vec![RenderExpr::List(vec![lit_int(1)])])),
            Some(RenderType::List)
        );
        // coalesce over strings → String
        assert_eq!(
            infer_render_type(&sfn("coalesce", vec![lit_str("a"), lit_str("b")])),
            Some(RenderType::String)
        );
        // no args → None (safe)
        assert_eq!(infer_render_type(&sfn("reverse", vec![])), None);
    }

    #[test]
    fn element_of_arg0_is_none() {
        // head/last return the element type — unknowable at render time.
        assert_eq!(
            infer_render_type(&sfn("head", vec![RenderExpr::List(vec![lit_int(1)])])),
            None
        );
        assert_eq!(
            infer_render_type(&sfn("last", vec![RenderExpr::List(vec![lit_int(1)])])),
            None
        );
    }

    #[test]
    fn unknown_function_is_none() {
        assert_eq!(
            infer_render_type(&sfn("someUnknownFn", vec![lit_int(1)])),
            None
        );
    }

    #[test]
    fn comparison_operators_are_boolean() {
        let op = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::GreaterThan,
            operands: vec![lit_int(3), lit_int(2)],
        });
        assert_eq!(infer_render_type(&op), Some(RenderType::Boolean));
    }

    #[test]
    fn arithmetic_all_numeric_is_number_else_none() {
        // int + int → Number
        let numeric = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![lit_int(1), lit_int(2)],
        });
        assert_eq!(infer_render_type(&numeric), Some(RenderType::Number));

        // string + string → NOT numeric → None (the concat gate, not this arm,
        // handles string +).
        let strings = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![lit_str("a"), lit_str("b")],
        });
        assert_eq!(infer_render_type(&strings), None);
    }

    #[test]
    fn arithmetic_promotes_to_float_when_any_operand_is_float() {
        // #1055: Cypher numeric promotion. Any Float operand → Float result, so
        // `toString` keeps the `.0`. Covers the common negative-whole-float case:
        // `-2.0` lowers to `0 - 2.0` (Subtraction), which must classify Float so
        // `toString(-2.0)` = "-2.0", not "-2".
        let neg_float = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Subtraction,
            operands: vec![lit_int(0), lit_float(2.0)],
        });
        assert_eq!(infer_render_type(&neg_float), Some(RenderType::Float));

        let mixed = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![lit_int(1), lit_float(2.0)],
        });
        assert_eq!(infer_render_type(&mixed), Some(RenderType::Float));

        // Exponentiation is always Float in Neo4j (`2^3` = 8.0), even int^int.
        let pow = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Exponentiation,
            operands: vec![lit_int(2), lit_int(3)],
        });
        assert_eq!(infer_render_type(&pow), Some(RenderType::Float));

        // int - int (non-exponent) stays unresolved Number — subtype unknown but
        // numeric, and `toString` must NOT add `.0` (`toString(5-2)` = "3").
        let int_sub = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Subtraction,
            operands: vec![lit_int(5), lit_int(2)],
        });
        assert_eq!(infer_render_type(&int_sub), Some(RenderType::Number));
    }

    #[test]
    fn bare_expr_shapes_are_none() {
        // Bare column / alias / parameter / raw carry no resolvable type here.
        assert_eq!(
            infer_render_type(&RenderExpr::TableAlias(TableAlias("a".into()))),
            None
        );
        assert_eq!(infer_render_type(&RenderExpr::Parameter("p".into())), None);
        assert_eq!(infer_render_type(&RenderExpr::Raw("x".into())), None);
    }

    #[test]
    fn property_access_without_context_is_none() {
        // Outside a task-local QueryContext (no schema), a property access can't
        // be typed — must degrade to None, never panic.
        let pa = RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("u".into()),
            column: PropertyValue::Column("name".into()),
        });
        assert_eq!(infer_render_type(&pa), None);
    }

    #[test]
    fn infer_list_element_type_uniform_and_mixed() {
        use crate::render_plan::render_expr::Literal;
        let s = |v: &str| RenderExpr::Literal(Literal::String(v.to_string()));
        let i = |v: i64| RenderExpr::Literal(Literal::Integer(v));

        // Uniform string list → String.
        assert_eq!(
            infer_list_element_type(&RenderExpr::List(vec![s("a"), s("b"), s("c")])),
            Some(RenderType::String)
        );
        // Uniform integer list → Integer.
        assert_eq!(
            infer_list_element_type(&RenderExpr::List(vec![i(1), i(2)])),
            Some(RenderType::Integer)
        );
        // Mixed → None.
        assert_eq!(
            infer_list_element_type(&RenderExpr::List(vec![s("a"), i(1)])),
            None
        );
        // Empty → None.
        assert_eq!(infer_list_element_type(&RenderExpr::List(vec![])), None);
        // A null element doesn't constrain: [null, 'a'] → String.
        assert_eq!(
            infer_list_element_type(&RenderExpr::List(vec![
                RenderExpr::Literal(Literal::Null),
                s("a")
            ])),
            Some(RenderType::String)
        );
        // Non-list → None.
        assert_eq!(infer_list_element_type(&s("a")), None);
    }
}
