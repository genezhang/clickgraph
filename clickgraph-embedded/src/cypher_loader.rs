//! Parser and loader for Cypher CREATE statements.
//!
//! Provides two levels of API:
//!
//! **Low-level (parser only):** Parse a Cypher CREATE block into structured
//! [`ParsedCreate`] data that you can inspect or transform before loading.
//!
//! **High-level (connection helper):** Use [`Connection::load_cypher_create`](crate::Connection::load_cypher_create)
//! to parse and insert data in one call, returning [`LoadStats`].
//!
//! # Supported syntax
//!
//! - Labeled and unlabeled nodes with optional properties: `(n:Person {name: 'Alice', age: 30})`
//! - Directed and undirected edges: `-[:KNOWS {since: 2020}]->`
//! - Multi-statement blocks (multiple CREATE statements separated by newlines or semicolons)
//! - Comma-separated patterns within a single CREATE
//! - Variable references to previously-created nodes across statements
//!
//! # Example
//!
//! ```no_run
//! use std::collections::HashMap;
//! use clickgraph_embedded::cypher_loader::parse_create_block;
//!
//! let mut vars: HashMap<String, String> = HashMap::new();
//! let data = parse_create_block(
//!     "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})",
//!     &mut vars,
//! );
//! assert_eq!(data.nodes.len(), 2);
//! assert_eq!(data.edges.len(), 1);
//! ```

use std::collections::HashMap;

use crate::Value;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A property value from a Cypher literal.
#[derive(Debug, Clone)]
pub enum PropValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
}

impl PropValue {
    /// Return a type-hint string for schema inference: `"int"`, `"float"`, `"bool"`, `"string"`.
    pub fn type_hint(&self) -> &'static str {
        match self {
            PropValue::Int(_) => "int",
            PropValue::Float(_) => "float",
            PropValue::Bool(_) => "bool",
            PropValue::Str(_) | PropValue::Null => "string",
        }
    }

    /// Convert to an embedded [`Value`].
    pub fn into_value(self) -> Value {
        match self {
            PropValue::Str(s) => Value::String(s),
            PropValue::Int(i) => Value::Int64(i),
            PropValue::Float(f) => Value::Float64(f),
            PropValue::Bool(b) => Value::Bool(b),
            PropValue::Null => Value::Null,
        }
    }

    /// Borrow as an embedded [`Value`] (clones the inner data).
    pub fn to_value(&self) -> Value {
        match self {
            PropValue::Str(s) => Value::String(s.clone()),
            PropValue::Int(i) => Value::Int64(*i),
            PropValue::Float(f) => Value::Float64(*f),
            PropValue::Bool(b) => Value::Bool(*b),
            PropValue::Null => Value::Null,
        }
    }
}

/// A node parsed from a CREATE pattern.
#[derive(Debug, Clone)]
pub struct ParsedNode {
    /// Variable name (e.g. `a` in `(a:Label)`). `None` if anonymous.
    pub var: Option<String>,
    /// First label only. `None` if unlabeled.
    pub label: Option<String>,
    /// Properties from the inline map.
    pub props: HashMap<String, PropValue>,
}

/// Edge direction relative to the chain (left node → right node).
#[derive(Debug, Clone, PartialEq)]
pub enum EdgeDir {
    /// `(a)-[:T]->(b)`
    Out,
    /// `(a)<-[:T]-(b)`
    In,
    /// `(a)-[:T]-(b)`
    Undirected,
}

/// An edge parsed from a CREATE pattern.
#[derive(Debug, Clone)]
pub struct ParsedEdge {
    /// Variable name of the source node (may be a synthetic `__anon_N` for anonymous nodes).
    pub from_var: String,
    /// Variable name of the target node.
    pub to_var: String,
    /// Relationship type (e.g. `KNOWS`).
    pub rel_type: String,
    /// Edge properties.
    pub props: HashMap<String, PropValue>,
    /// Edge direction.
    pub dir: EdgeDir,
}

/// Collected output from parsing one CREATE block.
#[derive(Debug, Default)]
pub struct ParsedCreate {
    pub nodes: Vec<ParsedNode>,
    pub edges: Vec<ParsedEdge>,
}

/// Statistics returned by [`Connection::load_cypher_create`](crate::Connection::load_cypher_create).
#[derive(Debug, Default, Clone)]
pub struct LoadStats {
    /// Number of nodes inserted.
    pub nodes_loaded: usize,
    /// Number of edges inserted.
    pub edges_loaded: usize,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Parse an entire Cypher CREATE block which may contain multiple CREATE
/// statements (separated by newlines or semicolons).
///
/// Variables accumulate across statements so that e.g.:
/// ```text
/// CREATE (a:A), (b:B)
/// CREATE (a)-[:R]->(b)
/// ```
/// correctly links the nodes created in the first statement.
///
/// `var_map` maps variable names to their assigned IDs. Pass the same map
/// across multiple calls to share variables between separate CREATE blocks.
pub fn parse_create_block(input: &str, var_map: &mut HashMap<String, String>) -> ParsedCreate {
    let mut result = ParsedCreate::default();
    let mut anon_counter = 0usize;

    for stmt in split_on_create(input) {
        let trimmed = stmt.trim();
        if !trimmed.is_empty() {
            parse_single_create(trimmed, &mut result, var_map, &mut anon_counter);
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Split `input` into per-CREATE pattern strings.
fn split_on_create(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let upper = input.to_uppercase();
    let mut pos = 0;

    while pos < input.len() {
        if let Some(rel) = find_create_keyword(&upper[pos..]) {
            let start = pos + rel + 6; // skip "CREATE"
            pos = start;
            if let Some(next_rel) = find_create_keyword(&upper[pos..]) {
                result.push(input[start..pos + next_rel].to_string());
            } else {
                result.push(input[start..].to_string());
                break;
            }
        } else {
            break;
        }
    }

    result
}

/// Find the byte offset of the next `CREATE` keyword (whole-word, case-insensitive).
fn find_create_keyword(haystack: &str) -> Option<usize> {
    let bytes = haystack.as_bytes();
    let target = b"CREATE";
    let mut i = 0;

    while i + 6 <= bytes.len() {
        if bytes[i..i + 6] == *target {
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphabetic();
            let after_ok = !bytes
                .get(i + 6)
                .copied()
                .unwrap_or(b' ')
                .is_ascii_alphabetic();
            if before_ok && after_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Parse one CREATE pattern-list (everything after the CREATE keyword).
fn parse_single_create(
    input: &str,
    result: &mut ParsedCreate,
    var_map: &mut HashMap<String, String>,
    anon_counter: &mut usize,
) {
    for pattern in split_top_level_commas(input) {
        let trimmed = pattern.trim();
        if !trimmed.is_empty() {
            parse_pattern(trimmed, result, var_map, anon_counter);
        }
    }
}

/// Split `input` on commas that are not inside `()`, `[]`, or `{}`.
fn split_top_level_commas(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth_paren = 0i32;
    let mut depth_bracket = 0i32;
    let mut depth_curly = 0i32;
    let mut in_string = false;
    let mut string_char = b'"';
    let mut current = String::new();
    let bytes = input.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let ch = bytes[i];
        if in_string {
            if ch == string_char && (i == 0 || bytes[i - 1] != b'\\') {
                in_string = false;
            }
            current.push(ch as char);
        } else {
            match ch {
                b'\'' | b'"' => {
                    in_string = true;
                    string_char = ch;
                    current.push(ch as char);
                }
                b'(' => {
                    depth_paren += 1;
                    current.push('(');
                }
                b')' => {
                    depth_paren -= 1;
                    current.push(')');
                }
                b'[' => {
                    depth_bracket += 1;
                    current.push('[');
                }
                b']' => {
                    depth_bracket -= 1;
                    current.push(']');
                }
                b'{' => {
                    depth_curly += 1;
                    current.push('{');
                }
                b'}' => {
                    depth_curly -= 1;
                    current.push('}');
                }
                b',' if depth_paren == 0 && depth_bracket == 0 && depth_curly == 0 => {
                    parts.push(current.trim().to_string());
                    current = String::new();
                }
                _ => {
                    current.push(ch as char);
                }
            }
        }
        i += 1;
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

/// Parse a single pattern which is either a node or a chain of nodes and edges.
fn parse_pattern(
    input: &str,
    result: &mut ParsedCreate,
    var_map: &mut HashMap<String, String>,
    anon_counter: &mut usize,
) {
    let mut pos = 0;
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut chain_nodes: Vec<String> = Vec::new();

    while pos < len {
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos >= len {
            break;
        }

        if chars[pos] == '(' {
            let (node, end) = parse_node(&chars, pos);
            pos = end;

            let var_name = node.var.clone().unwrap_or_else(|| {
                let name = format!("__anon_{}", *anon_counter);
                *anon_counter += 1;
                name
            });

            if !var_map.contains_key(&var_name) {
                result.nodes.push(ParsedNode {
                    var: Some(var_name.clone()),
                    label: node.label.clone(),
                    props: node.props.clone(),
                });
                var_map.insert(var_name.clone(), String::new());
            }

            chain_nodes.push(var_name);
        } else if chars[pos] == '-' || chars[pos] == '<' {
            let (rel_type, rel_props, dir, end) = parse_edge_connector(&chars, pos);
            pos = end;

            while pos < len && chars[pos].is_whitespace() {
                pos += 1;
            }

            if pos < len && chars[pos] == '(' {
                let (next_node, end2) = parse_node(&chars, pos);
                pos = end2;

                let next_var = next_node.var.clone().unwrap_or_else(|| {
                    let name = format!("__anon_{}", *anon_counter);
                    *anon_counter += 1;
                    name
                });

                if !var_map.contains_key(&next_var) {
                    result.nodes.push(ParsedNode {
                        var: Some(next_var.clone()),
                        label: next_node.label.clone(),
                        props: next_node.props.clone(),
                    });
                    var_map.insert(next_var.clone(), String::new());
                }

                if let Some(from_var) = chain_nodes.last() {
                    let (from, to) = match dir {
                        EdgeDir::Out | EdgeDir::Undirected => (from_var.clone(), next_var.clone()),
                        EdgeDir::In => (next_var.clone(), from_var.clone()),
                    };
                    if !rel_type.is_empty() {
                        result.edges.push(ParsedEdge {
                            from_var: from,
                            to_var: to,
                            rel_type: rel_type.clone(),
                            props: rel_props.clone(),
                            dir: dir.clone(),
                        });
                    }
                }

                chain_nodes.push(next_var);
            }
        } else {
            pos += 1;
        }
    }
}

/// Parse a node pattern starting at `pos` (which should point to `(`).
fn parse_node(chars: &[char], start: usize) -> (ParsedNode, usize) {
    assert_eq!(chars[start], '(');
    let mut pos = start + 1;
    let len = chars.len();

    while pos < len && chars[pos].is_whitespace() {
        pos += 1;
    }

    let var = if pos < len && is_ident_start(chars[pos]) {
        let (name, end) = read_identifier(chars, pos);
        pos = end;
        Some(name)
    } else {
        None
    };

    while pos < len && chars[pos].is_whitespace() {
        pos += 1;
    }

    let mut label: Option<String> = None;
    while pos < len && chars[pos] == ':' {
        pos += 1;
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        let (lbl, end) = read_identifier(chars, pos);
        pos = end;
        if label.is_none() && !lbl.is_empty() {
            label = Some(lbl);
        }
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
    }

    while pos < len && chars[pos].is_whitespace() {
        pos += 1;
    }

    let props = if pos < len && chars[pos] == '{' {
        let (p, end) = parse_prop_map(chars, pos);
        pos = end;
        p
    } else {
        HashMap::new()
    };

    while pos < len && chars[pos].is_whitespace() {
        pos += 1;
    }
    if pos < len && chars[pos] == ')' {
        pos += 1;
    }

    (ParsedNode { var, label, props }, pos)
}

/// Parse an edge connector: `-[..]->`  `<-[..]-`  `-[..]-`
fn parse_edge_connector(
    chars: &[char],
    start: usize,
) -> (String, HashMap<String, PropValue>, EdgeDir, usize) {
    let mut pos = start;
    let len = chars.len();

    let starts_with_lt = pos < len && chars[pos] == '<';
    if starts_with_lt {
        pos += 1;
    }
    if pos < len && chars[pos] == '-' {
        pos += 1;
    }

    let mut rel_type = String::new();
    let mut rel_props: HashMap<String, PropValue> = HashMap::new();

    if pos < len && chars[pos] == '[' {
        pos += 1;
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos < len && chars[pos] == ':' {
            pos += 1;
        }
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos < len && is_ident_start(chars[pos]) {
            let (t, end) = read_identifier(chars, pos);
            rel_type = t;
            pos = end;
        }
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        // Skip pipe-separated alternative types (take first only)
        while pos < len && chars[pos] == '|' {
            pos += 1;
            while pos < len && chars[pos].is_whitespace() {
                pos += 1;
            }
            let (_, end) = read_identifier(chars, pos);
            pos = end;
            while pos < len && chars[pos].is_whitespace() {
                pos += 1;
            }
        }
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        // Skip optional relationship variable name
        if pos < len && is_ident_start(chars[pos]) && chars[pos] != '{' {
            let (_, end) = read_identifier(chars, pos);
            pos = end;
            while pos < len && chars[pos].is_whitespace() {
                pos += 1;
            }
        }
        if pos < len && chars[pos] == '{' {
            let (p, end) = parse_prop_map(chars, pos);
            rel_props = p;
            pos = end;
        }
        while pos < len && chars[pos] != ']' {
            pos += 1;
        }
        if pos < len && chars[pos] == ']' {
            pos += 1;
        }
    }

    if pos < len && chars[pos] == '-' {
        pos += 1;
    }

    let dir = if starts_with_lt {
        EdgeDir::In
    } else if pos < len && chars[pos] == '>' {
        pos += 1;
        EdgeDir::Out
    } else {
        EdgeDir::Undirected
    };

    (rel_type, rel_props, dir, pos)
}

/// Parse a property map `{key: val, key2: val2}`.
fn parse_prop_map(chars: &[char], start: usize) -> (HashMap<String, PropValue>, usize) {
    let mut pos = start + 1; // skip '{'
    let len = chars.len();
    let mut map = HashMap::new();

    loop {
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos >= len || chars[pos] == '}' {
            if pos < len {
                pos += 1;
            }
            break;
        }
        if chars[pos] == ',' {
            pos += 1;
            continue;
        }
        let (key, end) = read_identifier(chars, pos);
        pos = end;
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos < len && chars[pos] == ':' {
            pos += 1;
        }
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        let (val, end) = parse_prop_value(chars, pos);
        pos = end;
        if !key.is_empty() {
            map.insert(key, val);
        }
    }

    (map, pos)
}

/// Parse a single property value.
fn parse_prop_value(chars: &[char], start: usize) -> (PropValue, usize) {
    let len = chars.len();
    let mut pos = start;

    if pos >= len {
        return (PropValue::Null, pos);
    }

    match chars[pos] {
        '\'' | '"' => {
            let quote = chars[pos];
            pos += 1;
            let mut s = String::new();
            while pos < len && chars[pos] != quote {
                if chars[pos] == '\\' && pos + 1 < len {
                    pos += 1;
                    match chars[pos] {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        'r' => s.push('\r'),
                        '\\' => s.push('\\'),
                        '\'' => s.push('\''),
                        '"' => s.push('"'),
                        other => {
                            s.push('\\');
                            s.push(other);
                        }
                    }
                } else {
                    s.push(chars[pos]);
                }
                pos += 1;
            }
            if pos < len {
                pos += 1;
            }
            (PropValue::Str(s), pos)
        }
        't' | 'T' if read_word(chars, pos).0.to_lowercase() == "true" => {
            let (_, end) = read_word(chars, pos);
            (PropValue::Bool(true), end)
        }
        'f' | 'F' if read_word(chars, pos).0.to_lowercase() == "false" => {
            let (_, end) = read_word(chars, pos);
            (PropValue::Bool(false), end)
        }
        'n' | 'N' if read_word(chars, pos).0.to_lowercase() == "null" => {
            let (_, end) = read_word(chars, pos);
            (PropValue::Null, end)
        }
        '-' | '0'..='9' => {
            let mut num_str = String::new();
            if chars[pos] == '-' {
                num_str.push('-');
                pos += 1;
            }
            while pos < len && (chars[pos].is_ascii_digit() || chars[pos] == '_') {
                if chars[pos] != '_' {
                    num_str.push(chars[pos]);
                }
                pos += 1;
            }
            let is_float = pos < len && chars[pos] == '.';
            if is_float {
                num_str.push('.');
                pos += 1;
                while pos < len && chars[pos].is_ascii_digit() {
                    num_str.push(chars[pos]);
                    pos += 1;
                }
                if pos < len && (chars[pos] == 'e' || chars[pos] == 'E') {
                    num_str.push(chars[pos]);
                    pos += 1;
                    if pos < len && (chars[pos] == '+' || chars[pos] == '-') {
                        num_str.push(chars[pos]);
                        pos += 1;
                    }
                    while pos < len && chars[pos].is_ascii_digit() {
                        num_str.push(chars[pos]);
                        pos += 1;
                    }
                }
                (PropValue::Float(num_str.parse::<f64>().unwrap_or(0.0)), pos)
            } else {
                (PropValue::Int(num_str.parse::<i64>().unwrap_or(0)), pos)
            }
        }
        '[' => {
            // List literal — capture as a JSON-compatible string.
            let list_start = pos;
            let mut depth = 0;
            while pos < len {
                match chars[pos] {
                    '[' => depth += 1,
                    ']' => {
                        depth -= 1;
                        if depth == 0 {
                            pos += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                pos += 1;
            }
            let raw: String = chars[list_start..pos].iter().collect();
            (PropValue::Str(cypher_list_to_json(&raw)), pos)
        }
        _ => {
            while pos < len && chars[pos] != ',' && chars[pos] != '}' {
                pos += 1;
            }
            (PropValue::Null, pos)
        }
    }
}

/// Convert a Cypher list literal `[1, 'hello', null]` to a JSON array string.
fn cypher_list_to_json(raw: &str) -> String {
    let chars: Vec<char> = raw.chars().collect();
    let len = chars.len();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0;

    while i < len {
        if chars[i] == '\'' {
            out.push('"');
            i += 1;
            while i < len && chars[i] != '\'' {
                if chars[i] == '\\' && i + 1 < len {
                    out.push(chars[i]);
                    i += 1;
                    out.push(chars[i]);
                } else if chars[i] == '"' {
                    out.push('\\');
                    out.push('"');
                } else {
                    out.push(chars[i]);
                }
                i += 1;
            }
            out.push('"');
            if i < len {
                i += 1;
            }
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Character / identifier utilities
// ---------------------------------------------------------------------------

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn read_identifier(chars: &[char], start: usize) -> (String, usize) {
    let mut pos = start;
    let len = chars.len();
    let mut s = String::new();
    while pos < len && is_ident_char(chars[pos]) {
        s.push(chars[pos]);
        pos += 1;
    }
    (s, pos)
}

fn read_word(chars: &[char], start: usize) -> (String, usize) {
    let mut pos = start;
    let len = chars.len();
    let mut s = String::new();
    while pos < len {
        let c = chars[pos];
        if c.is_whitespace() || matches!(c, ',' | '{' | '}' | '(' | ')' | '[' | ']') {
            break;
        }
        s.push(c);
        pos += 1;
    }
    (s, pos)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_map() -> HashMap<String, String> {
        HashMap::new()
    }

    #[test]
    fn test_simple_node() {
        let mut vm = empty_map();
        let result = parse_create_block("CREATE (:A {num: 42})", &mut vm);
        assert_eq!(result.nodes.len(), 1);
        assert_eq!(result.nodes[0].label.as_deref(), Some("A"));
        match result.nodes[0].props.get("num") {
            Some(PropValue::Int(42)) => {}
            other => panic!("expected Int(42), got {:?}", other),
        }
    }

    #[test]
    fn test_labeled_chain() {
        let mut vm = empty_map();
        let result = parse_create_block("CREATE (a:A {n: 1})-[:KNOWS]->(b:B {n: 2})", &mut vm);
        assert_eq!(result.nodes.len(), 2);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].rel_type, "KNOWS");
        assert_eq!(result.edges[0].dir, EdgeDir::Out);
    }

    #[test]
    fn test_multi_statement() {
        let mut vm = empty_map();
        let result = parse_create_block("CREATE (a:A), (b:B)\nCREATE (a)-[:REL]->(b)", &mut vm);
        assert_eq!(result.nodes.len(), 2);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].rel_type, "REL");
    }

    #[test]
    fn test_comma_separated_chains() {
        let mut vm = empty_map();
        let result =
            parse_create_block("CREATE (:A)-[:T1]->(:B),\n       (:B)-[:T2]->(:A)", &mut vm);
        assert_eq!(result.nodes.len(), 4);
        assert_eq!(result.edges.len(), 2);
    }

    #[test]
    fn test_string_prop() {
        let mut vm = empty_map();
        let result = parse_create_block("CREATE (:B {name: 'hello'})", &mut vm);
        assert_eq!(result.nodes.len(), 1);
        match result.nodes[0].props.get("name") {
            Some(PropValue::Str(s)) if s == "hello" => {}
            other => panic!("expected Str(hello), got {:?}", other),
        }
    }

    #[test]
    fn test_into_value() {
        assert!(matches!(PropValue::Int(5).into_value(), Value::Int64(5)));
        assert!(matches!(
            PropValue::Bool(true).into_value(),
            Value::Bool(true)
        ));
        assert!(matches!(PropValue::Null.into_value(), Value::Null));
    }
}
