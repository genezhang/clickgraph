//! Re-exports the Cypher CREATE parser from `clickgraph_embedded::cypher_loader`.
//!
//! The parser has been promoted to the embedded crate so it is available to all users.
//! This shim preserves backwards compatibility for tck.rs which still `mod create_parser`s.

pub use clickgraph_embedded::cypher_loader::{
    parse_create_block, EdgeDir, ParsedCreate, ParsedEdge, ParsedNode, PropValue,
};

//! Parser for Cypher CREATE statements as they appear in openCypher TCK feature files.
//!
//! Handles the subset of CREATE syntax used in TCK scenarios:
//! - Labeled and unlabeled nodes with optional properties
//! - Directed and undirected edges between node variables
//! - Multi-statement blocks (multiple CREATE statements in one docstring)
//! - Comma-separated patterns within a single CREATE
//! - Variable references to previously-created nodes

use std::collections::HashMap;

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
    /// Return a string label for type inference: "int", "float", "bool", "string".
    pub fn type_hint(&self) -> &'static str {
        match self {
            PropValue::Int(_) => "int",
            PropValue::Float(_) => "float",
            PropValue::Bool(_) => "bool",
            PropValue::Str(_) | PropValue::Null => "string",
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
    /// Variable name of the source node (may be a synthetic `__N` for anon nodes).
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

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Parse an entire `having_executed:` docstring which may contain multiple
/// CREATE statements (separated by semicolons or occurring as adjacent lines).
///
/// Variables accumulate across statements so that e.g.:
/// ```text
/// CREATE (a:A), (b:B)
/// CREATE (a)-[:R]->(b)
/// ```
/// correctly links the nodes created in the first statement.
///
/// `var_map` is passed in from the calling World so that variables from
/// *previous* `having_executed:` steps in the same scenario are also in scope.
pub fn parse_create_block(input: &str, var_map: &mut HashMap<String, String>) -> ParsedCreate {
    let mut result = ParsedCreate::default();
    let mut anon_counter = 0usize;

    // Split on CREATE keyword boundaries (case-insensitive).
    // Each chunk is the pattern-list that follows one CREATE keyword.
    let stmts = split_on_create(input);

    for stmt in stmts {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        parse_single_create(trimmed, &mut result, var_map, &mut anon_counter);
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
    let bytes = input.as_bytes();
    let mut pos = 0;

    while pos < bytes.len() {
        // Find next "CREATE" (whole-word, case-insensitive)
        if let Some(rel) = find_create_keyword(&upper[pos..]) {
            let start = pos + rel + 6; // skip "CREATE"
            pos = start;
            // The pattern continues until the next top-level CREATE or end of input
            if let Some(next_rel) = find_create_keyword(&upper[pos..]) {
                result.push(input[start..pos + next_rel].to_string());
                // don't advance pos — the outer loop will find it
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

/// Find the byte offset of the next `CREATE` keyword that is not inside quotes
/// or parentheses. Returns `None` if not found.
fn find_create_keyword(haystack: &str) -> Option<usize> {
    let bytes = haystack.as_bytes();
    let mut i = 0;
    let target = b"CREATE";

    while i + 6 <= bytes.len() {
        // Simple substring match (already uppercase)
        if bytes[i..i + 6] == *target {
            // Verify it is a word boundary
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphabetic();
            let after = bytes.get(i + 6).copied().unwrap_or(b' ');
            let after_ok = !after.is_ascii_alphabetic();
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
    // Tokenize into a flat stream of chars, then parse patterns separated by
    // top-level commas.
    let patterns = split_top_level_commas(input);

    for pattern in patterns {
        let trimmed = pattern.trim();
        if trimmed.is_empty() {
            continue;
        }
        parse_pattern(trimmed, result, var_map, anon_counter);
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

/// Parse a single pattern which is either:
/// - A node: `(var:Label {props})`
/// - A chain: `(node)-[:TYPE {props}]->(node)` or reverse
fn parse_pattern(
    input: &str,
    result: &mut ParsedCreate,
    var_map: &mut HashMap<String, String>,
    anon_counter: &mut usize,
) {
    // A pattern is a sequence of nodes and edge connectors.
    // We walk through and collect (node, edge, node, edge, node, ...)

    let mut pos = 0;
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();

    // Collect chain: alternating node_var and (edge_type, edge_dir)
    let mut chain_nodes: Vec<String> = Vec::new(); // var names

    while pos < len {
        // Skip whitespace
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos >= len {
            break;
        }

        if chars[pos] == '(' {
            // Parse node
            let (node, end) = parse_node(&chars, pos);
            pos = end;

            // Assign var name (synthetic if anonymous)
            let var_name = if let Some(ref v) = node.var {
                v.clone()
            } else {
                // Check if this anonymous node was already defined (by its position in chain)
                // — just give it a fresh synthetic name
                let name = format!("__anon_{}", *anon_counter);
                *anon_counter += 1;
                name
            };

            // Only add to result.nodes if this var is not already in var_map
            // (i.e., it's a reference to an existing node, not a new definition)
            if !var_map.contains_key(&var_name) {
                result.nodes.push(ParsedNode {
                    var: Some(var_name.clone()),
                    label: node.label.clone(),
                    props: node.props.clone(),
                });
                // Reserve the slot in var_map (actual ID assigned at load time)
                var_map.insert(var_name.clone(), String::new());
            }

            chain_nodes.push(var_name);
        } else if chars[pos] == '-' || chars[pos] == '<' {
            // Edge connector: -[:T]-> or <-[:T]- or -[:T]-
            let (rel_type, rel_props, dir, end) = parse_edge_connector(&chars, pos);
            pos = end;

            // The edge will be fully constructed once we have both nodes.
            // We'll add it after parsing the next node.
            // For now, just store it temporarily.
            // We need to track: prev_node is chain_nodes.last(), next_node is the next '('

            // Look ahead to get the next node
            while pos < len && chars[pos].is_whitespace() {
                pos += 1;
            }

            if pos < len && chars[pos] == '(' {
                let (next_node, end2) = parse_node(&chars, pos);
                pos = end2;

                let next_var = if let Some(ref v) = next_node.var {
                    v.clone()
                } else {
                    let name = format!("__anon_{}", *anon_counter);
                    *anon_counter += 1;
                    name
                };

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
            // Unexpected character — skip
            pos += 1;
        }
    }
}

/// Parse a node pattern starting at `pos` (which should point to `(`).
/// Returns `(ParsedNode, new_pos)` where `new_pos` is after the closing `)`.
fn parse_node(chars: &[char], start: usize) -> (ParsedNode, usize) {
    assert_eq!(chars[start], '(');
    let mut pos = start + 1;
    let len = chars.len();

    // Skip whitespace
    while pos < len && chars[pos].is_whitespace() {
        pos += 1;
    }

    // Read optional variable name (identifier)
    let var = if pos < len && is_ident_start(chars[pos]) {
        let (name, end) = read_identifier(chars, pos);
        pos = end;
        Some(name)
    } else {
        None
    };

    // Skip whitespace
    while pos < len && chars[pos].is_whitespace() {
        pos += 1;
    }

    // Read optional labels (:Label1:Label2 — take only first)
    let mut label: Option<String> = None;
    while pos < len && chars[pos] == ':' {
        pos += 1; // skip ':'
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        let (lbl, end) = read_identifier(chars, pos);
        pos = end;
        if label.is_none() && !lbl.is_empty() {
            label = Some(lbl);
        }
        // Skip whitespace before possible next ':'
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
    }

    // Skip whitespace
    while pos < len && chars[pos].is_whitespace() {
        pos += 1;
    }

    // Read optional property map
    let props = if pos < len && chars[pos] == '{' {
        let (p, end) = parse_prop_map(chars, pos);
        pos = end;
        p
    } else {
        HashMap::new()
    };

    // Skip whitespace
    while pos < len && chars[pos].is_whitespace() {
        pos += 1;
    }

    // Consume closing ')'
    if pos < len && chars[pos] == ')' {
        pos += 1;
    }

    (ParsedNode { var, label, props }, pos)
}

/// Parse an edge connector: `-[..]->`  `<-[..]-`  `-[..]-`
/// starting at `pos` which is `-` or `<`.
/// Returns `(rel_type, props, direction, new_pos)`.
fn parse_edge_connector(
    chars: &[char],
    start: usize,
) -> (String, HashMap<String, PropValue>, EdgeDir, usize) {
    let mut pos = start;
    let len = chars.len();

    // Determine if this is an incoming edge (`<-`)
    let starts_with_lt = pos < len && chars[pos] == '<';
    if starts_with_lt {
        pos += 1; // skip '<'
    }

    // Expect '-'
    if pos < len && chars[pos] == '-' {
        pos += 1;
    }

    // Expect optional '[' for relationship type
    let mut rel_type = String::new();
    let mut rel_props: HashMap<String, PropValue> = HashMap::new();

    if pos < len && chars[pos] == '[' {
        pos += 1; // skip '['
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        // Optional ':'
        if pos < len && chars[pos] == ':' {
            pos += 1;
        }
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        // Rel type (uppercase identifier)
        if pos < len && is_ident_start(chars[pos]) {
            let (t, end) = read_identifier(chars, pos);
            rel_type = t;
            pos = end;
        }
        // Skip whitespace
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        // Optional pipe-separated types (just take first, skip rest)
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
        // Skip whitespace
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        // Optional variable name for relationship (ignore it)
        if pos < len && is_ident_start(chars[pos]) && chars[pos] != '{' {
            // check if it's actually a property map start or variable
            let (_, end) = read_identifier(chars, pos);
            pos = end;
            while pos < len && chars[pos].is_whitespace() {
                pos += 1;
            }
        }
        // Optional property map
        if pos < len && chars[pos] == '{' {
            let (p, end) = parse_prop_map(chars, pos);
            rel_props = p;
            pos = end;
        }
        // Skip to ']'
        while pos < len && chars[pos] != ']' {
            pos += 1;
        }
        if pos < len && chars[pos] == ']' {
            pos += 1; // skip ']'
        }
    }

    // Now parse the arrow: `->`  `->` or `-`
    // Expect '-'
    if pos < len && chars[pos] == '-' {
        pos += 1;
    }

    let dir = if starts_with_lt {
        EdgeDir::In
    } else if pos < len && chars[pos] == '>' {
        pos += 1; // skip '>'
        EdgeDir::Out
    } else {
        EdgeDir::Undirected
    };

    (rel_type, rel_props, dir, pos)
}

/// Parse a property map `{key: val, key2: val2}`.
/// `start` points to the opening `{`.
fn parse_prop_map(chars: &[char], start: usize) -> (HashMap<String, PropValue>, usize) {
    let mut pos = start + 1; // skip '{'
    let len = chars.len();
    let mut map = HashMap::new();

    loop {
        // Skip whitespace
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos >= len || chars[pos] == '}' {
            if pos < len {
                pos += 1; // skip '}'
            }
            break;
        }
        // Skip comma
        if chars[pos] == ',' {
            pos += 1;
            continue;
        }
        // Read key (identifier or quoted string)
        let (key, end) = read_identifier(chars, pos);
        pos = end;

        // Skip whitespace
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        // Expect ':'
        if pos < len && chars[pos] == ':' {
            pos += 1;
        }
        // Skip whitespace
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        // Read value
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
            // String literal
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
                pos += 1; // closing quote
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
            // Number
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
                // Optional exponent
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
                let f = num_str.parse::<f64>().unwrap_or(0.0);
                (PropValue::Float(f), pos)
            } else {
                let i = num_str.parse::<i64>().unwrap_or(0);
                (PropValue::Int(i), pos)
            }
        }
        '[' => {
            // List literal — capture as a JSON-compatible string so it can be stored
            // in a Nullable(String) column and later parsed back to a list in the formatter.
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
            // Capture the raw content and convert to JSON (handle single-quoted strings).
            let raw: String = chars[list_start..pos].iter().collect();
            // Replace Cypher single-quoted strings with JSON double-quoted strings.
            let json_str = cypher_list_to_json(&raw);
            (PropValue::Str(json_str), pos)
        }
        _ => {
            // Unknown — skip to next , or }
            while pos < len && chars[pos] != ',' && chars[pos] != '}' {
                pos += 1;
            }
            (PropValue::Null, pos)
        }
    }
}

// ---------------------------------------------------------------------------
// List literal helpers
// ---------------------------------------------------------------------------

/// Convert a Cypher list literal like `[1, 'hello', null]` to a JSON array string.
/// Cypher uses single-quoted strings; JSON requires double-quoted strings.
fn cypher_list_to_json(raw: &str) -> String {
    // Replace single-quoted string literals with double-quoted JSON strings.
    // Simple approach: scan char-by-char replacing 'str' with "str".
    let chars: Vec<char> = raw.chars().collect();
    let len = chars.len();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0;
    while i < len {
        if chars[i] == '\'' {
            // Single-quoted string: collect until closing quote.
            out.push('"');
            i += 1;
            while i < len && chars[i] != '\'' {
                if chars[i] == '\\' && i + 1 < len {
                    // Pass through escape sequences
                    out.push(chars[i]);
                    i += 1;
                    out.push(chars[i]);
                } else if chars[i] == '"' {
                    // Escape double-quotes inside the string
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
            } // skip closing quote
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

/// Read a Cypher identifier (letters, digits, underscores).
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

/// Read a word (identifier-like, stops at whitespace, comma, `{`, `}`, `(`, `)`, `[`, `]`).
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
}
