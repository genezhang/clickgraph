# Spec: Labels, types and element ids spelled as SQL literals (#1513)

Status: IMPLEMENTED (#1513).
Owner: hari2353
Grounding: issue #1513 (found by the review of #1511).

## Problem

`Literal::String` holds Cypher literal text (its escapes undecoded) and is
printed by `cypher_string_to_sql_literal`, which decodes those escapes for
SQL. Several read-path lowering sites build a `Literal::String` from a
decoded schema name (a Rust `String`), so a label or type carrying a
backslash (e.g. `P\nQ` in the YAML) is re-read as an escape and silently
matches nothing.

The producer side (scan label/type columns) emits the name through `string()`,
which also treats the decoded name as Cypher text, so the column holds the
escaped form while the comparison holds the unescaped form — they disagree.
Every producer and consumer of these literals must change together.

## Sites

Producer (mod.rs):
- scan label column
- filter label comparison
- labels[0] comparison
- scan rel_type, from_node, to_node columns
- walk node label
- node_labels list

Consumer (expr.rs):
- Scan::Labels arms label comparison
- Scan::Rels arms rel_type comparison
- node_labels list comprehension

Consumer (subquery.rs):
- Members fields

## Fix

Build every `Literal::String` from a decoded schema name via
`utils::sql_literal::value_as_cypher_text(name)`, so the literal holds
proper Cypher text and `cypher_string_to_sql_literal` prints it correctly.

`value::string` (the text of element ids, keys and graph values) takes a
decoded text and escapes it itself, as do the `string` closures of
`rel_union_of`, `union_differs` and `definition_identity`.
**Their callers pass the decoded name and never escape it again**: escaping
twice spells `P\\\\nQ` for the name `P\nQ`, which no comparison reads back.
The sites that put a name straight into a `Literal::String` (the comparisons
above, `definition_is`, the `labels()` / `type()` / `label()` values, the
`written` label and type filters) call `value_as_cypher_text` themselves.

## Backward / forward compat

- Schemas without backslashes in names: `value_as_cypher_text` is a no-op,
  so SQL output is byte-identical (ratchet green).
- Schemas with backslashes: previously wrong (silent non-match); now correct.

## Success criteria

1. A label containing a backslash escape in YAML matches correctly in
   `MATCH (n:label)` and `RETURN n:label`.
2. Corpus goldens byte-identical (ratchet).
3. Regression test:
   `a_name_with_a_backslash_is_spelled_alike_by_its_columns_and_comparisons`
   (`lower/tests.rs`), over a schema whose label and type hold `\n` (a
   backslash and an `n`): the SQL of an untyped relationship pattern spells
   each name once, as the text it is, in the columns and in the comparisons.
   Live: an untyped `MATCH (a)-[r]->(b)` keeps its rows, `type(r)` and the
   relationship's element ids agree with the typed pattern's and the nodes'.
