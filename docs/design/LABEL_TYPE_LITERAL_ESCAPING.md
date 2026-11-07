# Spec: Labels, types and element ids spelled as SQL literals (#1513)

Status: DRAFT — committed with its implementation.
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

## Backward / forward compat

- Schemas without backslashes in names: `value_as_cypher_text` is a no-op,
  so SQL output is byte-identical (ratchet green).
- Schemas with backslashes: previously wrong (silent non-match); now correct.

## Success criteria

1. A label containing a backslash escape in YAML matches correctly in
   `MATCH (n:label)` and `RETURN n:label`.
2. Corpus goldens byte-identical (ratchet).
3. A regression test with a backslash-escape label (using `\n` or `\t`,
   not `\Q` which is not an escape sequence).
