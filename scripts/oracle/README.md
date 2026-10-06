# Neo4j result oracle (P-4c S1)

ClickGraph's answers are checked against **Neo4j** loaded with the same
logical graph (`docs/design/EXPLICIT_SCOPE.md` §6). SQL text is not the
acceptance test for P-4c slices; rows are.

| File | Role |
|---|---|
| `graph_loader.py` | builds a schema's logical graph from its YAML + ClickHouse tables and loads it into Neo4j (standard layout today; other layouts raise `Unsupported`) |
| `compare.py` | every normalization rule between Neo4j's and ClickGraph's results |
| `run_corpus.py` | runs corpus queries on both, classifies them, writes the result goldens |
| `triage_rules.py` | categories for the known-wrong entries |
| `test_compare.py` | offline unit tests for the comparator rules |

Generated files (commit them):
- `tests/corpus/expected/<schema>/<name>.json`: Neo4j's rows for each query.
- `tests/corpus/expected/scorecard.json`: today's verdict for each query.
- `tests/corpus/expected/triage.json`: the category and issue for each known-wrong entry (tracking issue #1316).

`tests/integration/test_neo4j_result_goldens.py` checks ClickGraph against the
goldens in the live suite, with no Neo4j needed there. A known-wrong entry
(MISMATCH / CG_ERROR) is a strict xfail. When a fix makes one correct, rerun
the scorecard so the flip is recorded.

## Regenerate

```bash
docker run -d --name cg-neo4j-oracle -p 17474:7474 -p 17687:7687 -e NEO4J_AUTH=none neo4j:5-community
cargo build --bin clickgraph
python3 scripts/oracle/run_corpus.py --schemas social_integration standard --write-expected
```

The script starts its own ClickGraph (port 17475, no Bolt) and registers each
corpus schema under a private name (`oracle__<schema>`). Schemas are resolved
through `tests/corpus/schema_map.json`, the same way the corpus sweep resolves
them. ClickHouse must hold the fixture data. Schemas whose tables are missing
(for example `test_fixtures`, created only while pytest runs) are reported and
skipped.
