# Databricks (DeltaGraph) parity mode for the integration suite

The result-asserting integration tests double as a **ClickHouse ↔ Databricks
parity gate**: set `CG_TEST_BACKEND=databricks` and `execute_cypher()` runs the
SAME Cypher through `cg --dialect databricks` against a Databricks warehouse
instead of the ClickHouse-backed server. Any test whose assertion fails is a real
Databricks dialect/parity gap (or a genuine engine bug affecting both).

## One-time setup
1. Databricks creds in env (see `~/.dbx.env`): `DATABRICKS_HOST`, `DATABRICKS_TOKEN`,
   `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_CATALOG`.
2. Load the Delta fixtures:
   ```
   python3 scripts/load_social_integration_databricks.py
   ```
   (creates `<catalog>.test_integration.{users_test,posts_test,user_follows_test,post_likes_test}`)

## Run
```
CG_TEST_BACKEND=databricks pytest tests/integration/test_optional_match.py -q
```
Only schemas listed in `conftest.DATABRICKS_SCHEMA_FILES` have Delta fixtures;
tests on other schemas `skip` in this mode. Start with `social_integration`.

## No-pytest smoke check
`python3 scripts/social_integration_parity_check.py` runs representative patterns
against both backends and prints a parity table (no pytest needed).

## Scope / status (POC)
- ✅ `social_integration` fixtures + backend switch wired; 8/10 sample patterns MATCH.
- Known findings from the smoke check:
  - `MATCH (u)-[:AUTHORED]->(p)` returns a duplicate row on ClickHouse (denormalized
    edge over-count) — DBX is correct.
  - 2-hop with an anonymous intermediate `()-[:FOLLOWS]->()-[:FOLLOWS]->()` errors on
    Databricks — dialect gap.
- TODO: load Delta fixtures for the other 18 schemas; mark CH-dialect-specific
  tests (sql_only string assertions, CH funcs) `@pytest.mark.clickhouse_only` to
  skip cleanly in databricks mode.
