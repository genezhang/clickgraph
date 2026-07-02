# Databricks (DeltaGraph) parity mode for the integration suite

The result-asserting integration tests double as a **ClickHouse ↔ Databricks
parity gate**: set `CG_TEST_BACKEND=databricks` and `execute_cypher()` runs the
SAME Cypher through `cg --dialect databricks` against a Databricks warehouse
instead of the ClickHouse-backed server. Any test whose assertion fails is a real
Databricks dialect/parity gap (or a genuine engine bug affecting both).

## One-time setup
1. Databricks creds in env (see `~/.dbx.env`): `DATABRICKS_HOST`, `DATABRICKS_TOKEN`,
   `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_CATALOG`.
2. Load the Delta fixtures (schema-driven; loads all mapped schemas, or name specific ones):
   ```
   python3 scripts/load_databricks_fixtures.py                 # all mapped schemas
   python3 scripts/load_databricks_fixtures.py social_integration
   ```
   Currently covers `social_integration`, `group_membership`, and `social_polymorphic`. To add a schema,
   append it to `SCHEMAS` in that script (list its CH tables) AND to
   `conftest.DATABRICKS_SCHEMA_FILES` (schema_name → YAML) — the DDL is
   auto-translated from conftest and row VALUES reused verbatim.

## Run
```
CG_TEST_BACKEND=databricks pytest tests/integration/test_optional_match.py -q
```
Only schemas listed in `conftest.DATABRICKS_SCHEMA_FILES` have Delta fixtures;
tests on other schemas `skip` in this mode.

## No-pytest smoke check
`python3 scripts/social_integration_parity_check.py` runs representative patterns
against both backends and prints a parity table (no pytest needed).

## Scope / status (POC)
- ✅ `social_integration` fixtures + backend switch wired; smoke check is **10/10 MATCH**
  (full CH↔Databricks parity on the sampled patterns).
- Two methodology learnings the POC surfaced (neither an engine bug):
  1. **Fixture hygiene** — the CH loaders use `CREATE TABLE IF NOT EXISTS` + `INSERT`
     with no reset, so the shared `Memory` tables accumulate across runs (posts_test
     was found at 52 rows vs the canonical 20). For a valid parity comparison BOTH
     backends must load the SAME canonical fixtures; the CH side needs a DROP/TRUNCATE
     before load. (An early false "AUTHORED duplicate" was just stale CH data.)
  2. **Dialect leniency divergence (minor)** — `RETURN DISTINCT x.a AS x ORDER BY x.b`
     works on ClickHouse (resolves `x.b` to the table alias) but errors on Databricks
     (the output alias `x` shadows the table alias under `SELECT DISTINCT`). Edge case;
     ClickGraph could harden the ORDER-BY qualification, low priority.
- ✅ Generalized loader (`scripts/load_databricks_fixtures.py`) + a 2nd schema
  (`group_membership`) wired and parity-verified.
- ✅ `@pytest.mark.clickhouse_only` marker registered; marked tests auto-skip when
  `CG_TEST_BACKEND=databricks`. Apply it to tests that assert CH-specific behaviour
  (CH functions, CH error messages) once a full databricks run surfaces them.
  (Note: `sql_only` string-assertion tests POST to the CH server directly rather
  than via `execute_cypher`, so they still run/pass against CH in this mode and do
  NOT need the marker — they simply don't contribute Databricks signal.)
- TODO: load Delta fixtures for the remaining schemas (extend `SCHEMAS` +
  `DATABRICKS_SCHEMA_FILES`); wire the parity sweep + a databricks-mode suite run
  into CI.
