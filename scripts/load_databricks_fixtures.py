#!/usr/bin/env python3
"""Load ClickHouse test fixtures into Databricks/Delta for the parity harness.

Mirrors the CH fixtures defined in tests/integration/conftest.py into Delta tables
so the pytest integration suite can run against Databricks
(`CG_TEST_BACKEND=databricks`). Schema-driven: add an entry to SCHEMAS to cover a
new graph schema — the DDL is auto-translated from the conftest CREATE TABLE and
the row VALUES are reused verbatim (CH tuples are valid Spark SQL VALUES).

Usage:
    python3 scripts/load_databricks_fixtures.py [schema_name ...]   # default: all
Env (from ~/.dbx.env): DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID,
DATABRICKS_CATALOG.
"""
import json
import os
import re
import sys
import urllib.request

CONFTEST = os.path.join(os.path.dirname(__file__), "..", "tests", "integration", "conftest.py")

# schema_name -> fully-qualified CH tables (db.table) to mirror into Delta.
SCHEMAS = {
    "social_integration": [
        "test_integration.users_test", "test_integration.posts_test",
        "test_integration.user_follows_test", "test_integration.post_likes_test",
    ],
    "group_membership": [
        "test_integration.gm_users", "test_integration.gm_groups",
        "test_integration.gm_memberships",
    ],
    "social_polymorphic": [
        "brahmand.users_bench", "brahmand.posts_bench", "brahmand.interactions",
    ],
}

# ClickHouse -> Spark/Delta type map (whole-word).
TYPE_MAP = [
    (r"\bU?Int(8|16|32|64)\b", "BIGINT"),
    (r"\bDateTime\b", "TIMESTAMP"),
    (r"\bDate\b", "DATE"),
    (r"\bFloat32\b", "FLOAT"),
    (r"\bFloat64\b", "DOUBLE"),
    (r"\bString\b", "STRING"),
    (r"\bBool\b", "BOOLEAN"),
]


def host():
    h = os.environ["DATABRICKS_HOST"].removeprefix("http://").removeprefix("https://").rstrip("/")
    return "https://" + h


def exec_sql(statement, catalog):
    body = json.dumps({
        "warehouse_id": os.environ["DATABRICKS_WAREHOUSE_ID"],
        "catalog": catalog, "statement": statement, "wait_timeout": "50s",
    }).encode()
    req = urllib.request.Request(f"{host()}/api/2.0/sql/statements", data=body, headers={
        "Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
        "Content-Type": "application/json",
    }, method="POST")
    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.load(r)
    if resp.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"{resp.get('status')}\n  SQL: {statement[:140]}")
    return resp


def _conftest():
    return open(CONFTEST).read()


def translate_ddl(table):
    """Extract the CREATE TABLE column list for `db.table` from conftest and
    translate CH types -> Spark. Returns the parenthesised column list."""
    src = _conftest()
    m = re.search(rf"CREATE TABLE IF NOT EXISTS {re.escape(table)}\s*\((.*?)\)\s*ENGINE", src, re.S)
    if not m:
        raise RuntimeError(f"no CREATE TABLE for {table}")
    cols = m.group(1)
    for pat, repl in TYPE_MAP:
        cols = re.sub(pat, repl, cols)
    return "(" + cols.strip().rstrip(",") + "\n)"


def extract_values(table):
    src = _conftest()
    m = re.search(rf"INSERT INTO {re.escape(table)} VALUES(.*?)\"\"\"", src, re.S)
    if not m:
        raise RuntimeError(f"no INSERT for {table}")
    return m.group(1).strip().rstrip(",")


def main():
    catalog = os.environ.get("DATABRICKS_CATALOG", "workspace")
    want = sys.argv[1:] or list(SCHEMAS)
    for schema_name in want:
        if schema_name not in SCHEMAS:
            print(f"! unknown schema '{schema_name}' (known: {', '.join(SCHEMAS)})")
            continue
        print(f"[{schema_name}] loading into {catalog}.*")
        for fq in SCHEMAS[schema_name]:
            db, table = fq.split(".", 1)
            exec_sql(f"CREATE SCHEMA IF NOT EXISTS {db}", catalog)
            exec_sql(f"DROP TABLE IF EXISTS {db}.{table}", catalog)
            exec_sql(f"CREATE TABLE {db}.{table} {translate_ddl(fq)} USING DELTA", catalog)
            exec_sql(f"INSERT INTO {db}.{table} VALUES {extract_values(fq)}", catalog)
            n = exec_sql(f"SELECT count(*) FROM {db}.{table}", catalog)["result"]["data_array"][0][0]
            print(f"    {fq:38} -> {n} rows")
    print("Done.")


if __name__ == "__main__":
    main()
