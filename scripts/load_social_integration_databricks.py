#!/usr/bin/env python3
"""Load the `social_integration` test fixtures into Databricks/Delta.

Mirrors tests/integration/conftest.py::load_social_integration_data (ClickHouse)
into Delta tables under `<catalog>.test_integration.*` so the pytest integration
suite can run against Databricks via `CG_TEST_BACKEND=databricks`.

The row VALUES are extracted verbatim from conftest.py (CH tuples are valid Spark
SQL VALUES); only the DDL types are translated (UInt32->BIGINT, UInt8->INT,
String->STRING, Date->DATE, ENGINE=Memory -> USING DELTA).

Env (from ~/.dbx.env): DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID,
DATABRICKS_CATALOG.
"""
import json
import os
import re
import sys
import urllib.request

CONFTEST = os.path.join(os.path.dirname(__file__), "..", "tests", "integration", "conftest.py")
SCHEMA = "test_integration"

DDL = {
    "users_test": """(
        user_id BIGINT, full_name STRING, email_address STRING, age INT,
        registration_date DATE, is_active INT, country STRING, city STRING
    ) USING DELTA""",
    "posts_test": """(
        post_id BIGINT, post_title STRING, post_content STRING, post_date DATE, author_id BIGINT
    ) USING DELTA""",
    "user_follows_test": """(
        follow_id BIGINT, follower_id BIGINT, followed_id BIGINT, follow_date DATE
    ) USING DELTA""",
    "post_likes_test": """(
        like_id BIGINT, user_id BIGINT, post_id BIGINT, like_date DATE
    ) USING DELTA""",
}


def host():
    h = os.environ["DATABRICKS_HOST"]
    h = h.removeprefix("http://").removeprefix("https://").rstrip("/")
    return "https://" + h


def exec_sql(statement, catalog):
    body = json.dumps({
        "warehouse_id": os.environ["DATABRICKS_WAREHOUSE_ID"],
        "catalog": catalog,
        "statement": statement,
        "wait_timeout": "50s",
    }).encode()
    req = urllib.request.Request(
        f"{host()}/api/2.0/sql/statements",
        data=body,
        headers={
            "Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.load(r)
    state = resp.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"FAILED ({state}): {resp.get('status', {}).get('error', resp)}\n  SQL: {statement[:120]}")
    return resp


def extract_values(table):
    """Pull the `INSERT INTO <schema>.<table> VALUES ...` tuple list from conftest.py."""
    src = open(CONFTEST).read()
    # match up to the closing triple-quote of the command block
    m = re.search(rf"INSERT INTO test_integration\.{table} VALUES(.*?)\"\"\"", src, re.S)
    if not m:
        raise RuntimeError(f"could not find INSERT for {table}")
    return m.group(1).strip().rstrip(",")


def main():
    catalog = os.environ.get("DATABRICKS_CATALOG", "workspace")
    print(f"Loading social_integration fixtures into {catalog}.{SCHEMA} ...")
    exec_sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}", catalog)
    for table, cols in DDL.items():
        exec_sql(f"DROP TABLE IF EXISTS {SCHEMA}.{table}", catalog)
        exec_sql(f"CREATE TABLE {SCHEMA}.{table} {cols}", catalog)
        values = extract_values(table)
        exec_sql(f"INSERT INTO {SCHEMA}.{table} VALUES {values}", catalog)
        cnt = exec_sql(f"SELECT count(*) FROM {SCHEMA}.{table}", catalog)
        n = cnt["result"]["data_array"][0][0]
        print(f"  {table:20} -> {n} rows")
    print("Done.")


if __name__ == "__main__":
    main()
