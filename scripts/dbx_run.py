#!/usr/bin/env python3
"""Minimal Databricks Statement Execution API runner (submit + poll).

Reads DATABRICKS_HOST / _WAREHOUSE_ID / _TOKEN / _CATALOG from the env
(source ~/.dbx.env first). Usage:
    dbx_run.py "SELECT 1"                # run one statement
    dbx_run.py --file seed.sql          # run every ;-separated statement
    dbx_run.py --schema ldbc "SELECT *" # set default schema
"""
import json, os, sys, time, urllib.request, urllib.error, argparse

HOST = os.environ["DATABRICKS_HOST"]
WAREHOUSE = os.environ["DATABRICKS_WAREHOUSE_ID"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
CATALOG = os.environ.get("DATABRICKS_CATALOG")
BASE = f"https://{HOST}/api/2.0/sql/statements"


def _req(url, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Authorization", f"Bearer {TOKEN}")
    r.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(r, timeout=60) as resp:
        return json.load(resp)


def run(sql, schema=None):
    body = {
        "warehouse_id": WAREHOUSE,
        "statement": sql,
        "wait_timeout": "50s",
        "on_wait_timeout": "CONTINUE",
    }
    if CATALOG:
        body["catalog"] = CATALOG
    if schema:
        body["schema"] = schema
    d = _req(BASE, "POST", body)
    sid = d.get("statement_id")
    while d.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(2)
        d = _req(f"{BASE}/{sid}")
    state = d.get("status", {}).get("state")
    if state != "SUCCEEDED":
        err = d.get("status", {}).get("error", {})
        return state, None, err.get("message", json.dumps(err))
    res = d.get("result", {})
    return state, res.get("data_array", []), None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sql", nargs="?")
    ap.add_argument("--file")
    ap.add_argument("--schema")
    a = ap.parse_args()
    if a.file:
        raw = open(a.file).read()
        # strip `--` line comments FIRST (they may contain ';'), then split
        no_comments = "\n".join(
            ln.split("--", 1)[0] for ln in raw.splitlines()
        )
        stmts = [s.strip() for s in no_comments.split(";") if s.strip()]
        ok = 0
        for i, s in enumerate(stmts, 1):
            preview = " ".join(s.split())[:70]
            state, _, err = run(s, a.schema)
            if state == "SUCCEEDED":
                ok += 1
                print(f"[{i}/{len(stmts)}] OK   {preview}")
            else:
                print(f"[{i}/{len(stmts)}] FAIL {state}: {err}\n     stmt: {preview}")
                sys.exit(1)
        print(f"\nSeeded {ok}/{len(stmts)} statements.")
    else:
        state, rows, err = run(a.sql, a.schema)
        if state != "SUCCEEDED":
            print(f"{state}: {err}")
            sys.exit(1)
        for row in rows:
            print("\t".join("" if v is None else str(v) for v in row))


if __name__ == "__main__":
    main()
