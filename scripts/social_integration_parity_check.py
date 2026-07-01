#!/usr/bin/env python3
"""Standalone CH↔Databricks parity check for the social_integration schema.

Runs representative Cypher patterns (the kinds the pytest integration suite
asserts) against BOTH backends and compares results order-insensitively:
  - ClickHouse: the running ClickGraph server (POST /query, USE social_integration)
  - Databricks: `cg --dialect databricks` (Delta fixtures loaded via
    scripts/load_social_integration_databricks.py)

This mirrors what `CG_TEST_BACKEND=databricks pytest` would do, but needs no
pytest (unavailable in this env). Env: DATABRICKS_* sourced (~/.dbx.env).
"""
import json
import os
import subprocess
import urllib.request

CG = os.getenv("CG_BIN", "/mnt/cargo-sd/cargo/target/debug/cg")
SCHEMA = "schemas/test/social_integration.yaml"
SERVER = os.getenv("CLICKGRAPH_URL", "http://localhost:7475")

QUERIES = [
    ("nodes+props", "MATCH (u:User) RETURN u.name AS name, u.age AS age ORDER BY u.user_id LIMIT 5"),
    ("where filter", "MATCH (u:User) WHERE u.age > 30 RETURN count(u) AS n"),
    ("follows 1-hop", "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.user_id = 1 RETURN b.name AS f ORDER BY b.user_id"),
    ("authored", "MATCH (u:User)-[:AUTHORED]->(p:Post) WHERE u.user_id = 1 RETURN p.title AS t ORDER BY p.post_id"),
    ("optional match", "MATCH (u:User) WHERE u.user_id IN [1,2,3] OPTIONAL MATCH (u)-[:FOLLOWS]->(b:User) RETURN u.user_id AS id, count(b) AS c ORDER BY id"),
    ("agg group", "MATCH (u:User) RETURN u.country AS c, count(u) AS n ORDER BY n DESC, c LIMIT 5"),
    ("2-hop", "MATCH (a:User)-[:FOLLOWS]->()-[:FOLLOWS]->(c:User) WHERE a.user_id = 1 RETURN DISTINCT c.name AS name ORDER BY name"),
    ("likes count", "MATCH (u:User)-[:LIKED]->(p:Post) WITH u.user_id AS id, count(p) AS likes RETURN id, likes ORDER BY likes DESC, id LIMIT 5"),
    ("string fn", "MATCH (u:User) WHERE u.user_id = 1 RETURN toUpper(u.name) AS up"),
    ("order+limit", "MATCH (u:User) RETURN u.name AS n ORDER BY u.age DESC, u.user_id LIMIT 3"),
]


def canon(rows):
    def coerce(v):
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, (int, float)):
            return str(int(v)) if float(v).is_integer() else str(v)
        return v
    norm = [tuple(sorted((k, coerce(x)) for k, x in r.items())) for r in rows]
    return sorted(norm)


def ch(query):
    body = json.dumps({"query": f"USE social_integration {query}"}).encode()
    req = urllib.request.Request(f"{SERVER}/query", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r).get("results", [])


def dbx(query):
    p = subprocess.run([CG, "query", "--schema", SCHEMA, "--dialect", "databricks",
                        "--format", "json", query], capture_output=True, text=True, timeout=180)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout).strip().splitlines()[-1][:120])
    return [json.loads(l) for l in p.stdout.splitlines() if l.strip()]


def main():
    print(f"{'pattern':16} {'CH':>6} {'DBX':>6}  verdict")
    print("-" * 52)
    tally = {}
    for name, q in QUERIES:
        try:
            c = ch(q)
        except Exception as e:
            print(f"{name:16} {'ERR':>6}         CH_err: {str(e)[:40]}")
            tally["ch_err"] = tally.get("ch_err", 0) + 1
            continue
        try:
            d = dbx(q)
        except Exception as e:
            print(f"{name:16} {len(c):>4}r  {'ERR':>6}  DBX_err: {str(e)[:40]}")
            tally["dbx_err"] = tally.get("dbx_err", 0) + 1
            continue
        ok = canon(c) == canon(d)
        v = "MATCH" if ok else "DIFF"
        tally[v] = tally.get(v, 0) + 1
        extra = "" if ok else f"  CH={canon(c)[:2]} DBX={canon(d)[:2]}"
        print(f"{name:16} {len(c):>4}r  {len(d):>4}r  {v}{extra}")
    print("-" * 52)
    print("tally:", tally)


if __name__ == "__main__":
    main()
