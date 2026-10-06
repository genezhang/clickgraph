#!/usr/bin/env python3
"""Score ClickGraph against Neo4j on the corpus (P-4c S1).

For each requested schema: build the schema's logical graph from its YAML and
ClickHouse tables, load it into Neo4j, run every corpus query of that schema
on both, and classify:

  MATCH         same rows (see compare.py for the exact rules)
  MISMATCH      different rows                 <- a ClickGraph bug, triaged
  CG_ERROR      Neo4j answers, ClickGraph errors
  UNVERIFIED    the answer cannot be checked (LIMIT over an order the
                comparator cannot see); not scored
  INCOMPARABLE  result shape the comparator does not handle (paths, id());
                not scored
  NEO_ERROR     Neo4j rejects the query (not Cypher 5, or a ClickGraph
                extension); not scored
  BOTH_ERROR    both reject it; not scored

Schemas are resolved exactly as the corpus sweep resolves them
(`tests/corpus/schema_map.json`), loaded into Neo4j from that document, and
registered on ClickGraph under private names (`oracle__<schema>`) from the
same document: a shared server's schema names can be re-registered by other
test runs (`/schemas/load`) and silently point at different tables. The script
starts its OWN server (`--cg-binary`, port 17475, no Bolt) unless `--cg-url`
names a running one.

With --write-expected it (re)writes, per schema:
  tests/corpus/expected/<schema>/<name>.json   Neo4j's answer (the golden)
  tests/corpus/expected/<schema>/_graph.json   the loaded graph's counts
and the schema's entries of tests/corpus/expected/scorecard.json (verdict, and
a signature of each wrong outcome) and triage.json. Stale entries of the
schema are removed. tests/integration/test_neo4j_result_goldens.py checks them.

Usage (from the repo root, with ClickHouse and Neo4j running):
  python3 scripts/oracle/run_corpus.py --schemas social_integration standard --write-expected
"""
import argparse
import glob
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
import compare  # noqa: E402
import graph_loader  # noqa: E402
import triage_rules  # noqa: E402

BOOT_SCHEMA_YAML = "schemas/test/social_integration.yaml"  # any valid schema to start the server
EXPECTED_DIR = "tests/corpus/expected"
KNOWN_WRONG = ("MISMATCH", "CG_ERROR")


def cg_query(cg_url, schema, cypher):
    body = json.dumps({"query": cypher, "schema_name": schema}).encode()
    req = urllib.request.Request(cg_url, data=body, headers={"Content-Type": "application/json"})
    try:
        r = json.loads(urllib.request.urlopen(req, timeout=120).read())
        return r.get("results", r), None
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code}: {e.read().decode()[:300]}"
    except Exception as e:  # noqa: BLE001 - recorded, never hidden
        return None, f"{type(e).__name__}: {e}"


def neo_query(neo_url, cypher):
    body = json.dumps({"statements": [{"statement": cypher, "resultDataContents": ["row"]}]}).encode()
    req = urllib.request.Request(neo_url, data=body, headers={"Content-Type": "application/json"})
    r = json.loads(urllib.request.urlopen(req, timeout=120).read())
    if r["errors"]:
        return None, r["errors"][0]["message"][:300]
    return r["results"][0], None


def register(cg_query_url, corpus_schema, private_name):
    base = cg_query_url.rsplit("/query", 1)[0]
    body = json.dumps({
        "schema_name": private_name,
        "config_content": graph_loader.registration_yaml(corpus_schema, private_name),
    }).encode()
    req = urllib.request.Request(f"{base}/schemas/load", data=body, headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=60).read()


def start_server(args):
    env = dict(
        os.environ,
        CLICKHOUSE_URL=args.ch.rstrip("/"),
        CLICKHOUSE_USER=args.ch_user,
        CLICKHOUSE_PASSWORD=args.ch_password,
        GRAPH_CONFIG_PATH=BOOT_SCHEMA_YAML,
    )
    os.makedirs(args.out, exist_ok=True)
    log = open(os.path.join(args.out, "clickgraph.log"), "w")
    proc = subprocess.Popen(
        [args.cg_binary, "--http-port", args.cg_port, "--disable-bolt"],
        env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    for _ in range(120):
        try:
            urllib.request.urlopen(f"http://localhost:{args.cg_port}/health", timeout=2)
            return proc
        except Exception:  # noqa: BLE001 - not up yet
            if proc.poll() is not None:
                raise RuntimeError(f"clickgraph exited; see {log.name}")
            time.sleep(0.5)
    proc.terminate()
    raise RuntimeError("clickgraph did not become healthy")


def neo_expected(args, cypher):
    """(expected, None) | (None, ("NEO_ERROR"|"INCOMPARABLE", detail))."""
    neo, err = neo_query(args.neo, cypher)
    if err:
        return None, ("NEO_ERROR", err)
    full = None
    stripped = compare.without_trailing_limit(cypher)
    if stripped:
        full, ferr = neo_query(args.neo, stripped)
        if ferr:
            full = None
    try:
        return compare.expected_from_neo4j(cypher, neo, full), None
    except compare.Incomparable as e:
        return None, ("INCOMPARABLE", str(e))


def classify(args, private, entry):
    """-> record dict with verdict, detail, and for wrong outcomes a signature
    and a triage category."""
    cypher = entry["cypher"]
    expected, neo_problem = neo_expected(args, cypher)
    cg, cg_err = cg_query(args.cg_url, private, cypher)
    rec = {"name": entry["name"], "cypher": cypher, "expected": expected}
    if neo_problem and neo_problem[0] == "NEO_ERROR":
        rec["verdict"], rec["detail"] = ("BOTH_ERROR" if cg_err else "NEO_ERROR"), neo_problem[1]
    elif cg_err:
        rec["verdict"], rec["detail"] = "CG_ERROR", cg_err
    elif neo_problem:
        rec["verdict"], rec["detail"] = neo_problem
    else:
        rec["verdict"], rec["detail"] = compare.compare_expected(cypher, expected, cg)
    if rec["verdict"] in KNOWN_WRONG:
        rec["signature"] = compare.signature(rec["verdict"], rec["detail"], None if cg_err else cg)
        rec["triage"] = triage(args, entry, rec, cg)
    return rec


# Rewrites that CONFIRM a non-bug category: if Neo4j, asked the rewritten
# query, returns exactly ClickGraph's rows, the difference is that semantic
# choice and nothing else. (pattern, replacement, category, ref)
CONFIRMING_REWRITES = [
    # a UInt8 flag compared with `true`: the engine reads it as `= 1`
    (r"= true\b", "= 1", "UNTYPED_BOOLEAN", triage_rules.TRACKING),
    # an unmapped property read from the edge table's column (AUTHORED lives
    # in the posts table, so `r.post_id` is the post's id)
    (r"\br\.post_id\b", "p.post_id", "EXTENSION", triage_rules.TRACKING),
    # `.id` aliases the node id (#411); User's id property is user_id
    (r"\b([ab])\.id\b", r"\1.user_id", "EXTENSION", "#411"),
]


def triage(args, entry, rec, cg):
    """Category for a wrong outcome: first a confirming rewrite (re-run on
    Neo4j), then the rules in triage_rules.py (which default to BUG)."""
    import re
    cypher = entry["cypher"]
    if rec["verdict"] == "MISMATCH":
        for pattern, repl, category, ref in CONFIRMING_REWRITES:
            if not re.search(pattern, cypher):
                continue
            variant = re.sub(pattern, repl, cypher)
            exp, _problem = neo_expected(args, variant)
            # the rewritten query names its columns after the rewrite too
            cg_variant = [{re.sub(pattern, repl, k): v for k, v in row.items()} for row in cg]
            if exp is not None and compare.compare_expected(variant, exp, cg_variant)[0] == "MATCH":
                return {"category": category, "ref": ref}
    return triage_rules.triage({"name": entry["name"], "cypher": cypher, "detail": rec["detail"]})


def run(args, entries):
    ch = lambda sql: graph_loader.ch_rows(args.ch, args.ch_user, args.ch_password, sql)  # noqa: E731
    scorecard_path = os.path.join(EXPECTED_DIR, "scorecard.json")
    triage_path = os.path.join(EXPECTED_DIR, "triage.json")
    scorecard = json.load(open(scorecard_path)) if os.path.exists(scorecard_path) else {}
    triage_map = json.load(open(triage_path)) if os.path.exists(triage_path) else {}
    total = {}
    for schema in args.schemas:
        gs = graph_loader.corpus_schema_doc(schema)["graph_schema"]
        try:
            nodes, rels, report = graph_loader.build_graph(gs, ch)
        except graph_loader.Unsupported as e:
            print(f"== {schema}: UNSUPPORTED layout ({e}); skipped")
            continue
        except urllib.error.HTTPError as e:
            print(f"== {schema}: tables unavailable ({e.read().decode()[:160]}); skipped")
            continue
        graph_loader.load_into_neo4j(args.neo, nodes, rels)
        private = f"oracle__{schema}"
        register(args.cg_url, schema, private)
        print(f"== {schema}: loaded {report}")
        if args.write_expected:
            d = os.path.join(EXPECTED_DIR, schema)
            os.makedirs(d, exist_ok=True)
            for f in glob.glob(os.path.join(d, "*.json")):
                os.remove(f)
            for key in [k for k in scorecard if k.startswith(schema + "/")]:
                scorecard.pop(key)
                triage_map.pop(key, None)
            with open(os.path.join(d, "_graph.json"), "w") as f:
                json.dump(report, f, indent=1, sort_keys=True)
                f.write("\n")
        verdicts = {}
        with open(os.path.join(args.out, f"{schema}.jsonl"), "w") as out:
            for e in (x for x in entries if x["schema"] == schema):
                rec = classify(args, private, e)
                verdicts[rec["verdict"]] = verdicts.get(rec["verdict"], 0) + 1
                out.write(json.dumps({k: v for k, v in rec.items() if k != "expected"}) + "\n")
                if not (args.write_expected and rec["expected"] is not None):
                    continue
                if rec["verdict"] not in ("MATCH",) + KNOWN_WRONG:
                    continue
                key = f"{schema}/{e['name']}"
                with open(os.path.join(EXPECTED_DIR, schema, e["name"] + ".json"), "w") as f:
                    json.dump({"schema": schema, "cypher": e["cypher"], **rec["expected"]}, f, indent=1, sort_keys=True)
                    f.write("\n")
                if rec["verdict"] == "MATCH":
                    scorecard[key] = {"verdict": "MATCH"}
                else:
                    scorecard[key] = {"verdict": rec["verdict"], "signature": rec["signature"]}
                    triage_map[key] = {**rec["triage"], "verdict": rec["verdict"]}
        print(f"   {dict(sorted(verdicts.items()))}")
        for k, v in verdicts.items():
            total[k] = total.get(k, 0) + v
    print(f"TOTAL {dict(sorted(total.items()))}")
    if args.write_expected:
        for path, data in ((scorecard_path, scorecard), (triage_path, triage_map)):
            with open(path, "w") as f:
                json.dump(dict(sorted(data.items())), f, indent=1)
                f.write("\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schemas", nargs="+", required=True)
    ap.add_argument("--corpus", default="tests/corpus/queries.jsonl")
    ap.add_argument("--cg-url", default=None, help="use this running server's /query instead of starting one")
    ap.add_argument("--cg-binary", default="target/debug/clickgraph")
    ap.add_argument("--cg-port", default="17475")
    ap.add_argument("--neo", default="http://localhost:17474/db/neo4j/tx/commit")
    ap.add_argument("--ch", default="http://localhost:8123/")
    ap.add_argument("--ch-user", default="test_user")
    ap.add_argument("--ch-password", default="test_pass")
    ap.add_argument("--out", default="target/oracle")
    ap.add_argument("--write-expected", action="store_true")
    args = ap.parse_args()

    entries = [json.loads(l) for l in open(args.corpus) if l.strip()]
    os.makedirs(args.out, exist_ok=True)
    server = None
    if args.cg_url is None:
        server = start_server(args)
        args.cg_url = f"http://localhost:{args.cg_port}/query"
    try:
        run(args, entries)
    finally:
        if server is not None:
            server.terminate()
            server.wait(timeout=30)


if __name__ == "__main__":
    main()
