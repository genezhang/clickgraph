#!/usr/bin/env python3
"""Score ClickGraph against Neo4j on the corpus (P-4c S1).

For each requested schema: build the schema's logical graph from its YAML and
ClickHouse tables, load it into Neo4j, run every corpus query of that schema
on both, and classify:

  MATCH              same rows (multiset; see compare.py for the rules)
  LIMIT_COUNT_ONLY   LIMIT without ORDER BY: same row count (set is arbitrary)
  MISMATCH           different rows               <- a ClickGraph bug or a
                                                       comparator gap to triage
  CG_ERROR           Neo4j answers, ClickGraph errors (loud gap)
  NEO_ERROR          Neo4j rejects the query (not valid Cypher for Neo4j,
                     or a ClickGraph extension); not scored
  BOTH_ERROR         both reject it
  INCOMPARABLE       result shape the comparator does not handle yet (paths)

Writes `--out/<schema>.jsonl` (one record per query) and prints a scorecard.
With --write-expected, also writes Neo4j's canonical rows to
tests/corpus/expected/<schema>/<name>.json (the result goldens, checked by
tests/integration/test_neo4j_result_goldens.py) and today's verdict per query
to tests/corpus/expected/scorecard.json (MISMATCH / CG_ERROR entries are the
known-wrong list that P-4c slices must flip).

Schemas are resolved exactly as the corpus sweep resolves them
(`tests/corpus/schema_map.json`), loaded into Neo4j from that document, and
registered on ClickGraph under private names (`oracle__<schema>`) from the same
document: a shared server's schema names can be re-registered by other test
runs (`/schemas/load`) and silently point at different tables. The script
starts its OWN server (`--cg-binary`, port 17475, no Bolt) unless `--cg-url`
names a running one.

Usage (from the repo root, with ClickHouse and Neo4j running):
  python3 scripts/oracle/run_corpus.py --schemas social_integration standard
"""
import argparse
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


def register(cg_query_url, corpus_schema, private_name):
    base = cg_query_url.rsplit("/query", 1)[0]
    body = json.dumps({
        "schema_name": private_name,
        "config_content": graph_loader.registration_yaml(corpus_schema, private_name),
    }).encode()
    req = urllib.request.Request(f"{base}/schemas/load", data=body, headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=60).read()


def neo_query(neo_url, cypher):
    body = json.dumps({"statements": [{"statement": cypher, "resultDataContents": ["row"]}]}).encode()
    req = urllib.request.Request(neo_url, data=body, headers={"Content-Type": "application/json"})
    r = json.loads(urllib.request.urlopen(req, timeout=120).read())
    if r["errors"]:
        return None, r["errors"][0]["message"][:300]
    return r["results"][0], None


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


def run(args, entries):
    ch = lambda sql: graph_loader.ch_rows(args.ch, args.ch_user, args.ch_password, sql)  # noqa: E731
    total = {}
    scorecard_path = os.path.join(EXPECTED_DIR, "scorecard.json")
    scorecard = json.load(open(scorecard_path)) if os.path.exists(scorecard_path) else {}
    triage_path = os.path.join(EXPECTED_DIR, "triage.json")
    triage = json.load(open(triage_path)) if os.path.exists(triage_path) else {}
    for schema in args.schemas:
        gs = graph_loader.corpus_schema_doc(schema)["graph_schema"]
        private = f"oracle__{schema}"
        register(args.cg_url, schema, private)
        try:
            nodes, rels, report = graph_loader.build_graph(gs, ch)
        except graph_loader.Unsupported as e:
            print(f"== {schema}: UNSUPPORTED layout ({e}); skipped")
            continue
        except urllib.error.HTTPError as e:
            print(f"== {schema}: tables unavailable ({e.read().decode()[:160]}); skipped")
            continue
        graph_loader.load_into_neo4j(args.neo, nodes, rels)
        print(f"== {schema}: loaded {report}")
        verdicts = {}
        with open(os.path.join(args.out, f"{schema}.jsonl"), "w") as out:
            for e in (x for x in entries if x["schema"] == schema):
                neo, neo_err = neo_query(args.neo, e["cypher"])
                cg, cg_err = cg_query(args.cg_url, private, e["cypher"])
                expected = None
                if not neo_err:
                    try:
                        expected = compare.expected_from_neo4j(e["cypher"], neo)
                    except compare.Incomparable as ex:
                        incomparable = str(ex)
                if neo_err and cg_err:
                    verdict, detail = "BOTH_ERROR", neo_err
                elif neo_err:
                    verdict, detail = "NEO_ERROR", neo_err
                elif expected is None:
                    verdict, detail = "INCOMPARABLE", incomparable
                elif cg_err:
                    verdict, detail = "CG_ERROR", cg_err
                else:
                    verdict, detail = compare.compare_expected(e["cypher"], expected, cg)
                # ORDER BY ... LIMIT with ties at the cut: also keep the full
                # (un-limited) answer, which decides when the limited rows differ.
                full = compare.without_trailing_limit(e["cypher"])
                if expected is not None and full and compare._ORDER.search(e["cypher"]):
                    neo_full, ne = neo_query(args.neo, full)
                    if not ne:
                        try:
                            expected["full"] = compare.expected_from_neo4j(full, neo_full)
                        except compare.Incomparable:
                            pass
                    if verdict == "MISMATCH" and "same count" in detail and "full" in expected:
                        cg_full, ce = cg_query(args.cg_url, private, full)
                        if not ce and compare.compare_expected(full, expected["full"], cg_full)[0] == "MATCH":
                            verdict, detail = "LIMIT_TIES", "equal without the trailing LIMIT (ties at the cut)"
                if args.write_expected and expected is not None:
                    d = os.path.join(EXPECTED_DIR, schema)
                    os.makedirs(d, exist_ok=True)
                    with open(os.path.join(d, e["name"] + ".json"), "w") as f:
                        json.dump({"schema": schema, "cypher": e["cypher"], **expected}, f, indent=1, sort_keys=True)
                        f.write("\n")
                    key = f"{schema}/{e['name']}"
                    scorecard[key] = verdict
                    triage.pop(key, None)
                    if verdict in ("MISMATCH", "CG_ERROR"):
                        triage[key] = {
                            **triage_rules.triage({"name": e["name"], "cypher": e["cypher"], "detail": detail}),
                            "verdict": verdict,
                        }
                verdicts[verdict] = verdicts.get(verdict, 0) + 1
                out.write(json.dumps({"name": e["name"], "cypher": e["cypher"], "verdict": verdict, "detail": detail}) + "\n")
        print(f"   {dict(sorted(verdicts.items()))}")
        for k, v in verdicts.items():
            total[k] = total.get(k, 0) + v
    print(f"TOTAL {dict(sorted(total.items()))}")
    if args.write_expected:
        for path, data in ((scorecard_path, scorecard), (triage_path, triage)):
            with open(path, "w") as f:
                json.dump(dict(sorted(data.items())), f, indent=1)
                f.write("\n")


if __name__ == "__main__":
    main()
