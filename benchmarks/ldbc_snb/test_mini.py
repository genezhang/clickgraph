#!/usr/bin/env python3
"""LDBC Mini - rapid functional test runner.
Tests SQL generation AND execution against the mini dataset.
Designed for <10s total runtime."""
import json, os, sys, requests, glob

SERVER = os.environ.get("CLICKGRAPH_URL", "http://localhost:8082")
QUERY_DIR = "benchmarks/ldbc_snb/queries/official"
TIMEOUT = 5  # 5s timeout - mini dataset should be instant

SKIP = {"bi-10", "bi-15", "bi-19", "bi-20"}  # APOC/GDS unsupported

# Override params for mini dataset (smaller IDs)
MINI_PARAMS = {
    "short-1": {"personId": 1},
    "short-2": {"personId": 1},
    "short-3": {"personId": 1},
    "short-4": {"messageId": 101},
    "short-5": {"messageId": 201},
    "short-6": {"messageId": 101},
    "short-7": {"messageId": 201},
    "complex-1": {"personId": 1, "firstName": "Bob"},
    "complex-2": {"personId": 1, "maxDate": 1276000000000},
    "complex-3": {"personId": 1, "countryXName": "United_States", "countryYName": "Germany", "startDate": 1275000000000, "endDate": 1277000000000},
    "complex-4": {"personId": 1, "startDate": 1275000000000, "endDate": 1277000000000},
    "complex-5": {"personId": 1, "minDate": 1275000000000},
    "complex-6": {"personId": 1, "tagName": "Databases"},
    "complex-7": {"personId": 1},
    "complex-8": {"personId": 1},
    "complex-9": {"personId": 1, "maxDate": 1276000000000},
    "complex-10": {"personId": 1, "month": 6, "nextMonth": 7},
    "complex-11": {"personId": 1, "countryName": "Germany", "workFromYear": 2010},
    "complex-12": {"personId": 1, "tagClassName": "Technology"},
    "complex-13": {"person1Id": 1, "person2Id": 5},
    "complex-14": {"person1Id": 1, "person2Id": 2},
    "bi-1": {"datetime": 1276000000000},
    "bi-2": {"date": 1275000000000, "tagClass": "Technology"},
    "bi-3": {"tagClass": "Technology", "country": "Germany"},
    "bi-4": {"date": 1275000000000},
    "bi-5": {"tag": "Databases"},
    "bi-6": {"tag": "Databases"},
    "bi-7": {"tag": "Databases"},
    "bi-8": {"tag": "Databases", "date": 1275000000000},
    "bi-9": {"tagClass1": "Technology", "tagClass2": "Entertainment", "threshold": 1, "startDate": 1275000000000, "endDate": 1277000000000},
    "bi-11": {"country": "Germany", "blocklist": ["Chrome"]},
    "bi-12": {"startDate": 1275000000000, "lengthThreshold": 10, "languages": ["en"]},
    "bi-13": {"country": "Germany", "endDate": 1277000000000},
    "bi-14": {"begin": 1275000000000, "end": 1277000000000},
    "bi-16": {"personId": 1, "country": "Germany", "tagClass": "Technology", "minPathDistance": 1, "maxPathDistance": 3},
    "bi-17": {"tag": "Databases", "delta": 86400000},
    "bi-18": {"person1Id": 1, "person2Id": 5},
}

results = {"pass": [], "sql_fail": [], "exec_fail": [], "crash": [], "skip": []}

def get_queries():
    """Find all query files, sorted."""
    files = []
    for pattern in ["interactive/short-*.cypher", "interactive/complex-*.cypher", "bi/bi-*.cypher"]:
        files.extend(sorted(glob.glob(f"{QUERY_DIR}/{pattern}"),
                           key=lambda f: (f.split('/')[-1].split('-')[0],
                                         int(''.join(c for c in f.split('/')[-1].split('-')[1] if c.isdigit()) or '0'))))
    return files

def test_query(qid, cypher, params):
    if qid in SKIP:
        results["skip"].append(qid)
        return "⏭️", ""

    # Test SQL generation
    try:
        r = requests.post(f"{SERVER}/query",
                         json={"query": cypher, "parameters": params, "sql_only": True},
                         timeout=TIMEOUT)
        if r.status_code != 200:
            results["sql_fail"].append((qid, f"HTTP {r.status_code}"))
            return "❌SQL", f"HTTP {r.status_code}"
        data = r.json()
        if "error" in data:
            results["sql_fail"].append((qid, data["error"][:100]))
            return "❌SQL", data["error"][:80]
        sql = data.get("generated_sql", "")
        if not sql:
            results["sql_fail"].append((qid, "empty SQL"))
            return "❌SQL", "empty SQL"
    except Exception as e:
        results["crash"].append((qid, str(e)[:80]))
        return "💥", str(e)[:60]

    # Test execution
    try:
        r2 = requests.post(f"{SERVER}/query",
                          json={"query": cypher, "parameters": params},
                          timeout=TIMEOUT)
        if r2.status_code == 200:
            data2 = r2.json()
            if "error" in data2:
                err = data2["error"][:120]
                results["exec_fail"].append((qid, err))
                return "❌RUN", err[:80]
            rows = data2.get("results", [])
            results["pass"].append(qid)
            return "✅", f"{len(rows)} rows"
        else:
            err = ""
            try:
                err = r2.json().get("error", "")[:120]
            except:
                err = r2.text[:120]
            results["exec_fail"].append((qid, err))
            return "❌RUN", err[:80]
    except requests.exceptions.Timeout:
        results["crash"].append((qid, "timeout"))
        return "💥", "timeout"
    except Exception as e:
        results["crash"].append((qid, str(e)[:80]))
        return "💥", str(e)[:60]

def main():
    files = get_queries()
    print(f"Testing {len(files)} LDBC queries against {SERVER}\n")

    for f in files:
        qid = os.path.basename(f).replace('.cypher', '')
        cypher = open(f).read()

        # Use mini params if available, else try .params.json
        params = MINI_PARAMS.get(qid, {})
        if not params:
            pf = f.replace('.cypher', '.params.json')
            if os.path.exists(pf):
                params = json.load(open(pf))

        status, detail = test_query(qid, cypher, params)
        detail_str = f" ({detail})" if detail else ""
        print(f"  {status} {qid}{detail_str}")

    # Summary
    total = len(files)
    print(f"\n{'='*60}")
    print(f"TOTAL: {total} queries")
    print(f"  ✅ PASS:      {len(results['pass']):3d}")
    print(f"  ❌ SQL Fail:  {len(results['sql_fail']):3d}")
    print(f"  ❌ Exec Fail: {len(results['exec_fail']):3d}")
    print(f"  💥 Crash:     {len(results['crash']):3d}")
    print(f"  ⏭️  Skip:      {len(results['skip']):3d}")

    effective = total - len(results['skip'])
    pass_rate = len(results['pass']) / effective * 100 if effective > 0 else 0
    print(f"\n  Pass rate: {len(results['pass'])}/{effective} ({pass_rate:.0f}%)")

    if results['exec_fail']:
        print(f"\nExec failures:")
        for qid, err in results['exec_fail']:
            print(f"  {qid}: {err}")

    if results['sql_fail']:
        print(f"\nSQL gen failures:")
        for qid, err in results['sql_fail']:
            print(f"  {qid}: {err}")

    if results['crash']:
        print(f"\nCrashes:")
        for qid, err in results['crash']:
            print(f"  {qid}: {err}")

if __name__ == "__main__":
    main()
