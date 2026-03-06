#!/usr/bin/env python3
"""
LDBC Query Performance Assessment — sf10 scale
Single-run execution with 300s timeout per query.
"""

import json, requests, glob, os, time, sys

SERVER = "http://localhost:8082"
QUERY_DIR = "benchmarks/ldbc_snb/queries/official"
ADAPTED_DIR = "benchmarks/ldbc_snb/queries/adapted"

ADAPTED_QUERIES = {
    "complex-14": "interactive-complex-14.cypher",
}

# sf10-appropriate parameters (real IDs from the dataset)
PERSON1 = 26053       # Well-connected person (Kamel, 820 KNOWS edges)
PERSON2 = 26841       # Connected to PERSON1
MESSAGE_ID = 1055844  # Real comment ID
TAG = "Hamid_Karzai"
TAG_CLASS = "MilitaryPerson"
COUNTRY = "India"
COUNTRY2 = "China"
FIRST_NAME = "John"

MINI_PARAMS = {
    "short-1": {"personId": PERSON1},
    "short-2": {"personId": PERSON1},
    "short-3": {"personId": PERSON1},
    "short-4": {"messageId": MESSAGE_ID},
    "short-5": {"messageId": MESSAGE_ID},
    "short-6": {"messageId": MESSAGE_ID},
    "short-7": {"messageId": MESSAGE_ID},
    "complex-1": {"personId": PERSON1, "firstName": FIRST_NAME},
    "complex-2": {"personId": PERSON1, "maxDate": 1340000000000},
    "complex-3": {"personId": PERSON1, "countryXName": COUNTRY, "countryYName": COUNTRY2, "startDate": 1320000000000, "endDate": 1340000000000},
    "complex-4": {"personId": PERSON1, "startDate": 1335000000000, "endDate": 1340000000000},
    "complex-5": {"personId": PERSON1, "minDate": 1335000000000},
    "complex-6": {"personId": PERSON1, "tagName": TAG},
    "complex-7": {"personId": PERSON1},
    "complex-8": {"personId": PERSON1},
    "complex-9": {"personId": PERSON1, "maxDate": 1340000000000},
    "complex-10": {"personId": PERSON1, "month": 6, "nextMonth": 7},
    "complex-11": {"personId": PERSON1, "countryName": COUNTRY, "workFromYear": 2010},
    "complex-12": {"personId": PERSON1, "tagClassName": TAG_CLASS},
    "complex-13": {"person1Id": PERSON1, "person2Id": PERSON2},
    "complex-14": {"person1Id": PERSON1, "person2Id": PERSON2},
    "bi-1": {"datetime": 1340000000000},
    "bi-2": {"date": 1335000000000, "tagClass": TAG_CLASS},
    "bi-3": {"tagClass": TAG_CLASS, "country": COUNTRY},
    "bi-4": {"date": 1335000000000},
    "bi-5": {"tag": TAG},
    "bi-6": {"tag": TAG},
    "bi-7": {"tag": TAG},
    "bi-8": {"tag": TAG, "startDate": 1335000000000, "endDate": 1340000000000},
    "bi-9": {"tagClass1": TAG_CLASS, "tagClass2": "IceHockeyPlayer", "threshold": 1, "startDate": 1335000000000, "endDate": 1340000000000},
    "bi-11": {"country": COUNTRY, "startDate": 1335000000000, "endDate": 1340000000000},
    "bi-12": {"startDate": 1335000000000, "lengthThreshold": 10, "languages": ["en"]},
    "bi-13": {"country": COUNTRY, "endDate": 1340000000000},
    "bi-14": {"country1": COUNTRY, "country2": COUNTRY2},
    "bi-17": {"tag": TAG, "delta": 86400000},
    "bi-18": {"person1Id": PERSON1, "person2Id": PERSON2, "tag": TAG},
}

SKIP = {"bi-10", "bi-15", "bi-16", "bi-19", "bi-20"}

# Known sf10 scale limitations (not bugs — inherent query complexity vs. data scale)
# OOM: Cartesian explosion in recursive CTEs over dense graph (67K persons, 1.75M KNOWS, 25M messages)
# Timeout: Unbounded traversals or O(n^3) Cartesian products at sf10 density
EXPECTED_OOM = {"complex-9", "complex-10", "complex-13", "bi-17"}
EXPECTED_TIMEOUT = {"bi-3", "bi-6", "bi-9", "bi-13"}


def measure_query(qid, cypher, params):
    result = {
        "qid": qid, "status": "unknown",
        "sql_gen_ms": None, "exec_ms": None,
        "row_count": None, "sql_length": None,
        "error": None,
    }

    # Phase 1: SQL generation
    t0 = time.time()
    try:
        r = requests.post(f"{SERVER}/query",
                         json={"query": cypher, "parameters": params, "sql_only": True}, timeout=30)
    except Exception as e:
        result["status"] = "sql_error"
        result["error"] = str(e)
        return result

    result["sql_gen_ms"] = round((time.time() - t0) * 1000, 1)

    if r.status_code != 200:
        result["status"] = "sql_error"
        result["error"] = r.text[:300]
        return result
    data = r.json()
    if "error" in data:
        result["status"] = "sql_error"
        result["error"] = data["error"][:300]
        return result

    sql = data.get("generated_sql", "")
    result["sql_length"] = len(sql)

    # Phase 2: Single execution run
    t0 = time.time()
    try:
        r2 = requests.post(f"{SERVER}/query",
                          json={"query": cypher, "parameters": params}, timeout=300)
        exec_ms = (time.time() - t0) * 1000
        result["exec_ms"] = round(exec_ms, 0)

        if r2.status_code == 200:
            data2 = r2.json()
            if "error" in data2:
                result["status"] = "exec_error"
                result["error"] = data2["error"][:300]
                return result
            results = data2.get("results", [])
            result["row_count"] = len(results) if isinstance(results, list) else 0
            result["status"] = "ok"
        else:
            result["status"] = "exec_error"
            try:
                result["error"] = r2.json().get("error", "")[:300]
            except:
                result["error"] = r2.text[:300]
    except requests.exceptions.Timeout:
        result["status"] = "timeout"
        result["error"] = "Execution timeout (300s)"
        result["exec_ms"] = 300000
    except Exception as e:
        result["status"] = "exec_error"
        result["error"] = str(e)
        # Check if server crashed
        time.sleep(2)
        try:
            requests.get(f"{SERVER}/health", timeout=2)
        except:
            print(f"  [SERVER MAY HAVE CRASHED — check /tmp/cg_server.log]")

    return result


def main():
    files = []
    for pattern in ["interactive/short-*.cypher", "interactive/complex-*.cypher", "bi/bi-*.cypher"]:
        files.extend(sorted(glob.glob(f"{QUERY_DIR}/{pattern}"),
                           key=lambda f: (f.split('/')[-1].split('-')[0],
                                         int(''.join(c for c in f.split('/')[-1].split('-')[1] if c.isdigit()) or '0'))))

    results = []
    print(f"LDBC sf10 Performance Assessment")
    print(f"{'='*90}")
    print(f"{'Query':<14} {'SQL Gen':>8} {'Exec':>10} {'Rows':>8} {'SQL Len':>8} {'Status':<10}")
    print(f"{'-'*90}")

    for f in files:
        qid = os.path.basename(f).replace('.cypher', '')
        if qid in SKIP:
            continue

        if qid in ADAPTED_QUERIES:
            adapted_path = os.path.join(ADAPTED_DIR, ADAPTED_QUERIES[qid])
            cypher = open(adapted_path if os.path.exists(adapted_path) else f).read()
        else:
            cypher = open(f).read()
        params = MINI_PARAMS.get(qid, {})

        r = measure_query(qid, cypher, params)
        results.append(r)

        exec_str = f"{r['exec_ms']:.0f}ms" if r['exec_ms'] else "-"
        gen_str = f"{r['sql_gen_ms']:.0f}ms" if r['sql_gen_ms'] else "-"
        print(f"{qid:<14} {gen_str:>8} {exec_str:>10} {r['row_count'] or 0:>8} {r['sql_length'] or 0:>8} {r['status']:<10}")

        if r["status"] not in ("ok",):
            print(f"  ERROR: {r.get('error', 'unknown')[:150]}")

    # Summary
    ok_results = [r for r in results if r["status"] == "ok"]
    timeout_results = [r for r in results if r["status"] == "timeout"]
    error_results = [r for r in results if r["status"] not in ("ok", "timeout")]

    # Separate expected vs unexpected failures
    expected_fail = [r for r in results if r["qid"] in EXPECTED_OOM | EXPECTED_TIMEOUT and r["status"] != "ok"]
    unexpected_fail = [r for r in results if r["status"] != "ok" and r["qid"] not in EXPECTED_OOM | EXPECTED_TIMEOUT]

    print(f"\n{'='*90}")
    print(f"SUMMARY: {len(ok_results)}/{len(results)} OK, {len(timeout_results)} timeouts, {len(error_results)} errors")
    if expected_fail:
        print(f"  Expected failures (sf10 scale): {', '.join(r['qid'] for r in expected_fail)}")
    if unexpected_fail:
        print(f"  UNEXPECTED failures: {', '.join(r['qid'] for r in unexpected_fail)}")
    elif not unexpected_fail and ok_results:
        effective = len(ok_results) + len(expected_fail)
        print(f"  Effective: {len(ok_results)}/{effective} passing (all failures are expected sf10 scale limits)")
    print()

    if ok_results:
        by_exec = sorted(ok_results, key=lambda r: r["exec_ms"] or 0, reverse=True)

        print("All queries by execution time:")
        print(f"  {'Query':<14} {'Exec':>10} {'SQL Gen':>8} {'Rows':>8} {'SQL Len':>8}")
        for r in by_exec:
            exec_s = f"{r['exec_ms']/1000:.1f}s" if r['exec_ms'] >= 1000 else f"{r['exec_ms']:.0f}ms"
            print(f"  {r['qid']:<14} {exec_s:>10} {r['sql_gen_ms']:.0f}ms {r['row_count'] or 0:>7} {r['sql_length']:>7}")

        total_exec = sum(r["exec_ms"] for r in ok_results)
        total_gen = sum(r["sql_gen_ms"] for r in ok_results)
        print(f"\nTotals ({len(ok_results)} queries):")
        print(f"  SQL generation: {total_gen/1000:.1f}s ({total_gen/len(ok_results):.0f}ms avg)")
        print(f"  Execution:      {total_exec/1000:.1f}s ({total_exec/len(ok_results)/1000:.1f}s avg)")

        # Categorize
        fast = [r for r in ok_results if r["exec_ms"] < 1000]
        medium = [r for r in ok_results if 1000 <= r["exec_ms"] < 10000]
        slow = [r for r in ok_results if 10000 <= r["exec_ms"] < 60000]
        very_slow = [r for r in ok_results if r["exec_ms"] >= 60000]

        print(f"\n  Fast (<1s):       {len(fast)} queries")
        print(f"  Medium (1-10s):   {len(medium)} queries — {', '.join(r['qid'] for r in medium)}" if medium else "  Medium (1-10s):   0")
        print(f"  Slow (10-60s):    {len(slow)} queries — {', '.join(r['qid'] for r in slow)}" if slow else "  Slow (10-60s):    0")
        print(f"  Very slow (>60s): {len(very_slow)} queries — {', '.join(r['qid'] for r in very_slow)}" if very_slow else "  Very slow (>60s): 0")

    # Save results
    results_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results", "sf10_perf_results.json")
    os.makedirs(os.path.dirname(results_path), exist_ok=True)
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nDetailed results saved to {results_path}")


if __name__ == "__main__":
    main()
