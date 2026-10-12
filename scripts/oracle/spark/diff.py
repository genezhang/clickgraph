"""Differential: each corpus query the bound plan lowers on both dialects, its
ClickHouse SQL on ClickHouse and its Databricks SQL on Spark, rows compared."""
import base64, collections, json, math, os, re, sys, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sparkq
S12 = sys.argv[1]
only = sys.argv[2] if len(sys.argv) > 2 else None
HERE = os.environ.get("CG_SPARK_WORK", ".")
AUTH = base64.b64encode(b"test_user:test_pass").decode()

def ch(sql):
    url = ("http://localhost:8123/?join_use_nulls=1&output_format_json_quote_64bit_integers=0"
           "&date_time_output_format=iso&output_format_json_quote_denormals=1")
    req = urllib.request.Request(url, data=(sql + "\nFORMAT JSONCompact").encode(),
                                 headers={"Authorization": f"Basic {AUTH}"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            d = json.loads(r.read().decode())
            return {"columns": [m["name"] for m in d["meta"]], "rows": d["data"]}
    except urllib.error.HTTPError as e:
        return {"error": e.read().decode()[:400]}
    except Exception as e:
        return {"error": repr(e)[:400]}

def norm(v):
    if isinstance(v, bool): return ("n", float(v))
    if isinstance(v, (int, float)):
        return ("n", round(float(v), 6)) if not (isinstance(v, float) and math.isnan(v)) else ("nan",)
    if isinstance(v, str):
        m = re.fullmatch(r"(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d:\d\d)(\.0+)?Z?", v)
        if m: return ("s", f"{m.group(1)}T{m.group(2)}")
        return ("s", v)
    if v is None: return ("null",)
    if isinstance(v, list): return ("l", tuple(norm(x) for x in v))
    if isinstance(v, dict): return ("m", tuple(sorted((k, norm(x)) for k, x in v.items())))
    return ("?", repr(v))

try:
    tables = json.load(open(os.path.join(HERE, "schema_tables.json")))
    have = {f"{t['db']}.{t['table']}" for t in json.load(open(os.path.join(HERE, "manifest.json")))}
    chq = {(j["schema"], j["name"]): j for j in map(json.loads, open(f"{S12}/clickhouse.jsonl"))}
    dbx = [json.loads(l) for l in open(f"{S12}/databricks.jsonl")]
except (OSError, ValueError) as e:
    sys.exit(f"diff.py: {e} (CG_SPARK_WORK holds schema_tables.json / manifest.json; "
             "the SQL directory clickhouse.jsonl / databricks.jsonl)")
verdicts = collections.Counter()
out = open(f"{S12}/diff.jsonl", "w")
for j in dbx:
    key = (j["schema"], j["name"])
    if only and only not in f"{key[0]}/{key[1]}": continue
    ts = tables.get(j["schema"])
    # The schema's tables the query reads must have been loaded.
    used = [t for t in (ts if isinstance(ts, list) else []) if re.search(r"\b" + re.escape(t) + r"\b", j["sql"])]
    # (A query that reads no table, `UNWIND [1, 2]`, needs none.)
    if not isinstance(ts, list) or not all(t in have for t in used):
        verdicts["NO_DATA"] += 1; continue
    c = chq.get(key)
    if c is None:
        verdicts["NOT_ON_CH"] += 1; continue
    # A final LIMIT without a total order picks any rows: ClickHouse's rows
    # without it, Spark's must be as many of them.
    lim = re.search(r"\nLIMIT (\d+)\s*$", c["sql"])
    ordered = bool(re.search(r"(?i)\bORDER\s+BY\b", j["cypher"]))
    limit = (int(lim.group(1)) if lim and not ordered
             and re.search(r"\nLIMIT \d+\s*$", j["sql"]) else None)
    a = ch(c["sql"][: lim.start()] if limit is not None else c["sql"]); b = sparkq.run(j["sql"])
    if b.get("error") == "TIMEOUT":
        sys.exit(f"Spark worker timed out on {key}: is it running? (worker.log)")
    # collect() of rows in no order: its list in any order. Not when an
    # ORDER BY comes before it (an ordered collect).
    first_collect = re.search(r"(?i)collect\s*\(", j["cypher"])
    unordered_lists = bool(first_collect) and not re.search(
        r"(?i)\bORDER\s+BY\b", j["cypher"][: first_collect.start()])
    if "error" in a and "error" in b: v = "BOTH_ERROR"
    elif "error" in a: v = "CH_ERROR"
    elif "error" in b: v = "SPARK_ERROR"
    else:
        def nrow(r):
            out = []
            for x in r:
                n = norm(x)
                if unordered_lists and n[0] == "l": n = ("l", tuple(sorted(n[1], key=repr)))
                out.append(n)
            return tuple(out)
        ra = [nrow(r) for r in a["rows"]]
        rb = [nrow(r) for r in b["rows"]]
        if limit is not None:
            ca, cb = collections.Counter(ra), collections.Counter(rb)
            ok = len(rb) == min(limit, len(ra)) and all(ca[k] >= n for k, n in cb.items())
            v = "MATCH" if ok else "MISMATCH"
        elif collections.Counter(ra) != collections.Counter(rb):
            v = "MISMATCH"
            # ORDER BY … LIMIT: rows tied at the cut may differ. Spark's
            # rows as many of ClickHouse's unlimited ones: TIES_LIMIT, to
            # check by hand (the sort keys are not known here).
            if ordered and lim and re.search(r"\nLIMIT \d+\s*$", j["sql"]):
                full = ch(c["sql"][: lim.start()])
                if "rows" in full:
                    cf = collections.Counter(nrow(r) for r in full["rows"])
                    if len(rb) == len(ra) and all(cf[k] >= n for k, n in collections.Counter(rb).items()):
                        v = "TIES_LIMIT"
        # Same rows, another order: ties under ORDER BY, or an ordering
        # bug: the keys are not known here, so check these by hand.
        elif ra != rb and ordered: v = "ORDER_DIFF"
        else: v = "MATCH"
    verdicts[v] += 1
    rec = {"key": f"{key[0]}/{key[1]}", "verdict": v, "cypher": j["cypher"]}
    if v != "MATCH":
        rec.update(ch=a.get("error") or a.get("rows", [])[:8], spark=b.get("error") or b.get("rows", [])[:8])
    out.write(json.dumps(rec, default=str) + "\n")
print(dict(verdicts))
