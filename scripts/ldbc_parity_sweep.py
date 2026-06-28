#!/usr/bin/env python3
"""LDBC interactive parity sweep: ClickHouse vs Databricks via cg.

For each official interactive query, substitutes :params (personId overridden to
a mini-dataset value), runs it through `cg` against both dialects, and compares
results order-insensitively. Reports an execution+result parity table.

Env: DATABRICKS_* sourced (~/.dbx.env). CH on localhost:8123 (test_user/test_pass).
"""
import json, os, re, subprocess, sys, glob

CG = "/mnt/cargo-sd/cargo/target/debug/cg"
SCHEMA = "benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml"
QDIR = "benchmarks/ldbc_snb/queries/official/interactive"
CH = ["--clickhouse", "http://localhost:8123", "--ch-user", "test_user",
      "--ch-password", "test_pass", "--ch-database", "ldbc"]

# Mini-dataset-valid overrides (Person ids are 1..5; tags/places from the seed).
OVERRIDES = {
    "personId": 1, "person1Id": 1, "person2Id": 2, "personIdQ": 1,
    "firstName": "Alice", "tagName": "Databases", "tagClassName": "Technology",
    "countryName": "Germany", "countryXName": "Germany", "countryYName": "Angola",
    "country1Name": "Germany", "country2Name": "Angola",
}


def parse_params(text):
    m = re.search(r":params\s*\{(.+?)\}", text, re.S)
    params = {}
    if m:
        for k, v in re.findall(r"(\w+)\s*:\s*('[^']*'|\"[^\"]*\"|[\w.]+)", m.group(1)):
            v = v.strip()
            if v[:1] in "'\"":
                params[k] = v[1:-1]
            else:
                try:
                    params[k] = int(v)
                except ValueError:
                    try:
                        params[k] = float(v)
                    except ValueError:
                        params[k] = v
    params.update({k: OVERRIDES[k] for k in params if k in OVERRIDES})
    # also add any override whose key the query uses even if absent from :params
    return params


def strip_comments(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return "\n".join(l for l in text.splitlines() if not l.strip().startswith("//")).strip()


def substitute(query, params):
    # add overrides for any $param present in the query
    used = set(re.findall(r"\$(\w+)", query))
    for k in used:
        if k not in params and k in OVERRIDES:
            params[k] = OVERRIDES[k]
    def repl(m):
        k = m.group(1)
        if k not in params:
            return m.group(0)
        v = params[k]
        return f"'{v}'" if isinstance(v, str) else str(v)
    return re.sub(r"\$(\w+)", repl, query), used


def run(query, dialect):
    cmd = [CG, "query", "--schema", SCHEMA, "--dialect", dialect, "--format", "json"]
    if dialect == "clickhouse":
        cmd += CH
    cmd.append(query)
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if p.returncode != 0:
        err = (p.stderr or p.stdout).strip().split("\n")[-1][:90]
        return None, err
    # cg --format json emits NDJSON: one JSON object per line.
    out = p.stdout.strip()
    if not out:
        return [], None
    try:
        rows = [json.loads(line) for line in out.splitlines() if line.strip()]
        return rows, None
    except json.JSONDecodeError as e:
        return None, f"unparseable JSON: {e}"


def _coerce(v):
    # Normalize known backend representation differences so the sweep surfaces
    # STRUCTURAL/semantic diffs, not type-spelling. Two such differences:
    #  - numbers: both backends now return native JSON numbers (Databricks since
    #    the #375 result-type coercion), but stringify to compare uniformly.
    #  - booleans: ClickHouse renders a boolean expression as UInt8 0/1, while
    #    Spark/Databricks returns native true/false (more Neo4j-faithful). Map
    #    bool -> "0"/"1" so false==0 and true==1 (e.g. complex-7 `isNew`), the
    #    same way numbers are normalized — this is a backend representation
    #    difference, not a ClickGraph translation bug.
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        try:
            f = float(v)
            return str(int(f)) if f.is_integer() else str(f)
        except ValueError:
            return v
    if isinstance(v, list):
        return [_coerce(x) for x in v]
    if isinstance(v, dict):
        return {k: _coerce(x) for k, x in v.items()}
    return v


def canon(rows):
    if not isinstance(rows, list):
        return rows
    return sorted(json.dumps(_coerce(r), sort_keys=True, default=str) for r in rows)


def main():
    files = sorted(glob.glob(f"{QDIR}/short-*.cypher") + glob.glob(f"{QDIR}/complex-*.cypher"),
                   key=lambda f: (("complex" in f), int(re.search(r"-(\d+)", f).group(1))))
    if len(sys.argv) > 1:
        files = [f for f in files if any(a in f for a in sys.argv[1:])]
    print(f"{'query':<12} {'CH':<10} {'DBX':<10} {'verdict'}")
    print("-" * 60)
    tally = {}
    for f in files:
        name = os.path.basename(f).replace(".cypher", "")
        raw = open(f).read()
        params = parse_params(raw)
        query, _ = substitute(strip_comments(raw), params)
        ch, ch_err = run(query, "clickhouse")
        dbx, dbx_err = run(query, "databricks")
        ch_s = "ERR" if ch is None else f"{len(ch)}r"
        dbx_s = "ERR" if dbx is None else f"{len(dbx)}r"
        if ch is None and dbx is None:
            verdict = "both_err"
        elif ch is None:
            verdict = f"CH_err: {ch_err}"
        elif dbx is None:
            verdict = f"DBX_err: {dbx_err}"
        elif canon(ch) == canon(dbx):
            verdict = "MATCH" + (" (empty)" if not ch else "")
        else:
            verdict = f"MISMATCH (CH {len(ch)} vs DBX {len(dbx)})"
        tally[verdict.split(":")[0].split(" ")[0]] = tally.get(verdict.split(":")[0].split(" ")[0], 0) + 1
        print(f"{name:<12} {ch_s:<10} {dbx_s:<10} {verdict}")
    print("-" * 60)
    print("tally:", dict(sorted(tally.items())))


if __name__ == "__main__":
    main()
