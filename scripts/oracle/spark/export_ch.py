"""Export the ClickHouse tables the corpus schemas reference: column types and rows."""
import json, os, sys, urllib.request, urllib.parse, base64
CH = "http://localhost:8123/"
AUTH = base64.b64encode(b"test_user:test_pass").decode()
OUT = sys.argv[1]  # the work directory (holds schema_tables.json)

def ch(sql):
    req = urllib.request.Request(CH, data=sql.encode(), headers={"Authorization": f"Basic {AUTH}"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.read().decode()
    except Exception as e:
        return None

tables = set()
for v in json.load(open(os.path.join(OUT, "schema_tables.json"))).values():
    if isinstance(v, list):
        tables.update(v)
os.makedirs(os.path.join(OUT, "data"), exist_ok=True)
manifest = []
for t in sorted(tables):
    db, name = t.split(".", 1)
    if not db:
        continue
    cols = ch(f"SELECT name, type FROM system.columns WHERE database = '{db}' AND table = '{name}' ORDER BY position FORMAT JSONEachRow")
    if not cols or not cols.strip():
        print("missing", t)
        continue
    cols = [json.loads(l) for l in cols.splitlines() if l.strip()]
    collist = ", ".join(f"`{c['name']}`" for c in cols)
    data = ch(f"SELECT {collist} FROM `{db}`.`{name}` FORMAT JSONEachRow "
              "SETTINGS output_format_json_quote_64bit_integers=0, output_format_json_quote_denormals=1, date_time_output_format='iso'")
    if data is None:
        print("unreadable", t)
        continue
    path = f"data/{db}.{name}.jsonl"
    open(os.path.join(OUT, path), "w").write(data)
    manifest.append({"db": db, "table": name, "columns": cols, "path": path, "rows": data.count("\n")})
    print("ok", t, manifest[-1]["rows"])
json.dump(manifest, open(os.path.join(OUT, "manifest.json"), "w"), indent=1)
