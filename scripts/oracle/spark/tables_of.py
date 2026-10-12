"""The `database.table`s each corpus schema reads (tests/corpus/schema_map.json),
as JSON on stdout. Run from the repository root."""
import json, sys, yaml
def walk(x, out):
    if isinstance(x, dict):
        if 'table' in x and isinstance(x.get('table'), str):
            out.add((x.get('database') or '', x['table']))
        for v in x.values(): walk(v, out)
    elif isinstance(x, list):
        for v in x: walk(v, out)
m=json.load(open('tests/corpus/schema_map.json'))
res={}
for name, e in m.items():
    try:
        docs=list(yaml.safe_load_all(open(e['yaml'])))
    except Exception as ex:
        res[name]={'error':str(ex)}; continue
    out=set()
    for d in docs: walk(d, out)
    res[name]=sorted(f"{d}.{t}" for d,t in out)
json.dump(res, sys.stdout, indent=1)
