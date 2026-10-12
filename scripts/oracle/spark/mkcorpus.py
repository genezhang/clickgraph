import json, sys
schema, src, dst = sys.argv[1], sys.argv[2], sys.argv[3]
with open(dst, 'w') as out:
    for i, q in enumerate(l.strip() for l in open(src) if l.strip() and not l.startswith('#')):
        out.write(json.dumps({"schema": schema, "name": f"q{i:03d}", "cypher": q}) + "\n")
