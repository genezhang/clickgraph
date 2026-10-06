"""Build the LOGICAL property graph of one ClickGraph schema from its YAML and
ClickHouse tables, and load it into Neo4j (the P-4c reference oracle).

Deliberately small and declarative: it is a second implementation of the
layout rules, so every rule here must be obvious from the YAML. Supported
layouts are listed in LAYOUT_RULES; anything else raises Unsupported.

Rules (standard / separate-edge-table layout):
  * one node per row of a node table, labelled with the node's label, with
    every mapped property (Cypher name -> column value) and an internal
    `__cg_id` = the node_id column value(s);
  * one relationship per row of an edge table, from the node whose id equals
    the row's from_id to the node whose id equals the row's to_id, typed with
    the edge type, with every mapped edge property;
  * an edge row whose endpoint does not exist as a node is DANGLING: Neo4j
    cannot hold it, so it is counted and reported (never silently dropped).
  * a property declared in the schema's `property_types` is converted to that
    type (`boolean` -> true/false); an undeclared property keeps the value
    ClickHouse returns (a UInt8 flag stays an integer, so `x.flag = true` is
    false in Neo4j, as strict Cypher typing says).
"""
import json
import urllib.request

import yaml


class Unsupported(Exception):
    pass


def ch_rows(ch_url, user, password, sql):
    req = urllib.request.Request(
        ch_url,
        data=(sql + " FORMAT JSONEachRow").encode(),
        headers={"X-ClickHouse-User": user, "X-ClickHouse-Key": password},
    )
    body = urllib.request.urlopen(req).read().decode()
    return [json.loads(l) for l in body.splitlines() if l]


SCHEMA_MAP = "tests/corpus/schema_map.json"


def corpus_schema_doc(corpus_schema, repo="."):
    """The schema document a corpus entry's `schema` tag names, resolved the
    way the corpus sweep resolves it (`tests/corpus/schema_map.json`: a YAML
    file, optionally a named block of a multi-schema file)."""
    entry = json.load(open(f"{repo}/{SCHEMA_MAP}"))[corpus_schema]
    doc = yaml.safe_load(open(f"{repo}/{entry['yaml']}"))
    sub = entry.get("subschema")
    if sub:
        return next(s for s in doc["schemas"] if s["name"] == sub)
    return doc


def registration_yaml(corpus_schema, private_name, repo="."):
    """YAML for `POST /schemas/load` registering the corpus schema under a
    private name, so no other registration can change its tables."""
    doc = dict(corpus_schema_doc(corpus_schema, repo))
    doc["name"] = private_name
    return yaml.safe_dump(doc, sort_keys=False)


def _typed(value, declared):
    if value is None or declared is None:
        return value
    t = declared.lower()
    if t in ("boolean", "bool"):
        return bool(value)
    if t in ("integer", "int", "int64", "long"):
        return int(value)
    if t in ("float", "double", "float64"):
        return float(value)
    return value


def _id_cols(spec):
    v = spec if isinstance(spec, list) else [spec]
    return [c if isinstance(c, str) else c["column"] for c in v]


def _check_standard(gs):
    for n in gs.get("nodes", []):
        for key in ("filter", "view_parameters", "label_column", "type_column"):
            if n.get(key):
                raise Unsupported(f"node {n['label']}: {key}")
    for e in gs.get("edges", []):
        for key in (
            "from_node_properties",
            "to_node_properties",
            "type_column",
            "from_label_column",
            "to_label_column",
            "filter",
            "view_parameters",
        ):
            if e.get(key):
                raise Unsupported(f"edge {e['type']}: {key}")
        if e.get("from_node") in (None, "$any") or e.get("to_node") in (None, "$any"):
            raise Unsupported(f"edge {e['type']}: polymorphic endpoints")


def build_graph(gs, ch):
    """Return (nodes, rels, report). nodes: list of (label, key, props);
    rels: list of (type, from_label, from_key, to_label, to_key, props)."""
    _check_standard(gs)
    nodes, index = [], {}
    report = {"nodes": {}, "rels": {}, "dangling": {}}
    for n in gs.get("nodes", []):
        idc = _id_cols(n["node_id"])
        pm = n.get("property_mappings") or {}
        cols = sorted(set(idc) | set(pm.values()))
        rows = ch(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM `{n['database']}`.`{n['table']}`")
        for r in rows:
            key = tuple(r[c] for c in idc)
            pt = n.get("property_types") or {}
            props = {p: _typed(r[c], pt.get(p)) for p, c in pm.items()}
            index[(n["label"], key)] = True
            nodes.append((n["label"], key, props))
        report["nodes"][n["label"]] = len(rows)
    rels = []
    for e in gs.get("edges", []):
        fc, tc = _id_cols(e["from_id"]), _id_cols(e["to_id"])
        pm = e.get("property_mappings") or {}
        cols = sorted(set(fc) | set(tc) | set(pm.values()))
        rows = ch(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM `{e['database']}`.`{e['table']}`")
        dangling = 0
        for r in rows:
            fk, tk = tuple(r[c] for c in fc), tuple(r[c] for c in tc)
            if any(v is None for v in fk + tk):
                continue  # no edge: a NULL endpoint column is not a relationship
            if (e["from_node"], fk) not in index or (e["to_node"], tk) not in index:
                dangling += 1
                continue
            pt = e.get("property_types") or {}
            rels.append((e["type"], e["from_node"], fk, e["to_node"], tk,
                         {p: _typed(r[c], pt.get(p)) for p, c in pm.items()}))
        report["rels"][e["type"]] = len(rows) - dangling
        if dangling:
            report["dangling"][e["type"]] = dangling
    return nodes, rels, report


def neo_run(neo_url, statements):
    body = json.dumps({"statements": [{"statement": s, "parameters": p} for s, p in statements]}).encode()
    req = urllib.request.Request(neo_url, data=body, headers={"Content-Type": "application/json"})
    r = json.loads(urllib.request.urlopen(req).read())
    if r["errors"]:
        raise RuntimeError(r["errors"])
    return r["results"]


def load_into_neo4j(neo_url, nodes, rels):
    neo_run(neo_url, [("MATCH (n) DETACH DELETE n", {})])
    by_label = {}
    for label, key, props in nodes:
        by_label.setdefault(label, []).append({"k": json.dumps(list(key)), "p": props})
    for label, rows in by_label.items():
        neo_run(neo_url, [(f"UNWIND $rows AS r CREATE (n:`{label}`) SET n = r.p, n.__cg_id = r.k", {"rows": rows})])
    try:
        neo_run(neo_url, [("CREATE INDEX cg_id IF NOT EXISTS FOR (n:__CG) ON (n.__cg_id)", {})])
    except RuntimeError:
        pass
    by_type = {}
    for t, fl, fk, tl, tk, props in rels:
        by_type.setdefault((t, fl, tl), []).append({"f": json.dumps(list(fk)), "t": json.dumps(list(tk)), "p": props})
    for (t, fl, tl), rows in by_type.items():
        for i in range(0, len(rows), 5000):
            neo_run(
                neo_url,
                [(
                    f"UNWIND $rows AS r MATCH (a:`{fl}` {{__cg_id: r.f}}), (b:`{tl}` {{__cg_id: r.t}}) "
                    f"CREATE (a)-[x:`{t}`]->(b) SET x = r.p",
                    {"rows": rows[i:i + 5000]},
                )],
            )
