"""Build the LOGICAL property graph of one ClickGraph schema from its YAML and
ClickHouse tables, and load it into Neo4j (the P-4c reference oracle).

Deliberately small and declarative: it is a second implementation of the
layout rules, so every rule here must be obvious from the YAML. Supported
layouts are listed in LAYOUT_RULES; anything else raises Unsupported.

Rules (standard / separate-edge-table layout; anything else raises
Unsupported, decided by an ALLOWLIST of schema keys):
  * one node per row of a node table, labelled with the node's label, with
    every mapped property (Cypher name -> column value) and an internal
    `__cg_id` = the node id value(s). `node_id` names a PROPERTY; its column
    is resolved through `property_mappings`;
  * relationships carry internal `__cg_from` / `__cg_to` keys (the endpoint
    node ids) so the comparator can check endpoints;
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
    # 64-bit integers as JSON numbers (exact in Python), whatever the server default
    sep = "&" if "?" in ch_url else "?"
    req = urllib.request.Request(
        f"{ch_url}{sep}output_format_json_quote_64bit_integers=0",
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
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true")
        return bool(value)
    if t in ("integer", "int", "int64", "long"):
        return int(value)
    if t in ("float", "double", "float64"):
        return float(value)
    return value


def _id_cols(spec):
    v = spec if isinstance(spec, list) else [spec]
    return [c if isinstance(c, str) else c["column"] for c in v]


NODE_KEYS = {"label", "database", "table", "node_id", "property_mappings", "property_types", "filter"}
EDGE_KEYS = {
    "type", "database", "table", "from_id", "to_id", "edge_id", "from_node", "to_node",
    "property_mappings", "property_types", "filter",
    # Not a schema field (serde ignores it): documents that the edge lives in
    # a node table (an FK edge). Loading it row by row is the same rule.
    "is_denormalized",
}
SCHEMA_KEYS = {"nodes", "edges"}


def _check_standard(gs):
    """Allowlist: any schema key this loader has no rule for is Unsupported."""
    extra = set(gs) - SCHEMA_KEYS
    if extra:
        raise Unsupported(f"graph_schema keys {sorted(extra)}")
    for n in gs.get("nodes", []):
        extra = set(n) - NODE_KEYS
        if extra:
            raise Unsupported(f"node {n.get('label')}: {sorted(extra)}")
    for e in gs.get("edges", []):
        extra = set(e) - EDGE_KEYS
        if extra:
            raise Unsupported(f"edge {e.get('type')}: {sorted(extra)}")
        if e.get("from_node") in (None, "$any") or e.get("to_node") in (None, "$any"):
            raise Unsupported(f"edge {e['type']}: polymorphic endpoints")


def build_graph(gs, ch):
    """Return (nodes, rels, report). nodes: list of (label, key, props);
    rels: list of (type, from_label, from_key, to_label, to_key, props)."""
    _check_standard(gs)
    nodes, index = [], {}
    report = {"nodes": {}, "rels": {}, "dangling": {}}
    for n in gs.get("nodes", []):
        pm = n.get("property_mappings") or {}
        idc = [pm.get(p, p) for p in _id_cols(n["node_id"])]
        cols = sorted(set(idc) | set(pm.values()))
        # A schema `filter:` is SQL over the table's columns: the label's nodes
        # are the rows it holds of.
        where = f" WHERE {n['filter']}" if n.get("filter") else ""
        rows = ch(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM `{n['database']}`.`{n['table']}`{where}")
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
        # A schema `filter:`: the type's relationships are the rows it holds of.
        where = f" WHERE {e['filter']}" if e.get("filter") else ""
        rows = ch(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM `{e['database']}`.`{e['table']}`{where}")
        dangling = 0
        for r in rows:
            fk, tk = tuple(r[c] for c in fc), tuple(r[c] for c in tc)
            if any(v is None for v in fk + tk):
                continue  # no edge: a NULL endpoint column is not a relationship
            if (e["from_node"], fk) not in index or (e["to_node"], tk) not in index:
                dangling += 1
                continue
            pt = e.get("property_types") or {}
            props = {p: _typed(r[c], pt.get(p)) for p, c in pm.items()}
            props["__cg_from"] = json.dumps(list(fk))
            props["__cg_to"] = json.dumps(list(tk))
            rels.append((e["type"], e["from_node"], fk, e["to_node"], tk, props))
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
