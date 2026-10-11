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
  * a SELF-REFERENCING FK edge (one label at both ends, its table that
    label's table) relates each row's node to the node its reference names,
    whichever of from_id / to_id holds the reference: the side whose columns
    are not the node's id (#632; `from_id: parent_id, to_id: object_id` is
    child -> parent, as the FK-edge wiki documents);
  * a DENORMALIZED (embedded) label, whose definitions' tables are tables of
    edges it is an end of, has no rows of its own: each role of each
    definition (from_node_properties / to_node_properties, plus the
    definition's non-id property_mappings, the role's winning) is a source,
    and its nodes are the distinct non-NULL ids the sources' rows hold.
    `node_id` names the id's properties (or a role's columns of them). A
    property is the value its sources hold for the id; an id whose rows hold
    different values of one property (NULL in a column a role declares is a
    value: NULL against a value is a difference) is INCONSISTENT: the
    property is left off that node and reported (the schema says the
    property is the node's);
  * an edge end embedded in a source of its label names the id's columns:
    they are read in the source's id order (a composite end may list them in
    another);
  * a denormalized definition whose table is no edge table of its label (a
    "foreign" one: edges carry its id) is a node table as above;
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


def _key(values):
    """A node key: the id's values, an array value as a tuple (hashable)."""
    return tuple(tuple(v) if isinstance(v, list) else v for v in values)


def _id_cols(spec):
    v = spec if isinstance(spec, list) else [spec]
    return [c if isinstance(c, str) else c["column"] for c in v]


NODE_KEYS = {
    "label", "database", "table", "node_id", "property_mappings", "property_types", "filter",
    # Denormalized nodes: their properties in an edge table's columns, per
    # role (`is_denormalized` is not a schema field; it documents them).
    "from_node_properties", "to_node_properties", "is_denormalized",
    # Labels sharing a table: a label's nodes are the rows whose label
    # column holds its value.
    "label_column", "label_value",
}
EDGE_KEYS = {
    "type", "database", "table", "from_id", "to_id", "edge_id", "from_node", "to_node",
    "property_mappings", "property_types", "filter",
    # Not a schema field (serde ignores it): documents that the edge lives in
    # a node table (an FK edge). Loading it row by row is the same rule.
    "is_denormalized",
    # Polymorphic edges: a row's type and end labels are its type / label
    # columns' values (an end with a fixed `from_node` / `to_node` is that
    # label), closed to `type_values` and the `*_label_values` when given.
    "polymorphic", "type_column", "type_values", "from_label_column", "to_label_column",
    "from_label_values", "to_label_values",
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
        for side in ("from", "to"):
            if e.get(f"{side}_node") in (None, "$any") and not e.get(f"{side}_label_column"):
                raise Unsupported(f"edge {e.get('type') or e.get('type_values')}: {side} end without a label")
        if e.get("polymorphic") and len(e.get("type_values") or []) > 1 and not e.get("type_column"):
            raise Unsupported(f"edge {e.get('type_values')}: several types without a type column")


def build_graph(gs, ch):
    """Return (nodes, rels, report). nodes: list of (label, key, props);
    rels: list of (type, from_label, from_key, to_label, to_key, props)."""
    _check_standard(gs)
    nodes, index = [], {}
    report = {"nodes": {}, "rels": {}, "dangling": {}}
    embedded = {}
    for n in gs.get("nodes", []):
        roles = [n.get("from_node_properties"), n.get("to_node_properties")]
        hosts = any(
            (e["database"], e["table"]) == (n["database"], n["table"])
            and n["label"] in (e.get("from_node"), e.get("to_node"))
            for e in gs.get("edges", [])
        )
        if any(roles) and hosts:
            embedded.setdefault(n["label"], []).append(n)
            continue
        pm = n.get("property_mappings") or {}
        idc = [pm.get(p, p) for p in _id_cols(n["node_id"])]
        cols = sorted(set(idc) | set(pm.values()))
        # A schema `filter:` is SQL over the table's columns: the label's nodes
        # are the rows it holds of.
        conds = [f"({n['filter']})"] if n.get("filter") else []
        if n.get("label_column"):
            conds.append(f"`{n['label_column']}` = '{n['label_value']}'")
        where = f" WHERE {' AND '.join(conds)}" if conds else ""
        rows = ch(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM `{n['database']}`.`{n['table']}`{where}")
        for r in rows:
            key = _key(r[c] for c in idc)
            pt = n.get("property_types") or {}
            props = {p: _typed(r[c], pt.get(p)) for p, c in pm.items()}
            index[(n["label"], key)] = True
            nodes.append((n["label"], key, props))
        report["nodes"][n["label"]] = len(rows)
    sources = {}
    for label, defs in embedded.items():
        sources[label] = _embedded_nodes(label, defs, ch, nodes, index, report)
    rels = []
    node_defs = {n["label"]: n for n in gs.get("nodes", [])}
    for e in gs.get("edges", []):
        if e.get("polymorphic"):
            _polymorphic_rels(e, ch, index, rels, report)
            continue
        fc, tc = _id_cols(e["from_id"]), _id_cols(e["to_id"])
        own = node_defs.get(e["from_node"])
        if (
            e["from_node"] == e["to_node"]
            and own is not None
            and (own["database"], own["table"]) == (e["database"], e["table"])
            and not (own.get("from_node_properties") or own.get("to_node_properties"))
        ):
            pm_own = own.get("property_mappings") or {}
            own_id = [pm_own.get(p, p) for p in _id_cols(own["node_id"])]
            fc, tc = own_id, (tc if fc == own_id else fc)
        fc = _ordered_end(sources.get(e["from_node"]), e, fc)
        tc = _ordered_end(sources.get(e["to_node"]), e, tc)
        pm = e.get("property_mappings") or {}
        cols = sorted(set(fc) | set(tc) | set(pm.values()))
        # A schema `filter:`: the type's relationships are the rows it holds of.
        where = f" WHERE {e['filter']}" if e.get("filter") else ""
        rows = ch(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM `{e['database']}`.`{e['table']}`{where}")
        dangling = 0
        for r in rows:
            fk, tk = _key(r[c] for c in fc), _key(r[c] for c in tc)
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


def _polymorphic_rels(e, ch, index, rels, report):
    """A polymorphic edge's relationships: each row of one of `type_values`
    (its type column's value; the one type without a column) between the
    nodes its ends name: a fixed end's label, else its label column's value
    (one of the `*_label_values` when given). Read as written (no
    self-referencing FK rule: legacy reads them so)."""
    fc, tc = _id_cols(e["from_id"]), _id_cols(e["to_id"])
    pm = e.get("property_mappings") or {}
    disc = [c for c in (e.get("type_column"), e.get("from_label_column"), e.get("to_label_column")) if c]
    cols = sorted(set(fc) | set(tc) | set(pm.values()) | set(disc))
    where = f" WHERE {e['filter']}" if e.get("filter") else ""
    rows = ch(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM `{e['database']}`.`{e['table']}`{where}")
    types = e["type_values"]
    kept, dangling = {}, {}

    def label(r, side):
        fixed = e.get(f"{side}_node")
        if fixed not in (None, "$any"):
            return fixed
        value = r[e[f"{side}_label_column"]]
        allowed = e.get(f"{side}_label_values")
        return value if allowed is None or value in allowed else None

    for r in rows:
        t = r[e["type_column"]] if e.get("type_column") else types[0]
        fl, tl = label(r, "from"), label(r, "to")
        if t not in types or fl is None or tl is None:
            continue
        fk, tk = _key(r[c] for c in fc), _key(r[c] for c in tc)
        if any(v is None for v in fk + tk):
            continue
        if (fl, fk) not in index or (tl, tk) not in index:
            dangling[t] = dangling.get(t, 0) + 1
            continue
        pt = e.get("property_types") or {}
        props = {p: _typed(r[c], pt.get(p)) for p, c in pm.items()}
        props["__cg_from"] = json.dumps(list(fk))
        props["__cg_to"] = json.dumps(list(tk))
        rels.append((t, fl, fk, tl, tk, props))
        kept[t] = kept.get(t, 0) + 1
    for t in types:
        report["rels"][t] = report["rels"].get(t, 0) + kept.get(t, 0)
        if dangling.get(t):
            report["dangling"][t] = report["dangling"].get(t, 0) + dangling[t]


def _ordered_end(label_sources, e, cols):
    """An edge end embedded in a source of its label, in the id's order."""
    if not label_sources:
        return cols
    id_props, srcs = label_sources
    for table, columns in srcs:
        if table == (e["database"], e["table"]) and sorted(columns[p] for p in id_props) == sorted(cols):
            return [columns[p] for p in id_props]
    return cols


def _embedded_nodes(label, defs, ch, nodes, index, report):
    """The nodes of a denormalized label (see the module doc); returns its id
    properties and its sources ((database, table), columns)."""
    srcs = []
    values, conflicts = {}, set()
    id_props = None
    for n in defs:
        pm = n.get("property_mappings") or {}
        roles = [r for r in (n.get("from_node_properties"), n.get("to_node_properties")) if r]
        props = []
        for name in _id_cols(n["node_id"]):
            if any(name in r for r in roles):
                props.append(name)
                continue
            found = [p for r in roles for p, c in r.items() if c == name]
            if not found:
                raise Unsupported(f"node {label}: id {name} is no role's property")
            props.append(found[0])
        if id_props not in (None, props):
            raise Unsupported(f"node {label}: definitions with different ids")
        id_props = props
        pt = n.get("property_types") or {}
        for role in roles:
            columns = {**{p: c for p, c in pm.items() if p not in props}, **role}
            if any(p not in columns for p in props):
                raise Unsupported(f"node {label}: a role without the id")
            srcs.append(((n["database"], n["table"]), columns))
            cols = sorted(set(columns.values()))
            where = f" WHERE {n['filter']}" if n.get("filter") else ""
            rows = ch(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM `{n['database']}`.`{n['table']}`{where}")
            for r in rows:
                key = _key(r[columns[p]] for p in props)
                if any(v is None for v in key):
                    continue
                held = values.setdefault(key, {})
                for p, c in columns.items():
                    # A NULL in a column a role declares is that row's
                    # value: against another row's value it is a conflict
                    # (the schema says the property is the node's).
                    v = _typed(r[c], pt.get(p))
                    if p in held and held[p] != v:
                        conflicts.add((key, p))
                    held.setdefault(p, v)
    inconsistent = set()
    for key, props in values.items():
        for (k, p) in conflicts:
            if k == key:
                props.pop(p, None)
                inconsistent.add(p)
        index[(label, key)] = True
        nodes.append((label, key, {p: v for p, v in props.items() if v is not None}))
    report["nodes"][label] = len(values)
    if inconsistent:
        report.setdefault("inconsistent", {})[label] = sorted(inconsistent)
    return id_props, srcs


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
