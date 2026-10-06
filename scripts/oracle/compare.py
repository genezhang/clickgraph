"""Result comparison between Neo4j (the reference) and ClickGraph.

Every normalization rule lives in this file, so a difference that is not a
bug is visible here instead of hidden in a golden.

Values
  * integers compare exactly; a non-integral float is rounded to 12
    significant digits; an integral float equals the same integer (ClickHouse
    and Neo4j may type an aggregate differently). Booleans stay booleans.
  * the loader's internal keys (`__cg_id`, `__cg_from`, `__cg_to`) are never
    compared as properties.

Entities
  * a node or relationship Neo4j returns is flattened to `col.prop` columns,
    the shape ClickGraph returns; NULL properties are dropped on both sides
    (Neo4j stores no NULL properties).
  * a relationship's endpoints ARE compared: the loader stores the endpoint
    node ids on each relationship, and they are matched against ClickGraph's
    `col.from_id` / `col.to_id`.
  * a NULL entity (an unmatched OPTIONAL variable) is no columns on either
    side.
  * paths are not comparable yet (ClickGraph encodes them differently):
    INCOMPARABLE, never MATCH.
  * ClickGraph's unlabeled multi-label node encoding (`x.__label__`, ...) is
    compared by row count only; equal counts are INCOMPARABLE, not MATCH.

Rows
  * rows compare as multisets, and when the final clause has ORDER BY, the
    sequence of ORDER BY key columns must also be equal (ties are equal keys,
    so any order among tied rows is accepted).
  * with a trailing LIMIT, the golden also holds Neo4j's answer WITHOUT the
    LIMIT. ClickGraph's rows must be a sub-multiset of it, of size
    min(n, full); with ORDER BY, the key sequence must equal Neo4j's limited
    key sequence. That accepts exactly the valid answers (any rows tied at the
    cut) and nothing else.
  * when the ORDER BY keys are not all result columns, order cannot be
    checked: without LIMIT only the multiset is compared; with LIMIT the
    query is UNVERIFIED (not scored).
  * in a UNION, a trailing ORDER BY / LIMIT belongs to the LAST arm only:
    order is not checked, and a LIMIT makes the query UNVERIFIED.
  * `id()` / `elementId()` values are engine-internal: INCOMPARABLE.
"""
import hashlib
import json
import math
import re
from collections import Counter

LOADER_KEYS = ("__cg_id", "__cg_from", "__cg_to")


class Incomparable(Exception):
    pass


def norm_value(v):
    if isinstance(v, bool) or v is None or isinstance(v, (str, int)):
        return v
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return str(v)
        if v.is_integer() and abs(v) < 2**53:
            return int(v)
        return float(f"{v:.12g}")
    if isinstance(v, list):
        return [norm_value(x) for x in v]
    if isinstance(v, dict):
        return {k: norm_value(x) for k, x in sorted(v.items()) if k not in LOADER_KEYS}
    return v


def _endpoint(value):
    ids = json.loads(value)
    return norm_value(ids[0] if len(ids) == 1 else ids)


def _flatten_entity(col, kind, props, out):
    for k, v in props.items():
        if k in LOADER_KEYS or v is None:
            continue
        out[f"{col}.{k}"] = norm_value(v)
    if kind == "relationship":
        out[f"{col}.from_id"] = _endpoint(props["__cg_from"])
        out[f"{col}.to_id"] = _endpoint(props["__cg_to"])


def _bare_name(col):
    return re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col) is not None


def neo_rows(result):
    """Neo4j tx-API result -> canonical row dicts, in Neo4j's order."""
    cols = result["columns"]
    rows = []
    for rec in result["data"]:
        out = {}
        for col, val, meta in zip(cols, rec["row"], rec["meta"]):
            if isinstance(meta, list) and any(isinstance(m, dict) for m in meta):
                raise Incomparable(f"column {col!r} holds a path or an entity list")
            kind = meta.get("type") if isinstance(meta, dict) else None
            if kind in ("node", "relationship"):
                if not isinstance(val, dict):
                    raise Incomparable(f"column {col!r}: {kind} meta on a non-map value")
                for k in val:
                    if f"{col}.{k}" in cols:
                        raise Incomparable(f"column {col!r} collides with scalar column {col}.{k}")
                _flatten_entity(col, kind, val, out)
            elif val is None and _bare_name(col):
                continue  # a NULL entity, or a NULL bare-named scalar: no key (see cg_rows)
            else:
                out[col] = norm_value(val)
        rows.append(out)
    return rows


def cg_rows(results, neo_columns):
    """ClickGraph `/query` results -> canonical row dicts, in ClickGraph's
    order. A key `c.p` where `c` is a Neo4j result column that ClickGraph did
    not return as a key of its own is a flattened entity property; NULLs of
    entity properties and of bare-named columns are dropped (as in neo_rows)."""
    neo_columns = set(neo_columns)
    rows = []
    for rec in results:
        out = {}
        for key, val in rec.items():
            if key in neo_columns:
                if val is None and _bare_name(key):
                    continue
                out[key] = norm_value(val)
                continue
            entity = next((c for c in neo_columns if key.startswith(c + ".") and c not in rec), None)
            if entity is not None and val is None:
                continue
            out[key] = norm_value(val)
        rows.append(out)
    return rows


def _dump(row):
    return json.dumps(row, sort_keys=True, default=str)


def canon(rows):
    """Canonical multiset form: sorted JSON texts, one per row."""
    return sorted(_dump(r) for r in rows)


_TRAILING_LIMIT = re.compile(r"\s+LIMIT\s+(\d+)\s*$", re.I)
_UNION = re.compile(r"\bUNION\b", re.I)
_ID_FN = re.compile(r"\b(id|elementId)\s*\(", re.I)
_FINAL_ORDER = re.compile(r"\bORDER\s+BY\s+(.+?)(?:\s+SKIP\s+\d+)?(?:\s+LIMIT\s+\d+)?\s*$", re.I | re.S)


def without_trailing_limit(cypher):
    """The query without its final `LIMIT n`, or None if it has none (or if
    it is a UNION, where that LIMIT belongs to the last arm only)."""
    if _UNION.search(cypher):
        return None
    stripped = _TRAILING_LIMIT.sub("", cypher.strip())
    return stripped if stripped != cypher.strip() else None


def trailing_limit(cypher):
    """n of a trailing `LIMIT n`; for a UNION, "arm" (it limits the last arm)."""
    m = _TRAILING_LIMIT.search(cypher.strip())
    if m and _UNION.search(cypher):
        return "arm"
    return int(m.group(1)) if m else None


def order_keys(cypher, columns):
    """The final ORDER BY's key columns; None without a final ORDER BY;
    "unverifiable" when a key is not a result column."""
    if _UNION.search(cypher):
        return None  # a trailing ORDER BY orders the last arm only
    text = cypher.strip()
    tail = text[max(text.upper().rfind("RETURN "), 0):]
    m = _FINAL_ORDER.search(tail)
    if not m:
        return None
    keys = []
    for part in m.group(1).split(","):
        expr = re.sub(r"\s+(ASC|ASCENDING|DESC|DESCENDING)\s*$", "", part.strip(), flags=re.I)
        if expr not in columns:
            return "unverifiable"
        keys.append(expr)
    return keys


def expected_from_neo4j(cypher, neo_result, neo_full_result=None):
    """Neo4j's answer as a stored expectation, or raise Incomparable.
    `neo_full_result` is the answer without the trailing LIMIT, if any."""
    if _ID_FN.search(cypher):
        raise Incomparable("uses id()/elementId() (engine-internal values)")
    rows = neo_rows(neo_result)
    exp = {
        "columns": neo_result["columns"],
        "rows": canon(rows),
        "order_keys": order_keys(cypher, neo_result["columns"]),
    }
    if isinstance(exp["order_keys"], list):
        exp["key_sequence"] = [[r.get(k) for k in exp["order_keys"]] for r in rows]
    limit = trailing_limit(cypher)
    if limit is not None:
        exp["limit"] = limit
        if neo_full_result is not None:
            exp["full_rows"] = canon(neo_rows(neo_full_result))
    return exp


def _is_submultiset(small, big):
    need, have = Counter(small), Counter(big)
    return all(have[k] >= n for k, n in need.items())


def compare_expected(cypher, expected, cg_results):
    """ClickGraph's rows vs a stored expectation -> (verdict, detail).
    verdict in MATCH, MISMATCH, UNVERIFIED, INCOMPARABLE."""
    rows = cg_rows(cg_results, expected["columns"])
    a = canon(rows)
    if any(k.endswith(".__label__") for r in cg_results for k in r):
        if len(a) != len(expected["rows"]):
            return "MISMATCH", f"row count {len(a)} vs neo4j {len(expected['rows'])}"
        return "INCOMPARABLE", "unlabeled multi-label node encoding (counts equal)"
    keys = expected["order_keys"]
    if keys == "unverifiable" and "limit" in expected:
        return "UNVERIFIED", "ORDER BY keys are not result columns; LIMIT picks an unverifiable subset"
    if isinstance(keys, list):
        seq = [[r.get(k) for k in keys] for r in rows]
        if seq != expected["key_sequence"]:
            return "MISMATCH", f"ORDER BY key sequence differs: cg {seq[:4]} vs neo4j {expected['key_sequence'][:4]}"
    if "limit" in expected:
        if expected["limit"] == "arm":
            return "UNVERIFIED", "LIMIT inside a UNION arm"
        if "full_rows" not in expected:
            return "UNVERIFIED", "no un-limited Neo4j answer stored"
        want = min(expected["limit"], len(expected["full_rows"]))
        if len(a) != want:
            return "MISMATCH", f"row count {len(a)} vs {want} (LIMIT {expected['limit']})"
        if not _is_submultiset(a, expected["full_rows"]):
            bad = [r for r in a if r not in expected["full_rows"]][:2]
            return "MISMATCH", f"rows outside Neo4j's full answer: {bad}"
        return "MATCH", f"{len(a)} rows (a valid LIMIT subset)"
    b = expected["rows"]
    if a == b:
        return "MATCH", f"{len(b)} rows"
    if len(a) != len(b):
        return "MISMATCH", f"row count {len(a)} vs neo4j {len(b)}"
    only_cg = [r for r in a if r not in b][:2]
    only_neo = [r for r in b if r not in a][:2]
    return "MISMATCH", f"same count {len(a)}; cg-only {only_cg}; neo4j-only {only_neo}"


def signature(verdict, detail, cg_results):
    """A stable fingerprint of a WRONG outcome, so that a known-wrong entry
    drifting to a different wrong answer is noticed, not silently accepted."""
    if cg_results is not None:
        text = verdict + "|" + "\n".join(canon([{k: norm_value(v) for k, v in r.items()} for r in cg_results]))
    else:
        # error text: generated aliases and numbers vary, the error kind does not
        text = verdict + "|" + re.sub(r"\d+", "N", detail)[:160]
    return hashlib.sha1(text.encode()).hexdigest()[:16]
