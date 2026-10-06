"""Result comparison between Neo4j (the reference) and ClickGraph.

Every normalization rule lives in this file, so a difference that is not a
bug is visible here instead of hidden in a golden.

Rules:
  * numbers compare as floats rounded to 9 significant digits (ClickHouse and
    Neo4j may type an integral aggregate differently; a wrong VALUE still
    differs). Booleans compare as 0/1 (ClickHouse booleans arrive as UInt8).
  * a node or relationship returned by Neo4j is flattened to `col.prop`
    columns, the shape ClickGraph returns. Properties that are NULL are
    dropped on both sides (Neo4j does not store NULL properties). ClickGraph's
    `col.from_id` / `col.to_id` endpoint columns of a relationship are
    dropped (Neo4j does not return them; endpoints are compared through the
    node columns when a query returns them).
  * the internal loader key `__cg_id` is never compared.
  * paths are not comparable yet (ClickGraph returns a different encoding);
    such queries are classified INCOMPARABLE, never MATCH.
  * rows compare as multisets. A LIMIT without ORDER BY makes the row SET
    nondeterministic: only the row count is compared (LIMIT_COUNT_ONLY).
  * ORDER BY ... LIMIT with ties at the cut also picks an arbitrary subset:
    when such a query's rows differ but its counts agree, the runner re-runs
    both sides without the trailing LIMIT; equal full results are LIMIT_TIES.
  * `id()` / `elementId()` values are engine-internal (Neo4j's ids are not
    ClickGraph's), so queries using them are INCOMPARABLE.
  * ClickGraph returns an unlabeled multi-label node as `x.__label__`,
    `x.id`, `x.properties` (JSON text); that encoding is INCOMPARABLE here
    (the HTTP shape is not changed by P-4c).
"""
import json
import math
import re

LOADER_KEY = "__cg_id"


class Incomparable(Exception):
    pass


def norm_value(v):
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        f = float(v)
        if f == 0 or math.isnan(f) or math.isinf(f):
            return f
        return float(f"{f:.9g}")
    if isinstance(v, list):
        return [norm_value(x) for x in v]
    if isinstance(v, dict):
        return {k: norm_value(x) for k, x in sorted(v.items()) if k != LOADER_KEY}
    return v


def _flatten_entity(col, props, out):
    for k, v in props.items():
        if k == LOADER_KEY or v is None:
            continue
        out[f"{col}.{k}"] = norm_value(v)


def neo_rows(result):
    """Neo4j tx-API result -> (canonical rows, entity column kinds)."""
    cols = result["columns"]
    kinds = {}
    rows = []
    for rec in result["data"]:
        out = {}
        for col, val, meta in zip(cols, rec["row"], rec["meta"]):
            kind = meta.get("type") if isinstance(meta, dict) else None
            if isinstance(meta, list) and meta and any(isinstance(m, dict) for m in meta):
                # a path, or a list of entities
                raise Incomparable(f"column {col!r} holds a path or entity list")
            if kind in ("node", "relationship"):
                if not isinstance(val, dict):
                    raise Incomparable(f"column {col!r}: {kind} meta on a non-map value")
                kinds[col] = kind
                _flatten_entity(col, val, out)
            else:
                out[col] = norm_value(val)
        rows.append(out)
    return rows, kinds


def cg_rows(results, kinds):
    """ClickGraph `/query` results -> canonical rows, using Neo4j's entity
    column kinds to recognize flattened entity columns."""
    rows = []
    for rec in results:
        out = {}
        for key, val in rec.items():
            entity = next((c for c in kinds if key.startswith(c + ".")), None)
            if entity is not None:
                prop = key[len(entity) + 1:]
                if kinds[entity] == "relationship" and prop in ("from_id", "to_id"):
                    continue
                if val is None:
                    continue
            out[key] = norm_value(val)
        rows.append(out)
    return rows


def canon(rows):
    """Canonical multiset form: sorted JSON texts, one per row."""
    return sorted(json.dumps(r, sort_keys=True, default=str) for r in rows)


_LIMIT = re.compile(r"\bLIMIT\b", re.I)
_ORDER = re.compile(r"\bORDER\s+BY\b", re.I)
_TRAILING_LIMIT = re.compile(r"\s+LIMIT\s+\d+\s*$", re.I)
_ID_FN = re.compile(r"\b(id|elementId)\s*\(", re.I)


def without_trailing_limit(cypher):
    """The query without its final `LIMIT n`, or None if it has none."""
    stripped = _TRAILING_LIMIT.sub("", cypher.strip())
    return stripped if stripped != cypher.strip() else None


def expected_from_neo4j(cypher, neo_result):
    """Neo4j's answer as a stored expectation: {"kinds", "rows"} with rows in
    canonical form, or raise Incomparable."""
    if _ID_FN.search(cypher):
        raise Incomparable("uses id()/elementId() (engine-internal values)")
    rows, kinds = neo_rows(neo_result)
    return {"kinds": kinds, "rows": canon(rows)}


def compare_expected(cypher, expected, cg_results):
    """ClickGraph's rows vs a stored expectation -> (verdict, detail).
    verdict in MATCH, MISMATCH, LIMIT_COUNT_ONLY, INCOMPARABLE."""
    if any(k.endswith(".__label__") for r in cg_results for k in r):
        return "INCOMPARABLE", "unlabeled multi-label node encoding"
    a = canon(cg_rows(cg_results, expected["kinds"]))
    b = expected["rows"]
    if _LIMIT.search(cypher) and not _ORDER.search(cypher):
        if len(a) == len(b):
            return "LIMIT_COUNT_ONLY", f"{len(b)} rows"
        return "MISMATCH", f"row count {len(a)} vs neo4j {len(b)} (LIMIT without ORDER BY)"
    if a == b:
        return "MATCH", f"{len(b)} rows"
    if len(a) != len(b):
        return "MISMATCH", f"row count {len(a)} vs neo4j {len(b)}"
    only_cg = [r for r in a if r not in b][:2]
    only_neo = [r for r in b if r not in a][:2]
    return "MISMATCH", f"same count {len(a)}; cg-only {only_cg}; neo4j-only {only_neo}"


def compare(cypher, neo_result, cg_results):
    """ClickGraph's rows vs a live Neo4j result -> (verdict, detail)."""
    try:
        expected = expected_from_neo4j(cypher, neo_result)
    except Incomparable as e:
        return "INCOMPARABLE", str(e)
    return compare_expected(cypher, expected, cg_results)
