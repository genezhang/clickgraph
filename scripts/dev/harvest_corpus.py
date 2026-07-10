#!/usr/bin/env python3
"""Harvest a Cypher-query corpus (+ its schema) from the Python test suite.

Part of Phase 0 slice P0.6 (docs/design/REFACTORING_SAFETY_PLAN.md §3.2): a
mass "translate everything, lock the bytes" regression net. This script
statically extracts Cypher query literals + the schema YAML each was
actually run against from `tests/integration/**/*.py` and
`tests/sql_generation/**/*.py`, and writes:

  - `tests/corpus/queries.jsonl`   — one `{"schema", "name", "cypher"}` JSON
    object per line, sorted by (schema, name), deduped on (schema, cypher).
  - `tests/corpus/schema_map.json` — `{schema_key: {"yaml": <path>,
    "subschema": <name-or-null>}}`, consumed by the Rust sweep
    (`tests/rust/integration/corpus_sweep.rs`) to load each schema.

Regenerate with:  python3 scripts/dev/harvest_corpus.py
(run from anywhere — paths are resolved from this file's location). The
output is committed; re-running should be diff-stable modulo genuine
test-suite changes (deterministic file/AST traversal order + stable naming).

WHY STATIC, NOT DYNAMIC: the corpus sweep (Rust side) needs no live
ClickHouse/server — it parses+plans+renders SQL directly. So the queries
just need to be lifted as literal Cypher strings bound to the correct schema
YAML; nothing here executes a query or requires a running stack.

HOW EXTRACTION WORKS (static, best-effort, conservative on ambiguity):
For each `test_*.py` file (pytest's collected-file glob), every `def
test_*` function is walked with a constant-folding evaluator (`resolve_str`)
threaded through a hand-rolled small "sequential statement flattener"
(`flatten_stmts`) that descends into `if`/`for`/`while`/`with`/`try` bodies
(but not nested `def`s — different scope) so that

    payload = {"query": q}
    if schema_name:
        payload["schema_name"] = schema_name
    requests.post(url, json=payload)

is traced just like an inline dict literal. A call is treated as
query-shaped if:
  - it's a direct `requests.post`/`.post`/`.get`-style call (any object —
    `requests.post`, `session.post`, ... — matched by method name only) with
    a `json=` payload (literal or traced) containing a `"query"`/`"cypher"`
    key, OR
  - it's a call to a function DEFINED IN THE SAME FILE (module- or
    class-level) whose own body matches the shape above — the call's
    argument matching the query-carrying parameter and the schema-carrying
    parameter (if any) is resolved at the CALL SITE using the same
    evaluator + the local variable environment built while walking the
    test function's body in order, or the target function's own default
    parameter value if the call site doesn't override it.

`pytest.mark.parametrize` decorators are expanded: each literal row in the
decorator's arg-value list produces its own harvested entry with those
parameter names bound for that row.

SCOPE (deliberately narrower than "every test"):
  - Only `test_*.py` files (pytest's collected-file glob per pytest.ini) —
    `conftest.py`, `check_*.py`, `debug_*.py`, `script_test_*.py`,
    `setup_*.py`, `load_schema_*.py` etc. are dev/debug scripts pytest never
    collects and are skipped. A handful of files DO match `test_*.py` but
    contain no `def test_...` at all (bare top-level scripts, e.g.
    `test_simple_query.py`, `test_use_clause.py`) — pytest collects them but
    runs zero test items, so the harvester correctly contributes nothing.
  - `tests/integration/matrix/*` is skipped: those tests build queries via
    f-string interpolation of label/edge/property names drawn at RUNTIME
    from a fixture-provided schema dict (`@pytest.fixture(params=...)`) —
    genuinely generative, not statically resolvable. Counted under
    `generative_matrix_dir`.
  - `tests/integration/suites/*` has no Cypher literals (SQL/YAML fixture
    dirs only) and `tests/integration/query_patterns/test_generator.py` /
    `validate_patterns.py` are codegen/validation utilities with zero actual
    `test_*` functions — nothing to harvest there either way.

SCHEMA RESOLUTION (the hard part — see docs/design/REFACTORING_SAFETY_PLAN.md
P0.6 spec: "queries harvested under the WRONG schema ... pollute the
corpus"):
  1. An embedded literal `USE <name> ` prefix inside the query string ALWAYS
     wins (matches `execute_cypher()`'s own precedence in conftest.py) — the
     harvester strips it from the stored Cypher and uses `<name>` as the
     schema key.
  2. Otherwise, the schema is resolved from the call site's `schema_name`
     (or equivalent) argument via the constant-folding pass described above,
     plus a couple of NAMED, DOCUMENTED special cases for indirections that
     are not plain string literals (`_SCHEMA_NAME_HELPER`,
     `_FIXTURE_SUBSCRIPT_OVERRIDES`, `_SELF_ATTR_SCHEMA_LOOKUP` below).
  3. If the schema cannot be resolved with confidence, the query is SKIPPED
     (reason `schema_unresolved`) rather than guessed — a wrong guess that
     happens to translate without erroring would silently lock wrong SQL,
     which is worse than a smaller corpus.

The schema-name -> YAML mapping itself has four sources, in precedence
order (later sources only apply if the name isn't already claimed):
  (a) `_BASE_SCHEMA_MAP`, transcribed from `tests/integration/conftest.py`'s
      `load_all_test_schemas` fixture — authoritative for `tests/integration`
      because that autouse, session-scoped fixture POSTs to `/schemas/load`
      AFTER server boot, so it overwrites whatever `GRAPH_CONFIG_PATH`
      (`schemas/test/unified_test_multi_schema.yaml`) registered at startup
      for any name it also defines (last-write-wins, confirmed at
      `src/server/graph_catalog.rs` — unconditional map insert).
  (b) Names that exist ONLY inside `schemas/test/unified_test_multi_schema.yaml`
      (never re-registered by (a)) — auto-discovered by parsing that file's
      `schemas:` list; these stay resolved to their server-boot definition
      via `subschema` (a named block inside the multi-schema YAML, not a
      standalone file).
  (c) `tests/sql_generation/test_schema_sql_generation.py` draws from a
      THIRD, non-overlapping schema source (`schemas/test/schema_variations.yaml`)
      with schema names (`standard`, `fk_edge`, ...) that collide with (a)'s
      names but mean DIFFERENT YAML content. These are namespaced with a
      `sqlgen_` prefix (`sqlgen_standard`, ...) to avoid silently conflating
      the two.
  (d) A handful of files load their OWN schema dynamically (a local
      `open(<path>).read()` POSTed to `/schemas/load` with an explicit
      `schema_name`) — detected generically (not by filename) by scanning
      for that call shape; the schema-name -> yaml_path pair it registers is
      added to the map before that file's queries are resolved.

Anything not resolved by (a)-(d) and not an embedded `USE` clause is skipped.
"""

from __future__ import annotations

import ast
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
INTEGRATION_DIR = REPO_ROOT / "tests" / "integration"
SQL_GENERATION_DIR = REPO_ROOT / "tests" / "sql_generation"
OUT_JSONL = REPO_ROOT / "tests" / "corpus" / "queries.jsonl"
OUT_SCHEMA_MAP = REPO_ROOT / "tests" / "corpus" / "schema_map.json"

# ---------------------------------------------------------------------------
# (a) Base schema map — transcribed from conftest.py's `load_all_test_schemas`
# (tests/integration/conftest.py, ~line 310) + its DATABRICKS_SCHEMA_FILES.
# Re-verify against that fixture if this script's output looks off.
# ---------------------------------------------------------------------------
_BASE_SCHEMA_MAP: Dict[str, str] = {
    "social_integration": "schemas/test/social_integration.yaml",
    "test_fixtures": "schemas/test/test_fixtures.yaml",
    "denormalized_flights_test": "schemas/test/denormalized_flights.yaml",
    "data_security": "examples/data_security/data_security.yaml",
    "property_expressions": "schemas/test/property_expressions.yaml",
    "property_expressions_simple": "schemas/test/property_expressions_simple.yaml",
    "group_membership": "schemas/test/group_membership_simple.yaml",
    "multi_tenant": "schemas/test/multi_tenant.yaml",
    "mixed_denorm_test": "schemas/test/mixed_denorm_test.yaml",
    "filesystem": "schemas/examples/filesystem.yaml",
    "ontime_flights": "schemas/examples/ontime_denormalized.yaml",
    "social_polymorphic": "schemas/examples/social_polymorphic.yaml",
    "zeek_dns": "schemas/examples/zeek_dns_log.yaml",
    "standard": "schemas/test/social_integration.yaml",
    "fk_edge": "schemas/examples/orders_customers_fk.yaml",
    "polymorphic": "schemas/examples/social_polymorphic.yaml",
    "composite_id": "schemas/examples/composite_node_id_test.yaml",
    "zeek_merged_test": "schemas/examples/zeek_merged.yaml",
}

_UNIFIED_MULTI_SCHEMA_YAML = "schemas/test/unified_test_multi_schema.yaml"
_SQLGEN_SCHEMA_VARIATIONS_YAML = "schemas/test/schema_variations.yaml"

# Default schema for `execute_cypher()` when no schema_name is given at all
# (conftest.py's own default).
_DEFAULT_SCHEMA = "social_integration"

# (e) Named, documented special-case resolvers for schema arguments that
# aren't plain string literals.
#   - `simple_graph["schema_name"]` (a Subscript on the `simple_graph`
#     fixture parameter) always evaluates to "test_fixtures" — see
#     conftest.py's `simple_graph` fixture return value.
#   - `denormalized_flights_graph["schema_name"]` (test_denormalized_edges.py's
#     module-scoped fixture) always evaluates to "denormalized_flights" — it
#     dynamically POSTs `schemas/test/denormalized_flights.yaml` under that
#     exact name and returns `{"schema_name": "denormalized_flights", ...}`.
_FIXTURE_SUBSCRIPT_OVERRIDES = {
    "simple_graph": "test_fixtures",
    "denormalized_flights_graph": "denormalized_flights",
}
# `_schema_name(loaded_schemas, "<key>")` in
# tests/sql_generation/test_schema_sql_generation.py always resolves to
# "sqlgen_<key>" (see `_schema_name`/`load_schema_to_server` in that file —
# schemas are loaded under a "test_" prefix drawn from schema_variations.yaml,
# a THIRD schema source distinct from conftest.py's identically-named keys).
_SCHEMA_NAME_HELPER = "_schema_name"
_SQLGEN_PREFIX = "sqlgen_"

WRITE_KEYWORDS = re.compile(r"\b(CREATE|MERGE|SET|DELETE|REMOVE)\b", re.IGNORECASE)
PARAM_REF = re.compile(r"\$[A-Za-z_]")
USE_PREFIX = re.compile(r"^\s*USE\s+`?(\w+)`?\s+", re.IGNORECASE)
_LINE_COMMENT = re.compile(r"//[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
QUERY_LOOKING = re.compile(r"\b(MATCH|RETURN|WITH|UNWIND|CALL)\b", re.IGNORECASE)


def strip_leading_comments(s: str) -> str:
    """Best-effort mirror of the engine's `strip_comments()` (called before
    parsing), just enough to reliably find a leading `USE` clause even when
    it's preceded by `// ...` or `/* ... */` comments (seen in the wild —
    `test_multi_database.py`'s USE-clause edge-case tests)."""
    i = 0
    while i < len(s):
        if s[i:].startswith("//"):
            j = s.find("\n", i)
            i = len(s) if j == -1 else j + 1
        elif s[i:].startswith("/*"):
            j = s.find("*/", i + 2)
            i = len(s) if j == -1 else j + 2
        elif s[i].isspace():
            i += 1
        else:
            break
    return s[i:]


# --- collision disambiguation by schema symbol coverage --------------------
# When one runtime schema-name string is backed by two DIFFERENT YAMLs across
# the suite (#463), a borrowing query is assigned to whichever candidate YAML
# actually DEFINES the relationship types / node labels it references — so a
# query using REQUESTED goes to the fixture YAML and one using DNS_REQUESTED to
# the examples YAML, regardless of which YAML the name defaults to.
_QUERY_REL_RE = re.compile(r"\[[^\]]*?:\s*([A-Za-z_][\w|`]*)")
_QUERY_LABEL_RE = re.compile(r"\(\s*\w*\s*:\s*([A-Za-z_]\w*)")
_SCHEMA_SYMBOLS_CACHE: Dict[Tuple[str, Optional[str]], Tuple[frozenset, frozenset]] = {}


def _query_symbols(cypher: str) -> Tuple[set, set]:
    """(node labels, relationship types) referenced in a Cypher string."""
    rels: set = set()
    for m in _QUERY_REL_RE.finditer(cypher):
        for t in m.group(1).replace("`", "").split("|"):
            if t:
                rels.add(t)
    labels = {m for m in _QUERY_LABEL_RE.findall(cypher)}
    return labels, rels


def _schema_symbols(entry: dict) -> Tuple[frozenset, frozenset]:
    """(node labels, relationship types) DEFINED by a schema_map entry's YAML
    (selecting the named sub-schema for a multi-schema file). Cached."""
    import yaml as pyyaml

    key = (entry["yaml"], entry.get("subschema"))
    cached = _SCHEMA_SYMBOLS_CACHE.get(key)
    if cached is not None:
        return cached
    labels: set = set()
    rels: set = set()
    try:
        doc = pyyaml.safe_load((REPO_ROOT / entry["yaml"]).read_text())
        graphs = []
        if entry.get("subschema"):
            for s in doc.get("schemas", []):
                if s.get("name") == entry["subschema"]:
                    graphs.append(s.get("graph_schema", {}))
        else:
            graphs.append(doc.get("graph_schema", doc))
        for g in graphs:
            for n in g.get("nodes", []) or []:
                if isinstance(n, dict) and n.get("label"):
                    labels.add(n["label"])
            for e in (g.get("edges", []) or []) + (g.get("relationships", []) or []):
                if isinstance(e, dict) and e.get("type"):
                    rels.add(e["type"])
    except Exception:
        pass
    result = (frozenset(labels), frozenset(rels))
    _SCHEMA_SYMBOLS_CACHE[key] = result
    return result


def _best_covering_entry(cypher: str, candidates: List[dict]) -> dict:
    """Pick the candidate schema entry whose defined labels/rel-types best cover
    the query's referenced symbols. Ties keep the FIRST candidate (callers pass
    the canonical/dynamically-registered entry first)."""
    q_labels, q_rels = _query_symbols(cypher)
    best, best_score = candidates[0], -1
    for cand in candidates:
        s_labels, s_rels = _schema_symbols(cand)
        score = len(q_labels & s_labels) + len(q_rels & s_rels)
        if score > best_score:
            best, best_score = cand, score
    return best


class Counters:
    def __init__(self):
        self.skips: Dict[str, int] = {}
        self.examples: Dict[str, List[str]] = {}

    def skip(self, reason: str, detail: str = ""):
        self.skips[reason] = self.skips.get(reason, 0) + 1
        if detail and len(self.examples.get(reason, [])) < 5:
            self.examples.setdefault(reason, []).append(detail)


@dataclass
class Harvested:
    schema: str
    name: str
    cypher: str


COUNTERS = Counters()
# schema_key -> {"yaml": path, "subschema": name-or-None}, accumulated as we
# discover dynamically-registered schemas per file.
SCHEMA_MAP: Dict[str, dict] = {k: {"yaml": v, "subschema": None} for k, v in _BASE_SCHEMA_MAP.items()}
# The schema definition ACTUALLY used to resolve each harvested query (which,
# for a schema key a file dynamically re-registers via `/schemas/load`, may
# differ from `SCHEMA_MAP`'s auto-discovered entry — see
# `_maybe_harvest_call`). This, not `SCHEMA_MAP`, is what gets exported to
# `schema_map.json`, so the Rust sweep loads the SAME definition each query
# was actually resolved against.
RESOLVED_SCHEMA_DEFS: Dict[str, dict] = {}
# schema_name -> the YAML/subschema a `test_*.py` file DYNAMICALLY registers for
# it via a `/schemas/load` POST (including POSTs inside pytest-fixture function
# bodies). Populated in a pre-pass over ALL files before any query is resolved,
# so the collision-splitting logic in `_maybe_harvest_call` knows the canonical
# ("locally-registered") definition of a name regardless of file iteration
# order. First-seen (sorted-file order) wins; the value is the same dict shape as
# SCHEMA_MAP entries. See the `zeek_merged_test` collision handling below.
LOCAL_REGISTRATIONS: Dict[str, dict] = {}


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", s).strip("_")


# ---------------------------------------------------------------------------
# Auto-discover (b) and (c): names living only inside multi-schema YAML files.
# ---------------------------------------------------------------------------
def _discover_multi_schema_names(yaml_rel_path: str, prefix: str = "") -> None:
    import yaml as pyyaml

    full = REPO_ROOT / yaml_rel_path
    if not full.exists():
        return
    with open(full) as f:
        doc = pyyaml.safe_load(f)
    schemas = doc.get("schemas") if isinstance(doc, dict) else None
    if not schemas:
        return
    for entry in schemas:
        name = entry.get("name")
        if not name:
            continue
        key = f"{prefix}{name}"
        if key in SCHEMA_MAP:
            continue  # (a) already claimed this name — last-write-wins at runtime.
        SCHEMA_MAP[key] = {"yaml": yaml_rel_path, "subschema": name}


_discover_multi_schema_names(_UNIFIED_MULTI_SCHEMA_YAML, prefix="")
_discover_multi_schema_names(_SQLGEN_SCHEMA_VARIATIONS_YAML, prefix=_SQLGEN_PREFIX)


# ---------------------------------------------------------------------------
# Sequential statement flattening: descends into if/for/while/with/try bodies
# (execution-order-ish) but stops at nested def/class (separate scope). Used
# everywhere we need "what did this variable end up bound to by the time
# execution reaches this point" without a full dataflow engine.
# ---------------------------------------------------------------------------
def flatten_stmts(stmts: List[ast.stmt]) -> List[ast.stmt]:
    out: List[ast.stmt] = []
    for s in stmts:
        if isinstance(s, ast.If):
            out.extend(flatten_stmts(s.body))
            out.extend(flatten_stmts(s.orelse))
        elif isinstance(s, (ast.For, getattr(ast, "AsyncFor", ast.For), ast.While)):
            out.extend(flatten_stmts(s.body))
            out.extend(flatten_stmts(s.orelse))
        elif isinstance(s, (ast.With, getattr(ast, "AsyncWith", ast.With))):
            out.extend(flatten_stmts(s.body))
        elif isinstance(s, ast.Try):
            out.extend(flatten_stmts(s.body))
            for h in s.handlers:
                out.extend(flatten_stmts(h.body))
            out.extend(flatten_stmts(s.orelse))
            out.extend(flatten_stmts(s.finalbody))
        elif isinstance(s, (ast.FunctionDef, getattr(ast, "AsyncFunctionDef", ast.FunctionDef), ast.ClassDef)):
            continue  # nested scope — do not flatten into the parent's env.
        else:
            out.append(s)
    return out


# ---------------------------------------------------------------------------
# Constant-folding string resolver.
# ---------------------------------------------------------------------------
def resolve_str(node: Optional[ast.AST], env: Dict[str, str]) -> Optional[str]:
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                r = resolve_str(v.value, env)
                if r is None:
                    return None
                parts.append(r)
            else:
                return None
        return "".join(parts)
    if isinstance(node, ast.Name):
        return env.get(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = resolve_str(node.left, env)
        right = resolve_str(node.right, env)
        if left is not None and right is not None:
            return left + right
        return None
    if isinstance(node, ast.Call):
        # `_schema_name(loaded_schemas, "<key>")` special case (see module doc).
        fname = _callee_name(node)
        if fname == _SCHEMA_NAME_HELPER and len(node.args) >= 2:
            key = resolve_str(node.args[1], env)
            if key is not None:
                return _SQLGEN_PREFIX + key
        return None
    if isinstance(node, ast.Subscript):
        base = node.value
        if isinstance(base, ast.Name) and base.id in _FIXTURE_SUBSCRIPT_OVERRIDES:
            return _FIXTURE_SUBSCRIPT_OVERRIDES[base.id]
        return None
    if isinstance(node, ast.Attribute):
        # `self.<attr1>.<attr2>` — resolved only via the class-scoped
        # `self_attr_schema` table built per-file (see
        # FileHarvester._index_self_attr_schema). Handled by the caller
        # (FileHarvester injects the resolved value into `env` under a
        # synthetic key before calling resolve_str on the Attribute chain's
        # source expression) — plain resolve_str cannot resolve arbitrary
        # attribute chains, so this branch intentionally returns None and
        # relies on the caller pre-resolving via `_self_attr_key`.
        return None
    return None


def _self_attr_key(node: ast.AST) -> Optional[str]:
    """`self.config.schema_name` -> "config.schema_name" (a lookup key into
    the per-class self_attr_schema table), else None."""
    if (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Attribute)
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "self"
    ):
        return f"{node.value.attr}.{node.attr}"
    return None


def _callee_name(call: ast.Call) -> Optional[str]:
    f = call.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return None


def _dict_key(node: ast.AST) -> Optional[str]:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _url_literal_contains(node: Optional[ast.AST], env: Dict[str, str], needle: str) -> bool:
    """True if the URL `node` literally contains `needle` — resolving a plain
    string/`+`-concat via `resolve_str`, else checking the literal (Constant)
    segments of an f-string. Request URLs are almost always
    `f"{BASE_URL}/schemas/load"` where `BASE_URL = os.getenv(...)` is NOT a
    resolvable constant, so a plain `resolve_str` returns None and the path
    literal (`/schemas/load`) must be recovered from the JoinedStr's constant
    parts."""
    s = resolve_str(node, env)
    if s is not None:
        return needle in s
    if isinstance(node, ast.JoinedStr):
        lit = "".join(v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
        return needle in lit
    return False


def resolve_payload_dict(leaf_stmts: List[ast.stmt], json_kwarg: ast.AST) -> Optional[Dict[str, ast.AST]]:
    """Resolve a `json=` kwarg to a {key: value_node} map, tracing simple
    `var = {...}` + `var[k] = v` + `var.update({...})` sequences within
    `leaf_stmts` (already flattened) if `json_kwarg` is a bare Name."""
    if isinstance(json_kwarg, ast.Dict):
        return {k: v for k, v in ((_dict_key(k), v) for k, v in zip(json_kwarg.keys, json_kwarg.values)) if k}
    if not isinstance(json_kwarg, ast.Name):
        return None
    varname = json_kwarg.id
    acc: Dict[str, ast.AST] = {}
    found = False
    for s in leaf_stmts:
        if isinstance(s, ast.Assign) and len(s.targets) == 1:
            tgt = s.targets[0]
            if isinstance(tgt, ast.Name) and tgt.id == varname and isinstance(s.value, ast.Dict):
                acc = {k: v for k, v in ((_dict_key(k), v) for k, v in zip(s.value.keys, s.value.values)) if k}
                found = True
            elif (
                isinstance(tgt, ast.Subscript)
                and isinstance(tgt.value, ast.Name)
                and tgt.value.id == varname
            ):
                key = _dict_key(tgt.slice) if not hasattr(tgt.slice, "value") or isinstance(tgt.slice, ast.Constant) else None
                if key is None and hasattr(tgt.slice, "value"):
                    key = _dict_key(tgt.slice.value)  # py3.8 ast.Index wrapper
                if key:
                    acc[key] = s.value
                    found = True
        elif isinstance(s, ast.Expr) and isinstance(s.value, ast.Call):
            c = s.value
            if (
                isinstance(c.func, ast.Attribute)
                and c.func.attr == "update"
                and isinstance(c.func.value, ast.Name)
                and c.func.value.id == varname
                and c.args
                and isinstance(c.args[0], ast.Dict)
            ):
                for k, v in zip(c.args[0].keys, c.args[0].values):
                    kk = _dict_key(k)
                    if kk:
                        acc[kk] = v
                        found = True
    return acc if found else None


# ---------------------------------------------------------------------------
# Per-file analysis.
# ---------------------------------------------------------------------------
@dataclass
class HelperSpec:
    """A recognized query-executing helper function.

    `cypher_expr`/`schema_expr` are AST expressions taken from the callee's
    OWN body, written in terms of the callee's OWN parameters (they might be
    a bare `Name`, an f-string combining several params — e.g. `f"USE
    {schema} {cypher}"` — or (for delegation, e.g. a thin wrapper that just
    calls another recognized helper) the very expression passed as that
    inner call's argument). Resolving a CALL to this helper is then just:
    build a substitution env mapping the callee's param names to the
    resolved call-site argument values (or the callee's own defaults), and
    `resolve_str(cypher_expr, substitution_env)` — this handles f-strings,
    delegation chains, and fixture-subscript overrides uniformly since
    `resolve_str` already knows how to resolve all of those shapes.
    """

    fn: Optional[ast.FunctionDef]  # None for the external conftest.execute_cypher
    cypher_expr: Optional[ast.AST]
    schema_expr: Optional[ast.AST]


class FileHarvester:
    def __init__(self, path: Path):
        self.path = path
        self.rel = path.relative_to(REPO_ROOT).as_posix()
        self.source = path.read_text()
        self.tree = ast.parse(self.source, filename=self.rel)
        self.module_env: Dict[str, str] = {}
        # Module-level `NAME = Path(__file__).parent / "a" / "b"` assignments,
        # resolved to a path string RELATIVE TO THIS FILE'S DIRECTORY (so
        # `open(NAME)` inside a fixture can be resolved even though it's not a
        # plain string literal). See `_resolve_path_components`.
        self.module_path_exprs: Dict[str, str] = {}
        self.helpers: Dict[str, HelperSpec] = {}
        self.local_schema_map: Dict[str, dict] = {}
        self.entries: List[Harvested] = []
        # (class_name, attr1, attr2) -> resolved schema string, e.g.
        # ("TestFKEdgeSchema", "config", "schema_name") -> "fk_edge".
        self.self_attr_schema: Dict[Tuple[str, str], str] = {}

    # -- pass 1: module-level constants, dict tables, fixtures, helpers ----
    def scan_module_level(self):
        top = flatten_stmts(self.tree.body)
        # Module-level constants and NAME = {key: Call(kw=..), ...} tables.
        dict_tables: Dict[str, Dict[str, Dict[str, str]]] = {}
        for node in top:
            if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                target = node.targets[0].id
                val = resolve_str(node.value, self.module_env)
                if val is not None:
                    self.module_env[target] = val
                elif isinstance(node.value, ast.Dict):
                    table: Dict[str, Dict[str, str]] = {}
                    for k, v in zip(node.value.keys, node.value.values):
                        kk = _dict_key(k)
                        if kk is None or not isinstance(v, ast.Call):
                            continue
                        kwargs = {}
                        for kw in v.keywords:
                            if kw.arg:
                                rv = resolve_str(kw.value, self.module_env)
                                if rv is not None:
                                    kwargs[kw.arg] = rv
                        table[kk] = kwargs
                    if table:
                        dict_tables[target] = table
                else:
                    # `SCHEMA_PATH = Path(__file__).parent / "a" / "b"` — a
                    # file-relative path used by a fixture's `open(SCHEMA_PATH)`.
                    pc = self._resolve_path_components(node.value)
                    if pc is not None:
                        self.module_path_exprs[target] = pc
            self._maybe_register_dynamic_schema(node)
        # Dynamic-schema POSTs that live INSIDE function/method bodies (pytest
        # fixtures like `def setup_zeek_merged(...): ... requests.post(
        # "/schemas/load", json={...})`). `flatten_stmts` deliberately does NOT
        # descend into nested scopes for the module ENV, so those POSTs are
        # invisible to the top-level loop above — scan each function/method body
        # explicitly here (module-level `open()`/env constants are already
        # recorded, so `_find_open_path_for`/`resolve_str` can see them). Scope
        # is the function itself, so `_find_open_path_for` correlates the schema
        # YAML `open()` with the POST within the same fixture.
        for node in ast.walk(self.tree):
            if isinstance(node, (ast.FunctionDef, getattr(ast, "AsyncFunctionDef", ast.FunctionDef))):
                self._maybe_register_dynamic_schema(node)
        # Module-level `@pytest.fixture def NAME(): return TABLE["key"]`.
        fixture_returns: Dict[str, str] = {}
        for node in self.tree.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            self._maybe_register_helper(node)
            body = flatten_stmts(node.body)
            for s in body:
                if isinstance(s, ast.Return) and isinstance(s.value, ast.Subscript):
                    sub = s.value
                    if isinstance(sub.value, ast.Name) and sub.value.id in dict_tables:
                        key = _dict_key(sub.slice) or (
                            _dict_key(sub.slice.value) if hasattr(sub.slice, "value") else None
                        )
                        if key and key in dict_tables[sub.value.id]:
                            fixture_returns[node.name] = f"{sub.value.id}::{key}"
        # Class-level helpers + self-attr-to-fixture bindings.
        for node in self.tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            for sub in node.body:
                if isinstance(sub, ast.FunctionDef):
                    self._maybe_register_helper(sub)
                    self._maybe_index_self_attr(node.name, sub, fixture_returns, dict_tables)

    def _maybe_index_self_attr(
        self,
        class_name: str,
        fn: ast.FunctionDef,
        fixture_returns: Dict[str, str],
        dict_tables: Dict[str, Dict[str, Dict[str, str]]],
    ):
        """`def setup(self, standard_schema, ...): self.config = standard_schema`
        where fixture `standard_schema` returns `SCHEMAS["standard"]` — index
        every attribute of that SCHEMAS entry as `(class, "config", attr)`."""
        param_names = {a.arg for a in fn.args.args}
        for s in flatten_stmts(fn.body):
            if not (
                isinstance(s, ast.Assign)
                and len(s.targets) == 1
                and isinstance(s.targets[0], ast.Attribute)
                and isinstance(s.targets[0].value, ast.Name)
                and s.targets[0].value.id == "self"
            ):
                continue
            attr = s.targets[0].attr
            src = None
            if isinstance(s.value, ast.Name) and s.value.id in param_names:
                src = s.value.id
            elif isinstance(s.value, ast.Subscript) and isinstance(s.value.value, ast.Name):
                # self.config = SCHEMAS["standard"] (direct, no fixture indirection)
                dict_name = s.value.value.id
                key = _dict_key(s.value.slice) or (
                    _dict_key(s.value.slice.value) if hasattr(s.value.slice, "value") else None
                )
                if dict_name in dict_tables and key in dict_tables.get(dict_name, {}):
                    for field, val in dict_tables[dict_name][key].items():
                        self.self_attr_schema[(class_name, attr, field)] = val
                continue
            if src and src in fixture_returns:
                dict_name, key = fixture_returns[src].split("::", 1)
                for field, val in dict_tables.get(dict_name, {}).get(key, {}).items():
                    self.self_attr_schema[(class_name, attr, field)] = val

    def _maybe_register_helper(self, fn: ast.FunctionDef):
        """Recognize `fn` as a query-executing helper, either directly (its
        body POSTs a `json=` payload with a "query"/"cypher" key) or by
        DELEGATION (its body calls another already-recognized helper, or the
        external conftest `execute_cypher`, passing its own params through —
        e.g. a thin `def query_api(query, schema_name=...): return
        execute_cypher(query, schema_name=schema_name, ...)` wrapper).
        `cypher_expr`/`schema_expr` end up as AST nodes expressed in terms of
        `fn`'s OWN parameters; call sites resolve them by substitution (see
        `_build_substitution_env`), which composes correctly through both
        f-strings and delegation chains via `resolve_str`.
        """
        leaf = flatten_stmts(fn.body)
        for s in leaf:
            for n in ast.walk(s):
                if not isinstance(n, ast.Call):
                    continue
                fname = _callee_name(n)
                if fname in ("post", "get"):
                    json_kw = next((kw.value for kw in n.keywords if kw.arg == "json"), None)
                    if json_kw is None:
                        continue
                    url_arg = n.args[0] if n.args else None
                    if _url_literal_contains(url_arg, self.module_env, "/schemas/load"):
                        continue
                    d = resolve_payload_dict(leaf, json_kw)
                    if d is None:
                        continue
                    cypher_expr = d.get("query") or d.get("cypher")
                    schema_expr = d.get("schema_name") or d.get("schema")
                    if cypher_expr is not None:
                        self.helpers[fn.name] = HelperSpec(fn=fn, cypher_expr=cypher_expr, schema_expr=schema_expr)
                        return
                else:
                    target = self._resolve_helper(fname) if fname else None
                    if target is None:
                        continue
                    cypher_expr = self._matching_call_arg(n, target, target_role="cypher")
                    schema_expr = self._matching_call_arg(n, target, target_role="schema")
                    if cypher_expr is not None:
                        self.helpers[fn.name] = HelperSpec(fn=fn, cypher_expr=cypher_expr, schema_expr=schema_expr)
                        return

    def _matching_call_arg(self, call: ast.Call, target: "HelperSpec", target_role: str) -> Optional[ast.AST]:
        """Given a call to `target` (a recognized helper), find the argument
        expression at the position/keyword that feeds `target`'s cypher_expr
        (`target_role="cypher"`) or schema_expr (`target_role="schema"`) —
        i.e. one level of inlining. Only handles the common case where that
        expr is itself a bare `Name` referencing one of `target.fn`'s params
        (true for every delegation wrapper found in this suite)."""
        expr = target.cypher_expr if target_role == "cypher" else target.schema_expr
        if target.fn is None:
            # Synthetic external spec (conftest.execute_cypher(query,
            # schema_name="social_integration", raise_on_error=True)): fixed
            # positional layout, since it's not a def in this file to inspect.
            param_name = "query" if target_role == "cypher" else "schema_name"
            names = ["query", "schema_name", "raise_on_error"]
        elif isinstance(expr, ast.Name):
            param_name = expr.id
            all_names = [p.arg for p in target.fn.args.args]
            names = all_names[1:] if all_names and all_names[0] == "self" else all_names
        else:
            return None  # can't trace through a non-trivial target expr generically
        if param_name not in names:
            return None
        pos = names.index(param_name)
        if pos < len(call.args):
            return call.args[pos]
        for kw in call.keywords:
            if kw.arg == param_name:
                return kw.value
        return None

    def _maybe_register_dynamic_schema(self, node: ast.AST):
        for n in ast.walk(node):
            if not isinstance(n, ast.Call) or _callee_name(n) != "post":
                continue
            json_kw = next((kw.value for kw in n.keywords if kw.arg == "json"), None)
            if json_kw is None or not isinstance(json_kw, ast.Dict):
                continue
            url_arg = n.args[0] if n.args else None
            if not _url_literal_contains(url_arg, self.module_env, "/schemas/load"):
                continue
            name = resolve_str(_dict_key_node(json_kw, "schema_name"), self.module_env)
            content_node = _dict_key_node(json_kw, "config_content")
            if name is None or content_node is None:
                continue
            path_lit = self._find_open_path_for(node)
            if path_lit is None:
                COUNTERS.skip("dynamic_schema_unresolved_path", f"{self.rel}: schema_name={name}")
                continue
            resolved = (self.path.parent / path_lit).resolve()
            if not resolved.exists():
                resolved2 = (REPO_ROOT / path_lit).resolve()
                resolved = resolved2 if resolved2.exists() else None
            if resolved is None:
                COUNTERS.skip("dynamic_schema_file_missing", f"{self.rel}: {path_lit}")
                continue
            try:
                rel_to_root = resolved.relative_to(REPO_ROOT).as_posix()
            except ValueError:
                COUNTERS.skip("dynamic_schema_outside_repo", f"{self.rel}: {resolved}")
                continue
            self.local_schema_map.setdefault(name, {"yaml": rel_to_root, "subschema": None})

    def _find_open_path_for(self, scope_node: ast.AST) -> Optional[str]:
        """Find the schema-YAML path passed to an `open(...)` within `scope_node`
        (a fixture/setup function or a top-level statement). Prefers a `.yaml`/
        `.yml` path — some fixtures also `open()` a `.sql` data-setup file BEFORE
        the schema YAML (e.g. test_denormalized_edges.py), so returning the first
        `open()` unconditionally would grab the wrong file. Resolves both plain
        string-literal paths and `Path(__file__).parent / "a" / "b"`-style path
        expressions (via `_resolve_path_components`)."""
        yaml_paths: List[str] = []
        other_paths: List[str] = []
        for n in ast.walk(scope_node):
            if isinstance(n, ast.Call) and _callee_name(n) == "open" and n.args:
                p = resolve_str(n.args[0], self.module_env)
                if p is None:
                    p = self._resolve_path_components(n.args[0])
                if not p:
                    continue
                (yaml_paths if p.endswith((".yaml", ".yml")) else other_paths).append(p)
        if yaml_paths:
            return yaml_paths[0]
        if other_paths:
            return other_paths[0]
        return None

    def _resolve_path_components(self, node: ast.AST) -> Optional[str]:
        """Resolve a `Path(__file__).parent / "a" / "b"` expression (or a bare
        `Name` bound to one, via `self.module_path_exprs`) to a path string
        RELATIVE TO THIS FILE'S DIRECTORY, else None. `""` means the file's own
        directory. Only the `Path(__file__).parent`-anchored, string-component
        form is handled — enough for the fixture `SCHEMA_PATH` idiom without a
        general path engine."""
        if isinstance(node, ast.Name):
            return self.module_path_exprs.get(node.id)
        if isinstance(node, ast.Attribute) and node.attr == "parent":
            inner = node.value
            # Unwrap a `.resolve()` call: Path(__file__).resolve().parent
            if isinstance(inner, ast.Call) and _callee_name(inner) == "resolve":
                inner = inner.func.value if isinstance(inner.func, ast.Attribute) else inner
            if isinstance(inner, ast.Call) and _callee_name(inner) == "Path" and inner.args:
                a = inner.args[0]
                if isinstance(a, ast.Name) and a.id == "__file__":
                    return ""  # the test file's own directory
            return None
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            base = self._resolve_path_components(node.left)
            if base is None:
                return None
            comp = resolve_str(node.right, self.module_env)
            if comp is None:
                return None
            return f"{base}/{comp}" if base else comp
        return None

    # -- pass 2: walk test functions -----------------------------------
    def harvest(self):
        """Convenience single-pass entry (scan + harvest). `main()` instead
        drives the two phases separately across ALL files so
        `LOCAL_REGISTRATIONS` is fully populated before any query resolves."""
        self.scan_module_level()
        self.harvest_tests()

    def harvest_tests(self):
        for node in ast.walk(self.tree):
            if isinstance(node, ast.ClassDef):
                for sub in node.body:
                    if isinstance(sub, ast.FunctionDef) and sub.name.startswith("test_"):
                        self._harvest_test_function(sub, node.name)
        # Also module-level (non-class) test functions.
        for node in self.tree.body:
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                self._harvest_test_function(node, None)

    def _parametrize_bindings(self, fn: ast.FunctionDef) -> List[Dict[str, str]]:
        bindings: List[Dict[str, str]] = [{}]
        for dec in fn.decorator_list:
            if not (isinstance(dec, ast.Call) and _callee_name(dec) == "parametrize"):
                continue
            if len(dec.args) < 2:
                continue
            argnames_node, argvalues_node = dec.args[0], dec.args[1]
            names_raw = resolve_str(argnames_node, {})
            if names_raw is not None:
                names = [n.strip() for n in names_raw.split(",")]
            elif isinstance(argnames_node, (ast.List, ast.Tuple)):
                names = [resolve_str(e, {}) for e in argnames_node.elts]
                if any(n is None for n in names):
                    continue
            else:
                continue
            if not isinstance(argvalues_node, ast.List):
                continue
            new_bindings = []
            for row in argvalues_node.elts:
                if len(names) == 1:
                    val = resolve_str(row, self.module_env)
                    if val is None:
                        continue
                    row_binding = {names[0]: val}
                elif isinstance(row, (ast.Tuple, ast.List)):
                    vals = [resolve_str(e, self.module_env) for e in row.elts]
                    if any(v is None for v in vals) or len(vals) != len(names):
                        continue
                    row_binding = dict(zip(names, vals))
                else:
                    continue
                for base in bindings:
                    merged = dict(base)
                    merged.update(row_binding)
                    new_bindings.append(merged)
            bindings = new_bindings if new_bindings else bindings
        return bindings

    def _harvest_test_function(self, fn: ast.FunctionDef, class_name: Optional[str]):
        bindings = self._parametrize_bindings(fn)
        for binding in bindings:
            env = dict(self.module_env)
            env.update(binding)
            idx = [0]
            leaf = flatten_stmts(fn.body)
            for stmt in leaf:
                if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                    val = resolve_str(stmt.value, env)
                    if val is not None:
                        env[stmt.targets[0].id] = val
                for n in ast.walk(stmt):
                    if isinstance(n, ast.Call):
                        self._maybe_harvest_call(n, env, fn, class_name, binding, idx, leaf)

    def _resolve_helper(self, callee: str) -> Optional[HelperSpec]:
        if callee in self.helpers:
            return self.helpers[callee]
        if callee == "execute_cypher":
            # conftest.py: execute_cypher(query, schema_name="social_integration", raise_on_error=True)
            return HelperSpec(fn=None, cypher_expr=ast.Name(id="query"), schema_expr=ast.Name(id="schema_name"))
        return None

    def _build_substitution_env(
        self, spec: HelperSpec, call: ast.Call, caller_env: Dict[str, str], class_name: Optional[str]
    ) -> Dict[str, str]:
        """Map `spec.fn`'s OWN parameter names to resolved strings, using the
        call site's arguments (resolved in the CALLER's env/class context)
        falling back to `spec.fn`'s own defaults."""
        sub_env: Dict[str, str] = {}
        # Parameters an argument targeted at all (whether or not it actually
        # resolved to a string) — MUST NOT fall back to the callee's default
        # for these. An argument that's present but statically unresolvable
        # (e.g. `schema_name=some_fixture["schema_name"]` where the fixture
        # isn't in `_FIXTURE_SUBSCRIPT_OVERRIDES`) means the call site is
        # OVERRIDING the default with something we just can't see — silently
        # substituting the default in that case would fabricate a schema the
        # call never asked for (confirmed to happen in practice: an
        # unresolvable `schema_name=` kwarg was falling back to
        # `execute_cypher`'s "social_integration" default and mis-harvesting
        # Airport/FLIGHT-labeled queries under the wrong schema).
        provided: set = set()
        if spec.fn is None:
            names = ["query", "schema_name", "raise_on_error"]
            defaults_by_name: Dict[str, ast.AST] = {"schema_name": ast.Constant(value=_DEFAULT_SCHEMA)}
        else:
            all_names = [p.arg for p in spec.fn.args.args]
            defaults = spec.fn.args.defaults
            defaults_by_name = {
                spec.fn.args.args[len(all_names) - len(defaults) + i].arg: d for i, d in enumerate(defaults)
            }
            # A class-level helper (method) is called as `self.foo(a, b)` —
            # `self` is implicit at the call site, never passed positionally,
            # so it must not consume position 0 when we zip call.args here.
            names = all_names[1:] if all_names and all_names[0] == "self" else all_names
        for i, arg_node in enumerate(call.args):
            if i >= len(names):
                break
            provided.add(names[i])
            v = resolve_str(arg_node, caller_env)
            if v is None:
                v = self._schema_from_self_attr(arg_node, class_name)
            if v is not None:
                sub_env[names[i]] = v
        for kw in call.keywords:
            if kw.arg is None or kw.arg not in names:
                continue
            provided.add(kw.arg)
            v = resolve_str(kw.value, caller_env)
            if v is None:
                v = self._schema_from_self_attr(kw.value, class_name)
            if v is not None:
                sub_env[kw.arg] = v
        for pname, dnode in defaults_by_name.items():
            if pname not in sub_env and pname not in provided:
                v = resolve_str(dnode, self.module_env)
                if v is not None:
                    sub_env[pname] = v
        return sub_env

    def _schema_from_self_attr(self, node: ast.AST, class_name: Optional[str]) -> Optional[str]:
        key = _self_attr_key(node)
        if key is None or class_name is None:
            return None
        attr1, attr2 = key.split(".", 1)
        return self.self_attr_schema.get((class_name, attr1, attr2))

    def _maybe_harvest_call(
        self,
        call: ast.Call,
        env: Dict[str, str],
        fn: ast.FunctionDef,
        class_name: Optional[str],
        binding: dict,
        idx: list,
        leaf_stmts: List[ast.stmt],
    ):
        callee = _callee_name(call)
        if callee is None:
            return

        cypher: Optional[str] = None
        schema: Optional[str] = None

        if callee in ("post", "get"):
            # Inline request call (no named helper wrapping it).
            json_kw = next((kw.value for kw in call.keywords if kw.arg == "json"), None)
            if json_kw is None:
                return
            url_arg = call.args[0] if call.args else None
            if _url_literal_contains(url_arg, env, "/schemas/load"):
                return
            d = resolve_payload_dict(leaf_stmts, json_kw)
            if d is None:
                return
            q = d.get("query") or d.get("cypher")
            cypher = resolve_str(q, env)
            sch_node = d.get("schema_name") or d.get("schema")
            # NOTE: no blanket "default to social_integration" fallback here
            # when `sch_node is None` (payload has no schema key at all) — that
            # would be GUESSING which schema an arbitrary local wrapper
            # implicitly targets (it depends on server state at test-run
            # time, not on anything statically visible), and a wrong guess
            # that happens to translate without erroring silently locks
            # wrong SQL (the exact failure mode the P0.6 spec warns against —
            # confirmed empirically: `test_mixed_expressions.py`'s
            # `run_query(query)` has no schema_name key and tests Airport/
            # FLIGHT-labeled queries, nowhere near social_integration's
            # User/Post schema). Only `execute_cypher`'s OWN documented
            # default (applied below, in `_build_substitution_env`'s
            # defaults-by-name pass for the exact `spec.fn is None` synthetic
            # case) is trustworthy.
            schema = resolve_str(sch_node, env) if sch_node is not None else None
            if schema is None and sch_node is not None:
                schema = self._schema_from_self_attr(sch_node, class_name)
        else:
            spec = self._resolve_helper(callee)
            if spec is None:
                return
            sub_env = self._build_substitution_env(spec, call, env, class_name)
            cypher = resolve_str(spec.cypher_expr, sub_env)
            schema = resolve_str(spec.schema_expr, sub_env) if spec.schema_expr is not None else None
            # (No blanket default when `spec.schema_expr is None` either — see
            # the note above; a locally-defined helper with no schema concept
            # at all has no reliable default. `execute_cypher`'s default is
            # already handled via `_build_substitution_env`'s defaults-by-name
            # pass, since its synthetic `schema_expr` is `Name("schema_name")`,
            # not `None`.)

        if cypher is None:
            COUNTERS.skip("cypher_unresolved", f"{self.rel}:{fn.name}")
            return

        stripped = strip_leading_comments(cypher)
        m = USE_PREFIX.match(stripped)
        if m:
            schema = m.group(1)
            cypher = stripped[m.end():]

        if schema is None:
            COUNTERS.skip("schema_unresolved", f"{self.rel}:{fn.name}")
            return

        # Resolve the schema NAME to a YAML/subschema entry. A file's own
        # dynamic registration (self.local_schema_map) is authoritative for its
        # own queries; otherwise the global SCHEMA_MAP.
        local_here = self.local_schema_map.get(schema)
        base_entry = SCHEMA_MAP.get(schema)
        canonical = LOCAL_REGISTRATIONS.get(schema)  # the dynamically-registered def, if any

        corpus_key = schema
        collision_note = None

        if local_here is not None:
            # This file dynamically registers the name -> authoritative, no split.
            this_entry = local_here
        elif canonical is not None and base_entry is not None and canonical != base_entry:
            # COLLISION (#463 GLOBAL_SCHEMAS last-writer-wins): the same name is
            # dynamically registered by some fixture (`canonical`) AND present in
            # the global map with a DIFFERENT YAML (`base_entry`) — e.g.
            # `zeek_merged_test` -> fixtures/schemas/zeek_merged_test.yaml
            # (REQUESTED/RESOLVED_TO/ACCESSED, from test_zeek_merged.py) vs
            # schemas/examples/zeek_merged.yaml (DNS_REQUESTED/CONNECTED_TO, the
            # conftest/global entry). Neither "owner" is right for every borrower,
            # so assign THIS query to whichever candidate YAML actually defines
            # the relationship types / labels it references (coverage). A query
            # resolved to the non-canonical YAML gets a suffixed corpus key so
            # both YAMLs lock correct SQL under distinct keys.
            chosen = _best_covering_entry(cypher, [canonical, base_entry])
            this_entry = chosen
            if chosen != canonical:
                corpus_key = f"{schema}__{_slug(class_name or self.path.stem)}"
                collision_note = (
                    f"schema name '{schema}' collides (issue #463 GLOBAL_SCHEMAS last-writer-wins): "
                    f"this variant's queries reference symbols defined by {chosen['yaml']}"
                    f"{'::' + chosen['subschema'] if chosen.get('subschema') else ''}, whereas the "
                    f"dynamically-registered owner uses {canonical['yaml']}; split into a distinct "
                    f"corpus key so both lock correct SQL"
                )
        elif base_entry is not None:
            this_entry = base_entry
        elif canonical is not None:
            this_entry = canonical
        else:
            COUNTERS.skip("schema_key_unknown", f"{self.rel}:{fn.name}: schema={schema}")
            return

        existing = RESOLVED_SCHEMA_DEFS.get(corpus_key)
        if existing is not None and {k: existing[k] for k in ("yaml", "subschema")} != this_entry:
            COUNTERS.skip(
                "schema_key_ambiguous",
                f"{self.rel}:{fn.name}: key={corpus_key} resolved to {this_entry} here vs "
                f"{existing} elsewhere (kept the first-seen definition)",
            )
        else:
            entry_out = dict(this_entry)
            if collision_note:
                entry_out["note"] = collision_note
            RESOLVED_SCHEMA_DEFS.setdefault(corpus_key, entry_out)

        cypher = cypher.strip()
        if not QUERY_LOOKING.search(cypher):
            COUNTERS.skip("not_query_shaped", f"{self.rel}:{fn.name}")
            return
        if WRITE_KEYWORDS.search(cypher):
            COUNTERS.skip("write_query", f"{self.rel}:{fn.name}")
            return
        if PARAM_REF.search(cypher):
            COUNTERS.skip("parameterized_unresolved", f"{self.rel}:{fn.name}")
            return

        idx[0] += 1
        suffix = f"__{idx[0]}" if idx[0] > 1 else ""
        param_suffix = "__" + "_".join(_slug(str(v)) for v in binding.values()) if binding else ""
        cls_prefix = f"{class_name}_" if class_name else ""
        name = _slug(f"{self.path.stem}__{cls_prefix}{fn.name}{param_suffix}{suffix}")
        self.entries.append(Harvested(schema=corpus_key, name=name, cypher=cypher))


def _dict_key_node(d: ast.Dict, key: str) -> Optional[ast.AST]:
    for k, v in zip(d.keys, d.values):
        if _dict_key(k) == key:
            return v
    return None


def iter_test_files() -> List[Path]:
    files = []
    for base in (INTEGRATION_DIR, SQL_GENERATION_DIR):
        if not base.exists():
            continue
        for p in sorted(base.rglob("test_*.py")):
            rel = p.relative_to(REPO_ROOT).as_posix()
            if "/matrix/" in rel:
                COUNTERS.skip("generative_matrix_dir", rel)
                continue
            files.append(p)
    return files


def main():
    all_entries: List[Harvested] = []

    # PASS 1: parse + module-level scan for every file, collecting each file's
    # dynamic schema registrations into the global LOCAL_REGISTRATIONS (sorted-
    # file order, first-seen wins) so the collision-splitting resolution in
    # PASS 2 is independent of which file is harvested first.
    harvesters: List[FileHarvester] = []
    for path in iter_test_files():
        try:
            fh = FileHarvester(path)
        except SyntaxError as e:
            COUNTERS.skip("file_syntax_error", f"{path}: {e}")
            continue
        fh.scan_module_level()
        for name, entry in sorted(fh.local_schema_map.items()):
            LOCAL_REGISTRATIONS.setdefault(name, entry)
        harvesters.append(fh)

    # PASS 2: harvest test queries.
    for fh in harvesters:
        fh.harvest_tests()
        all_entries.extend(fh.entries)

    # Dedupe identical (schema, cypher) pairs — keep first occurrence in
    # file-sorted, in-file AST order (both already deterministic).
    seen: Dict[Tuple[str, str], Harvested] = {}
    dupes = 0
    for e in all_entries:
        key = (e.schema, e.cypher)
        if key in seen:
            dupes += 1
            continue
        seen[key] = e
    deduped = list(seen.values())
    deduped.sort(key=lambda e: (e.schema, e.name))

    OUT_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSONL, "w") as f:
        for e in deduped:
            f.write(json.dumps({"schema": e.schema, "name": e.name, "cypher": e.cypher}, sort_keys=True))
            f.write("\n")

    used_schemas = {e.schema for e in deduped}
    schema_map_out = {k: v for k, v in sorted(RESOLVED_SCHEMA_DEFS.items()) if k in used_schemas}
    with open(OUT_SCHEMA_MAP, "w") as f:
        json.dump(schema_map_out, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"Harvested {len(deduped)} unique queries ({len(all_entries)} raw, {dupes} deduped) "
          f"across {len(used_schemas)} schemas.")
    print(f"Wrote {OUT_JSONL.relative_to(REPO_ROOT)} and {OUT_SCHEMA_MAP.relative_to(REPO_ROOT)}")
    print("\nSchema breakdown:")
    schema_counts = Counter(e.schema for e in deduped)
    for schema, count in sorted(schema_counts.items()):
        print(f"  {schema}: {count}")
    print("\nSkips by reason (raw entries, before dedup):")
    for reason, count in sorted(COUNTERS.skips.items(), key=lambda kv: -kv[1]):
        print(f"  {reason}: {count}")
        for ex in COUNTERS.examples.get(reason, []):
            print(f"    e.g. {ex}")
    print(f"\nDeduped identical (schema, cypher) pairs: {dupes}")


if __name__ == "__main__":
    sys.exit(main())
