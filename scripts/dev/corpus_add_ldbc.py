#!/usr/bin/env python3
"""Add the official LDBC SNB queries (and a few curated client queries) to the
census corpus, `tests/corpus/queries.jsonl` + `schema_map.json`.

`scripts/dev/harvest_corpus.py` harvests the Python test suite only; the
official LDBC queries (`benchmarks/ldbc_snb/queries/official/{bi,interactive}`)
were never in the corpus, so the P-4c cutover census did not see them (10 of
them lowered on the legacy pipeline only, P-4c S11o). This script inserts them
idempotently under the schema key `ldbc_official`, each with its sibling
`.params.json` substituted as Cypher literals (as `tests/spark_smoke/
test_ldbc_sweep.py` does), plus the `CURATED` entries below.

Run from anywhere: python3 scripts/dev/corpus_add_ldbc.py
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CORPUS = ROOT / "tests" / "corpus" / "queries.jsonl"
SCHEMA_MAP = ROOT / "tests" / "corpus" / "schema_map.json"
CENSUS_ONLY = ROOT / "tests" / "corpus" / "census_only.txt"
OFFICIAL = ROOT / "benchmarks" / "ldbc_snb" / "queries" / "official"
SCHEMA_KEY = "ldbc_official"
SCHEMA_YAML = "benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml"

# Queries clients send that no test file holds verbatim.
CURATED = [
    {
        # The Neo4j Browser's node expand (double-click), as Bolt passes it
        # on after rewriting the Browser's `id(a) = X`.
        "schema": "social_integration",
        "name": "neo4j_browser_expand",
        "cypher": "MATCH (a:User) WHERE a.user_id = 1 "
        "WITH a, size([(a)--() | 1]) AS allNeighboursCount "
        "MATCH path = (a)--(o) RETURN path, allNeighboursCount "
        "ORDER BY id(o) LIMIT 100",
    },
]

PARAM = re.compile(r"\$([a-zA-Z_]\w*)")


def literal(value):
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, list):
        return "[" + ", ".join(literal(v) for v in value) + "]"
    sys.exit(f"unsupported parameter value: {value!r}")


def official_entries():
    out = []
    for path in sorted(OFFICIAL.glob("*/*.cypher")):
        params_path = path.with_suffix(".params.json")
        params = json.loads(params_path.read_text()) if params_path.exists() else {}

        def sub(m):
            if m.group(1) not in params:
                sys.exit(f"{path}: no parameter `{m.group(1)}` in {params_path.name}")
            return literal(params[m.group(1)])

        name = f"{path.parent.name}_{path.stem}".replace("-", "_")
        out.append({"schema": SCHEMA_KEY, "name": name, "cypher": PARAM.sub(sub, path.read_text())})
    return out


def main():
    added = official_entries() + CURATED
    keys = {(e["schema"], e["name"]) for e in added}
    entries = [json.loads(l) for l in CORPUS.read_text().splitlines() if l.strip()]
    # The others as they are (in the file's order), these after them.
    entries = [e for e in entries if (e["schema"], e["name"]) not in keys] + added
    CORPUS.write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in entries))
    # The legacy pipeline's corpus sweep does not lock them.
    CENSUS_ONLY.write_text(
        "# Corpus entries of the bound plan's census only, not byte-locked by the\n"
        "# legacy pipeline's corpus sweep (tests/rust/integration/corpus_sweep.rs).\n"
        "# Written by scripts/dev/corpus_add_ldbc.py; format: schema/name<TAB>source.\n"
        + "".join(
            f"{e['schema']}/{e['name']}\t{'LDBC official' if e['schema'] == SCHEMA_KEY else 'curated'}\n"
            for e in added
        )
    )
    schema_map = json.loads(SCHEMA_MAP.read_text())
    schema_map[SCHEMA_KEY] = {"subschema": None, "yaml": SCHEMA_YAML}
    SCHEMA_MAP.write_text(json.dumps(dict(sorted(schema_map.items())), indent=2) + "\n")
    print(f"{len(added)} entries in {CORPUS.relative_to(ROOT)} ({len(entries)} in all)")


if __name__ == "__main__":
    main()
