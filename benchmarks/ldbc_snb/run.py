"""Run the official LDBC SNB queries against a ClickGraph HTTP endpoint."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
QUERY_ROOT = ROOT / "queries" / "official"
RESULTS_DIR = ROOT / "results"

CATEGORY_ORDER = {"IS": 0, "IC": 1, "BI": 2}
QUERY_PATTERNS = (
    ("IS", "interactive/short-*.cypher"),
    ("IC", "interactive/complex-*.cypher"),
    ("BI", "bi/bi-*.cypher"),
)


class HarnessError(Exception):
    """A benchmark setup or protocol error."""


@dataclass(frozen=True)
class QueryCase:
    id: str
    category: str
    path: Path


def discover_queries(root: Path = QUERY_ROOT) -> list[QueryCase]:
    cases: list[QueryCase] = []
    for category, pattern in QUERY_PATTERNS:
        for path in root.glob(pattern):
            match = re.fullmatch(r"[a-z]+-(\d+)", path.stem)
            if match is None:
                continue
            cases.append(QueryCase(path.stem, category, path))
    cases.sort(key=lambda case: (CATEGORY_ORDER[case.category], int(case.id.rsplit("-", 1)[1])))
    return cases


def strip_comments(cypher: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", cypher, flags=re.DOTALL)
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("//")]
    return "\n".join(lines).strip()


def placeholders(cypher: str) -> set[str]:
    return set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", strip_comments(cypher)))


def load_sidecar(case: QueryCase) -> dict[str, Any]:
    path = case.path.with_suffix(".params.json")
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"cannot read {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise HarnessError(f"{path} must contain a JSON object")
    return data


def load_overrides(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"cannot read {path}: {exc}") from exc
    if not isinstance(data, dict) or not all(
        isinstance(key, str) and isinstance(value, dict) for key, value in data.items()
    ):
        raise HarnessError(f"{path} must contain an object of query-id objects")
    return data


def resolve_params(
    case: QueryCase,
    overrides: dict[str, dict[str, Any]],
    global_params: dict[str, Any],
) -> dict[str, Any]:
    params = dict(load_sidecar(case))
    params.update(overrides.get(case.id, {}))
    params.update(global_params)
    return params


def validate_params(cypher: str, params: dict[str, Any]) -> None:
    missing = sorted(placeholders(cypher) - set(params))
    if missing:
        raise HarnessError(
            "missing parameter(s): " + ", ".join(f"${name}" for name in missing)
        )


def parse_param_overrides(values: list[str]) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for item in values:
        if "=" not in item:
            raise HarnessError("--param must be KEY=VALUE")
        key, raw = item.split("=", 1)
        key = key.strip()
        if not key:
            raise HarnessError("--param must be KEY=VALUE")
        try:
            overrides[key] = json.loads(raw)
        except json.JSONDecodeError:
            overrides[key] = raw
    return overrides
