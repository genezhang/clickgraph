import importlib.util
import sys
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "run.py"
_spec = importlib.util.spec_from_file_location("ldbc_run", MODULE_PATH)
run = importlib.util.module_from_spec(_spec)
sys.modules["ldbc_run"] = run
_spec.loader.exec_module(run)


def test_discover_queries_returns_all_official_queries_in_numeric_order():
    cases = run.discover_queries()
    assert len(cases) == 41
    assert [case.id for case in cases[:8]] == [
        "short-1", "short-2", "short-3", "short-4",
        "short-5", "short-6", "short-7", "complex-1",
    ]
    assert cases[-1].id == "bi-20"
    assert {case.category for case in cases} == {"IS", "IC", "BI"}


def test_strip_comments_removes_comment_blocks_but_keeps_urls():
    cypher = "// leading\n/* params */\nRETURN 'http://example.com' AS url\n"
    assert run.strip_comments(cypher) == "RETURN 'http://example.com' AS url"


def test_missing_parameter_raises_with_the_name():
    with pytest.raises(run.HarnessError, match="personId"):
        run.validate_params("MATCH (n {id: $personId}) RETURN n", {})


def test_commented_parameter_is_not_required():
    run.validate_params("// $ignored\nRETURN $kept AS value", {"kept": 1})


def test_resolve_params_merges_sidecar_then_override_then_global(tmp_path):
    query_path = tmp_path / "short-1.cypher"
    query_path.write_text("RETURN 1")
    (tmp_path / "short-1.params.json").write_text(
        '{"personId": 1, "shared": "sidecar"}'
    )
    case = run.QueryCase("short-1", "IS", query_path)

    params = run.resolve_params(
        case,
        {"short-1": {"personId": 2}},
        {"shared": "global"},
    )

    assert params == {"personId": 2, "shared": "global"}


def test_parse_param_overrides_uses_json_types_and_falls_back_to_string():
    assert run.parse_param_overrides(
        ["personId=14", 'firstName="Alice"', "flag=true", "country=India"]
    ) == {
        "personId": 14,
        "firstName": "Alice",
        "flag": True,
        "country": "India",
    }


def test_parse_param_overrides_rejects_missing_equals():
    with pytest.raises(run.HarnessError, match="KEY=VALUE"):
        run.parse_param_overrides(["personId"])
