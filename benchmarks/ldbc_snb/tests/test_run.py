import importlib.util
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


class FakeHandler(BaseHTTPRequestHandler):
    responses: list[tuple[int, object]] = []
    requests: list[dict] = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        type(self).requests.append(json.loads(self.rfile.read(length) or b"{}"))
        status, payload = type(self).responses.pop(0)
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            body = b"OK"
        elif self.path == "/schemas":
            body = json.dumps({"schemas": [{"name": "ldbc"}]}).encode()
        else:
            body = b"not found"
        self.send_response(200 if self.path in ("/health", "/schemas") else 404)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_server():
    FakeHandler.responses = []
    FakeHandler.requests = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", FakeHandler
    finally:
        server.shutdown()
        server.server_close()


def test_post_query_sends_schema_and_parameters(fake_server):
    url, handler = fake_server
    handler.responses = [(200, {"results": [{"a": 1}, {"a": 2}]})]

    rows, error = run.post_query(url, "RETURN 1", {"x": 1}, "ldbc", 5)

    assert (rows, error) == (2, None)
    assert handler.requests == [
        {"query": "RETURN 1", "parameters": {"x": 1}, "schema_name": "ldbc"}
    ]


def test_post_query_treats_200_error_body_as_failure(fake_server):
    url, handler = fake_server
    handler.responses = [(200, {"error": "bad schema"})]

    assert run.post_query(url, "RETURN 1", {}, None, 5) == (None, "bad schema")


def test_post_query_treats_http_error_body_as_failure(fake_server):
    url, handler = fake_server
    handler.responses = [(500, {"error": "boom"})]

    assert run.post_query(url, "RETURN 1", {}, None, 5) == (None, "HTTP 500: boom")


def test_benchmark_case_discards_warmup_and_records_median(fake_server):
    url, handler = fake_server
    handler.responses = [(200, {"results": [{"a": 1}, {"a": 2}]})] * 4
    case = run.QueryCase("short-1", "IS", Path("short-1.cypher"))

    result = run.benchmark_case(
        case, "RETURN 1", {}, url, None, iterations=3, warmup=1, timeout=5
    )

    assert result.status == "pass"
    assert result.runs == 3
    assert result.rows == 2
    assert result.min_ms <= result.median_ms <= result.max_ms
    assert len(handler.requests) == 4


def test_benchmark_case_stops_on_a_failed_measured_run(fake_server):
    url, handler = fake_server
    handler.responses = [
        (200, {"results": [{"a": 1}]}),
        (200, {"error": "bad query"}),
    ]
    case = run.QueryCase("short-1", "IS", Path("short-1.cypher"))

    result = run.benchmark_case(
        case, "RETURN 1", {}, url, None, iterations=3, warmup=1, timeout=5
    )

    assert result.status == "fail"
    assert result.runs == 0
    assert result.error == "bad query"
    assert len(handler.requests) == 2


def test_summarize_counts_passes_and_failures_by_category():
    results = [
        run.QueryResult("short-1", "IS", "pass", rows=1),
        run.QueryResult("short-2", "IS", "fail", error="bad"),
        run.QueryResult("bi-1", "BI", "pass", rows=2),
    ]

    summary = run.summarize(results)

    assert summary["total"] == 3
    assert summary["passed"] == 2
    assert summary["failed"] == 1
    assert summary["by_category"]["IS"] == {"total": 2, "passed": 1}
    assert summary["by_category"]["BI"] == {"total": 1, "passed": 1}


def test_write_json_round_trips_query_results(tmp_path):
    path = tmp_path / "result.json"
    results = [run.QueryResult("short-1", "IS", "pass", runs=5, rows=1,
                               min_ms=1.0, median_ms=2.0, max_ms=3.0)]

    run.write_json(path, {"label": "test"}, run.summarize(results), results)

    data = json.loads(path.read_text())
    assert data["schema_version"] == 1
    assert data["metadata"]["label"] == "test"
    assert data["summary"]["passed"] == 1
    assert data["queries"][0]["id"] == "short-1"
    assert data["queries"][0]["median_ms"] == 2.0


def test_write_markdown_contains_summary_and_full_failure_text(tmp_path):
    path = tmp_path / "report.md"
    results = [
        run.QueryResult("short-1", "IS", "pass", runs=5, rows=1,
                        min_ms=1.0, median_ms=2.0, max_ms=3.0),
        run.QueryResult("short-2", "IS", "fail",
                        error="server said no | because reasons\nsecond line"),
    ]

    run.write_markdown(path, {"label": "test"}, run.summarize(results), results)
    text = path.read_text()

    assert "# LDBC SNB Benchmark — test" in text
    assert "| **Total** | **1** | **2** |" in text
    assert "## Failures" in text
    assert "server said no | because reasons" in text
    assert "second line" in text


def test_check_server_accepts_healthy_server_and_known_schema(fake_server):
    url, _ = fake_server
    run.check_server(url, "ldbc", 5)


def test_check_server_rejects_unknown_schema(fake_server):
    url, _ = fake_server
    with pytest.raises(run.HarnessError, match="nope"):
        run.check_server(url, "nope", 5)


def test_main_writes_reports_and_returns_zero(fake_server, tmp_path):
    url, handler = fake_server
    handler.responses = [(200, {"results": [{"firstName": "Alice"}]})]

    code = run.main([
        "--url", url, "--query", "short-1", "--iterations", "1",
        "--warmup", "0", "--label", "test", "--out-dir", str(tmp_path),
    ])

    assert code == 0
    assert list(tmp_path.glob("*_test.json"))
    assert list(tmp_path.glob("*_test.md"))


def test_main_strict_returns_one_when_a_query_fails(fake_server, tmp_path):
    url, handler = fake_server
    handler.responses = [(200, {"error": "bad query"})]

    code = run.main([
        "--url", url, "--query", "short-1", "--iterations", "1",
        "--warmup", "0", "--label", "test", "--out-dir", str(tmp_path),
        "--strict",
    ])

    assert code == 1
    assert list(tmp_path.glob("*_test.json"))


def test_main_rejects_unknown_query_before_touching_the_server(tmp_path, capsys):
    code = run.main([
        "--url", "http://127.0.0.1:1", "--query", "nope",
        "--out-dir", str(tmp_path),
    ])
    assert code == 2
    assert "unknown query" in capsys.readouterr().err
