import pytest

from clickgraph_mcp.client import ClickGraphClient, ClickGraphError


def test_run_cypher_sends_query_schema_and_parameters(fake_clickgraph):
    fake_clickgraph.responses[("POST", "/query")] = (200, [{"p.name": "Alice"}])
    client = ClickGraphClient(base_url=fake_clickgraph.url)
    assert client.run_cypher(
        "MATCH (p:Person) RETURN p.name",
        schema="social",
        parameters={"name": "Alice"},
    ) == [{"p.name": "Alice"}]
    assert fake_clickgraph.requests[0]["body"] == {
        "query": "MATCH (p:Person) RETURN p.name",
        "format": "JSONEachRow",
        "schema_name": "social",
        "parameters": {"name": "Alice"},
    }


def test_run_cypher_omits_unset_options(fake_clickgraph):
    fake_clickgraph.responses[("POST", "/query")] = (200, [])
    ClickGraphClient(base_url=fake_clickgraph.url).run_cypher("RETURN 1")
    assert fake_clickgraph.requests[0]["body"] == {
        "query": "RETURN 1",
        "format": "JSONEachRow",
    }


def test_list_schemas_gets_schemas(fake_clickgraph):
    fake_clickgraph.responses[("GET", "/schemas")] = (
        200,
        {"schemas": [{"name": "social"}]},
    )
    assert ClickGraphClient(base_url=fake_clickgraph.url).list_schemas() == {
        "schemas": [{"name": "social"}]
    }
    assert fake_clickgraph.requests[0]["method"] == "GET"


def test_explain_query_posts_to_query_sql(fake_clickgraph):
    fake_clickgraph.responses[("POST", "/query/sql")] = (200, {"sql": ["SELECT 1"]})
    result = ClickGraphClient(base_url=fake_clickgraph.url).explain_query(
        "RETURN 1", schema="social"
    )
    assert result == {"sql": ["SELECT 1"]}
    assert fake_clickgraph.requests[0]["body"] == {
        "query": "RETURN 1",
        "schema_name": "social",
    }


def test_json_error_field_is_surfaced(fake_clickgraph):
    fake_clickgraph.responses[("POST", "/query")] = (
        400,
        {"error": "Schema not found: nope"},
    )
    with pytest.raises(ClickGraphError) as exc:
        ClickGraphClient(base_url=fake_clickgraph.url).run_cypher(
            "RETURN 1", schema="nope"
        )
    assert "HTTP 400" in str(exc.value)
    assert "Schema not found: nope" in str(exc.value)


def test_plain_text_error_is_surfaced(fake_clickgraph):
    fake_clickgraph.responses[("POST", "/query")] = (400, b"Unsupported query")
    with pytest.raises(ClickGraphError, match="Unsupported query"):
        ClickGraphClient(base_url=fake_clickgraph.url).run_cypher("BAD")


def test_non_json_success_is_reported(fake_clickgraph):
    fake_clickgraph.responses[("GET", "/schemas")] = (200, b"<html>proxy</html>")
    with pytest.raises(ClickGraphError, match="non-JSON"):
        ClickGraphClient(base_url=fake_clickgraph.url).list_schemas()


def test_unreachable_server_names_the_url():
    with pytest.raises(
        ClickGraphError, match="Cannot reach ClickGraph at http://127.0.0.1:1"
    ):
        ClickGraphClient(
            base_url="http://127.0.0.1:1", timeout=0.5
        ).list_schemas()


def test_trailing_slash_in_base_url(fake_clickgraph):
    fake_clickgraph.responses[("GET", "/schemas")] = (200, {"schemas": []})
    ClickGraphClient(base_url=fake_clickgraph.url + "/").list_schemas()
    assert fake_clickgraph.requests[0]["path"] == "/schemas"


def test_env_defaults(monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_URL", "http://example.test:9000/")
    monkeypatch.setenv("CLICKGRAPH_TIMEOUT", "5")
    client = ClickGraphClient()
    assert client.base_url == "http://example.test:9000"
    assert client.timeout == 5


def test_bad_timeout_env_is_a_clear_error(monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_TIMEOUT", "soon")
    with pytest.raises(ClickGraphError, match="CLICKGRAPH_TIMEOUT"):
        ClickGraphClient()
