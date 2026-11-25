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


def test_default_url_is_the_servers_default_http_port(monkeypatch):
    # ClickGraph's HTTP server listens on 7475 by default (src/config.rs, the
    # Dockerfile, the README); 8080 was the default before #310. An MCP server
    # started with no CLICKGRAPH_URL must reach a default ClickGraph.
    monkeypatch.delenv("CLICKGRAPH_URL", raising=False)
    assert ClickGraphClient().base_url == "http://localhost:7475"


def test_get_schema_calls_apoc_meta_schema(fake_clickgraph):
    payload = {"records": [{"value": {"Person": {"properties": {"name": {}}}}}]}
    fake_clickgraph.responses[("POST", "/query")] = (200, payload)
    client = ClickGraphClient(base_url=fake_clickgraph.url)

    assert client.get_schema(schema="social") == payload
    assert fake_clickgraph.requests[0]["body"] == {
        "query": "CALL apoc.meta.schema() YIELD value RETURN value",
        "format": "JSONEachRow",
        "schema_name": "social",
    }


def test_default_max_rows_is_1000(monkeypatch):
    monkeypatch.delenv("CLICKGRAPH_MAX_ROWS", raising=False)
    assert ClickGraphClient(base_url="http://example.test").max_rows == 1000


def test_max_rows_env_is_parsed(monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_MAX_ROWS", "2")
    assert ClickGraphClient(base_url="http://example.test").max_rows == 2


def test_bad_max_rows_env_is_a_clear_error(monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_MAX_ROWS", "lots")
    with pytest.raises(ClickGraphError, match="CLICKGRAPH_MAX_ROWS"):
        ClickGraphClient(base_url="http://example.test")


def test_negative_max_rows_env_is_a_clear_error(monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_MAX_ROWS", "-1")
    with pytest.raises(ClickGraphError, match="CLICKGRAPH_MAX_ROWS"):
        ClickGraphClient(base_url="http://example.test")


def test_run_cypher_caps_results_with_explicit_marker(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_MAX_ROWS", "2")
    fake_clickgraph.responses[("POST", "/query")] = (
        200,
        {"results": [{"n": 1}, {"n": 2}, {"n": 3}]},
    )

    result = ClickGraphClient(base_url=fake_clickgraph.url).run_cypher("RETURN 1")

    assert result["results"] == [{"n": 1}, {"n": 2}]
    assert result["truncated"] is True
    assert result["returned"] == 2
    assert result["total"] == 3
    assert "LIMIT" in result["message"]


def test_run_cypher_caps_records_with_explicit_marker(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_MAX_ROWS", "1")
    fake_clickgraph.responses[("POST", "/query")] = (
        200,
        {"records": [{"a": 1}, {"a": 2}]},
    )

    result = ClickGraphClient(base_url=fake_clickgraph.url).run_cypher("RETURN 1")

    assert result["records"] == [{"a": 1}]
    assert result["truncated"] is True
    assert result["returned"] == 1
    assert result["total"] == 2


def test_zero_max_rows_is_unlimited(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_MAX_ROWS", "0")
    fake_clickgraph.responses[("POST", "/query")] = (
        200,
        {"results": [{"n": 1}, {"n": 2}, {"n": 3}]},
    )

    result = ClickGraphClient(base_url=fake_clickgraph.url).run_cypher("RETURN 1")

    assert len(result["results"]) == 3
    assert "truncated" not in result


def test_result_at_or_below_cap_is_unchanged(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_MAX_ROWS", "3")
    fake_clickgraph.responses[("POST", "/query")] = (
        200,
        {"results": [{"n": 1}, {"n": 2}, {"n": 3}]},
    )

    result = ClickGraphClient(base_url=fake_clickgraph.url).run_cypher("RETURN 1")

    assert result == {"results": [{"n": 1}, {"n": 2}, {"n": 3}]}


def test_view_parameters_and_role_are_forwarded(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_VIEW_PARAMETERS", '{"tenant_id": "acme"}')
    monkeypatch.setenv("CLICKGRAPH_ROLE", "analyst")
    fake_clickgraph.responses[("POST", "/query")] = (200, {"results": []})

    ClickGraphClient(base_url=fake_clickgraph.url).run_cypher(
        "RETURN 1", schema="social"
    )

    body = fake_clickgraph.requests[0]["body"]
    assert body["view_parameters"] == {"tenant_id": "acme"}
    assert body["role"] == "analyst"


def test_explain_query_forwards_tenant_controls(fake_clickgraph, monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_VIEW_PARAMETERS", '{"tenant_id": "acme"}')
    monkeypatch.setenv("CLICKGRAPH_ROLE", "analyst")
    fake_clickgraph.responses[("POST", "/query/sql")] = (200, {"sql": ["SELECT 1"]})

    ClickGraphClient(base_url=fake_clickgraph.url).explain_query("RETURN 1")

    body = fake_clickgraph.requests[0]["body"]
    assert body["view_parameters"] == {"tenant_id": "acme"}
    assert body["role"] == "analyst"


def test_bad_view_parameters_json_is_a_clear_error(monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_VIEW_PARAMETERS", "{not json")
    with pytest.raises(ClickGraphError, match="CLICKGRAPH_VIEW_PARAMETERS"):
        ClickGraphClient(base_url="http://example.test")


def test_view_parameters_must_be_a_json_object(monkeypatch):
    monkeypatch.setenv("CLICKGRAPH_VIEW_PARAMETERS", '["acme"]')
    with pytest.raises(ClickGraphError, match="CLICKGRAPH_VIEW_PARAMETERS"):
        ClickGraphClient(base_url="http://example.test")
