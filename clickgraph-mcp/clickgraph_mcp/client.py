"""HTTP client for a running ClickGraph server."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

DEFAULT_URL = "http://localhost:7475"
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_ROWS = 1000


class ClickGraphError(RuntimeError):
    """A ClickGraph request could not be completed."""


def _error_text(body: str) -> str:
    """ClickGraph errors are JSON on some routes and plain text on others."""
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return body.strip() or "(empty response)"
    if isinstance(payload, dict):
        for key in ("error", "message"):
            if key in payload:
                return str(payload[key])
    return body.strip() or "(empty response)"


def _env_json_object(name: str) -> dict[str, Any] | None:
    """Read an optional JSON object from the environment."""
    raw = os.environ.get(name)
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        raise ClickGraphError(
            f"{name} must be a JSON object, got {raw!r}"
        ) from None
    if not isinstance(value, dict):
        raise ClickGraphError(f"{name} must be a JSON object, got {raw!r}")
    return value


def _cap_rows(payload: Any, max_rows: int) -> Any:
    """Cap a query payload's rows without hiding that it was capped."""
    if max_rows == 0 or not isinstance(payload, dict):
        return payload
    for key in ("results", "records"):
        rows = payload.get(key)
        if isinstance(rows, list) and len(rows) > max_rows:
            capped = dict(payload)
            capped[key] = rows[:max_rows]
            capped["truncated"] = True
            capped["returned"] = max_rows
            capped["total"] = len(rows)
            capped["message"] = (
                f"Result truncated to the first {max_rows} rows; add a LIMIT "
                "to the Cypher query to choose a range."
            )
            return capped
    return payload


class ClickGraphClient:
    def __init__(
        self, base_url: str | None = None, timeout: float | None = None
    ) -> None:
        self.base_url = (
            base_url or os.environ.get("CLICKGRAPH_URL") or DEFAULT_URL
        ).rstrip("/")
        raw_timeout: object = (
            timeout
            if timeout is not None
            else os.environ.get("CLICKGRAPH_TIMEOUT", str(DEFAULT_TIMEOUT))
        )
        try:
            self.timeout = float(raw_timeout)
        except (TypeError, ValueError):
            raise ClickGraphError(
                f"CLICKGRAPH_TIMEOUT must be a number, got {raw_timeout!r}"
            ) from None
        raw_max_rows = os.environ.get(
            "CLICKGRAPH_MAX_ROWS", str(DEFAULT_MAX_ROWS)
        )
        try:
            self.max_rows = int(raw_max_rows)
        except (TypeError, ValueError):
            raise ClickGraphError(
                f"CLICKGRAPH_MAX_ROWS must be an integer, got {raw_max_rows!r}"
            ) from None
        if self.max_rows < 0:
            raise ClickGraphError(
                "CLICKGRAPH_MAX_ROWS must be zero or greater, "
                f"got {self.max_rows}"
            )
        self.view_parameters = _env_json_object("CLICKGRAPH_VIEW_PARAMETERS")
        self.role = os.environ.get("CLICKGRAPH_ROLE") or None

    def _with_scope(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.view_parameters:
            payload["view_parameters"] = self.view_parameters
        if self.role:
            payload["role"] = self.role
        return payload

    def _request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> Any:
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            method=method,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read().decode()
        except urllib.error.HTTPError as exc:
            detail = _error_text(exc.read().decode(errors="replace"))
            raise ClickGraphError(
                f"ClickGraph returned HTTP {exc.code}: {detail}"
            ) from None
        except urllib.error.URLError as exc:
            raise ClickGraphError(
                f"Cannot reach ClickGraph at {self.base_url}: {exc.reason}"
            ) from None
        except TimeoutError:
            raise ClickGraphError(
                f"ClickGraph request timed out after {self.timeout}s"
            ) from None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            raise ClickGraphError(
                f"ClickGraph returned non-JSON response: {body[:500]}"
            ) from None

    def list_schemas(self) -> Any:
        """Return the schemas registered on the server."""
        return self._request("GET", "/schemas")

    def get_schema(self, schema: str | None = None) -> Any:
        """Return a schema's labels, relationship types and properties."""
        payload: dict[str, Any] = {
            "query": "CALL apoc.meta.schema() YIELD value RETURN value",
            "format": "JSONEachRow",
        }
        if schema:
            payload["schema_name"] = schema
        return _cap_rows(
            self._request("POST", "/query", self._with_scope(payload)),
            self.max_rows,
        )

    def run_cypher(
        self,
        query: str,
        schema: str | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> Any:
        """Run a Cypher query and return its rows."""
        payload: dict[str, Any] = {"query": query, "format": "JSONEachRow"}
        if schema:
            payload["schema_name"] = schema
        if parameters:
            payload["parameters"] = parameters
        return _cap_rows(
            self._request("POST", "/query", self._with_scope(payload)),
            self.max_rows,
        )

    def explain_query(
        self,
        query: str,
        schema: str | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> Any:
        """Translate a Cypher query to SQL without executing it."""
        payload: dict[str, Any] = {"query": query}
        if schema:
            payload["schema_name"] = schema
        if parameters:
            payload["parameters"] = parameters
        return self._request("POST", "/query/sql", self._with_scope(payload))
