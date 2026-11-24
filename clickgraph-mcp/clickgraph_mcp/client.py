"""HTTP client for a running ClickGraph server."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

DEFAULT_URL = "http://localhost:8080"
DEFAULT_TIMEOUT = 30.0


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
        return self._request("POST", "/query", payload)

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
        return self._request("POST", "/query/sql", payload)
