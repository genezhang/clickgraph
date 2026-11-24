import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


class FakeClickGraph:
    """A tiny HTTP server that records requests and replays configured responses."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.responses: dict[tuple[str, str], tuple[int, object]] = {}
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def _handle(self) -> None:
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw) if raw else None
                outer.requests.append(
                    {"method": self.command, "path": self.path, "body": body}
                )
                status, payload = outer.responses.get(
                    (self.command, self.path), (404, {"error": "no route"})
                )
                data = (
                    payload
                    if isinstance(payload, bytes)
                    else json.dumps(payload).encode()
                )
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = _handle
            do_POST = _handle

            def log_message(self, *args) -> None:
                pass

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._httpd.server_address[1]}"
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, daemon=True
        )
        self._thread.start()

    def close(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=2)


@pytest.fixture
def fake_clickgraph():
    server = FakeClickGraph()
    try:
        yield server
    finally:
        server.close()
