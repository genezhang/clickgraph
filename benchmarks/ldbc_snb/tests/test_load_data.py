import importlib.util
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "load_data.py"
_spec = importlib.util.spec_from_file_location("ldbc_load", MODULE_PATH)
load = importlib.util.module_from_spec(_spec)
sys.modules["ldbc_load"] = load
_spec.loader.exec_module(load)


class FakeClickHouse(BaseHTTPRequestHandler):
    requests: list[tuple[str, bytes]] = []

    def do_POST(self):
        query = parse_qs(urlparse(self.path).query)["query"][0]
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        type(self).requests.append((query, body))
        payload = b"1\n" if query.startswith("SELECT count()") else b""
        self.send_response(200)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_clickhouse():
    FakeClickHouse.requests = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeClickHouse)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", FakeClickHouse.requests
    finally:
        server.shutdown()
        server.server_close()


def test_load_table_streams_every_part_file(fake_clickhouse, tmp_path):
    url, requests = fake_clickhouse
    (tmp_path / "part-1.csv").write_text("id|name\n1|a\n")
    (tmp_path / "part-2.csv").write_text("id|name\n2|b\n")

    count = load.load_table(url, {}, "Place", tmp_path, 5)

    assert count == 1
    assert [query.split()[0] for query, _ in requests] == [
        "TRUNCATE", "INSERT", "INSERT", "SELECT",
    ]
    inserts = [body for query, body in requests if query.startswith("INSERT")]
    assert inserts == [b"id|name\n1|a\n", b"id|name\n2|b\n"]


def test_load_table_uses_the_person_transform(fake_clickhouse, tmp_path):
    url, requests = fake_clickhouse
    (tmp_path / "part-0.csv").write_text("header\n")

    load.load_table(url, {}, "Person", tmp_path, 5)

    insert = next(query for query, _ in requests if query.startswith("INSERT"))
    assert "splitByChar(';', language)" in insert
    assert "splitByChar(';', email)" in insert
    assert "FROM input(" in insert


def test_apply_schema_splits_the_committed_ddl(fake_clickhouse):
    url, requests = fake_clickhouse

    load.apply_schema(url, {}, Path(load.SCHEMA_DIR / "clickhouse_ddl.sql"), 5)

    statements = [query for query, _ in requests]
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS ldbc"
    assert any("CREATE TABLE IF NOT EXISTS ldbc.Person" in s for s in statements)
    assert statements[-1].startswith(
        "CREATE OR REPLACE VIEW ldbc.Message_replyOf_Message"
    )
    assert all(statement.startswith("CREATE") for statement in statements)
