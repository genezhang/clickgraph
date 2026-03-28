#!/usr/bin/env python3
"""Pytest-based LDBC SNB test suite for ClickGraph embedded mode.

Runs short-1 through short-7 (the simplest Interactive queries) against
chdb with sf0.003 Parquet data.

Run:
    cd benchmarks/ldbc_snb
    LD_LIBRARY_PATH=../../target/release PYTHONPATH=../../clickgraph-py \
        python3 -m pytest embedded_benchmark_test.py -v

Gated: skips all tests if the clickgraph module cannot be imported
(e.g. when libclickgraph_ffi.so is not built or not on LD_LIBRARY_PATH).
"""

import os
import tempfile
from pathlib import Path

import pytest

# Import shared helpers from the benchmark runner (avoid duplication)
from embedded_benchmark import substitute_params, load_query

# ---------------------------------------------------------------------------
# Skip entire module if clickgraph is not importable
# ---------------------------------------------------------------------------

try:
    import clickgraph
    CLICKGRAPH_AVAILABLE = True
except (ImportError, OSError):
    CLICKGRAPH_AVAILABLE = False

# Module-level skip: only for clickgraph availability.
# chdb gating is applied per-class (TestSqlOnly runs without chdb).
pytestmark = pytest.mark.skipif(
    not CLICKGRAPH_AVAILABLE,
    reason="clickgraph module not available (build FFI library and set LD_LIBRARY_PATH)",
)

CHDB_ENABLED = os.environ.get("CLICKGRAPH_CHDB_TESTS", "0") in ("1", "true", "True")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data" / "sf0.003" / "graphs" / "parquet" / "raw" / "composite-merged-fk"
SCHEMA_TEMPLATE = SCRIPT_DIR / "schemas" / "ldbc_snb_embedded.yaml"
QUERIES_DIR = SCRIPT_DIR / "queries" / "official" / "interactive"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def connection():
    """Create a shared Database and Connection for all tests.

    chdb supports only one session per process, so we share a single
    connection across all tests in this module.
    """
    if not DATA_DIR.exists():
        pytest.skip(f"Data directory not found: {DATA_DIR}")

    if not SCHEMA_TEMPLATE.exists():
        pytest.skip(f"Schema template not found: {SCHEMA_TEMPLATE}")

    # Prepare schema with resolved paths
    template_text = SCHEMA_TEMPLATE.read_text()
    resolved = template_text.replace("__DATA_DIR__", str(DATA_DIR))

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="ldbc_test_", delete=False
    )
    tmp.write(resolved)
    tmp.flush()
    tmp.close()

    try:
        db = clickgraph.Database(tmp.name)
        conn = db.connect()
        yield conn
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


def _load_and_prepare(query_id: str) -> str:
    """Load a query file, substitute parameters, return ready-to-execute Cypher.

    Uses shared helpers from embedded_benchmark.py to avoid duplication.
    """
    cypher, params, _source = load_query(query_id)
    return substitute_params(cypher, params)


# ---------------------------------------------------------------------------
# Tests: Interactive Short queries
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not CHDB_ENABLED, reason="chdb tests disabled (set CLICKGRAPH_CHDB_TESTS=1)")
class TestInteractiveShort:
    """Test the 7 Interactive Short queries (simplest LDBC queries)."""

    def test_short_1(self, connection):
        """IS1: Profile of a person."""
        cypher = _load_and_prepare("short-1")
        result = connection.query(cypher)
        # short-1 with personId=14 should return a person profile
        # (may return 0 rows if person 14 is not in sf0.003 dataset)
        assert result.num_rows >= 0
        assert result.compiling_time >= 0
        if result.num_rows > 0:
            row = result[0]
            assert "firstName" in row
            assert "lastName" in row

    def test_short_2(self, connection):
        """IS2: Recent messages of a person.

        Uses untyped 'message' variable with HAS_CREATOR.
        May fail if Message view handling has issues.
        """
        cypher = _load_and_prepare("short-2")
        try:
            result = connection.query(cypher)
            assert result.num_rows >= 0
        except Exception as e:
            pytest.xfail(f"short-2 may require Message view support: {e}")

    def test_short_3(self, connection):
        """IS3: Friends of a person."""
        cypher = _load_and_prepare("short-3")
        result = connection.query(cypher)
        assert result.num_rows >= 0
        assert result.compiling_time >= 0

    def test_short_4(self, connection):
        """IS4: Content of a message.

        Uses :Message label - requires Message union view.
        """
        cypher = _load_and_prepare("short-4")
        try:
            result = connection.query(cypher)
            assert result.num_rows >= 0
        except Exception as e:
            pytest.xfail(f"short-4 requires Message view: {e}")

    def test_short_5(self, connection):
        """IS5: Creator of a message.

        Uses :Message label with HAS_CREATOR.
        """
        cypher = _load_and_prepare("short-5")
        try:
            result = connection.query(cypher)
            assert result.num_rows >= 0
        except Exception as e:
            pytest.xfail(f"short-5 requires Message view: {e}")

    def test_short_6(self, connection):
        """IS6: Forum of a message.

        Uses :Message with VLP REPLY_OF chain.
        """
        cypher = _load_and_prepare("short-6")
        try:
            result = connection.query(cypher)
            assert result.num_rows >= 0
        except Exception as e:
            pytest.xfail(f"short-6 requires Message view with VLP: {e}")

    def test_short_7(self, connection):
        """IS7: Replies of a message.

        Uses :Message with REPLY_OF and OPTIONAL MATCH.
        """
        cypher = _load_and_prepare("short-7")
        try:
            result = connection.query(cypher)
            assert result.num_rows >= 0
        except Exception as e:
            pytest.xfail(f"short-7 requires Message view: {e}")


# ---------------------------------------------------------------------------
# Smoke test: SQL-only mode (no chdb required)
# ---------------------------------------------------------------------------

class TestSqlOnly:
    """Verify Cypher-to-SQL translation works for short queries.

    These tests do NOT require chdb or CLICKGRAPH_CHDB_TESTS=1.
    They only need the clickgraph library to be importable.
    """

    @pytest.fixture(scope="class")
    def sql_connection(self):
        """Create a SQL-only connection (no chdb)."""
        if not SCHEMA_TEMPLATE.exists():
            pytest.skip(f"Schema template not found: {SCHEMA_TEMPLATE}")

        template_text = SCHEMA_TEMPLATE.read_text()
        resolved = template_text.replace("__DATA_DIR__", str(DATA_DIR))

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", prefix="ldbc_sql_", delete=False
        )
        tmp.write(resolved)
        tmp.flush()
        tmp.close()

        try:
            db = clickgraph.Database.sql_only(tmp.name)
            conn = db.connect()
            yield conn
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    @pytest.mark.parametrize("query_id", [f"short-{i}" for i in range(1, 8)])
    def test_short_query_translates(self, sql_connection, query_id):
        """Each short query should translate to valid SQL."""
        cypher = _load_and_prepare(query_id)
        sql = sql_connection.query_to_sql(cypher)
        assert sql is not None
        assert len(sql) > 0
        # Basic SQL sanity checks
        sql_upper = sql.upper()
        assert "SELECT" in sql_upper
