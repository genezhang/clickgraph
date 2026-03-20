"""ClickGraph — embedded graph query engine for Python (UniFFI backend).

Run Cypher queries over Parquet, Iceberg, Delta Lake and S3 data
without a ClickHouse server.

Quick start::

    import clickgraph

    db = clickgraph.Database("schema.yaml")
    conn = db.connect()
    for row in conn.query("MATCH (u:User) RETURN u.name LIMIT 5"):
        print(row["u.name"])

Kuzu-compatible style::

    from clickgraph import Database, Connection

    db = Database("schema.yaml")
    conn = Connection(db)
    result = conn.execute("MATCH (u:User) RETURN u.name")
    while result.has_next():
        row = result.get_next()
        print(row[0])

With S3 credentials::

    db = clickgraph.Database(
        "schema.yaml",
        s3_access_key_id="AKIA...",
        s3_secret_access_key="...",
        s3_region="us-east-1",
    )
"""

from __future__ import annotations

from clickgraph._ffi import (
    Database as _FfiDatabase,
    Connection as _FfiConnection,
    QueryResult as _FfiQueryResult,
    GraphResult as _FfiGraphResult,
    ExportOptions as _FfiExportOptions,
    SystemConfig as _FfiSystemConfig,
    RemoteConfig as _FfiRemoteConfig,
    Value as _FfiValue,
    Row as _FfiRow,
    ClickGraphError,
)

__all__ = [
    "Database",
    "Connection",
    "QueryResult",
    "GraphResult",
    "StoreStats",
    "ClickGraphError",
]
__version__ = "0.1.0"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXPORT_FORMAT_ALIASES = {
    "parquet": "parquet",
    "pq": "parquet",
    "csv": "csv",
    "csvwithnames": "csv",
    "csvnoheader": "csvnoheader",
    "tsv": "tsv",
    "tabseparated": "tsv",
    "tsvwithnames": "tsv",
    "json": "json",
    "jsoneachrow": "ndjson",
    "ndjson": "ndjson",
    "jsonl": "ndjson",
}


def _parse_format(name: str | None) -> str | None:
    """Normalize a user-facing format name to the FFI-expected string."""
    if name is None:
        return None
    key = name.lower().replace("_", "")
    canonical = _EXPORT_FORMAT_ALIASES.get(key)
    if canonical is None:
        raise RuntimeError(
            f"Unknown export format '{name}'. "
            "Supported: parquet, csv, tsv, json, ndjson"
        )
    return canonical


def _value_to_python(v):
    """Convert a UniFFI Value enum variant to a native Python object."""
    if isinstance(v, _FfiValue.NULL):
        return None
    if isinstance(v, _FfiValue.BOOL):
        return v.v
    if isinstance(v, _FfiValue.INT64):
        return v.v
    if isinstance(v, _FfiValue.FLOAT64):
        return v.v
    if isinstance(v, _FfiValue.STRING):
        return v.v
    if isinstance(v, _FfiValue.LIST):
        return [_value_to_python(item) for item in v.items]
    if isinstance(v, _FfiValue.MAP):
        return {entry.key: _value_to_python(entry.value) for entry in v.entries}
    return v  # fallback


def _row_to_dict(row: _FfiRow) -> dict:
    """Convert a UniFFI Row (columns + values) to a Python dict."""
    return {
        col: _value_to_python(val)
        for col, val in zip(row.columns, row.values)
    }


def _row_to_list(row: _FfiRow) -> list:
    """Convert a UniFFI Row to a flat list of Python values."""
    return [_value_to_python(val) for val in row.values]


# ---------------------------------------------------------------------------
# QueryResult
# ---------------------------------------------------------------------------

class QueryResult:
    """Result of a Cypher query. Iterable and indexable.

    Rows can be accessed as dicts (default iteration) or tuples (Kuzu-compat).

    Dict-style (ClickGraph default)::

        for row in result:
            print(row["u.name"])

    Tuple-style (Kuzu-compatible)::

        while result.has_next():
            row = result.get_next()   # returns a list of values
            print(row[0])
    """

    def __init__(self, ffi_result: _FfiQueryResult):
        self._ffi = ffi_result
        # Eagerly materialise all rows to support random access / len / iteration
        self._column_names = ffi_result.column_names()
        self._rows = ffi_result.get_all_rows()
        self._position = 0

    @property
    def column_names(self) -> list[str]:
        """Column names in result order."""
        return list(self._column_names)

    @property
    def num_rows(self) -> int:
        """Number of rows."""
        return len(self._rows)

    def has_next(self) -> bool:
        """Return True if there are more rows (Kuzu-compatible cursor)."""
        return self._position < len(self._rows)

    def get_next(self, *, as_dict: bool = False):
        """Return the next row as a list of values (Kuzu-compatible cursor).

        Returns a flat list in column order (like ``kuzu.FlatTuple``).
        Pass ``as_dict=True`` to get a dict instead.
        """
        if self._position >= len(self._rows):
            raise RuntimeError("No more rows")
        row = self._rows[self._position]
        self._position += 1
        return _row_to_dict(row) if as_dict else _row_to_list(row)

    def reset_iterator(self):
        """Reset the cursor to the beginning."""
        self._position = 0

    def as_dicts(self) -> list[dict]:
        """Get all rows as a list of dicts."""
        return [_row_to_dict(row) for row in self._rows]

    def get_row(self, index: int) -> dict:
        """Get a single row by index as a dict."""
        if index < 0 or index >= len(self._rows):
            raise RuntimeError(
                f"Row index {index} out of range (0..{len(self._rows)})"
            )
        return _row_to_dict(self._rows[index])

    # -- Python dunder methods for ergonomic usage --

    def __iter__(self):
        self._position = 0
        return self

    def __next__(self) -> dict:
        if self._position >= len(self._rows):
            raise StopIteration
        row = self._rows[self._position]
        self._position += 1
        return _row_to_dict(row)

    def __len__(self) -> int:
        return len(self._rows)

    def __getitem__(self, index: int) -> dict:
        length = len(self._rows)
        idx = index if index >= 0 else length + index
        if idx < 0 or idx >= length:
            raise IndexError(f"row index {index} out of range")
        return _row_to_dict(self._rows[idx])

    def __repr__(self) -> str:
        return f"<QueryResult columns={self._column_names!r} rows={len(self._rows)}>"


# ---------------------------------------------------------------------------
# GraphResult / StoreStats
# ---------------------------------------------------------------------------


class StoreStats:
    """Statistics from :meth:`Connection.store_subgraph`."""

    def __init__(self, nodes_stored: int, edges_stored: int):
        self.nodes_stored = nodes_stored
        self.edges_stored = edges_stored

    def __repr__(self) -> str:
        return f"StoreStats(nodes_stored={self.nodes_stored}, edges_stored={self.edges_stored})"


class GraphResult:
    """Structured graph result containing deduplicated nodes and edges.

    Returned by :meth:`Connection.query_graph` and
    :meth:`Connection.query_remote_graph`.  Can be passed to
    :meth:`Connection.store_subgraph` to persist locally.
    """

    def __init__(self, ffi_result: _FfiGraphResult):
        self._ffi = ffi_result

    @property
    def nodes(self) -> list[dict]:
        """Nodes as dicts with ``id``, ``labels``, ``properties`` keys."""
        return [
            {
                "id": n.id,
                "labels": list(n.labels),
                "properties": {
                    k: _value_to_python(v) for k, v in n.properties.items()
                },
            }
            for n in self._ffi.nodes()
        ]

    @property
    def edges(self) -> list[dict]:
        """Edges as dicts with ``id``, ``type_name``, ``from_id``, ``to_id``, ``properties``."""
        return [
            {
                "id": e.id,
                "type_name": e.type_name,
                "from_id": e.from_id,
                "to_id": e.to_id,
                "properties": {
                    k: _value_to_python(v) for k, v in e.properties.items()
                },
            }
            for e in self._ffi.edges()
        ]

    @property
    def node_count(self) -> int:
        return self._ffi.node_count()

    @property
    def edge_count(self) -> int:
        return self._ffi.edge_count()

    def __repr__(self) -> str:
        return f"<GraphResult nodes={self.node_count} edges={self.edge_count}>"


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class Connection:
    """A connection to an embedded ClickGraph database.

    Create via ``Database.connect()`` or ``Connection(db)`` (Kuzu-compatible).
    """

    def __init__(self, db: Database):
        """Kuzu-compatible constructor: ``conn = Connection(db)``."""
        if not isinstance(db, Database):
            raise TypeError("Expected a Database instance")
        self._db = db
        self._ffi = db._ffi.connect()

    def query(self, cypher: str) -> QueryResult:
        """Execute a Cypher query and return a QueryResult.

        >>> result = conn.query("MATCH (u:User) RETURN u.name LIMIT 5")
        >>> for row in result:
        ...     print(row["u.name"])
        """
        ffi_result = self._ffi.query(cypher)
        return QueryResult(ffi_result)

    def execute(self, cypher: str) -> QueryResult:
        """Alias for ``query()`` — Kuzu-compatible."""
        return self.query(cypher)

    def run(self, cypher: str) -> QueryResult:
        """Alias for ``query()`` — Neo4j driver-compatible."""
        return self.query(cypher)

    def query_to_sql(self, cypher: str) -> str:
        """Translate Cypher to SQL without executing."""
        return self._ffi.query_to_sql(cypher)

    def export(
        self,
        cypher: str,
        output_path: str,
        *,
        format: str | None = None,
        compression: str | None = None,
    ) -> None:
        """Export query results to a file (Parquet, CSV, TSV, JSON, NDJSON).

        >>> conn.export("MATCH (u:User) RETURN u.name", "users.parquet")
        """
        opts = _FfiExportOptions(
            format=_parse_format(format),
            compression=compression,
        )
        self._ffi.export(cypher, output_path, opts)

    def export_to_sql(
        self,
        cypher: str,
        output_path: str,
        *,
        format: str | None = None,
        compression: str | None = None,
    ) -> str:
        """Generate export SQL without executing (for debugging).

        >>> sql = conn.export_to_sql("MATCH (u:User) RETURN u.name", "users.parquet")
        """
        opts = _FfiExportOptions(
            format=_parse_format(format),
            compression=compression,
        )
        return self._ffi.export_to_sql(cypher, output_path, opts)

    def query_remote(self, cypher: str) -> QueryResult:
        """Execute a Cypher query against the remote ClickHouse cluster.

        Requires ``remote_url``, ``remote_user``, ``remote_password`` to have
        been provided when opening the ``Database``.
        """
        ffi_result = self._ffi.query_remote(cypher)
        return QueryResult(ffi_result)

    def query_graph(self, cypher: str) -> GraphResult:
        """Execute a Cypher query locally and return a structured graph result.

        >>> graph = conn.query_graph("MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, r, f")
        >>> print(graph.node_count, graph.edge_count)
        """
        ffi_result = self._ffi.query_graph(cypher)
        return GraphResult(ffi_result)

    def query_remote_graph(self, cypher: str) -> GraphResult:
        """Execute a Cypher query on the remote cluster and return a graph result.

        The result can be passed to :meth:`store_subgraph` to persist locally.
        """
        ffi_result = self._ffi.query_remote_graph(cypher)
        return GraphResult(ffi_result)

    def store_subgraph(self, graph: GraphResult) -> StoreStats:
        """Store a ``GraphResult`` into local writable tables.

        >>> graph = conn.query_remote_graph("MATCH (u:User) RETURN u LIMIT 100")
        >>> stats = conn.store_subgraph(graph)
        >>> print(stats.nodes_stored)
        """
        ffi_stats = self._ffi.store_subgraph(graph._ffi)
        return StoreStats(
            nodes_stored=ffi_stats.nodes_stored,
            edges_stored=ffi_stats.edges_stored,
        )

    def __repr__(self) -> str:
        return "<Connection>"


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class Database:
    """An embedded ClickGraph database.

    >>> db = Database("schema.yaml")
    >>> conn = db.connect()
    >>> result = conn.query("MATCH (u:User) RETURN u.name")
    """

    def __init__(
        self,
        schema_path: str,
        *,
        session_dir: str | None = None,
        data_dir: str | None = None,
        max_threads: int | None = None,
        s3_access_key_id: str | None = None,
        s3_secret_access_key: str | None = None,
        s3_region: str | None = None,
        s3_endpoint_url: str | None = None,
        s3_session_token: str | None = None,
        gcs_access_key_id: str | None = None,
        gcs_secret_access_key: str | None = None,
        azure_storage_account_name: str | None = None,
        azure_storage_account_key: str | None = None,
        azure_storage_connection_string: str | None = None,
        remote_url: str | None = None,
        remote_user: str | None = None,
        remote_password: str | None = None,
        remote_database: str | None = None,
        remote_cluster_name: str | None = None,
    ):
        # Build remote config if any remote_* args are provided
        remote_cfg = None
        if remote_url is not None:
            remote_cfg = _FfiRemoteConfig(
                url=remote_url,
                user=remote_user or "",
                password=remote_password or "",
                database=remote_database,
                cluster_name=remote_cluster_name,
            )

        has_config = remote_cfg is not None or any(v is not None for v in [
            session_dir, data_dir, max_threads,
            s3_access_key_id, s3_secret_access_key, s3_region, s3_endpoint_url,
            s3_session_token, gcs_access_key_id, gcs_secret_access_key,
            azure_storage_account_name, azure_storage_account_key,
            azure_storage_connection_string,
        ])

        if has_config:
            config = _FfiSystemConfig(
                session_dir=session_dir,
                data_dir=data_dir,
                max_threads=max_threads,
                s3_access_key_id=s3_access_key_id,
                s3_secret_access_key=s3_secret_access_key,
                s3_region=s3_region,
                s3_endpoint_url=s3_endpoint_url,
                s3_session_token=s3_session_token,
                gcs_access_key_id=gcs_access_key_id,
                gcs_secret_access_key=gcs_secret_access_key,
                azure_storage_account_name=azure_storage_account_name,
                azure_storage_account_key=azure_storage_account_key,
                azure_storage_connection_string=azure_storage_connection_string,
                remote=remote_cfg,
            )
            self._ffi = _FfiDatabase.open_with_config(schema_path, config)
        else:
            self._ffi = _FfiDatabase.open(schema_path)

    @classmethod
    def _from_ffi(cls, ffi_db: _FfiDatabase) -> Database:
        """Internal: wrap an existing FFI Database object."""
        obj = object.__new__(cls)
        obj._ffi = ffi_db
        return obj

    @classmethod
    def sql_only(cls, schema_path: str) -> Database:
        """Open in SQL-only mode (no chdb required).

        Useful for testing Cypher → SQL translation.
        """
        ffi_db = _FfiDatabase.open_sql_only(schema_path)
        return cls._from_ffi(ffi_db)

    def connect(self) -> Connection:
        """Create a connection to this database."""
        return Connection(self)

    def execute(self, cypher: str) -> QueryResult:
        """Shorthand: execute a query directly (creates a temporary connection)."""
        conn = self.connect()
        return conn.query(cypher)

    def __repr__(self) -> str:
        return "<Database>"
